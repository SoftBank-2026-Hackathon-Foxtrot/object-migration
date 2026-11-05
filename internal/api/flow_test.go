package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/api"
	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/engine"
	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/migration"
	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/store"
	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/worker"
)

type client struct {
	t   *testing.T
	url string
}

type response struct {
	status int
	header http.Header
	body   map[string]any
	raw    string
}

func (c client) call(method, path, body string, header ...string) response {
	c.t.Helper()
	req, err := http.NewRequest(method, c.url+path, bytes.NewBufferString(body))
	if err != nil {
		c.t.Fatal(err)
	}
	for i := 0; i+1 < len(header); i += 2 {
		req.Header.Set(header[i], header[i+1])
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatal(err)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	var v map[string]any
	_ = json.Unmarshal(raw, &v)
	return response{status: res.StatusCode, header: res.Header, body: v, raw: string(raw)}
}

func (c client) expect(r response, status int) response {
	c.t.Helper()
	if r.status != status {
		c.t.Fatalf("status = %d, want %d: %s", r.status, status, r.raw)
	}
	return r
}

func (c client) waitFor(id, phase, state string) map[string]any {
	c.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var last map[string]any
	for time.Now().Before(deadline) {
		last = c.expect(c.call(http.MethodGet, "/v1/migrations/"+id, ""), http.StatusOK).body
		if last["phase"] == phase && (state == "" || last["state"] == state) {
			return last
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.t.Fatalf("timed out waiting for %s/%s, last: phase=%v state=%v", phase, state, last["phase"], last["state"])
	return nil
}

// flows returns the forward and reverse replication states of a status body.
func flows(body map[string]any) (string, string) {
	r, _ := body["replication"].(map[string]any)
	f, _ := r["forward"].(map[string]any)
	b, _ := r["reverse"].(map[string]any)
	fs, _ := f["state"].(string)
	bs, _ := b["state"].(string)
	return fs, bs
}

// TestMigrationFlowOverHTTP is the M1 completion check: a migration goes from
// CREATED to COMPLETED using only the HTTP API.
func TestMigrationFlowOverHTTP(t *testing.T) {
	st := store.NewMemory()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	w := worker.New(st, engine.NewFake(2*time.Millisecond), worker.Config{PollInterval: 5 * time.Millisecond, RetryBackoff: 5 * time.Millisecond}, log)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = w.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })

	srv := httptest.NewServer(api.NewHandler(api.Deps{Store: st, Notifier: w, Engine: "fake", Logger: log}))
	t.Cleanup(srv.Close)
	c := client{t: t, url: srv.URL}

	created := c.expect(c.call(http.MethodPost, "/v1/migrations",
		`{"source":{"kind":"s3","bucket":"foxtrot-src","region":"ap-northeast-1","ref":"env:AWS"},"target":{"kind":"gcs","bucket":"foxtrot-dst","ref":"env:GCP"}}`,
		"Idempotency-Key", "create-1"), http.StatusAccepted)
	id := created.body["id"].(string)

	ready := c.waitFor(id, "READY", "waiting")
	if f, r := flows(ready); f != "STREAMING" || r != "OFF" {
		t.Fatalf("at READY: forward=%s reverse=%s", f, r)
	}

	// A command in the wrong phase is rejected.
	r := c.expect(c.call(http.MethodPost, "/v1/migrations/"+id+"/switch", ""), http.StatusConflict)
	if r.body["code"] != "INVALID_STATE" {
		t.Fatalf("code = %v", r.body["code"])
	}

	freeze := `{"cutoverId":"c-1","deadlineMs":3000}`
	first := c.expect(c.call(http.MethodPost, "/v1/migrations/"+id+"/freeze", freeze, "Idempotency-Key", "freeze-1"), http.StatusAccepted)
	c.waitFor(id, "FROZEN", "")

	// Retrying after the worker already ran it replays the first answer
	// instead of failing with INVALID_STATE.
	again := c.expect(c.call(http.MethodPost, "/v1/migrations/"+id+"/freeze", freeze, "Idempotency-Key", "freeze-1"), http.StatusAccepted)
	if again.raw != first.raw || again.header.Get("Idempotent-Replayed") != "true" {
		t.Fatalf("retry was not replayed: %s", again.raw)
	}

	c.expect(c.call(http.MethodPost, "/v1/migrations/"+id+"/switch", `{"cutoverId":"c-1"}`), http.StatusAccepted)
	c.waitFor(id, "SWITCHED", "")
	c.expect(c.call(http.MethodPost, "/v1/migrations/"+id+"/resume", `{"cutoverId":"c-1"}`), http.StatusAccepted)
	verified := c.waitFor(id, "VERIFYING", "succeeded")
	if f, r := flows(verified); f != "STOPPED" || r != "STREAMING" {
		t.Fatalf("after resume: forward=%s reverse=%s", f, r)
	}
	c.expect(c.call(http.MethodPost, "/v1/migrations/"+id+"/finalize", ""), http.StatusAccepted)
	completed := c.waitFor(id, "COMPLETED", "succeeded")
	if f, r := flows(completed); f != "STOPPED" || r != "STOPPED" {
		t.Fatalf("after finalize: forward=%s reverse=%s", f, r)
	}

	// Nothing is allowed after completion.
	c.expect(c.call(http.MethodPost, "/v1/migrations/"+id+"/rollback", ""), http.StatusConflict)
}

// TestActionFlowOverHTTP drives a plan through the common API, from
// create to FINALIZE, following each action at GET /v1/operations/{id}.
func TestActionFlowOverHTTP(t *testing.T) {
	st := store.NewMemory()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	w := worker.New(st, engine.NewFake(2*time.Millisecond), worker.Config{PollInterval: 5 * time.Millisecond, RetryBackoff: 5 * time.Millisecond}, log)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = w.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })

	resources := map[string]migration.Endpoint{
		"aws": {Kind: migration.KindS3, Bucket: "foxtrot-src", Region: "ap-northeast-2", Ref: "env:AWS"},
		"gcp": {Kind: migration.KindGCS, Bucket: "foxtrot-dst", Ref: "env:GCP"},
	}
	srv := httptest.NewServer(api.NewHandler(api.Deps{Store: st, Notifier: w, Engine: "fake", Logger: log, Token: "t", Resources: resources}))
	t.Cleanup(srv.Close)
	c := client{t: t, url: srv.URL}
	auth := []string{"Authorization", "Bearer t"}
	get := func(path string) map[string]any {
		return c.expect(c.call(http.MethodGet, path, "", auth...), http.StatusOK).body
	}

	caps := get("/v1/capabilities")
	if profiles, _ := caps["profiles"].([]any); caps["apiVersion"] != "v1" || len(profiles) != 1 {
		t.Fatalf("capabilities = %v", caps)
	}

	created := c.expect(c.call(http.MethodPost, "/v1/migrations",
		`{"workflowId":"wf-1","resourceId":"receipts","kind":"OBJECT_STORAGE","profile":"object-copy-route-v1",`+
			`"sourceRef":"resource:aws","targetRef":"resource:gcp","workloadScopeRef":"workload:demo","bindingRef":"binding:receipts",`+
			`"policy":{"rollbackMode":"REVERSE_REQUIRED","maxQuiesceSeconds":30}}`,
		append([]string{"Idempotency-Key", "create-1"}, auth...)...), http.StatusCreated)
	id := created.body["migrationId"].(string)

	waitFor := func(want string, cond func(map[string]any) bool) map[string]any {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		var last map[string]any
		for time.Now().Before(deadline) {
			last = get("/v1/migrations/" + id)
			if last["lifecycle"] == want && last["activeOperationId"] == nil && (cond == nil || cond(last)) {
				return last
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatalf("timed out waiting for %s, last lifecycle=%v phase=%v allowed=%v", want, last["lifecycle"], last["phase"], last["allowedActions"])
		return nil
	}
	act := func(action, extra string) {
		t.Helper()
		rev := get("/v1/migrations/" + id)["revision"].(float64)
		body := fmt.Sprintf(`{"action":%q,"expectedRevision":%d%s}`, action, int(rev), extra)
		r := c.expect(c.call(http.MethodPost, "/v1/migrations/"+id+"/actions", body,
			append([]string{"Idempotency-Key", "k-" + action}, auth...)...), http.StatusAccepted)
		opURL := r.header.Get("Location")
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			op := get(opURL)
			if op["status"] == "SUCCEEDED" {
				return
			}
			if op["status"] == "FAILED" || op["status"] == "UNKNOWN" {
				t.Fatalf("%s: %v", action, op)
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatalf("%s did not finish", action)
	}
	offers := func(a string) func(map[string]any) bool {
		return func(s map[string]any) bool { return strings.Contains(fmt.Sprint(s["allowedActions"]), a) }
	}

	// A plan waits: nothing is copied before PREPARE and START_SYNC.
	time.Sleep(30 * time.Millisecond)
	waitFor("CREATED", nil)
	act("PREPARE", "")
	waitFor("PREPARED", nil)
	act("START_SYNC", "")
	waitFor("READY", offers("QUIESCE"))
	tx := `,"transitionId":"tx-1","quiesceRef":"q-1"`
	act("QUIESCE", tx)
	waitFor("QUIESCED", nil)
	act("CUTOVER", tx)
	waitFor("SWITCHED", nil)
	act("RESUME", tx+`,"expectedLocation":"TARGET"`)
	waitFor("ACTIVE", offers("FINALIZE"))
	act("FINALIZE", "")
	final := waitFor("COMPLETED", nil)
	if final["transitionState"] != "CLOSED" {
		t.Fatalf("transitionState = %v", final["transitionState"])
	}
	if ops, _ := final["operations"].([]any); len(ops) != 6 {
		t.Fatalf("operations = %v", ops)
	}
}
