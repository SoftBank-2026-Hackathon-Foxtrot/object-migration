package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/migration"
	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/store"
)

const validBody = `{"source":{"kind":"s3","bucket":"src","region":"ap-northeast-1","ref":"env:SRC"},"target":{"kind":"gcs","bucket":"dst","ref":"env:DST"}}`

type recordingNotifier struct {
	mu  sync.Mutex
	ids []string
}

func (n *recordingNotifier) Notify(id string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.ids = append(n.ids, id)
}

func newTestHandler() (http.Handler, *recordingNotifier) {
	h, n, _ := newTestHandlerWithStore()
	return h, n
}

func newTestHandlerWithStore() (http.Handler, *recordingNotifier, *store.Memory) {
	n := &recordingNotifier{}
	st := store.NewMemory()
	return NewHandler(Deps{Store: st, Notifier: n, Engine: "fake"}), n, st
}

// setState stands in for the worker in tests that need a migration in a
// particular phase.
func setState(t *testing.T, st *store.Memory, id string, fn func(*migration.Migration)) {
	t.Helper()
	m, err := st.Get(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	fn(&m)
	if _, err := st.Update(context.Background(), m); err != nil {
		t.Fatal(err)
	}
}

func do(t *testing.T, h http.Handler, method, path, body string, header ...string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	for i := 0; i+1 < len(header); i += 2 {
		req.Header.Set(header[i], header[i+1])
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func decodeBody(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var v map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatalf("decode %q: %v", rec.Body.String(), err)
	}
	return v
}

func wantError(t *testing.T, rec *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	if rec.Code != status {
		t.Fatalf("status = %d, want %d (body %s)", rec.Code, status, rec.Body)
	}
	if got := decodeBody(t, rec)["code"]; got != code {
		t.Fatalf("code = %v, want %s", got, code)
	}
}

func TestHealthz(t *testing.T) {
	h, _ := newTestHandler()
	rec := do(t, h, http.MethodGet, "/healthz", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}
}

func TestHealthzRejectsOtherMethods(t *testing.T) {
	h, _ := newTestHandler()
	if rec := do(t, h, http.MethodPost, "/healthz", ""); rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
}

func TestCreateReturnsAcceptedAndNotifies(t *testing.T) {
	h, n := newTestHandler()
	rec := do(t, h, http.MethodPost, "/v1/migrations", validBody)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body)
	}
	m := decodeBody(t, rec)
	id, _ := m["id"].(string)
	if m["phase"] != "CREATED" || id == "" {
		t.Fatalf("body = %v", m)
	}
	if got := rec.Header().Get("Location"); got != "/v1/migrations/"+id {
		t.Fatalf("Location = %q", got)
	}
	if len(n.ids) != 1 || n.ids[0] != id {
		t.Fatalf("notified %v, want [%s]", n.ids, id)
	}
	if got := do(t, h, http.MethodGet, "/v1/migrations/"+id, ""); got.Code != http.StatusOK {
		t.Fatalf("get status = %d", got.Code)
	}
}

func TestCreateRejectsBadInput(t *testing.T) {
	h, _ := newTestHandler()
	for name, body := range map[string]string{
		"not json":      `{`,
		"unknown field": `{"source":{},"target":{},"extra":1}`,
		"invalid kind":  `{"source":{"kind":"ftp","bucket":"a","ref":"r"},"target":{"kind":"gcs","bucket":"b","ref":"r"}}`,
		"empty body":    ``,
	} {
		t.Run(name, func(t *testing.T) {
			wantError(t, do(t, h, http.MethodPost, "/v1/migrations", body), http.StatusBadRequest, "INVALID_ARGUMENT")
		})
	}
}

func TestGetUnknownIs404(t *testing.T) {
	h, _ := newTestHandler()
	wantError(t, do(t, h, http.MethodGet, "/v1/migrations/om-missing", ""), http.StatusNotFound, "NOT_FOUND")
}

func TestCommandErrors(t *testing.T) {
	h, _ := newTestHandler()
	id := decodeBody(t, do(t, h, http.MethodPost, "/v1/migrations", validBody))["id"].(string)

	wantError(t, do(t, h, http.MethodPost, "/v1/migrations/"+id+"/explode", ""), http.StatusNotFound, "NOT_FOUND")
	wantError(t, do(t, h, http.MethodPost, "/v1/migrations/"+id+"/switch", ""), http.StatusConflict, "INVALID_PHASE")
	wantError(t, do(t, h, http.MethodPost, "/v1/migrations/om-missing/freeze", ""), http.StatusNotFound, "NOT_FOUND")
	wantError(t, do(t, h, http.MethodPost, "/v1/migrations/"+id+"/freeze", `{"deadlineMs":-1}`), http.StatusBadRequest, "INVALID_ARGUMENT")
}

func TestAbortAcceptedBeforeSwitch(t *testing.T) {
	h, _ := newTestHandler()
	id := decodeBody(t, do(t, h, http.MethodPost, "/v1/migrations", validBody))["id"].(string)
	rec := do(t, h, http.MethodDelete, "/v1/migrations/"+id, "")
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body)
	}
	pending, _ := decodeBody(t, rec)["pendingCommand"].(map[string]any)
	if pending["command"] != "abort" {
		t.Fatalf("pendingCommand = %v", pending)
	}
}

func TestIdempotentCreateReplays(t *testing.T) {
	h, n := newTestHandler()
	first := do(t, h, http.MethodPost, "/v1/migrations", validBody, idempotencyHeader, "create-1")
	second := do(t, h, http.MethodPost, "/v1/migrations", validBody, idempotencyHeader, "create-1")
	if first.Code != http.StatusAccepted || second.Code != http.StatusAccepted {
		t.Fatalf("status = %d, %d", first.Code, second.Code)
	}
	if first.Body.String() != second.Body.String() || second.Header().Get("Idempotent-Replayed") != "true" {
		t.Fatal("second response is not a replay of the first")
	}
	if second.Header().Get("Location") != first.Header().Get("Location") {
		t.Fatal("Location not replayed")
	}
	if len(n.ids) != 1 {
		t.Fatalf("created %d migrations, want 1", len(n.ids))
	}
	other := `{"source":{"kind":"s3","bucket":"other","region":"ap-northeast-1","ref":"env:SRC"},"target":{"kind":"gcs","bucket":"dst","ref":"env:DST"}}`
	wantError(t, do(t, h, http.MethodPost, "/v1/migrations", other, idempotencyHeader, "create-1"), http.StatusUnprocessableEntity, "IDEMPOTENCY_KEY_REUSED")
}

func TestCapabilities(t *testing.T) {
	h, _ := newTestHandler()
	body := decodeBody(t, do(t, h, http.MethodGet, "/v1/capabilities", ""))
	if body["type"] != "object" || body["engine"] != "fake" {
		t.Fatalf("body = %v", body)
	}
}

func TestPreflightReportsInvalidEndpoint(t *testing.T) {
	h, _ := newTestHandler()
	bad := `{"source":{"kind":"s3","bucket":"src","region":"ap-northeast-1","ref":"env:SRC"},"target":{"kind":"seaweedfs","bucket":"dst","ref":"env:DST"}}`
	body := decodeBody(t, do(t, h, http.MethodPost, "/v1/preflight", bad))
	if body["ok"] != false {
		t.Fatalf("ok = %v, want false", body["ok"])
	}
	if body := decodeBody(t, do(t, h, http.MethodPost, "/v1/preflight", validBody)); body["ok"] != true {
		t.Fatalf("valid request: ok = %v", body["ok"])
	}
}

type stubPreflight struct {
	calls  int
	checks []migration.Check
}

func (p *stubPreflight) Preflight(context.Context, migration.Endpoint, migration.Endpoint) []migration.Check {
	p.calls++
	return p.checks
}

func TestPreflightRunsEngineChecks(t *testing.T) {
	p := &stubPreflight{checks: []migration.Check{
		{Name: "source_read", OK: true},
		{Name: "target_write", OK: false, Message: "access denied"},
	}}
	h := NewHandler(Deps{Store: store.NewMemory(), Preflight: p})

	body := decodeBody(t, do(t, h, http.MethodPost, "/v1/preflight", validBody))
	checks, _ := body["checks"].([]any)
	if body["ok"] != false || len(checks) != 4 {
		t.Fatalf("body = %v, want ok=false with 2 request and 2 engine checks", body)
	}
	if last := checks[3].(map[string]any); last["name"] != "target_write" || last["message"] != "access denied" {
		t.Fatalf("last check = %v", last)
	}

	// An invalid request is not sent to the engine.
	bad := `{"source":{"kind":"s3","bucket":"src","ref":"env:SRC"},"target":{"kind":"gcs","bucket":"dst","ref":"env:DST"}}`
	if body := decodeBody(t, do(t, h, http.MethodPost, "/v1/preflight", bad)); body["ok"] != false || p.calls != 1 {
		t.Fatalf("invalid request: ok=%v, engine calls=%d", body["ok"], p.calls)
	}
}

func TestRetryableErrorIsNotReplayed(t *testing.T) {
	h, _, st := newTestHandlerWithStore()
	id := decodeBody(t, do(t, h, http.MethodPost, "/v1/migrations", validBody))["id"].(string)
	setState(t, st, id, func(m *migration.Migration) { m.Phase = migration.PhaseReady })

	if rec := do(t, h, http.MethodPost, "/v1/migrations/"+id+"/freeze", "", idempotencyHeader, "freeze-1"); rec.Code != http.StatusAccepted {
		t.Fatalf("freeze: %d %s", rec.Code, rec.Body)
	}
	busy := do(t, h, http.MethodDelete, "/v1/migrations/"+id, "", idempotencyHeader, "abort-1")
	wantError(t, busy, http.StatusConflict, "COMMAND_PENDING")
	if decodeBody(t, busy)["retryable"] != true {
		t.Fatal("COMMAND_PENDING should be retryable")
	}

	// The worker clears the pending command; the same retry now succeeds.
	setState(t, st, id, func(m *migration.Migration) { m.Pending = nil })
	retry := do(t, h, http.MethodDelete, "/v1/migrations/"+id, "", idempotencyHeader, "abort-1")
	if retry.Code != http.StatusAccepted || retry.Header().Get("Idempotent-Replayed") != "" {
		t.Fatalf("retry: %d replayed=%q %s", retry.Code, retry.Header().Get("Idempotent-Replayed"), retry.Body)
	}
}

func TestNonRetryableErrorIsReplayed(t *testing.T) {
	h, _ := newTestHandler()
	id := decodeBody(t, do(t, h, http.MethodPost, "/v1/migrations", validBody))["id"].(string)
	first := do(t, h, http.MethodPost, "/v1/migrations/"+id+"/switch", "", idempotencyHeader, "switch-1")
	wantError(t, first, http.StatusConflict, "INVALID_PHASE")
	again := do(t, h, http.MethodPost, "/v1/migrations/"+id+"/switch", "", idempotencyHeader, "switch-1")
	if again.Header().Get("Idempotent-Replayed") != "true" || again.Body.String() != first.Body.String() {
		t.Fatalf("INVALID_PHASE was not replayed: %s", again.Body)
	}
}

func TestFinalizeBeforeVerificationIsPreconditionFailed(t *testing.T) {
	h, _, st := newTestHandlerWithStore()
	id := decodeBody(t, do(t, h, http.MethodPost, "/v1/migrations", validBody))["id"].(string)
	setState(t, st, id, func(m *migration.Migration) {
		m.Phase, m.State = migration.PhaseVerifying, migration.StateRunning
	})
	rec := do(t, h, http.MethodPost, "/v1/migrations/"+id+"/finalize", "")
	wantError(t, rec, http.StatusConflict, "PRECONDITION_FAILED")
}

func TestConcurrentRequestsWithSameKeyCreateOnce(t *testing.T) {
	h, _, st := newTestHandlerWithStore()
	const n = 10
	ids := make([]string, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodPost, "/v1/migrations", strings.NewReader(validBody))
			req.Header.Set(idempotencyHeader, "same")
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			var body map[string]any
			_ = json.Unmarshal(rec.Body.Bytes(), &body)
			ids[i], _ = body["id"].(string)
		}()
	}
	wg.Wait()

	list, err := st.ListActive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 {
		t.Fatalf("created %d migrations, want 1", len(list))
	}
	for i, id := range ids {
		if id != list[0].ID {
			t.Fatalf("response %d has id %q, want %q", i, id, list[0].ID)
		}
	}
}

func TestStatusShowsAllowedActionsAndReplication(t *testing.T) {
	h, _, st := newTestHandlerWithStore()
	id := decodeBody(t, do(t, h, http.MethodPost, "/v1/migrations", validBody))["id"].(string)
	setState(t, st, id, func(m *migration.Migration) {
		m.Phase = migration.PhaseReady
		m.Replication.Forward.State = migration.FlowStreaming
	})

	body := decodeBody(t, do(t, h, http.MethodGet, "/v1/migrations/"+id, ""))
	allowed, _ := json.Marshal(body["allowedActions"])
	cmds, _ := json.Marshal(body["allowedCommands"])
	if string(allowed) != `["QUIESCE","CANCEL"]` || string(cmds) != `["freeze","abort"]` {
		t.Fatalf("allowedActions = %s, allowedCommands = %s", allowed, cmds)
	}
	if body["lifecycle"] != "READY" || body["health"] != "OK" || body["kind"] != "OBJECT_STORAGE" {
		t.Fatalf("lifecycle = %v, health = %v, kind = %v", body["lifecycle"], body["health"], body["kind"])
	}
	repl, _ := body["replication"].(map[string]any)
	forward, _ := repl["forward"].(map[string]any)
	reverse, _ := repl["reverse"].(map[string]any)
	if forward["state"] != "STREAMING" || reverse["state"] != "OFF" {
		t.Fatalf("replication = %v", repl)
	}
	readiness, _ := body["readiness"].(map[string]any)
	for _, k := range []string{"cutover", "advance", "rollback"} {
		if _, ok := readiness[k].(map[string]any)["result"]; !ok {
			t.Fatalf("readiness.%s missing result: %v", k, readiness)
		}
	}
}

func TestStaleExpectedVersionIsConflict(t *testing.T) {
	h, _, st := newTestHandlerWithStore()
	id := decodeBody(t, do(t, h, http.MethodPost, "/v1/migrations", validBody))["id"].(string)
	setState(t, st, id, func(m *migration.Migration) { m.Phase = migration.PhaseReady })
	current := int(decodeBody(t, do(t, h, http.MethodGet, "/v1/migrations/"+id, ""))["version"].(float64))

	stale := `{"expectedVersion":` + strconv.Itoa(current-1) + `}`
	wantError(t, do(t, h, http.MethodPost, "/v1/migrations/"+id+"/freeze", stale), http.StatusConflict, "VERSION_CONFLICT")
	wantError(t, do(t, h, http.MethodDelete, "/v1/migrations/"+id, stale), http.StatusConflict, "VERSION_CONFLICT")

	ok := `{"expectedVersion":` + strconv.Itoa(current) + `}`
	if rec := do(t, h, http.MethodPost, "/v1/migrations/"+id+"/freeze", ok); rec.Code != http.StatusAccepted {
		t.Fatalf("current version: %d %s", rec.Code, rec.Body)
	}
}

func TestActionsCheckTheirFields(t *testing.T) {
	h, _, st := newTestHandlerWithStore()
	id := decodeBody(t, do(t, h, http.MethodPost, "/v1/migrations", validBody))["id"].(string)
	setState(t, st, id, func(m *migration.Migration) { m.Phase = migration.PhaseReady })
	rev := strconv.Itoa(int(decodeBody(t, do(t, h, http.MethodGet, "/v1/migrations/"+id, ""))["revision"].(float64)))
	path := "/v1/migrations/" + id + "/actions"

	for _, c := range []struct{ body, code string }{
		{`{"action":"START_SYNC","expectedRevision":` + rev + `}`, "UNSUPPORTED_ACTION"},
		{`{"action":"RECONCILE","expectedRevision":` + rev + `}`, "UNSUPPORTED_ACTION"},
		{`{"action":"JUMP","expectedRevision":` + rev + `}`, "INVALID_ARGUMENT"},
		{`{"action":"QUIESCE","transitionId":"tx-1"}`, "INVALID_ARGUMENT"},
		{`{"action":"QUIESCE","expectedRevision":` + rev + `}`, "INVALID_ARGUMENT"},
		{`{"action":"RESUME","expectedRevision":` + rev + `,"transitionId":"tx-1","expectedLocation":"SOURCE"}`, "INVALID_ARGUMENT"},
	} {
		wantError(t, do(t, h, http.MethodPost, path, c.body), http.StatusBadRequest, c.code)
	}
	wantError(t, do(t, h, http.MethodPost, path, `{"action":"QUIESCE","expectedRevision":999,"transitionId":"tx-1"}`), http.StatusConflict, "VERSION_CONFLICT")
	wantError(t, do(t, h, http.MethodPost, path, `{"action":"CUTOVER","expectedRevision":`+rev+`,"transitionId":"tx-1"}`), http.StatusConflict, "INVALID_PHASE")

	// Progress updates change the version but not the revision.
	setState(t, st, id, func(m *migration.Migration) { m.Replication.Forward.Backlog = 3 })
	rec := do(t, h, http.MethodPost, path, `{"action":"QUIESCE","expectedRevision":`+rev+`,"transitionId":"tx-1","quiesceRef":"q-1"}`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("quiesce: %d %s", rec.Code, rec.Body)
	}
	body := decodeBody(t, rec)
	opID, _ := body["operationId"].(string)
	if body["status"] != "QUEUED" || rec.Header().Get("Location") != "/v1/operations/"+opID || body["statusUrl"] != "/v1/operations/"+opID {
		t.Fatalf("accepted = %v, Location %q", body, rec.Header().Get("Location"))
	}
	op := decodeBody(t, do(t, h, http.MethodGet, "/v1/operations/"+opID, ""))
	if op["status"] != "RUNNING" || op["action"] != "QUIESCE" || op["transitionId"] != "tx-1" || op["quiesceRef"] != "q-1" || op["migrationId"] != id {
		t.Fatalf("operation = %v", op)
	}
	wantError(t, do(t, h, http.MethodGet, "/v1/operations/"+id+"-op9", ""), http.StatusNotFound, "NOT_FOUND")
	wantError(t, do(t, h, http.MethodGet, "/v1/operations/nope", ""), http.StatusNotFound, "NOT_FOUND")
}

func TestTokenGuardsControlRoutesOnly(t *testing.T) {
	st := store.NewMemory()
	h := NewHandler(Deps{Store: st, Engine: "fake", Token: "s3cret"})
	wantError(t, do(t, h, http.MethodPost, "/v1/migrations", validBody), http.StatusUnauthorized, "UNAUTHENTICATED")
	wantError(t, do(t, h, http.MethodGet, "/v1/capabilities", "", "Authorization", "Bearer wrong"), http.StatusUnauthorized, "UNAUTHENTICATED")
	rec := do(t, h, http.MethodPost, "/v1/migrations", validBody, "Authorization", "Bearer s3cret")
	if rec.Code != http.StatusAccepted {
		t.Fatalf("with token: %d %s", rec.Code, rec.Body)
	}
	id := decodeBody(t, rec)["id"].(string)
	if rec := do(t, h, http.MethodGet, "/v1/migrations/"+id+"/routing", ""); rec.Code != http.StatusOK {
		t.Fatalf("routing without token: %d %s", rec.Code, rec.Body)
	}
	if rec := do(t, h, http.MethodGet, "/healthz", ""); rec.Code != http.StatusOK {
		t.Fatalf("healthz without token: %d", rec.Code)
	}
}
