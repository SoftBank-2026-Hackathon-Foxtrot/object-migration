// Package api serves the common migration API. Handlers validate a request,
// record it in the store, wake the worker, and return at once; the worker
// does the long-running work.
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"time"

	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/migration"
	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/routing"
	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/store"
)

// Notifier wakes the worker for a migration.
type Notifier interface {
	Notify(id string)
}

// Preflighter checks that both buckets of a migration can be reached.
type Preflighter interface {
	Preflight(ctx context.Context, source, target migration.Endpoint) []migration.Check
}

type Deps struct {
	Observe  func(context.Context, migration.Migration)
	Store    store.Store
	Notifier Notifier
	// Engine is the engine name reported by /v1/capabilities.
	Engine string
	// Preflight runs the access checks of POST /v1/preflight. Nil checks
	// only the request.
	Preflight Preflighter
	// Apps records the route each app reports having applied. Nil drops
	// the reports.
	Apps *routing.Apps
	// Deletes records the keys apps delete during a migration.
	Deletes *routing.Deletes
	// Directions, when set, is reported by /v1/capabilities.
	Directions any
	// Resources are the buckets a common-API plan may name, as
	// "resource:<name>" in sourceRef and targetRef.
	Resources map[string]migration.Endpoint
	// Token, when set, is the Bearer token the orchestrator must send on
	// every control route. App routes (routing, deletes) stay open.
	Token  string
	Logger *slog.Logger
	Now    func() time.Time
}

type handler struct {
	Deps
	idem *idempotency
}

const maxBodyBytes = 1 << 20

// NewHandler returns the HTTP handler with all routes registered.
func NewHandler(d Deps) http.Handler {
	if d.Notifier == nil {
		d.Notifier = nopNotifier{}
	}
	if d.Logger == nil {
		d.Logger = slog.Default()
	}
	if d.Now == nil {
		d.Now = time.Now
	}
	saved, ok := d.Store.(responses)
	if !ok {
		saved = store.NewMemory()
	}
	h := &handler{Deps: d, idem: newIdempotency(saved)}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handleHealthz)
	auth := func(next http.HandlerFunc) http.HandlerFunc { return bearer(d.Token, next) }
	mux.HandleFunc("GET /v1/capabilities", auth(h.capabilities))
	mux.HandleFunc("POST /v1/preflight", auth(h.preflight))
	mux.HandleFunc("POST /v1/migrations", auth(h.idem.wrap(h.create)))
	mux.HandleFunc("GET /v1/migrations/{id}", auth(h.get))
	mux.HandleFunc("POST /v1/migrations/{id}/actions", auth(h.idem.wrap(h.action)))
	mux.HandleFunc("GET /v1/operations/{id}", auth(h.operation))
	mux.HandleFunc("POST /v1/migrations/{id}/{command}", auth(h.idem.wrap(h.command)))
	mux.HandleFunc("DELETE /v1/migrations/{id}", auth(h.idem.wrap(h.abort)))
	// Apps call these with no token; they reveal no credentials.
	mux.HandleFunc("GET /v1/migrations/{id}/routing", h.routing)
	mux.HandleFunc("POST /v1/migrations/{id}/deletes", h.deletes)
	return mux
}

type nopNotifier struct{}

func (nopNotifier) Notify(string) {}

func handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

var allKinds = []migration.Kind{migration.KindS3, migration.KindGCS, migration.KindSeaweedFS}

type endpointsRequest struct {
	Source migration.Endpoint `json:"source"`
	Target migration.Endpoint `json:"target"`
}

// preflightTimeout bounds the access checks of one preflight request.
const preflightTimeout = time.Minute

// preflight checks that a migration could start: the request is valid and
// both buckets can be reached. It changes nothing the app can see.
func (h *handler) preflight(w http.ResponseWriter, r *http.Request) {
	var req endpointsRequest
	if err := decode(w, r, &req); err != nil {
		writeError(w, err)
		return
	}
	checks := []migration.Check{
		endpointCheck("source_valid", req.Source),
		endpointCheck("target_valid", req.Target),
	}
	if checks[0].OK && checks[1].OK && h.Preflight != nil {
		ctx, cancel := context.WithTimeout(r.Context(), preflightTimeout)
		checks = append(checks, h.Preflight.Preflight(ctx, req.Source, req.Target)...)
		cancel()
	}
	ok := true
	for _, c := range checks {
		ok = ok && c.OK
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": ok, "checks": checks})
}

func endpointCheck(name string, e migration.Endpoint) migration.Check {
	if err := e.Validate(); err != nil {
		return migration.Check{Name: name, OK: false, Message: err.Error()}
	}
	return migration.Check{Name: name, OK: true}
}

// create registers a common-API plan when the body names a workflowId, and
// otherwise creates a migration from inline endpoints and starts it at once.
func (h *handler) create(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		writeError(w, badRequest("read body: "+err.Error()))
		return
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	var probe map[string]json.RawMessage
	if json.Unmarshal(body, &probe) == nil {
		if _, ok := probe["workflowId"]; ok {
			h.createPlan(w, r)
			return
		}
	}
	var req endpointsRequest
	if err := decode(w, r, &req); err != nil {
		writeError(w, err)
		return
	}
	m, err := migration.New(req.Source, req.Target, h.Now())
	if err != nil {
		writeError(w, badRequest(err.Error()))
		return
	}
	m, err = h.Store.Create(r.Context(), m)
	if err != nil {
		writeError(w, err)
		return
	}
	h.Notifier.Notify(m.ID)
	w.Header().Set("Location", "/v1/migrations/"+m.ID)
	h.writeMigration(w, http.StatusAccepted, m)
}

func (h *handler) get(w http.ResponseWriter, r *http.Request) {
	m, err := h.Store.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	h.writeMigration(w, http.StatusOK, m)
}

// routeEndpoint is a bucket as apps see it: where it is, but not the
// server's credentials for it. Apps use their own.
type routeEndpoint struct {
	Kind   migration.Kind `json:"kind"`
	Bucket string         `json:"bucket"`
	Region string         `json:"region,omitempty"`
	URL    string         `json:"url,omitempty"`
}

type routeView struct {
	MigrationID string          `json:"migrationId"`
	Phase       migration.Phase `json:"phase"`
	Generation  int64           `json:"generation"`
	Write       routeEndpoint   `json:"write"`
	Read        []routeEndpoint `json:"read"`
	Delete      []routeEndpoint `json:"delete"`
	// AppTTLMS is how long the server keeps waiting for an app that stopped
	// asking. An app must stop using its route well before that.
	AppTTLMS int64 `json:"appTtlMs,omitempty"`
}

// routing tells an app where to write and read. An app asks for it every
// second or so, passing app (a name unique among the app's servers) and
// applied (the generation it uses now), so a switch can wait for it.
func (h *handler) routing(w http.ResponseWriter, r *http.Request) {
	m, err := h.Store.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	if app := r.URL.Query().Get("app"); app != "" && h.Apps != nil {
		applied, err := strconv.ParseInt(r.URL.Query().Get("applied"), 10, 64)
		if err != nil || applied < 0 {
			writeError(w, badRequest("applied must be the generation the app uses, 0 if none"))
			return
		}
		h.Apps.Report(m.ID, app, applied)
	}
	view := func(s migration.Side) routeEndpoint {
		e := m.Endpoint(s)
		return routeEndpoint{Kind: e.Kind, Bucket: e.Bucket, Region: e.Region, URL: e.URL}
	}
	v := routeView{MigrationID: m.ID, Phase: m.Phase, Generation: m.Routing.Generation, Write: view(m.Routing.Write)}
	if h.Apps != nil {
		v.AppTTLMS = h.Apps.TTL().Milliseconds()
	}
	for _, s := range m.Routing.Read {
		v.Read = append(v.Read, view(s))
	}
	for _, s := range m.Routing.Delete {
		v.Delete = append(v.Delete, view(s))
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, v)
}

type deletesBody struct {
	Keys []string `json:"keys"`
}

// maxDeleteKeys bounds one POST .../deletes.
const maxDeleteKeys = 1000

// deletes records keys an app is about to delete, before it deletes them
// from the buckets, so a copy running at the same time cannot bring them
// back.
func (h *handler) deletes(w http.ResponseWriter, r *http.Request) {
	var body deletesBody
	if err := decode(w, r, &body); err != nil {
		writeError(w, err)
		return
	}
	if len(body.Keys) == 0 || len(body.Keys) > maxDeleteKeys || slices.Contains(body.Keys, "") {
		writeError(w, badRequest(fmt.Sprintf("keys must hold 1 to %d non-empty keys", maxDeleteKeys)))
		return
	}
	m, err := h.Store.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	if h.Deletes != nil {
		if err := h.Deletes.Add(r.Context(), m.ID, body.Keys...); err != nil {
			writeError(w, err)
			return
		}
	}
	w.WriteHeader(http.StatusNoContent)
}

// pathCommands are the commands sent as POST /v1/migrations/{id}/{command}.
// Abort is sent as DELETE.
var pathCommands = map[string]migration.Command{
	"freeze":   migration.CommandFreeze,
	"switch":   migration.CommandSwitch,
	"resume":   migration.CommandResume,
	"rollback": migration.CommandRollback,
	"finalize": migration.CommandFinalize,
}

// maxDeadlineMS bounds how long a command may wait for apps: one hour.
const maxDeadlineMS = 3_600_000

type commandBody struct {
	CutoverID       string `json:"cutoverId"`
	DeadlineMS      int64  `json:"deadlineMs"`
	ExpectedVersion int64  `json:"expectedVersion"`
}

func (h *handler) command(w http.ResponseWriter, r *http.Request) {
	cmd, ok := pathCommands[r.PathValue("command")]
	if !ok {
		writeError(w, notFound(fmt.Sprintf("unknown command %q", r.PathValue("command"))))
		return
	}
	var body commandBody
	if err := decodeOptional(w, r, &body); err != nil {
		writeError(w, err)
		return
	}
	if body.DeadlineMS < 0 || body.DeadlineMS > maxDeadlineMS || body.ExpectedVersion < 0 {
		writeError(w, badRequest(fmt.Sprintf("deadlineMs must be 0 to %d and expectedVersion not negative", maxDeadlineMS)))
		return
	}
	h.request(w, r, migration.CommandRequest{
		Command:         cmd,
		CutoverID:       body.CutoverID,
		DeadlineMS:      body.DeadlineMS,
		ExpectedVersion: body.ExpectedVersion,
	})
}

type abortBody struct {
	ExpectedVersion int64 `json:"expectedVersion"`
}

func (h *handler) abort(w http.ResponseWriter, r *http.Request) {
	var body abortBody
	if err := decodeOptional(w, r, &body); err != nil {
		writeError(w, err)
		return
	}
	if body.ExpectedVersion < 0 {
		writeError(w, badRequest("expectedVersion must not be negative"))
		return
	}
	h.request(w, r, migration.CommandRequest{Command: migration.CommandAbort, ExpectedVersion: body.ExpectedVersion})
}

func (h *handler) request(w http.ResponseWriter, r *http.Request, req migration.CommandRequest) {
	req.IdempotencyKey = r.Header.Get(idempotencyHeader)
	req.RequestedAt = h.Now()
	m, err := h.Store.RequestCommand(r.Context(), r.PathValue("id"), req)
	if err != nil {
		writeError(w, err)
		return
	}
	h.Notifier.Notify(m.ID)
	h.writeMigration(w, http.StatusAccepted, m)
}

// decode reads a required JSON body and rejects unknown fields.
func decode(w http.ResponseWriter, r *http.Request, v any) error {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return badRequest("invalid JSON body: " + err.Error())
	}
	if dec.More() {
		return badRequest("invalid JSON body: trailing data")
	}
	return nil
}

// decodeOptional is decode that also accepts an empty body.
func decodeOptional(w http.ResponseWriter, r *http.Request, v any) error {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil && !errors.Is(err, io.EOF) {
		return badRequest("invalid JSON body: " + err.Error())
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
