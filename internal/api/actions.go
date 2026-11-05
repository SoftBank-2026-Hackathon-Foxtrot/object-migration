package api

import (
	"crypto/subtle"
	"fmt"
	"net/http"
	"strings"

	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/migration"
)

// This file serves the parts of the team's common API draft 0.2 that fit
// object storage: POST /v1/migrations/{id}/actions, GET
// /v1/operations/{id}, Bearer tokens, revisions and common lifecycle
// names. The older path commands stay for existing tools.

// Action is a command name of the common API.
type Action string

const (
	ActionPrepare   Action = "PREPARE"
	ActionStartSync Action = "START_SYNC"
	ActionQuiesce   Action = "QUIESCE"
	ActionCutover   Action = "CUTOVER"
	ActionResume    Action = "RESUME"
	ActionRollback  Action = "ROLLBACK"
	ActionCancel    Action = "CANCEL"
	ActionReconcile Action = "RECONCILE"
	ActionFinalize  Action = "FINALIZE"
)

// actionCommands maps the common actions to this server's commands.
var actionCommands = map[Action]migration.Command{
	ActionQuiesce:  migration.CommandFreeze,
	ActionCutover:  migration.CommandSwitch,
	ActionResume:   migration.CommandResume,
	ActionRollback: migration.CommandRollback,
	ActionFinalize: migration.CommandFinalize,
	ActionCancel:   migration.CommandAbort,
}

// supportedActions is the order actions are listed in.
var supportedActions = []Action{ActionQuiesce, ActionCutover, ActionResume, ActionRollback, ActionFinalize, ActionCancel}

// unsupportedActions explains the actions this server does not take.
var unsupportedActions = map[Action]string{
	ActionPrepare:   "object storage has no PREPARE: check access with POST /v1/preflight; POST /v1/migrations starts the initial copy",
	ActionStartSync: "object storage has no START_SYNC: POST /v1/migrations starts the initial copy",
	ActionReconcile: "RECONCILE is not supported: the worker reads the buckets and app routes again after every restart",
}

func commandAction(c migration.Command) Action {
	for a, cc := range actionCommands {
		if cc == c {
			return a
		}
	}
	return Action(strings.ToUpper(string(c)))
}

type actionBody struct {
	Action           Action `json:"action"`
	ExpectedRevision int64  `json:"expectedRevision"`
	TransitionID     string `json:"transitionId"`
	QuiesceRef       string `json:"quiesceRef"`
	ExpectedLocation string `json:"expectedLocation"`
	DeadlineMS       int64  `json:"deadlineMs"`
}

// needsTransition are the actions that must name the cutover attempt.
var needsTransition = map[Action]bool{ActionQuiesce: true, ActionCutover: true, ActionResume: true, ActionRollback: true}

func (h *handler) action(w http.ResponseWriter, r *http.Request) {
	var body actionBody
	if err := decode(w, r, &body); err != nil {
		writeError(w, err)
		return
	}
	if msg, ok := unsupportedActions[body.Action]; ok {
		writeError(w, &apiError{Code: "UNSUPPORTED_ACTION", Message: msg, status: http.StatusBadRequest})
		return
	}
	cmd, ok := actionCommands[body.Action]
	switch {
	case !ok:
		writeError(w, badRequest(fmt.Sprintf("unknown action %q", body.Action)))
		return
	case body.ExpectedRevision <= 0:
		writeError(w, badRequest("expectedRevision is required"))
		return
	case needsTransition[body.Action] && body.TransitionID == "":
		writeError(w, badRequest("transitionId is required for "+string(body.Action)))
		return
	case body.DeadlineMS < 0 || body.DeadlineMS > maxDeadlineMS:
		writeError(w, badRequest(fmt.Sprintf("deadlineMs must be 0 to %d", maxDeadlineMS)))
		return
	}
	if body.Action == ActionResume && body.ExpectedLocation != "TARGET" {
		// Rollback already moves apps back to the source, so only the
		// target is ever resumed.
		writeError(w, badRequest(`expectedLocation must be "TARGET"`))
		return
	}
	req := migration.CommandRequest{
		Command:          cmd,
		IdempotencyKey:   r.Header.Get(idempotencyHeader),
		CutoverID:        body.TransitionID,
		QuiesceRef:       body.QuiesceRef,
		DeadlineMS:       body.DeadlineMS,
		ExpectedRevision: body.ExpectedRevision,
		RequestedAt:      h.Now(),
	}
	m, err := h.Store.RequestCommand(r.Context(), r.PathValue("id"), req)
	if err != nil {
		writeError(w, err)
		return
	}
	h.Notifier.Notify(m.ID)
	op, _ := m.Operation(m.ActiveOperation)
	statusURL := "/v1/operations/" + op.ID
	w.Header().Set("Location", statusURL)
	w.Header().Set("Retry-After", "1")
	writeJSON(w, http.StatusAccepted, map[string]any{
		"operationId": op.ID,
		"migrationId": m.ID,
		"action":      body.Action,
		"status":      migration.OperationQueued,
		"statusUrl":   statusURL,
		"revision":    m.Revision,
	})
}

// operationView is an operation as GET /v1/operations/{id} returns it.
type operationView struct {
	migration.Operation
	MigrationID string           `json:"migrationId"`
	Action      Action           `json:"action"`
	Lifecycle   string           `json:"lifecycle"`
	Revision    int64            `json:"revision"`
	LastError   *migration.Error `json:"lastError"`
}

func (h *handler) operation(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	mid, ok := migration.OperationMigrationID(id)
	if !ok {
		writeError(w, notFound("operation "+id+" not found"))
		return
	}
	m, err := h.Store.Get(r.Context(), mid)
	if err != nil {
		writeError(w, err)
		return
	}
	op, ok := m.Operation(id)
	if !ok {
		writeError(w, notFound("operation "+id+" not found"))
		return
	}
	op.Status = operationStatus(m, op)
	if op.Status == migration.OperationQueued || op.Status == migration.OperationRunning {
		w.Header().Set("Retry-After", "1")
	}
	writeJSON(w, http.StatusOK, operationView{
		Operation: op, MigrationID: m.ID, Action: commandAction(op.Command),
		Lifecycle: lifecycle(m.Phase), Revision: m.Revision, LastError: m.LastError,
	})
}

// operationStatus reports a still active operation from the migration's
// state: UNKNOWN when the worker could not confirm its outcome.
func operationStatus(m migration.Migration, op migration.Operation) migration.OperationStatus {
	if op.ID != m.ActiveOperation {
		return op.Status
	}
	if m.State == migration.StateUnknown {
		return migration.OperationUnknown
	}
	return migration.OperationRunning
}

// lifecycle names a phase with the common lifecycle values. ROLLING_BACK
// has no common name; object storage keeps it.
func lifecycle(p migration.Phase) string {
	switch p {
	case migration.PhaseInitialSync, migration.PhaseSyncing:
		return "SYNCING"
	case migration.PhaseFrozen:
		return "QUIESCED"
	case migration.PhaseResumed, migration.PhaseVerifying:
		return "ACTIVE"
	case migration.PhaseRolledBack:
		// Apps are already back on the source; nothing waits for RESUME.
		return "RESTORED"
	case migration.PhaseAborted:
		return "CANCELLED"
	}
	return string(p)
}

func health(m migration.Migration) string {
	switch {
	case m.State == migration.StateUnknown:
		return "UNKNOWN"
	case m.Phase == migration.PhaseFailed || m.State == migration.StateFailed:
		return "FAILED"
	}
	return "OK"
}

// bearer requires Authorization: Bearer <token> when a token is set.
func bearer(token string, next http.HandlerFunc) http.HandlerFunc {
	if token == "" {
		return next
	}
	want := []byte("Bearer " + token)
	return func(w http.ResponseWriter, r *http.Request) {
		got := []byte(r.Header.Get("Authorization"))
		if subtle.ConstantTimeCompare(got, want) != 1 {
			w.Header().Set("WWW-Authenticate", "Bearer")
			writeError(w, &apiError{Code: "UNAUTHENTICATED", Message: "a valid Bearer token is required", status: http.StatusUnauthorized})
			return
		}
		next(w, r)
	}
}
