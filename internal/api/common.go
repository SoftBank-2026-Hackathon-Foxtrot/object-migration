package api

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/migration"
	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/store"
)

// This file serves the team's common migration API (draft 0.2, the
// contract the DB and Redis servers share): plans, the nine actions,
// operations, and the common status. The older routes stay for existing
// tools; they share the same migrations.

const (
	kindObject = "OBJECT_STORAGE"
	// Profile is the one migration profile this server offers.
	Profile = "object-copy-route-v1"
	// evidenceTTL is how long a check's evidence stays valid.
	evidenceTTL = 30 * time.Second
	// stabilityWindow is how long the target must have served apps before
	// STABILITY_WINDOW passes.
	stabilityWindow = 30 * time.Second
)

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

var allActions = []Action{
	ActionPrepare, ActionStartSync, ActionQuiesce, ActionCutover, ActionResume,
	ActionRollback, ActionCancel, ActionReconcile, ActionFinalize,
}

// actionCommands maps the actions the worker runs to its commands.
var actionCommands = map[Action]migration.Command{
	ActionPrepare:   migration.CommandPrepare,
	ActionStartSync: migration.CommandStartSync,
	ActionQuiesce:   migration.CommandFreeze,
	ActionCutover:   migration.CommandSwitch,
	ActionResume:    migration.CommandResume,
	ActionRollback:  migration.CommandRollback,
	ActionFinalize:  migration.CommandFinalize,
	ActionCancel:    migration.CommandAbort,
	ActionReconcile: migration.CommandReconcile,
}

func commandAction(c migration.Command) Action {
	for a, cc := range actionCommands {
		if cc == c {
			return a
		}
	}
	return Action(strings.ToUpper(string(c)))
}

// heldActions run inside the one cutover attempt and name it.
var heldActions = map[Action]bool{ActionQuiesce: true, ActionCutover: true, ActionResume: true, ActionRollback: true}

// ---- plans ----------------------------------------------------------------

type planRequest struct {
	WorkflowID       string            `json:"workflowId"`
	ResourceID       string            `json:"resourceId"`
	Kind             string            `json:"kind"`
	Profile          string            `json:"profile"`
	SourceRef        string            `json:"sourceRef"`
	TargetRef        string            `json:"targetRef"`
	WorkloadScopeRef string            `json:"workloadScopeRef"`
	BindingRef       string            `json:"bindingRef"`
	Policy           *migration.Policy `json:"policy"`
	ReplicationRef   *string           `json:"replicationRef"`
}

// createPlan registers a plan without touching the buckets; PREPARE and
// START_SYNC start the work.
func (h *handler) createPlan(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get(idempotencyHeader) == "" {
		writeError(w, badRequest("Idempotency-Key is required"))
		return
	}
	var req planRequest
	if err := decode(w, r, &req); err != nil {
		writeError(w, err)
		return
	}
	for name, v := range map[string]string{
		"workflowId": req.WorkflowID, "resourceId": req.ResourceID, "kind": req.Kind, "profile": req.Profile,
		"sourceRef": req.SourceRef, "targetRef": req.TargetRef, "workloadScopeRef": req.WorkloadScopeRef, "bindingRef": req.BindingRef,
	} {
		if v == "" {
			writeError(w, badRequest(name+" is required"))
			return
		}
	}
	switch {
	case req.Policy == nil:
		writeError(w, badRequest("policy is required"))
		return
	case req.Policy.MaxQuiesceSeconds < 1:
		writeError(w, badRequest("policy.maxQuiesceSeconds must be at least 1"))
		return
	case req.Kind != kindObject || req.Profile != Profile || req.Policy.RollbackMode != "REVERSE_REQUIRED" || req.ReplicationRef != nil:
		writeError(w, newError(http.StatusUnprocessableEntity, "UNSUPPORTED_PROFILE",
			fmt.Sprintf("supported: kind %s, profile %s, rollbackMode REVERSE_REQUIRED, no replicationRef", kindObject, Profile)))
		return
	}
	src, err := h.resource(req.SourceRef)
	if err != nil {
		writeError(w, err)
		return
	}
	dst, err := h.resource(req.TargetRef)
	if err != nil {
		writeError(w, err)
		return
	}
	m, err := migration.New(src, dst, h.Now())
	if err != nil {
		writeError(w, newError(http.StatusUnprocessableEntity, "PRECONDITION_FAILED", err.Error()))
		return
	}
	m.Plan = &migration.Plan{
		WorkflowID: req.WorkflowID, ResourceID: req.ResourceID, Profile: req.Profile,
		SourceRef: req.SourceRef, TargetRef: req.TargetRef, WorkloadScopeRef: req.WorkloadScopeRef,
		BindingRef: req.BindingRef, Policy: *req.Policy,
	}
	m, err = h.Store.Create(r.Context(), m)
	if err != nil {
		writeError(w, err)
		return
	}
	w.Header().Set("Location", "/v1/migrations/"+m.ID)
	h.writeMigration(w, http.StatusCreated, m)
}

// resource resolves "resource:<name>" from the registered buckets.
func (h *handler) resource(ref string) (migration.Endpoint, error) {
	name, ok := strings.CutPrefix(ref, "resource:")
	e, known := h.Resources[name]
	if !ok || !known {
		names := make([]string, 0, len(h.Resources))
		for n := range h.Resources {
			names = append(names, "resource:"+n)
		}
		slices.Sort(names)
		return migration.Endpoint{}, newError(http.StatusUnprocessableEntity, "PRECONDITION_FAILED",
			fmt.Sprintf("unknown resource %s; registered: %s", ref, strings.Join(names, ", ")))
	}
	return e, nil
}

// ---- actions --------------------------------------------------------------

type actionBody struct {
	Action           Action `json:"action"`
	ExpectedRevision *int64 `json:"expectedRevision"`
	TransitionID     string `json:"transitionId"`
	QuiesceRef       string `json:"quiesceRef"`
	ExpectedLocation string `json:"expectedLocation"`
	// DeadlineMS is this server's addition: how long CUTOVER, FINALIZE or
	// CANCEL waits for apps to take a new route.
	DeadlineMS int64 `json:"deadlineMs"`
}

func (b actionBody) validate() error {
	switch {
	case !slices.Contains(allActions, b.Action):
		return badRequest(fmt.Sprintf("unknown action %q", b.Action))
	case b.ExpectedRevision == nil || *b.ExpectedRevision < 0:
		return badRequest("expectedRevision is required")
	case heldActions[b.Action] && (b.TransitionID == "" || b.QuiesceRef == ""):
		return badRequest(string(b.Action) + " needs transitionId and quiesceRef")
	case !heldActions[b.Action] && (b.TransitionID != "" || b.QuiesceRef != ""):
		return badRequest("transitionId and quiesceRef are only for QUIESCE, CUTOVER, RESUME and ROLLBACK")
	case b.Action == ActionResume && b.ExpectedLocation != "SOURCE" && b.ExpectedLocation != "TARGET":
		return badRequest("RESUME needs expectedLocation SOURCE or TARGET")
	case b.Action != ActionResume && b.ExpectedLocation != "":
		return badRequest("expectedLocation is only for RESUME")
	case b.DeadlineMS < 0 || b.DeadlineMS > maxDeadlineMS:
		return badRequest(fmt.Sprintf("deadlineMs must be 0 to %d", maxDeadlineMS))
	}
	return nil
}

func (h *handler) action(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get(idempotencyHeader) == "" {
		writeError(w, badRequest("Idempotency-Key is required"))
		return
	}
	var body actionBody
	if err := decode(w, r, &body); err != nil {
		writeError(w, err)
		return
	}
	if err := body.validate(); err != nil {
		writeError(w, err)
		return
	}
	m, err := h.Store.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	if err := h.checkAction(m, body); err != nil {
		writeError(w, err)
		return
	}

	var op migration.Operation
	switch lc := lifecycle(m); {
	case body.Action == ActionReconcile:
		op, err = h.reconcile(r.Context(), m.ID, *body.ExpectedRevision)
	case body.Action == ActionResume && lc == "ROLLED_BACK_HELD":
		op, err = h.resumeSource(r.Context(), m.ID, body)
	case body.Action == ActionFinalize && lc == "RESTORED":
		op, err = h.finalizeRestored(r.Context(), m.ID, *body.ExpectedRevision)
	default:
		m, err = h.Store.RequestCommand(r.Context(), m.ID, migration.CommandRequest{
			Command:          actionCommands[body.Action],
			IdempotencyKey:   r.Header.Get(idempotencyHeader),
			CutoverID:        body.TransitionID,
			QuiesceRef:       body.QuiesceRef,
			DeadlineMS:       body.DeadlineMS,
			ExpectedRevision: *body.ExpectedRevision,
			RequestedAt:      h.Now(),
		})
		if err == nil {
			h.Notifier.Notify(m.ID)
			op, _ = m.Operation(m.ActiveOperation)
		}
	}
	if err != nil {
		writeError(w, err)
		return
	}
	statusURL := "/v1/operations/" + op.ID
	w.Header().Set("Location", statusURL)
	w.Header().Set("Retry-After", "1")
	writeJSON(w, http.StatusAccepted, map[string]any{
		"operationId": op.ID,
		"migrationId": m.ID,
		"action":      body.Action,
		"status":      op.Status,
		"statusUrl":   statusURL,
	})
}

// checkAction applies the common rules before an action is accepted: one
// command at a time, the expected revision, RECONCILE after an unknown
// outcome, the allowed actions, and one cutover attempt per migration.
func (h *handler) checkAction(m migration.Migration, b actionBody) error {
	unknown := m.State == migration.StateUnknown
	stuckSwitch := unknown && m.Reconciled && m.Pending != nil && m.Pending.Command == migration.CommandSwitch
	if m.ActiveOperation != "" && b.Action != ActionReconcile && !(b.Action == ActionRollback && stuckSwitch) {
		return newError(http.StatusConflict, "RESOURCE_BUSY", m.ActiveOperation+" is running")
	}
	if *b.ExpectedRevision != m.Revision {
		return newError(http.StatusConflict, "REVISION_CONFLICT", fmt.Sprintf("revision is %d, not %d", m.Revision, *b.ExpectedRevision))
	}
	if b.Action == ActionReconcile {
		return nil
	}
	if unknown && !m.Reconciled {
		return newError(http.StatusConflict, "UNKNOWN_STATE", "the last command's outcome is unknown: RECONCILE first")
	}
	lc := lifecycle(m)
	if !slices.Contains(allowedActions(m), b.Action) {
		return invalidState(fmt.Sprintf("%s is not allowed in %s", b.Action, lc))
	}
	if heldActions[b.Action] {
		switch {
		case m.TransitionID != "" && m.TransitionID != b.TransitionID:
			return invalidState("the transition is " + m.TransitionID)
		case m.TransitionID == "" && b.Action != ActionQuiesce:
			return invalidState("no transition yet: QUIESCE first")
		}
	}
	if b.Action == ActionResume {
		want := "TARGET"
		if lc == "ROLLED_BACK_HELD" {
			want = "SOURCE"
		}
		if b.ExpectedLocation != want {
			return invalidState("expectedLocation must be " + want)
		}
	}
	return nil
}

// allowedActions lists what the common API accepts now. RECONCILE is
// always allowed; after an unknown outcome it is the only one until run.
func allowedActions(m migration.Migration) []Action {
	if m.State == migration.StateUnknown {
		if m.Reconciled && m.Pending != nil && m.Pending.Command == migration.CommandSwitch && m.Readiness.Rollback.Result == migration.ResultPass {
			return []Action{ActionRollback, ActionReconcile}
		}
		return []Action{ActionReconcile}
	}
	if m.ActiveOperation != "" {
		return []Action{ActionReconcile}
	}
	out := []Action{}
	switch lifecycle(m) {
	case "ROLLED_BACK_HELD":
		out = append(out, ActionResume)
	case "RESTORED":
		out = append(out, ActionFinalize)
	default:
		cmds := m.AllowedCommands()
		for _, a := range allActions {
			if c, ok := actionCommands[a]; ok && slices.Contains(cmds, c) {
				out = append(out, a)
			}
		}
	}
	return append(out, ActionReconcile)
}

// mutate applies fn to the latest migration and saves it, rereading when
// the worker wrote in between.
func (h *handler) mutate(ctx context.Context, id string, fn func(*migration.Migration) error) (migration.Migration, error) {
	for range 10 {
		m, err := h.Store.Get(ctx, id)
		if err != nil {
			return migration.Migration{}, err
		}
		if err := fn(&m); err != nil {
			return migration.Migration{}, err
		}
		m, err = h.Store.Update(ctx, m)
		if !errors.Is(err, store.ErrConflict) {
			return m, err
		}
	}
	return migration.Migration{}, newError(http.StatusServiceUnavailable, "INTERNAL", "migration kept changing; retry")
}

func revisionIs(m *migration.Migration, want int64) error {
	if m.Revision != want {
		return newError(http.StatusConflict, "REVISION_CONFLICT", fmt.Sprintf("revision is %d, not %d", m.Revision, want))
	}
	return nil
}

// reconcile compares the record with what apps report now and changes
// nothing outside the record. After an unknown outcome it lets the
// orchestrator act again, and when every app has caught up it wakes the
// worker to finish the command.
func (h *handler) reconcile(ctx context.Context, id string, rev int64) (migration.Operation, error) {
	var op migration.Operation
	_, err := h.mutate(ctx, id, func(m *migration.Migration) error {
		if err := revisionIs(m, rev); err != nil {
			return err
		}
		changes := []string{m.Routing.String()}
		waiting := []string{}
		if h.Apps != nil {
			waiting = h.Apps.Waiting(m.ID, m.Routing.Generation)
			changes = append(changes, fmt.Sprintf("apps reporting: %d", h.Apps.Live(m.ID)))
			if len(waiting) > 0 {
				changes = append(changes, "apps not on generation "+fmt.Sprint(m.Routing.Generation)+": "+strings.Join(waiting, ", "))
			}
		}
		if m.Pending != nil {
			changes = append(changes, "pending: "+string(commandAction(m.Pending.Command)))
		}
		if m.State == migration.StateUnknown {
			m.Reconciled = true
		}
		op = m.RecordDone(migration.CommandReconcile, "", migration.OperationSucceeded, "", changes, h.Now())
		return nil
	})
	if err == nil {
		h.Notifier.Notify(id) // a command waiting on apps checks again now
	}
	return op, err
}

// resumeSource confirms apps are back on the source after a rollback. The
// rollback already moved them, so nothing else changes.
func (h *handler) resumeSource(ctx context.Context, id string, b actionBody) (migration.Operation, error) {
	var op migration.Operation
	_, err := h.mutate(ctx, id, func(m *migration.Migration) error {
		if err := revisionIs(m, *b.ExpectedRevision); err != nil {
			return err
		}
		if route(*m) != "SOURCE" {
			return invalidState("apps are not on the source: " + m.Routing.String())
		}
		m.SourceResumed, m.TransitionClosed = true, true
		op = m.RecordDone(migration.CommandResume, b.TransitionID, migration.OperationSucceeded, "", []string{m.Routing.String()}, h.Now())
		return nil
	})
	return op, err
}

// finalizeRestored closes a plan that was rolled back. The rollback already
// released the change feeds; the target's data is kept.
func (h *handler) finalizeRestored(ctx context.Context, id string, rev int64) (migration.Operation, error) {
	var op migration.Operation
	_, err := h.mutate(ctx, id, func(m *migration.Migration) error {
		if err := revisionIs(m, rev); err != nil {
			return err
		}
		m.Finalized, m.TransitionClosed = true, true
		op = m.RecordDone(migration.CommandFinalize, "", migration.OperationSucceeded, "", []string{"target data kept"}, h.Now())
		return nil
	})
	return op, err
}

// ---- operations -----------------------------------------------------------

type operationView struct {
	OperationID      string           `json:"operationId"`
	MigrationID      string           `json:"migrationId"`
	Action           Action           `json:"action"`
	Status           string           `json:"status"`
	Error            *apiError        `json:"error"`
	ConfirmedChanges []string         `json:"confirmedChanges"`
	TransitionID     *string          `json:"transitionId"`
	RequestedAt      time.Time        `json:"requestedAt"`
	FinishedAt       *time.Time       `json:"finishedAt"`
	Lifecycle        string           `json:"lifecycle"`
	Revision         int64            `json:"revision"`
	LastError        *migration.Error `json:"lastError"`
}

func (h *handler) operation(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	mid, ok := migration.OperationMigrationID(id)
	if !ok {
		writeError(w, notFound("no operation "+id))
		return
	}
	m, err := h.Store.Get(r.Context(), mid)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			err = notFound("no operation " + id)
		}
		writeError(w, err)
		return
	}
	op, ok := m.Operation(id)
	if !ok {
		writeError(w, notFound("no operation "+id))
		return
	}
	status := operationStatus(m, op)
	v := operationView{
		OperationID: op.ID, MigrationID: m.ID, Action: commandAction(op.Command), Status: string(status),
		ConfirmedChanges: op.Changes, TransitionID: optional(op.TransitionID),
		RequestedAt: op.RequestedAt, FinishedAt: op.FinishedAt,
		Lifecycle: lifecycle(m), Revision: m.Revision, LastError: m.LastError,
	}
	if v.ConfirmedChanges == nil {
		v.ConfirmedChanges = []string{}
	}
	switch status {
	case migration.OperationQueued, migration.OperationRunning:
		w.Header().Set("Retry-After", "1")
	case migration.OperationFailed, migration.OperationUnknown:
		code, msg := "OPERATION_FAILED", op.Message
		if status == migration.OperationUnknown {
			code = "UNKNOWN_STATE"
		}
		if m.LastError != nil {
			if status == migration.OperationFailed && m.LastError.Code != "" {
				code = m.LastError.Code
			}
			if msg == "" {
				msg = m.LastError.Message
			}
		}
		if msg == "" {
			msg = strings.ToLower(string(status))
		}
		v.Error = newError(0, code, msg)
		v.Error.OperationID = &v.OperationID
	}
	writeJSON(w, http.StatusOK, v)
}

// operationStatus reports a still active operation from the migration's
// state: UNKNOWN while the worker cannot confirm its outcome.
func operationStatus(m migration.Migration, op migration.Operation) migration.OperationStatus {
	if op.ID != m.ActiveOperation {
		return op.Status
	}
	if m.State == migration.StateUnknown {
		return migration.OperationUnknown
	}
	return migration.OperationRunning
}

// ---- status ---------------------------------------------------------------

// lifecycle names where a migration is with the common lifecycle values.
// While a rollback runs it stays where the rollback started.
func lifecycle(m migration.Migration) string {
	switch m.Phase {
	case migration.PhaseInitialSync, migration.PhaseSyncing:
		return "SYNCING"
	case migration.PhaseFrozen:
		return "QUIESCED"
	case migration.PhaseResumed, migration.PhaseVerifying:
		return "ACTIVE"
	case migration.PhaseRollingBack:
		if m.RolledBackFrom != "" && m.RolledBackFrom != migration.PhaseRollingBack {
			from := m
			from.Phase = m.RolledBackFrom
			return lifecycle(from)
		}
		return "SWITCHED"
	case migration.PhaseRolledBack:
		switch {
		case m.Finalized:
			return "COMPLETED"
		case m.SourceResumed:
			return "RESTORED"
		}
		return "ROLLED_BACK_HELD"
	case migration.PhaseAborted:
		return "CANCELLED"
	}
	return string(m.Phase)
}

func health(m migration.Migration) string {
	switch {
	case m.State == migration.StateUnknown && !m.Reconciled:
		return "UNKNOWN"
	case m.Phase == migration.PhaseFailed || m.State == migration.StateFailed || m.State == migration.StateUnknown:
		return "FAILED"
	}
	return "OK"
}

// route is where apps read and write: SOURCE or TARGET when one bucket
// serves both, MIXED while reads span both.
func route(m migration.Migration) string {
	r := m.Routing
	switch {
	case r.Write == migration.SideSource && slices.Equal(r.Read, []migration.Side{migration.SideSource}):
		return "SOURCE"
	case r.Write == migration.SideTarget && slices.Equal(r.Read, []migration.Side{migration.SideTarget}):
		return "TARGET"
	}
	return "MIXED"
}

type decision struct {
	Result  migration.Result `json:"result"`
	Reasons []string         `json:"reasons"`
}

func pass() decision { return decision{Result: migration.ResultPass, Reasons: []string{}} }

func fail(reasons ...string) decision {
	return decision{Result: migration.ResultFail, Reasons: reasons}
}

// fromDecision turns this server's decision into the common form: the
// reasons are the checks that did not pass.
func fromDecision(d migration.Decision) decision {
	out := decision{Result: d.Result, Reasons: []string{}}
	for _, c := range d.Checks {
		if !c.OK {
			out.Reasons = append(out.Reasons, strings.ToUpper(c.Name))
		}
	}
	if d.Result == migration.ResultUnknown {
		out.Reasons = append(out.Reasons, "NOT_OBSERVED_YET")
	}
	return out
}

type check struct {
	Code             string   `json:"code"`
	Stage            string   `json:"stage"`
	Required         bool     `json:"required"`
	Decision         decision `json:"decision"`
	EvidenceRef      *string  `json:"evidenceRef"`
	ObservedAt       string   `json:"observedAt"`
	ValidUntil       string   `json:"validUntil"`
	TransitionID     *string  `json:"transitionId"`
	TopologyRevision int64    `json:"topologyRevision"`
}

type observed struct {
	ObservedAt           string `json:"observedAt"`
	Route                string `json:"route"`
	BusinessWriteAccess  string `json:"businessWriteAccess"`
	TargetWritesPossible bool   `json:"targetWritesPossible"`
}

type progress struct {
	Completed *int64 `json:"completed"`
	Total     *int64 `json:"total"`
	Unit      string `json:"unit"`
}

type rollbackView struct {
	Method  string   `json:"method"`
	Reasons []string `json:"reasons"`
}

// migrationView is a migration as the API returns it: the common status,
// followed by this server's own fields (phase, state, source, target,
// replication, routing, version, operations, ...), and
// details.object.readiness with every check behind the decisions.
type migrationView struct {
	migration.Migration
	MigrationID       string                  `json:"migrationId"`
	WorkflowID        string                  `json:"workflowId"`
	ResourceID        string                  `json:"resourceId"`
	Kind              string                  `json:"kind"`
	Lifecycle         string                  `json:"lifecycle"`
	Health            string                  `json:"health"`
	ActiveOperationID *string                 `json:"activeOperationId"`
	TransitionID      *string                 `json:"transitionId"`
	TransitionState   string                  `json:"transitionState"`
	TopologyRevision  int64                   `json:"topologyRevision"`
	AllowedActions    []Action                `json:"allowedActions"`
	AllowedCommands   []migration.Command     `json:"allowedCommands"`
	Observed          observed                `json:"observed"`
	Readiness         map[string]decision     `json:"readiness"`
	Rollback          rollbackView            `json:"rollback"`
	Progress          progress                `json:"progress"`
	Checks            []check                 `json:"checks"`
	Details           map[string]objectDetail `json:"details"`
}

type objectDetail struct {
	Readiness migration.Readiness `json:"readiness"`
	AppsLive  int                 `json:"appsLive"`
}

func optional(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func (h *handler) writeMigration(w http.ResponseWriter, status int, m migration.Migration) {
	writeJSON(w, status, h.view(m))
}

func (h *handler) view(m migration.Migration) migrationView {
	now := h.Now().UTC()
	lc := lifecycle(m)
	rt := route(m)
	v := migrationView{
		Migration:         m,
		MigrationID:       m.ID,
		Kind:              kindObject,
		Lifecycle:         lc,
		Health:            health(m),
		ActiveOperationID: optional(m.ActiveOperation),
		TransitionID:      optional(m.TransitionID),
		TopologyRevision:  m.Routing.Generation,
		AllowedActions:    allowedActions(m),
		AllowedCommands:   m.AllowedCommands(),
	}
	if m.Plan != nil {
		v.WorkflowID, v.ResourceID = m.Plan.WorkflowID, m.Plan.ResourceID
	}
	switch {
	case m.TransitionID == "":
		v.TransitionState = "NONE"
	case m.TransitionClosed || m.Phase == migration.PhaseCompleted || m.Phase == migration.PhaseAborted:
		v.TransitionState = "CLOSED"
	default:
		v.TransitionState = "OPEN"
	}

	access := "SOURCE_ONLY"
	if m.Routing.Write == migration.SideTarget {
		access = "TARGET_ONLY"
	}
	v.Observed = observed{ObservedAt: now.Format(time.RFC3339), Route: rt, BusinessWriteAccess: access, TargetWritesPossible: m.TargetWritesPossible}

	if p := m.Replication.InitialCopy; p.Total > 0 || p.Completed > 0 {
		c, t := p.Completed, p.Total
		v.Progress = progress{Completed: &c, Total: &t, Unit: "objects"}
	} else {
		v.Progress = progress{Unit: "objects"}
	}

	// Checks. Readiness comes from the worker's last round, recorded at
	// UpdatedAt; it is valid for evidenceTTL from then.
	at := m.UpdatedAt.UTC()
	mk := func(code, stage string, d decision) check {
		c := check{
			Code: code, Stage: stage, Required: true, Decision: d,
			ObservedAt: at.Format(time.RFC3339), ValidUntil: at.Add(evidenceTTL).Format(time.RFC3339),
			TransitionID: optional(m.TransitionID), TopologyRevision: m.Routing.Generation,
		}
		if d.Result == migration.ResultPass {
			ref := fmt.Sprintf("evidence:%s:%s:r%d", m.ID, code, m.Revision)
			c.EvidenceRef = &ref
		}
		return c
	}
	notIn := fail("LIFECYCLE_" + lc)
	quiesced, cutover, resume, rollback, followup := notIn, notIn, notIn, notIn, notIn
	if lc == "QUIESCED" {
		quiesced = pass()
	}
	if lc == "READY" || lc == "QUIESCED" {
		cutover = fromDecision(m.Readiness.Cutover)
	}
	switch {
	case lc == "SWITCHED" && m.ActiveOperation == "":
		resume = fail("TARGET_ROUTE_NOT_VERIFIED")
		if m.Routing.Write == migration.SideTarget {
			resume = pass()
		}
	case lc == "ROLLED_BACK_HELD":
		resume = fail("SOURCE_ROUTE_NOT_VERIFIED")
		if rt == "SOURCE" {
			resume = pass()
		}
	}
	if !m.Phase.Terminal() {
		rollback = fromDecision(m.Readiness.Rollback)
	}
	if lc == "ACTIVE" {
		followup = fromDecision(m.Readiness.Advance)
	}
	healthy := pass()
	if v.Health != "OK" {
		healthy = fail("UNHEALTHY")
	}
	routeMatch := fail("ROUTE_MISMATCH")
	if (lc == "ACTIVE" && rt == "TARGET") || (lc == "RESTORED" && rt == "SOURCE") {
		routeMatch = pass()
	}
	stable := fail("NOT_STABLE_YET")
	if lc == "ACTIVE" && h.activeSince(m).Add(stabilityWindow).Before(now) {
		stable = pass()
	}
	orchestrator := decision{Result: migration.ResultUnknown, Reasons: []string{"ORCHESTRATOR_EVIDENCE_REQUIRED"}}
	v.Checks = []check{
		mk("QUIESCE_COMPLETE", "QUIESCE", quiesced),
		mk("CUTOVER_SAFE", "CUTOVER", cutover),
		mk("RESUME_SAFE", "RESUME", resume),
		mk("ROLLBACK_SAFE", "ROLLBACK", rollback),
		mk("RESOURCE_HEALTH", "ADVANCE", healthy),
		mk("ROUTE_MATCH", "ADVANCE", routeMatch),
		mk("WORKLOAD_RUNNING", "ADVANCE", orchestrator),
		mk("BUSINESS_PROBE", "ADVANCE", orchestrator),
		mk("FOLLOWUP_COMPLETE", "ADVANCE", followup),
		mk("STABILITY_WINDOW", "ADVANCE", stable),
	}
	advance := decision{Result: migration.ResultUnknown, Reasons: []string{}}
	for _, c := range v.Checks {
		if c.Stage == "ADVANCE" && c.Decision.Result == migration.ResultFail {
			advance.Result = migration.ResultFail
			advance.Reasons = append(advance.Reasons, c.Decision.Reasons...)
		}
	}
	if advance.Result != migration.ResultFail {
		advance.Reasons = []string{"ORCHESTRATOR_EVIDENCE_REQUIRED"}
	}
	v.Readiness = map[string]decision{"cutover": cutover, "resume": resume, "rollback": rollback, "advance": advance}

	switch {
	case !m.TargetWritesPossible:
		v.Rollback = rollbackView{Method: "DIRECT", Reasons: []string{}}
	case rollback.Result == migration.ResultPass:
		v.Rollback = rollbackView{Method: "REVERSE_SYNC", Reasons: []string{}}
	default:
		v.Rollback = rollbackView{Method: "UNAVAILABLE", Reasons: rollback.Reasons}
	}

	d := objectDetail{Readiness: m.Readiness}
	if h.Apps != nil {
		d.AppsLive = h.Apps.Live(m.ID)
	}
	v.Details = map[string]objectDetail{"object": d}
	return v
}

// activeSince is when apps started using the target: the end of RESUME.
func (h *handler) activeSince(m migration.Migration) time.Time {
	for i := len(m.Operations) - 1; i >= 0; i-- {
		op := m.Operations[i]
		if op.Command == migration.CommandResume && op.Status == migration.OperationSucceeded && op.FinishedAt != nil {
			return *op.FinishedAt
		}
	}
	return h.Now()
}

// ---- capabilities ---------------------------------------------------------

func (h *handler) capabilities(w http.ResponseWriter, _ *http.Request) {
	conditions := []string{
		"AWS S3 (region required), GCS, or SeaweedFS through its S3 API",
		"registered resource: " + strings.Join(h.resourceNames(), ", "),
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"apiVersion": "v1",
		"profiles": []map[string]any{{
			"kind":             kindObject,
			"profile":          Profile,
			"sourceConditions": conditions,
			"targetConditions": append(conditions, "empty, or holding only copies of source objects"),
			"actions":          allActions,
			"rollbackModes":    []string{"REVERSE_REQUIRED"},
			"constraints": []string{
				"apps use the app storage module and follow GET .../routing",
				"QUIESCE records the attempt only: apps keep writing through the cutover",
				"CUTOVER waits until every app has taken each new route",
				"ROLLBACK copies the target's new objects back and returns apps to the source",
				"a source that already sends S3 object events elsewhere cannot be followed",
			},
		}},
		// This server's own fields.
		"type":        migration.Type,
		"engine":      h.Engine,
		"sourceKinds": allKinds,
		"targetKinds": allKinds,
		"commands": []migration.Command{
			migration.CommandFreeze, migration.CommandSwitch, migration.CommandResume,
			migration.CommandRollback, migration.CommandFinalize, migration.CommandAbort,
		},
		"directions": h.Directions,
	})
}

func (h *handler) resourceNames() []string {
	names := make([]string, 0, len(h.Resources))
	for n := range h.Resources {
		names = append(names, "resource:"+n)
	}
	slices.Sort(names)
	if len(names) == 0 {
		return []string{"none (set OBJECT_RESOURCES)"}
	}
	return names
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
			writeError(w, newError(http.StatusUnauthorized, "UNAUTHENTICATED", "a valid Bearer token is required"))
			return
		}
		next(w, r)
	}
}
