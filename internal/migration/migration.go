// Package migration defines an object migration and the rules for moving it
// between phases. It has no I/O; the API and the worker share these rules.
package migration

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

// Type is the migration type reported in the common API.
const Type = "object"

// Kind is the storage system behind an endpoint.
type Kind string

const (
	KindS3        Kind = "s3"
	KindGCS       Kind = "gcs"
	KindSeaweedFS Kind = "seaweedfs"
)

// Endpoint is one side of a migration.
type Endpoint struct {
	Kind   Kind   `json:"kind"`
	Bucket string `json:"bucket"`
	// Region is the bucket's region. Required for s3.
	Region string `json:"region,omitempty"`
	// URL is the S3-compatible endpoint. Required for seaweedfs.
	URL string `json:"url,omitempty"`
	// Filer is a seaweedfs filer's gRPC address, host:port. With it the
	// server follows the bucket's changes as they happen; without it the
	// bucket is mirrored in full every sync round.
	Filer string `json:"filer,omitempty"`
	// Ref points to credentials in the secret store, never the secret itself.
	Ref string `json:"ref"`
}

// Validate reports whether the endpoint has the fields its kind needs.
func (e Endpoint) Validate() error {
	switch e.Kind {
	case KindS3:
		if e.Region == "" {
			return errors.New("region is required for s3")
		}
	case KindGCS:
	case KindSeaweedFS:
		if e.URL == "" {
			return errors.New("url is required for seaweedfs")
		}
	default:
		return fmt.Errorf("unknown kind %q", e.Kind)
	}
	if e.Filer != "" && e.Kind != KindSeaweedFS {
		return errors.New("filer is only for seaweedfs")
	}
	if e.Bucket == "" {
		return errors.New("bucket is required")
	}
	if e.Ref == "" {
		return errors.New("ref is required")
	}
	return nil
}

// Phase is the step a migration is in. The values are shared with the DB and
// cache migration servers.
type Phase string

const (
	PhaseCreated Phase = "CREATED"
	// PhasePrepared: the change feed is set up; the copy waits for
	// START_SYNC. Only plans created through the common API stop here.
	PhasePrepared    Phase = "PREPARED"
	PhaseInitialSync Phase = "INITIAL_SYNC"
	PhaseSyncing     Phase = "SYNCING"
	PhaseReady       Phase = "READY"
	PhaseFrozen      Phase = "FROZEN"
	PhaseSwitched    Phase = "SWITCHED"
	PhaseResumed     Phase = "RESUMED"
	PhaseVerifying   Phase = "VERIFYING"
	PhaseCompleted   Phase = "COMPLETED"
	PhaseRollingBack Phase = "ROLLING_BACK"
	PhaseRolledBack  Phase = "ROLLED_BACK"
	PhaseFailed      Phase = "FAILED"
	PhaseAborted     Phase = "ABORTED"
)

// Terminal reports whether no further transition is possible.
func (p Phase) Terminal() bool {
	switch p {
	case PhaseCompleted, PhaseRolledBack, PhaseFailed, PhaseAborted:
		return true
	}
	return false
}

// next lists the phases the worker may move to from each phase.
var next = map[Phase][]Phase{
	PhaseCreated:     {PhasePrepared, PhaseInitialSync, PhaseAborted, PhaseFailed},
	PhasePrepared:    {PhaseInitialSync, PhaseAborted, PhaseFailed},
	PhaseInitialSync: {PhaseSyncing, PhaseAborted, PhaseFailed},
	PhaseSyncing:     {PhaseReady, PhaseAborted, PhaseFailed},
	PhaseReady:       {PhaseSyncing, PhaseFrozen, PhaseAborted, PhaseFailed},
	PhaseFrozen:      {PhaseSwitched, PhaseRollingBack, PhaseAborted, PhaseFailed},
	PhaseSwitched:    {PhaseResumed, PhaseRollingBack, PhaseFailed},
	PhaseResumed:     {PhaseVerifying, PhaseRollingBack, PhaseFailed},
	PhaseVerifying:   {PhaseCompleted, PhaseRollingBack, PhaseFailed},
	PhaseRollingBack: {PhaseRolledBack, PhaseFailed},
}

// State tells whether the current phase is still doing work.
type State string

const (
	StateRunning   State = "running"
	StateWaiting   State = "waiting"
	StateSucceeded State = "succeeded"
	StateFailed    State = "failed"
	// StateUnknown means the outcome of the last action could not be
	// confirmed. The orchestrator must not proceed automatically.
	StateUnknown State = "unknown"
)

// Command is an action the orchestrator asks for.
type Command string

const (
	CommandFreeze   Command = "freeze"
	CommandSwitch   Command = "switch"
	CommandResume   Command = "resume"
	CommandRollback Command = "rollback"
	CommandFinalize Command = "finalize"
	CommandAbort    Command = "abort"
	// CommandPrepare checks access and sets up the change feed of a plan.
	CommandPrepare Command = "prepare"
	// CommandStartSync starts the initial copy of a prepared plan.
	CommandStartSync Command = "start_sync"
	// CommandReconcile and the commands below are recorded as operations
	// but never pending: they finish when accepted.
	CommandReconcile Command = "reconcile"
)

// allowedIn lists the phases in which each command is accepted.
var allowedIn = map[Command][]Phase{
	CommandFreeze:    {PhaseReady},
	CommandSwitch:    {PhaseFrozen},
	CommandResume:    {PhaseSwitched},
	CommandRollback:  {PhaseFrozen, PhaseSwitched, PhaseResumed, PhaseVerifying},
	CommandFinalize:  {PhaseVerifying},
	CommandAbort:     {PhaseCreated, PhasePrepared, PhaseInitialSync, PhaseSyncing, PhaseReady, PhaseFrozen},
	CommandPrepare:   {PhaseCreated},
	CommandStartSync: {PhasePrepared},
}

// commandOrder fixes the order of allowedActions in responses.
var commandOrder = []Command{
	CommandPrepare, CommandStartSync,
	CommandFreeze, CommandSwitch, CommandResume, CommandRollback, CommandFinalize, CommandAbort,
}

// CommandRequest is a command waiting for the worker.
type CommandRequest struct {
	Command        Command `json:"command"`
	IdempotencyKey string  `json:"idempotencyKey,omitempty"`
	CutoverID      string  `json:"cutoverId,omitempty"`
	DeadlineMS     int64   `json:"deadlineMs,omitempty"`
	// ExpectedVersion, when set, must match the migration's version. It stops
	// a client from acting on a state it has not seen.
	ExpectedVersion int64 `json:"expectedVersion,omitempty"`
	// ExpectedRevision, when set, must match the migration's revision. The
	// revision moves less often than the version, so an orchestrator can
	// act on a status it read a while ago.
	ExpectedRevision int64 `json:"expectedRevision,omitempty"`
	// QuiesceRef is the orchestrator's record that app writes were paused.
	// Object storage does not need a pause; it is kept on the operation
	// only so the orchestrator can trace it.
	QuiesceRef  string    `json:"quiesceRef,omitempty"`
	RequestedAt time.Time `json:"requestedAt"`
}

// OperationStatus is how far one accepted command got.
type OperationStatus string

const (
	OperationQueued    OperationStatus = "QUEUED"
	OperationRunning   OperationStatus = "RUNNING"
	OperationSucceeded OperationStatus = "SUCCEEDED"
	OperationFailed    OperationStatus = "FAILED"
	// OperationUnknown means the command may or may not have taken effect.
	OperationUnknown OperationStatus = "UNKNOWN"
)

// Operation is one accepted command and its outcome.
type Operation struct {
	ID           string          `json:"operationId"`
	Command      Command         `json:"command"`
	TransitionID string          `json:"transitionId,omitempty"`
	QuiesceRef   string          `json:"quiesceRef,omitempty"`
	Status       OperationStatus `json:"status"`
	Message      string          `json:"message,omitempty"`
	// Changes lists what the operation is known to have changed.
	Changes     []string   `json:"confirmedChanges,omitempty"`
	RequestedAt time.Time  `json:"requestedAt"`
	FinishedAt  *time.Time `json:"finishedAt,omitempty"`
}

// doneIn is the phase in which each command has finished.
var doneIn = map[Command]Phase{
	CommandFreeze:    PhaseFrozen,
	CommandSwitch:    PhaseSwitched,
	CommandResume:    PhaseResumed,
	CommandRollback:  PhaseRolledBack,
	CommandFinalize:  PhaseCompleted,
	CommandAbort:     PhaseAborted,
	CommandPrepare:   PhasePrepared,
	CommandStartSync: PhaseInitialSync,
}

type Progress struct {
	Completed int64  `json:"completed"`
	Total     int64  `json:"total"`
	Unit      string `json:"unit"`
	// Bytes and TotalBytes give the same progress by size, when known. A
	// few large objects can keep Completed still for a long time.
	Bytes      int64 `json:"bytes,omitempty"`
	TotalBytes int64 `json:"totalBytes,omitempty"`
}

// Verification describes the comparison actually performed, not a claim of full content hashing.
type Verification struct {
	Method    string `json:"method"`
	Scope     string `json:"scope"`
	Matched   int64  `json:"matched"`
	Missing   int64  `json:"missing"`
	Different int64  `json:"different"`
	Extra     int64  `json:"extra"`
}

type Check struct {
	Verification *Verification `json:"verification,omitempty"`
	Name         string        `json:"name"`
	OK           bool          `json:"ok"`
	Message      string        `json:"message,omitempty"`
}

// Result is the outcome of a decision.
type Result string

const (
	ResultPass    Result = "PASS"
	ResultFail    Result = "FAIL"
	ResultUnknown Result = "UNKNOWN"
)

// Decision is a judgment backed by the checks that led to it.
type Decision struct {
	Result Result  `json:"result"`
	Checks []Check `json:"checks"`
}

// Decide passes when there is at least one check and all of them pass. No
// checks means nothing was observed, which is UNKNOWN, not PASS.
func Decide(checks ...Check) Decision {
	d := Decision{Result: ResultPass, Checks: checks}
	if len(checks) == 0 {
		d.Result, d.Checks = ResultUnknown, []Check{}
	}
	for _, c := range checks {
		if !c.OK {
			d.Result = ResultFail
		}
	}
	return d
}

// Readiness holds the judgments that gate each step. They are kept apart so
// a failed check after cutover is not read as "not ready to cut over".
type Readiness struct {
	// Cutover: can freeze start? Judged while syncing.
	Cutover Decision `json:"cutover"`
	// Advance: after resume, does the target match the source so the
	// orchestrator can finalize and move to the next resource?
	Advance Decision `json:"advance"`
	// Rollback: can the migration go back to the source now?
	Rollback Decision `json:"rollback"`
}

// FlowState is what one direction of replication is doing.
type FlowState string

const (
	FlowNotStarted  FlowState = "NOT_STARTED"
	FlowInitialCopy FlowState = "INITIAL_COPY"
	// FlowStreaming means changes are followed as they happen (CDC).
	FlowStreaming FlowState = "STREAMING"
	// FlowDraining means new changes stopped coming and the rest is applied.
	FlowDraining FlowState = "DRAINING"
	FlowStopped  FlowState = "STOPPED"
	FlowOff      FlowState = "OFF"
)

// Flow is one direction of replication.
type Flow struct {
	State FlowState `json:"state"`
	// Backlog is changes seen but not applied yet.
	Backlog    int64   `json:"backlog"`
	LagSeconds float64 `json:"lagSeconds"`
}

// Replication shows how data is moving right now: source to target
// (forward) and, after the switch, target back to source (reverse).
type Replication struct {
	Forward     Flow     `json:"forward"`
	Reverse     Flow     `json:"reverse"`
	InitialCopy Progress `json:"initialCopy"`
}

type Error struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

// Migration is one source-to-target object migration.
type Migration struct {
	ID          string      `json:"id"`
	Type        string      `json:"type"`
	Phase       Phase       `json:"phase"`
	State       State       `json:"state"`
	Detail      string      `json:"detail,omitempty"`
	Source      Endpoint    `json:"source"`
	Target      Endpoint    `json:"target"`
	Replication Replication `json:"replication"`
	Readiness   Readiness   `json:"readiness"`
	// Routing is where apps read and write the migrated objects.
	Routing   Route           `json:"routing"`
	Pending   *CommandRequest `json:"pendingCommand,omitempty"`
	LastError *Error          `json:"lastError"`
	// Version increases on every write so concurrent writers can detect
	// conflicts.
	Version int64 `json:"version"`
	// Revision increases only when the phase changes or a command is
	// accepted, not on progress updates.
	Revision int64 `json:"revision"`
	// Operations lists every accepted command, oldest first.
	Operations []Operation `json:"operations,omitempty"`
	// ActiveOperation is the operation still running, if any.
	ActiveOperation string `json:"activeOperationId,omitempty"`
	// Plan is set when the migration was created through the common API.
	// Such a migration waits for PREPARE and START_SYNC instead of starting
	// at once.
	Plan *Plan `json:"plan,omitempty"`
	// TransitionID is the one cutover attempt this migration allows; the
	// first QUIESCE sets it.
	TransitionID string `json:"transitionId,omitempty"`
	// TransitionClosed is set when the attempt is over: finalized,
	// cancelled after QUIESCE, or resumed on the source.
	TransitionClosed bool `json:"transitionClosed,omitempty"`
	// TargetWritesPossible turns true when a switch is accepted and never
	// back: from then on apps may have written to the target.
	TargetWritesPossible bool `json:"targetWritesPossible,omitempty"`
	// RolledBackFrom is the phase a rollback started in.
	RolledBackFrom Phase `json:"rolledBackFrom,omitempty"`
	// Reconciled means RECONCILE looked at the current unknown outcome, so
	// commands other than RECONCILE are accepted again.
	Reconciled bool `json:"reconciled,omitempty"`
	// SourceResumed: RESUME SOURCE confirmed apps are back on the source
	// after a rollback. Finalized: FINALIZE closed a restored plan.
	SourceResumed         bool       `json:"sourceResumed,omitempty"`
	Finalized             bool       `json:"finalized,omitempty"`
	InitialCopyObservedAt *time.Time `json:"initialCopyObservedAt,omitempty"`
	ForwardObservedAt     *time.Time `json:"forwardObservedAt,omitempty"`
	ReverseObservedAt     *time.Time `json:"reverseObservedAt,omitempty"`
	LastSuccessfulSyncAt  *time.Time `json:"lastSuccessfulSyncAt,omitempty"`
	LastVerificationAt    *time.Time `json:"lastVerificationAt,omitempty"`
	LastProgressAt        *time.Time `json:"lastProgressAt,omitempty"`
	CreatedAt             time.Time  `json:"createdAt"`
	UpdatedAt             time.Time  `json:"updatedAt"`
}

var (
	// ErrInvalidPhase means the command or transition is not allowed in the
	// current phase.
	ErrInvalidPhase = errors.New("invalid phase")
	// ErrCommandPending means a different command is still waiting for the
	// worker.
	ErrCommandPending = errors.New("another command is pending")
	// ErrPrecondition means the phase allows the command but the migration
	// is not in a state to run it yet.
	ErrPrecondition = errors.New("precondition failed")
	// ErrVersionMismatch means the request's expected version is stale.
	ErrVersionMismatch = errors.New("version mismatch")
)

// New returns a migration in CREATED with a fresh ID.
func New(source, target Endpoint, now time.Time) (Migration, error) {
	if err := source.Validate(); err != nil {
		return Migration{}, fmt.Errorf("source: %w", err)
	}
	if err := target.Validate(); err != nil {
		return Migration{}, fmt.Errorf("target: %w", err)
	}
	if source.Kind == target.Kind && source.Bucket == target.Bucket && source.URL == target.URL {
		return Migration{}, errors.New("source and target are the same bucket")
	}
	return Migration{
		ID:       newID(),
		Type:     Type,
		Phase:    PhaseCreated,
		Revision: 1,
		State:    StateWaiting,
		Source:   source,
		Target:   target,
		Replication: Replication{
			Forward: Flow{State: FlowNotStarted},
			Reverse: Flow{State: FlowOff},
		},
		Readiness: Readiness{
			Cutover: Decide(),
			Advance: Decide(),
			// Nothing has moved yet, so going back is always possible.
			Rollback: Decide(Check{Name: "source_unchanged", OK: true}),
		},
		Routing:   Route{Generation: 1, Write: SideSource, Read: []Side{SideSource}, Delete: slices.Clone(both)},
		CreatedAt: now,
		UpdatedAt: now,
	}, nil
}

// RequestCommand records req as pending. Repeating the pending command with
// the same idempotency key is a no-op.
func (m *Migration) RequestCommand(req CommandRequest) error {
	replacing := false
	if m.Pending != nil {
		if m.Pending.Command == req.Command && m.Pending.IdempotencyKey == req.IdempotencyKey {
			return nil
		}
		// A switch stuck waiting for apps that never confirm would otherwise
		// leave no way out. A stuck finalize is not replaced: it may already
		// have stopped apps reading the source, and going back from there
		// would lose deletes made since.
		stuck := m.State == StateUnknown && m.Pending.Command == CommandSwitch
		if !stuck || req.Command != CommandRollback {
			return fmt.Errorf("%w: %s", ErrCommandPending, m.Pending.Command)
		}
		replacing = true
	}
	if req.ExpectedVersion != 0 && req.ExpectedVersion != m.Version {
		return fmt.Errorf("%w: expected %d, current %d", ErrVersionMismatch, req.ExpectedVersion, m.Version)
	}
	if req.ExpectedRevision != 0 && req.ExpectedRevision != m.Revision {
		return fmt.Errorf("%w: expected revision %d, current %d", ErrVersionMismatch, req.ExpectedRevision, m.Revision)
	}
	if (req.Command == CommandPrepare || req.Command == CommandStartSync) && m.Plan == nil {
		return fmt.Errorf("%w: %s is only for plans created through the common API", ErrInvalidPhase, req.Command)
	}
	phases, ok := allowedIn[req.Command]
	if !ok {
		return fmt.Errorf("unknown command %q", req.Command)
	}
	if !slices.Contains(phases, m.Phase) {
		return fmt.Errorf("%w: cannot %s in %s", ErrInvalidPhase, req.Command, m.Phase)
	}
	if req.Command == CommandRollback && m.Readiness.Rollback.Result != ResultPass {
		return fmt.Errorf("%w: rollback readiness is %s", ErrPrecondition, m.Readiness.Rollback.Result)
	}
	if req.Command == CommandFinalize && m.Readiness.Advance.Result != ResultPass {
		return fmt.Errorf("%w: verification is %s", ErrPrecondition, m.Readiness.Advance.Result)
	}
	if replacing {
		m.finishOperation(OperationFailed, "replaced by rollback", req.RequestedAt)
	}
	r := req
	m.Pending = &r
	m.Revision++
	switch req.Command {
	case CommandFreeze:
		if m.TransitionID == "" {
			m.TransitionID = req.CutoverID
		}
	case CommandSwitch:
		m.TargetWritesPossible = true
	case CommandRollback:
		m.RolledBackFrom = m.Phase
	}
	op := Operation{
		ID:           fmt.Sprintf("%s-op%d", m.ID, len(m.Operations)+1),
		Command:      req.Command,
		TransitionID: req.CutoverID,
		QuiesceRef:   req.QuiesceRef,
		Status:       OperationQueued,
		RequestedAt:  req.RequestedAt,
	}
	m.Operations = append(m.Operations, op)
	m.ActiveOperation = op.ID
	return nil
}

// finishOperation records the outcome of the active operation.
func (m *Migration) finishOperation(status OperationStatus, msg string, at time.Time) {
	for i := range m.Operations {
		if m.Operations[i].ID == m.ActiveOperation {
			t := at
			m.Operations[i].Status, m.Operations[i].Message, m.Operations[i].FinishedAt = status, msg, &t
			m.Operations[i].Changes = append(m.Operations[i].Changes, m.Routing.String())
		}
	}
	m.ActiveOperation = ""
}

// Operation returns the operation with the given ID.
func (m Migration) Operation(id string) (Operation, bool) {
	for _, op := range m.Operations {
		if op.ID == id {
			return op, true
		}
	}
	return Operation{}, false
}

// OperationMigrationID returns the migration an operation ID belongs to.
func OperationMigrationID(opID string) (string, bool) {
	i := strings.LastIndex(opID, "-op")
	if i <= 0 {
		return "", false
	}
	return opID[:i], true
}

// AllowedCommands lists the commands RequestCommand would accept right now.
func (m Migration) AllowedCommands() []Command {
	out := []Command{}
	if m.Pending != nil {
		return out
	}
	for _, c := range commandOrder {
		probe := m.Clone()
		if probe.RequestCommand(CommandRequest{Command: c}) == nil {
			out = append(out, c)
		}
	}
	return out
}

// TransitionTo moves the migration to phase to if the move is allowed.
func (m *Migration) TransitionTo(to Phase, now time.Time) error {
	if !slices.Contains(next[m.Phase], to) {
		return fmt.Errorf("%w: %s to %s", ErrInvalidPhase, m.Phase, to)
	}
	m.Phase = to
	m.Detail = ""
	m.UpdatedAt = now
	m.Revision++
	if m.ActiveOperation != "" {
		if op, ok := m.Operation(m.ActiveOperation); ok {
			switch {
			case doneIn[op.Command] == to:
				m.finishOperation(OperationSucceeded, "", now)
			case to == PhaseFailed:
				msg := ""
				if m.LastError != nil {
					msg = m.LastError.Message
				}
				m.finishOperation(OperationFailed, msg, now)
			}
		}
	}
	if to.Terminal() {
		m.Pending = nil
	}
	return nil
}

// Clone returns a deep copy, so callers can change it without touching the
// original.
func (m Migration) Clone() Migration {
	c := m
	c.Readiness.Cutover.Checks = slices.Clone(m.Readiness.Cutover.Checks)
	c.Readiness.Advance.Checks = slices.Clone(m.Readiness.Advance.Checks)
	c.Readiness.Rollback.Checks = slices.Clone(m.Readiness.Rollback.Checks)
	c.Routing.Read = slices.Clone(m.Routing.Read)
	c.Routing.Delete = slices.Clone(m.Routing.Delete)
	c.Operations = slices.Clone(m.Operations)
	if m.Pending != nil {
		p := *m.Pending
		c.Pending = &p
	}
	if m.LastError != nil {
		e := *m.LastError
		c.LastError = &e
	}
	return c
}

func newID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return "om-" + hex.EncodeToString(b)
}

// Side names one bucket of a migration.
type Side string

const (
	SideSource Side = "source"
	SideTarget Side = "target"
)

// Route tells apps where to write, in which order to read, and where to
// delete. An object missing from the first read side is looked up in the
// next. Generation grows with every change, so apps can say which route
// they use.
type Route struct {
	Generation int64  `json:"generation"`
	Write      Side   `json:"write"`
	Read       []Side `json:"read"`
	// Delete lists every side that may hold a copy, in the order to delete
	// from. While both do, a delete goes to both, or a read falling back to
	// the other side would find the object again. The target comes first:
	// the reverse copy reads the target, and checks after each copy that
	// the object is still there.
	Delete []Side `json:"delete"`
}

// The routes a migration moves through. Each step changes one thing and
// waits for every app, so two apps are never more than one step apart:
//
//   - Writes move only once every app reads both sides, so an object
//     written by any app is readable by every app.
//   - Deletes stop going to a side only once no app reads it any more, so
//     a deleted object cannot be found on the other side.
var (
	both = []Side{SideTarget, SideSource}

	RouteSource = Route{Write: SideSource, Read: []Side{SideSource}, Delete: both}
	RouteBoth   = Route{Write: SideSource, Read: []Side{SideSource, SideTarget}, Delete: both}
	RouteTarget = Route{Write: SideTarget, Read: []Side{SideTarget, SideSource}, Delete: both}
	// RouteTargetAlone reads only the target but still deletes from both,
	// for apps still reading the source.
	RouteTargetAlone = Route{Write: SideTarget, Read: []Side{SideTarget}, Delete: both}
	RouteTargetOnly  = Route{Write: SideTarget, Read: []Side{SideTarget}, Delete: []Side{SideTarget}}
	// RouteSourceOnly is where an aborted or rolled back migration ends:
	// the target is no longer the app's business.
	RouteSourceOnly = Route{Write: SideSource, Read: []Side{SideSource}, Delete: []Side{SideSource}}
)

// SetRoute moves the migration to r unless it is there already, and
// returns the generation apps must apply. Setting the same route again
// keeps the generation, so a retried step waits for the same apps.
func (m *Migration) SetRoute(r Route) int64 {
	if m.Routing.Write == r.Write && slices.Equal(m.Routing.Read, r.Read) && slices.Equal(m.Routing.Delete, r.Delete) {
		return m.Routing.Generation
	}
	m.Routing = Route{
		Generation: m.Routing.Generation + 1,
		Write:      r.Write,
		Read:       slices.Clone(r.Read),
		Delete:     slices.Clone(r.Delete),
	}
	return m.Routing.Generation
}

// Endpoint returns the endpoint on a side.
func (m Migration) Endpoint(s Side) Endpoint {
	if s == SideTarget {
		return m.Target
	}
	return m.Source
}

// Policy is the common API's rollback policy for a plan.
type Policy struct {
	RollbackMode      string `json:"rollbackMode"`
	MaxQuiesceSeconds int    `json:"maxQuiesceSeconds"`
}

// Plan is what a common-API create request registered.
type Plan struct {
	WorkflowID       string `json:"workflowId"`
	ResourceID       string `json:"resourceId"`
	Profile          string `json:"profile"`
	SourceRef        string `json:"sourceRef"`
	TargetRef        string `json:"targetRef"`
	WorkloadScopeRef string `json:"workloadScopeRef"`
	BindingRef       string `json:"bindingRef"`
	Policy           Policy `json:"policy"`
}

// RecordDone records a command that finished as soon as it was accepted:
// it changes nothing in the buckets or routes, so it needs no worker.
func (m *Migration) RecordDone(cmd Command, transitionID string, status OperationStatus, msg string, changes []string, now time.Time) Operation {
	t := now
	op := Operation{
		ID:           fmt.Sprintf("%s-op%d", m.ID, len(m.Operations)+1),
		Command:      cmd,
		TransitionID: transitionID,
		Status:       status,
		Message:      msg,
		Changes:      changes,
		RequestedAt:  now,
		FinishedAt:   &t,
	}
	m.Operations = append(m.Operations, op)
	m.Revision++
	m.UpdatedAt = now
	return op
}

// FailActive ends the active operation as failed without moving the
// migration, for a command whose check failed before it changed anything.
func (m *Migration) FailActive(msg string, now time.Time) {
	m.Pending = nil
	m.finishOperation(OperationFailed, msg, now)
	m.Revision++
	m.UpdatedAt = now
}

// String describes the route for operation records.
func (r Route) String() string {
	read := make([]string, len(r.Read))
	for i, s := range r.Read {
		read[i] = string(s)
	}
	return fmt.Sprintf("route generation %d: write %s, read %s", r.Generation, r.Write, strings.Join(read, "+"))
}
