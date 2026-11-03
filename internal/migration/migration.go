// Package migration defines an object migration and the rules for moving it
// between phases. It has no I/O; the API and the worker share these rules.
package migration

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
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
	// URL is the S3-compatible endpoint. Required for seaweedfs.
	URL string `json:"url,omitempty"`
	// Ref points to credentials in the secret store, never the secret itself.
	Ref string `json:"ref"`
}

// Validate reports whether the endpoint has the fields its kind needs.
func (e Endpoint) Validate() error {
	switch e.Kind {
	case KindS3, KindGCS:
	case KindSeaweedFS:
		if e.URL == "" {
			return errors.New("url is required for seaweedfs")
		}
	default:
		return fmt.Errorf("unknown kind %q", e.Kind)
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
	PhaseCreated     Phase = "CREATED"
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
	PhaseCreated:     {PhaseInitialSync, PhaseAborted, PhaseFailed},
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
)

// allowedIn lists the phases in which each command is accepted.
var allowedIn = map[Command][]Phase{
	CommandFreeze:   {PhaseReady},
	CommandSwitch:   {PhaseFrozen},
	CommandResume:   {PhaseSwitched},
	CommandRollback: {PhaseFrozen, PhaseSwitched, PhaseResumed, PhaseVerifying},
	CommandFinalize: {PhaseVerifying},
	CommandAbort:    {PhaseCreated, PhaseInitialSync, PhaseSyncing, PhaseReady, PhaseFrozen},
}

// CommandRequest is a command waiting for the worker.
type CommandRequest struct {
	Command        Command   `json:"command"`
	IdempotencyKey string    `json:"idempotencyKey,omitempty"`
	CutoverID      string    `json:"cutoverId,omitempty"`
	DeadlineMS     int64     `json:"deadlineMs,omitempty"`
	RequestedAt    time.Time `json:"requestedAt"`
}

type Progress struct {
	Done  int64  `json:"done"`
	Total int64  `json:"total"`
	Unit  string `json:"unit"`
}

// Lag is how far the target is behind the source.
type Lag struct {
	Seconds float64 `json:"seconds"`
	Items   int64   `json:"items"`
}

type Check struct {
	Name    string `json:"name"`
	OK      bool   `json:"ok"`
	Message string `json:"message,omitempty"`
}

// Readiness is the worker's judgment of whether cutover can start.
type Readiness struct {
	Ready  bool    `json:"ready"`
	Checks []Check `json:"checks"`
}

type Rollback struct {
	Allowed bool   `json:"allowed"`
	Reason  string `json:"reason,omitempty"`
}

type Error struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

// Migration is one source-to-target object migration.
type Migration struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Phase     Phase           `json:"phase"`
	State     State           `json:"state"`
	Detail    string          `json:"detail,omitempty"`
	Source    Endpoint        `json:"source"`
	Target    Endpoint        `json:"target"`
	Progress  Progress        `json:"progress"`
	Lag       Lag             `json:"lag"`
	Readiness Readiness       `json:"readiness"`
	Rollback  Rollback        `json:"rollback"`
	Pending   *CommandRequest `json:"pendingCommand,omitempty"`
	LastError *Error          `json:"lastError"`
	// Version increases on every write so concurrent writers can detect
	// conflicts.
	Version   int64     `json:"version"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

var (
	// ErrInvalidPhase means the command or transition is not allowed in the
	// current phase.
	ErrInvalidPhase = errors.New("invalid phase")
	// ErrCommandPending means a different command is still waiting for the
	// worker.
	ErrCommandPending = errors.New("another command is pending")
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
		ID:        newID(),
		Type:      Type,
		Phase:     PhaseCreated,
		State:     StateWaiting,
		Source:    source,
		Target:    target,
		Readiness: Readiness{Checks: []Check{}},
		Rollback:  Rollback{Allowed: true},
		CreatedAt: now,
		UpdatedAt: now,
	}, nil
}

// RequestCommand records req as pending. Repeating the pending command with
// the same idempotency key is a no-op.
func (m *Migration) RequestCommand(req CommandRequest) error {
	if m.Pending != nil {
		if m.Pending.Command == req.Command && m.Pending.IdempotencyKey == req.IdempotencyKey {
			return nil
		}
		return fmt.Errorf("%w: %s", ErrCommandPending, m.Pending.Command)
	}
	phases, ok := allowedIn[req.Command]
	if !ok {
		return fmt.Errorf("unknown command %q", req.Command)
	}
	if !slices.Contains(phases, m.Phase) {
		return fmt.Errorf("%w: cannot %s in %s", ErrInvalidPhase, req.Command, m.Phase)
	}
	if req.Command == CommandRollback && !m.Rollback.Allowed {
		return fmt.Errorf("%w: rollback not allowed: %s", ErrInvalidPhase, m.Rollback.Reason)
	}
	r := req
	m.Pending = &r
	return nil
}

// TransitionTo moves the migration to phase to if the move is allowed.
func (m *Migration) TransitionTo(to Phase, now time.Time) error {
	if !slices.Contains(next[m.Phase], to) {
		return fmt.Errorf("%w: %s to %s", ErrInvalidPhase, m.Phase, to)
	}
	m.Phase = to
	m.Detail = ""
	m.UpdatedAt = now
	if to.Terminal() {
		m.Pending = nil
	}
	return nil
}

// Clone returns a deep copy, so callers can change it without touching the
// original.
func (m Migration) Clone() Migration {
	c := m
	c.Readiness.Checks = slices.Clone(m.Readiness.Checks)
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
