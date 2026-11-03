package migration

import (
	"errors"
	"slices"
	"testing"
	"time"
)

var (
	t0  = time.Date(2026, 11, 3, 0, 0, 0, 0, time.UTC)
	src = Endpoint{Kind: KindS3, Bucket: "foxtrot-src", Region: "ap-northeast-1", Ref: "secret://aws-s3"}
	dst = Endpoint{Kind: KindGCS, Bucket: "foxtrot-dst", Ref: "secret://gcp-gcs"}
)

func newMigration(t *testing.T) Migration {
	t.Helper()
	m, err := New(src, dst, t0)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestNewValidatesEndpoints(t *testing.T) {
	tests := []struct {
		name     string
		src, dst Endpoint
	}{
		{"unknown kind", Endpoint{Kind: "ftp", Bucket: "b", Ref: "r"}, dst},
		{"missing bucket", Endpoint{Kind: KindS3, Region: "ap-northeast-1", Ref: "r"}, dst},
		{"missing ref", Endpoint{Kind: KindS3, Region: "ap-northeast-1", Bucket: "b"}, dst},
		{"missing s3 region", Endpoint{Kind: KindS3, Bucket: "b", Ref: "r"}, dst},
		{"seaweedfs without url", src, Endpoint{Kind: KindSeaweedFS, Bucket: "b", Ref: "r"}},
		{"same bucket", src, src},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := New(tt.src, tt.dst, t0); err == nil {
				t.Fatal("want error, got nil")
			}
		})
	}
}

func TestNewStartsCreated(t *testing.T) {
	m := newMigration(t)
	if m.Phase != PhaseCreated || m.Type != Type || m.ID == "" {
		t.Fatalf("got phase=%s type=%s id=%q", m.Phase, m.Type, m.ID)
	}
	if m.Replication.Forward.State != FlowNotStarted || m.Replication.Reverse.State != FlowOff {
		t.Fatalf("replication = %+v", m.Replication)
	}
	if m.Readiness.Cutover.Result != ResultUnknown || m.Readiness.Advance.Result != ResultUnknown || m.Readiness.Rollback.Result != ResultPass {
		t.Fatalf("readiness = %+v", m.Readiness)
	}
}

func TestHappyPath(t *testing.T) {
	m := newMigration(t)
	steps := []struct {
		cmd Command // empty when the worker moves on its own
		to  Phase
	}{
		{"", PhaseInitialSync},
		{"", PhaseSyncing},
		{"", PhaseReady},
		{CommandFreeze, PhaseFrozen},
		{CommandSwitch, PhaseSwitched},
		{CommandResume, PhaseResumed},
		{"", PhaseVerifying},
		{CommandFinalize, PhaseCompleted},
	}
	for _, s := range steps {
		if s.cmd == CommandFinalize {
			m.Readiness.Advance = Decide(Check{Name: "diff", OK: true}) // the worker sets this when verification passes
		}
		if s.cmd != "" {
			if err := m.RequestCommand(CommandRequest{Command: s.cmd}); err != nil {
				t.Fatalf("%s in %s: %v", s.cmd, m.Phase, err)
			}
			m.Pending = nil // the worker clears it after acting
		}
		if err := m.TransitionTo(s.to, t0); err != nil {
			t.Fatal(err)
		}
	}
	if !m.Phase.Terminal() {
		t.Fatalf("phase %s is not terminal", m.Phase)
	}
}

func TestTransitionRejectsSkippingPhases(t *testing.T) {
	m := newMigration(t)
	err := m.TransitionTo(PhaseSwitched, t0)
	if !errors.Is(err, ErrInvalidPhase) {
		t.Fatalf("err = %v, want ErrInvalidPhase", err)
	}
	if m.Phase != PhaseCreated {
		t.Fatalf("phase changed to %s", m.Phase)
	}
}

func TestCommandAllowedOnlyInItsPhases(t *testing.T) {
	tests := []struct {
		phase Phase
		cmd   Command
		ok    bool
	}{
		{PhaseSyncing, CommandFreeze, false},
		{PhaseReady, CommandFreeze, true},
		{PhaseReady, CommandSwitch, false},
		{PhaseFrozen, CommandSwitch, true},
		{PhaseFrozen, CommandAbort, true},
		{PhaseSwitched, CommandAbort, false},
		{PhaseResumed, CommandRollback, true},
		{PhaseCompleted, CommandRollback, false},
	}
	for _, tt := range tests {
		m := newMigration(t)
		m.Phase = tt.phase
		err := m.RequestCommand(CommandRequest{Command: tt.cmd})
		if tt.ok && err != nil {
			t.Errorf("%s in %s: unexpected error %v", tt.cmd, tt.phase, err)
		}
		if !tt.ok && !errors.Is(err, ErrInvalidPhase) {
			t.Errorf("%s in %s: err = %v, want ErrInvalidPhase", tt.cmd, tt.phase, err)
		}
	}
}

func TestRepeatedCommandIsIdempotent(t *testing.T) {
	m := newMigration(t)
	m.Phase = PhaseReady
	req := CommandRequest{Command: CommandFreeze, IdempotencyKey: "k1"}
	if err := m.RequestCommand(req); err != nil {
		t.Fatal(err)
	}
	if err := m.RequestCommand(req); err != nil {
		t.Fatalf("repeat: %v", err)
	}
	err := m.RequestCommand(CommandRequest{Command: CommandAbort, IdempotencyKey: "k2"})
	if !errors.Is(err, ErrCommandPending) {
		t.Fatalf("err = %v, want ErrCommandPending", err)
	}
}

func TestRollbackBlockedWhenNotAllowed(t *testing.T) {
	m := newMigration(t)
	m.Phase = PhaseVerifying
	m.Readiness.Rollback = Decide(Check{Name: "reverse_sync", OK: false, Message: "reverse sync stopped"})
	err := m.RequestCommand(CommandRequest{Command: CommandRollback})
	if !errors.Is(err, ErrPrecondition) {
		t.Fatalf("err = %v, want ErrPrecondition", err)
	}
}

func TestFinalizeNeedsPassedVerification(t *testing.T) {
	m := newMigration(t)
	m.Phase = PhaseVerifying
	for _, d := range []Decision{Decide(), Decide(Check{Name: "diff", OK: false})} {
		m.Readiness.Advance = d
		if err := m.RequestCommand(CommandRequest{Command: CommandFinalize}); !errors.Is(err, ErrPrecondition) {
			t.Fatalf("advance %s: err = %v, want ErrPrecondition", d.Result, err)
		}
	}
	m.Readiness.Advance = Decide(Check{Name: "diff", OK: true})
	if err := m.RequestCommand(CommandRequest{Command: CommandFinalize}); err != nil {
		t.Fatal(err)
	}
}

func TestTerminalTransitionClearsPending(t *testing.T) {
	m := newMigration(t)
	m.Phase = PhaseReady
	if err := m.RequestCommand(CommandRequest{Command: CommandAbort}); err != nil {
		t.Fatal(err)
	}
	if err := m.TransitionTo(PhaseAborted, t0); err != nil {
		t.Fatal(err)
	}
	if m.Pending != nil {
		t.Fatal("pending command left on a terminal migration")
	}
}

func TestCloneIsDeep(t *testing.T) {
	m := newMigration(t)
	m.Readiness.Cutover = Decide(Check{Name: "lag_stable"})
	m.Pending = &CommandRequest{Command: CommandFreeze}
	c := m.Clone()
	c.Readiness.Cutover.Checks[0].OK = true
	c.Pending.Command = CommandAbort
	if m.Readiness.Cutover.Checks[0].OK || m.Pending.Command != CommandFreeze {
		t.Fatal("clone shares memory with the original")
	}
}

func TestDecide(t *testing.T) {
	tests := []struct {
		name   string
		checks []Check
		want   Result
	}{
		{"no checks is unknown", nil, ResultUnknown},
		{"all ok", []Check{{Name: "a", OK: true}, {Name: "b", OK: true}}, ResultPass},
		{"one failing", []Check{{Name: "a", OK: true}, {Name: "b", OK: false}}, ResultFail},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := Decide(tt.checks...)
			if d.Result != tt.want || d.Checks == nil {
				t.Fatalf("got %s checks=%v, want %s", d.Result, d.Checks, tt.want)
			}
		})
	}
}

func TestAllowedCommands(t *testing.T) {
	tests := []struct {
		phase Phase
		setup func(*Migration)
		want  []Command
	}{
		{PhaseCreated, nil, []Command{CommandAbort}},
		{PhaseReady, nil, []Command{CommandFreeze, CommandAbort}},
		{PhaseFrozen, nil, []Command{CommandSwitch, CommandRollback, CommandAbort}},
		{PhaseVerifying, nil, []Command{CommandRollback}},
		{PhaseVerifying, func(m *Migration) { m.Readiness.Advance = Decide(Check{Name: "diff", OK: true}) }, []Command{CommandRollback, CommandFinalize}},
		{PhaseVerifying, func(m *Migration) { m.Readiness.Rollback = Decide(Check{Name: "reverse", OK: false}) }, []Command{}},
		{PhaseReady, func(m *Migration) { m.Pending = &CommandRequest{Command: CommandFreeze} }, []Command{}},
		{PhaseCompleted, nil, []Command{}},
	}
	for _, tt := range tests {
		m := newMigration(t)
		m.Phase = tt.phase
		if tt.setup != nil {
			tt.setup(&m)
		}
		got := m.AllowedCommands()
		if !slices.Equal(got, tt.want) {
			t.Errorf("%s: got %v, want %v", tt.phase, got, tt.want)
		}
	}
}

func TestExpectedVersion(t *testing.T) {
	m := newMigration(t)
	m.Phase, m.Version = PhaseReady, 7
	if err := m.RequestCommand(CommandRequest{Command: CommandFreeze, ExpectedVersion: 6}); !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("stale version: err = %v, want ErrVersionMismatch", err)
	}
	if err := m.RequestCommand(CommandRequest{Command: CommandFreeze, ExpectedVersion: 7, IdempotencyKey: "k"}); err != nil {
		t.Fatalf("current version: %v", err)
	}
	// A repeat of the accepted request is still a no-op even though the
	// version it carries is now old.
	m.Version = 8
	if err := m.RequestCommand(CommandRequest{Command: CommandFreeze, ExpectedVersion: 7, IdempotencyKey: "k"}); err != nil {
		t.Fatalf("repeat: %v", err)
	}
}
