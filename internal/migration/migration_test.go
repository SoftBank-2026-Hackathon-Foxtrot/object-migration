package migration

import (
	"errors"
	"testing"
	"time"
)

var (
	t0  = time.Date(2026, 11, 3, 0, 0, 0, 0, time.UTC)
	src = Endpoint{Kind: KindS3, Bucket: "foxtrot-src", Ref: "secret://aws-s3"}
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
		{"missing bucket", Endpoint{Kind: KindS3, Ref: "r"}, dst},
		{"missing ref", Endpoint{Kind: KindS3, Bucket: "b"}, dst},
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
	m.Rollback = Rollback{Allowed: false, Reason: "reverse sync stopped"}
	err := m.RequestCommand(CommandRequest{Command: CommandRollback})
	if !errors.Is(err, ErrInvalidPhase) {
		t.Fatalf("err = %v, want ErrInvalidPhase", err)
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
	m.Readiness.Checks = []Check{{Name: "lag_stable"}}
	m.Pending = &CommandRequest{Command: CommandFreeze}
	c := m.Clone()
	c.Readiness.Checks[0].OK = true
	c.Pending.Command = CommandAbort
	if m.Readiness.Checks[0].OK || m.Pending.Command != CommandFreeze {
		t.Fatal("clone shares memory with the original")
	}
}
