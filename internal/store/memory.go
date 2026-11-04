package store

import (
	"context"
	"sort"
	"sync"

	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/migration"
)

// Memory is a Store that keeps migrations in process memory. State is lost on
// restart, so it is for tests and local runs.
type Memory struct {
	mu        sync.Mutex
	byID      map[string]migration.Migration
	responses map[string]SavedResponse
}

func NewMemory() *Memory {
	return &Memory{byID: make(map[string]migration.Migration), responses: make(map[string]SavedResponse)}
}

func (s *Memory) Create(_ context.Context, m migration.Migration) (migration.Migration, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.byID[m.ID]; ok {
		return migration.Migration{}, ErrExists
	}
	m.Version = 1
	s.byID[m.ID] = m.Clone()
	return m.Clone(), nil
}

func (s *Memory) Get(_ context.Context, id string) (migration.Migration, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.byID[id]
	if !ok {
		return migration.Migration{}, ErrNotFound
	}
	return m.Clone(), nil
}

func (s *Memory) RequestCommand(_ context.Context, id string, req migration.CommandRequest) (migration.Migration, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.byID[id]
	if !ok {
		return migration.Migration{}, ErrNotFound
	}
	m = m.Clone()
	before := m.Pending
	if err := m.RequestCommand(req); err != nil {
		return migration.Migration{}, err
	}
	// A repeated request leaves the pending command as it was; no write. A
	// new one, or one that replaced a stuck command, is saved.
	if before == nil || !samePending(*before, *m.Pending) {
		m.Version++
		s.byID[id] = m.Clone()
	}
	return m, nil
}

func (s *Memory) Update(_ context.Context, m migration.Migration) (migration.Migration, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.byID[m.ID]
	if !ok {
		return migration.Migration{}, ErrNotFound
	}
	if cur.Version != m.Version {
		return migration.Migration{}, ErrConflict
	}
	m.Version++
	s.byID[m.ID] = m.Clone()
	return m.Clone(), nil
}

func (s *Memory) ListActive(_ context.Context) ([]migration.Migration, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []migration.Migration
	for _, m := range s.byID {
		if !m.Phase.Terminal() {
			out = append(out, m.Clone())
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out, nil
}

func samePending(a, b migration.CommandRequest) bool {
	return a.Command == b.Command && a.IdempotencyKey == b.IdempotencyKey && a.RequestedAt.Equal(b.RequestedAt)
}

func (s *Memory) SavedResponse(_ context.Context, key string) (SavedResponse, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.responses[key]
	return r, ok, nil
}

func (s *Memory) SaveResponse(_ context.Context, key string, r SavedResponse) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.responses[key]; !ok {
		s.responses[key] = r
	}
	return nil
}
