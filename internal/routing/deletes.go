package routing

import (
	"context"
	"slices"
	"sync"
	"time"
)

// Deletes records the keys apps delete while a migration runs. An app
// records a key before deleting it from the buckets, and the engine checks
// after each copy whether the key it copied was deleted meanwhile. A copy
// and a delete can then happen in any order: the copy is removed whichever
// finishes last.
type Deletes struct {
	log DeleteLog
	now func() time.Time

	mu   sync.Mutex
	keys map[string]map[string]time.Time // when each key was recorded
}

// DeleteLog keeps the recorded keys across restarts.
type DeleteLog interface {
	RecordDeletes(ctx context.Context, migration string, keys []string, at time.Time) error
	LoadDeletes(ctx context.Context) (map[string]map[string]time.Time, error)
	ForgetDeletes(ctx context.Context, migration string) error
}

func NewDeletes() *Deletes {
	return &Deletes{now: time.Now, keys: make(map[string]map[string]time.Time)}
}

// LoadDeletes returns Deletes that saves every key to log, starting with
// the keys log already holds.
func LoadDeletes(ctx context.Context, log DeleteLog) (*Deletes, error) {
	keys, err := log.LoadDeletes(ctx)
	if err != nil {
		return nil, err
	}
	d := NewDeletes()
	d.log, d.keys = log, keys
	return d, nil
}

// Add records that apps deleted keys of a migration. The keys are saved
// before Add returns, since the app deletes them once it does.
func (d *Deletes) Add(ctx context.Context, migration string, keys ...string) error {
	at := d.now()
	if d.log != nil {
		if err := d.log.RecordDeletes(ctx, migration, keys, at); err != nil {
			return err
		}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	m, ok := d.keys[migration]
	if !ok {
		m = make(map[string]time.Time)
		d.keys[migration] = m
	}
	for _, k := range keys {
		if _, seen := m[k]; !seen {
			m[k] = at
		}
	}
	return nil
}

// Has reports whether apps deleted key during the migration.
func (d *Deletes) Has(migration, key string) bool {
	if d == nil {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	_, ok := d.keys[migration][key]
	return ok
}

// Since returns the keys of a migration recorded at or after t, sorted.
// After a bulk copy that started at some time, the keys recorded since a
// little before then are the ones it may have copied back.
func (d *Deletes) Since(migration string, t time.Time) []string {
	if d == nil {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	var keys []string
	for k, at := range d.keys[migration] {
		if !at.Before(t) {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	return keys
}

// Forget drops a migration's deleted keys once it is over.
func (d *Deletes) Forget(ctx context.Context, migration string) error {
	if d == nil {
		return nil
	}
	if d.log != nil {
		if err := d.log.ForgetDeletes(ctx, migration); err != nil {
			return err
		}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.keys, migration)
	return nil
}
