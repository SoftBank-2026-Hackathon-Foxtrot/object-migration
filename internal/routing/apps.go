// Package routing keeps track of which route each app server has applied,
// so a switch can wait until every live app reads and writes where it
// should before taking the next step.
package routing

import (
	"context"
	"slices"
	"sync"
	"time"
)

// Apps records, per migration, the route generation each app reported
// when it last asked for its route. An app that has not asked for longer
// than the TTL is taken to be gone and is not waited for.
//
// Reports are not kept across restarts. For one TTL after it starts, Apps
// waits for apps it has not heard from yet: an app still using its route
// asks again within that time, since its lease lasts at most half of it.
type Apps struct {
	ttl     time.Duration
	now     func() time.Time
	started time.Time

	mu   sync.Mutex
	seen map[string]map[string]report
}

type report struct {
	applied int64
	at      time.Time
}

func NewApps(ttl time.Duration) *Apps {
	return &Apps{ttl: ttl, now: time.Now, started: time.Now(), seen: make(map[string]map[string]report)}
}

// TTL is how long an app may go without asking before it is not waited for.
func (a *Apps) TTL() time.Duration { return a.ttl }

// Report records that app uses generation applied of a migration's route.
func (a *Apps) Report(migration, app string, applied int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	apps, ok := a.seen[migration]
	if !ok {
		apps = make(map[string]report)
		a.seen[migration] = apps
	}
	apps[app] = report{applied: applied, at: a.now()}
}

// Unheard stands for the apps not heard from since the server started,
// which Waiting lists during the first TTL.
const Unheard = "(apps not heard from since the server started)"

// Waiting returns the live apps that have not applied generation gen yet,
// sorted by name, and Unheard during the first TTL.
func (a *Apps) Waiting(migration string, gen int64) []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	var waiting []string
	for app, r := range a.seen[migration] {
		if a.now().Sub(r.at) <= a.ttl && r.applied < gen {
			waiting = append(waiting, app)
		}
	}
	slices.Sort(waiting)
	if a.now().Sub(a.started) <= a.ttl {
		waiting = append(waiting, Unheard)
	}
	return waiting
}

// Live returns how many apps asked for the route within the TTL.
func (a *Apps) Live(migration string) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	n := 0
	for _, r := range a.seen[migration] {
		if a.now().Sub(r.at) <= a.ttl {
			n++
		}
	}
	return n
}

// Wait returns once every live app has applied generation gen, or with
// ctx's error.
func (a *Apps) Wait(ctx context.Context, migration string, gen int64) error {
	t := time.NewTicker(20 * time.Millisecond)
	defer t.Stop()
	for len(a.Waiting(migration, gen)) > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
	return nil
}

// Forget drops what is known about a migration's apps.
func (a *Apps) Forget(migration string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.seen, migration)
}

// Snapshot is one atomic view of live reports. Expected membership is configured by the controller.
type Snapshot struct {
	Generation     int64      `json:"generation"`
	Live           int        `json:"live"`
	Acked          int        `json:"acked"`
	Waiting        int        `json:"waiting"`
	Recovering     bool       `json:"recovering"`
	OldestReportAt *time.Time `json:"oldestReportAt"`
	ObservedAt     time.Time  `json:"observedAt"`
}

func (a *Apps) Snapshot(migration string, generation int64) Snapshot {
	a.mu.Lock()
	defer a.mu.Unlock()
	now := a.now()
	out := Snapshot{Generation: generation, ObservedAt: now, Recovering: now.Sub(a.started) <= a.ttl}
	for _, r := range a.seen[migration] {
		if now.Sub(r.at) > a.ttl {
			continue
		}
		out.Live++
		if r.applied >= generation {
			out.Acked++
		} else {
			out.Waiting++
		}
		if out.OldestReportAt == nil || r.at.Before(*out.OldestReportAt) {
			at := r.at
			out.OldestReportAt = &at
		}
	}
	return out
}
