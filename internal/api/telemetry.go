package api

import (
	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/migration"
	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/routing"
	"time"
)

// Observation deliberately does not embed Migration: endpoints, keys, plans and raw errors
// are not telemetry. The typed status fields below contain only migration decisions/counters.
func Observation(m migration.Migration, apps *routing.Apps) map[string]any {
	h := &handler{Deps: Deps{Now: time.Now, Apps: apps}}
	v := h.view(m)
	// Worker checks may contain provider diagnostics; only names, booleans and verification counts leave.
	d := v.Details["object"]
	clean := func(decision migration.Decision) migration.Decision {
		checks := make([]migration.Check, 0, len(decision.Checks))
		for _, c := range decision.Checks {
			checks = append(checks, migration.Check{Name: c.Name, OK: c.OK, Verification: c.Verification})
		}
		decision.Checks = checks
		return decision
	}
	d.Readiness.Cutover = clean(d.Readiness.Cutover)
	d.Readiness.Rollback = clean(d.Readiness.Rollback)
	d.Readiness.Advance = clean(d.Readiness.Advance)
	v.Details = map[string]objectDetail{"object": d}
	return map[string]any{
		"workflowId": v.WorkflowID, "migrationId": v.MigrationID, "resourceId": v.ResourceID, "kind": v.Kind,
		"lifecycle": v.Lifecycle, "health": v.Health, "phase": m.Phase, "revision": m.Revision,
		"activeOperationId": v.ActiveOperationID, "transitionId": v.TransitionID, "topologyRevision": v.TopologyRevision,
		"observed": v.Observed, "progress": v.Progress, "readiness": v.Readiness, "rollback": v.Rollback,
		"checks": v.Checks, "details": v.Details, "replication": m.Replication, "routing": m.Routing,
	}
}
