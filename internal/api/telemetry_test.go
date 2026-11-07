package api

import (
	"encoding/json"
	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/migration"
	"strings"
	"testing"
	"time"
)

func TestObservationNeverExportsProviderMessages(t *testing.T) {
	m := migration.Migration{ID: "m", UpdatedAt: time.Now()}
	m.Readiness.Cutover = migration.Decide(migration.Check{Name: "copy", OK: false, Message: "password=secret"})
	body, err := json.Marshal(Observation(m, nil))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(body), "secret") || strings.Contains(string(body), "message") {
		t.Fatal("diagnostic leaked into telemetry")
	}
	if m.Readiness.Cutover.Checks[0].Message != "password=secret" {
		t.Fatal("projection mutated control evidence")
	}
}
