package creds

import (
	"errors"
	"os"
	"testing"
)

func TestGCSFilePrefersTheFileThenTheJSON(t *testing.T) {
	t.Setenv("GCSA_CREDENTIALS_FILE", "/keys/a.json")
	t.Setenv("GCSA_CREDENTIALS_JSON", `{"type":"service_account"}`)
	if got, err := GCSFile("env:GCSA"); err != nil || got != "/keys/a.json" {
		t.Fatalf("file first: got %q, %v", got, err)
	}

	key := `{"type":"service_account","project_id":"p"}`
	t.Setenv("GCSB_CREDENTIALS_JSON", key)
	path, err := GCSFile("env:GCSB")
	if err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(path)
	if err != nil || string(body) != key {
		t.Fatalf("key file holds %q, %v", body, err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("key file mode %v, %v", info.Mode().Perm(), err)
	}
	again, err := GCSFile("env:GCSB")
	if err != nil || again != path {
		t.Fatalf("written once: %q then %q, %v", path, again, err)
	}
}

func TestGCSFileNeedsOneOfThem(t *testing.T) {
	if _, err := GCSFile("env:GCSNONE"); !errors.Is(err, ErrBadRef) {
		t.Fatalf("want ErrBadRef, got %v", err)
	}
	if got, err := GCSFile(Default); err != nil || got != "" {
		t.Fatalf("default is ADC: got %q, %v", got, err)
	}
}
