package copier

import (
	"errors"
	"testing"

	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/creds"
	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/migration"
)

func TestBackendConfigS3FromEnv(t *testing.T) {
	t.Setenv("OMT_ACCESS_KEY_ID", "id")
	t.Setenv("OMT_SECRET_ACCESS_KEY", "secret")
	t.Setenv("OMT_SESSION_TOKEN", "token")
	backend, conf, err := backendConfig(migration.Endpoint{Kind: migration.KindS3, Bucket: "b", Region: "ap-northeast-1", Ref: "env:OMT"})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"provider": "AWS", "region": "ap-northeast-1", "no_check_bucket": "true",
		"access_key_id": "id", "secret_access_key": "secret", "session_token": "token",
	}
	if backend != "s3" {
		t.Fatalf("backend = %q", backend)
	}
	for k, v := range want {
		if conf[k] != v {
			t.Errorf("%s = %q, want %q", k, conf[k], v)
		}
	}
	if _, ok := conf["env_auth"]; ok {
		t.Error("env_auth set for an env: ref")
	}
}

func TestBackendConfigDefaultRef(t *testing.T) {
	for _, e := range []migration.Endpoint{
		{Kind: migration.KindS3, Bucket: "b", Region: "ap-northeast-1", Ref: creds.Default},
		{Kind: migration.KindGCS, Bucket: "b", Ref: creds.Default},
	} {
		_, conf, err := backendConfig(e)
		if err != nil || conf["env_auth"] != "true" {
			t.Errorf("%s: env_auth = %q, err %v", e.Kind, conf["env_auth"], err)
		}
	}
}

func TestBackendConfigGCSAndSeaweedFS(t *testing.T) {
	t.Setenv("OMG_CREDENTIALS_FILE", "/keys/sa.json")
	backend, conf, err := backendConfig(migration.Endpoint{Kind: migration.KindGCS, Bucket: "b", Ref: "env:OMG"})
	if err != nil || backend != "google cloud storage" || conf["service_account_file"] != "/keys/sa.json" || conf["bucket_policy_only"] != "true" {
		t.Fatalf("gcs: %q %v %v", backend, conf, err)
	}

	backend, conf, err = backendConfig(migration.Endpoint{Kind: migration.KindSeaweedFS, Bucket: "b", URL: "http://10.0.0.5:8333", Ref: creds.Default})
	if err != nil || backend != "s3" || conf["provider"] != "SeaweedFS" || conf["endpoint"] != "http://10.0.0.5:8333" || conf["force_path_style"] != "true" {
		t.Fatalf("seaweedfs: %q %v %v", backend, conf, err)
	}
}

func TestBackendConfigBadRefs(t *testing.T) {
	tests := []migration.Endpoint{
		{Kind: migration.KindS3, Bucket: "b", Region: "r", Ref: "env:OM_NOT_SET"},
		{Kind: migration.KindGCS, Bucket: "b", Ref: "env:OM_NOT_SET"},
		{Kind: migration.KindS3, Bucket: "b", Region: "r", Ref: "secret://aws"},
		{Kind: migration.KindS3, Bucket: "b", Region: "r", Ref: "env:lower"},
	}
	for _, e := range tests {
		if _, _, err := backendConfig(e); !errors.Is(err, creds.ErrBadRef) {
			t.Errorf("%s %q: err = %v, want creds.ErrBadRef", e.Kind, e.Ref, err)
		}
	}
}
