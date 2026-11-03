// Package copier moves objects between buckets with rclone, used as a
// library. It knows how to reach each kind of storage and how to copy, compare
// and count; when to do which is up to the engine.
package copier

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"

	_ "github.com/rclone/rclone/backend/googlecloudstorage" // registers "google cloud storage"
	_ "github.com/rclone/rclone/backend/s3"                 // registers "s3"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"

	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/migration"
)

// ErrBadRef means an endpoint's credentials reference cannot be resolved.
// Retrying does not help until the server's environment changes.
var ErrBadRef = errors.New("bad credentials ref")

// RefDefault uses the platform's default credentials: the AWS SDK chain for
// s3 and seaweedfs, Application Default Credentials for gcs.
const RefDefault = "default"

var envName = regexp.MustCompile(`^[A-Z][A-Z0-9_]*$`)

// Open returns the rclone filesystem rooted at an endpoint's bucket. name
// labels it in rclone's logs. The bucket must already exist; Open never
// creates one.
func Open(ctx context.Context, name string, e migration.Endpoint) (fs.Fs, error) {
	if err := e.Validate(); err != nil {
		return nil, err
	}
	backend, conf, err := backendConfig(e)
	if err != nil {
		return nil, err
	}
	info, err := fs.Find(backend)
	if err != nil {
		return nil, err
	}
	// An empty config name keeps rclone from reading a config file or
	// per-remote environment variables.
	return info.NewFs(ctx, name, e.Bucket, fs.ConfigMap(info.Prefix, info.Options, "", conf))
}

func backendConfig(e migration.Endpoint) (string, configmap.Simple, error) {
	switch e.Kind {
	case migration.KindS3, migration.KindSeaweedFS:
		conf := configmap.Simple{"no_check_bucket": "true"}
		if e.Kind == migration.KindS3 {
			conf["provider"], conf["region"] = "AWS", e.Region
		} else {
			conf["provider"], conf["endpoint"], conf["force_path_style"] = "SeaweedFS", e.URL, "true"
		}
		if err := s3Credentials(e.Ref, conf); err != nil {
			return "", nil, err
		}
		return "s3", conf, nil
	case migration.KindGCS:
		// Objects follow the bucket's IAM policy; ACLs are not migrated.
		conf := configmap.Simple{"no_check_bucket": "true", "bucket_policy_only": "true"}
		if err := gcsCredentials(e.Ref, conf); err != nil {
			return "", nil, err
		}
		return "google cloud storage", conf, nil
	}
	return "", nil, fmt.Errorf("unknown kind %q", e.Kind)
}

// s3Credentials resolves "default" or "env:NAME", which reads
// NAME_ACCESS_KEY_ID, NAME_SECRET_ACCESS_KEY and optionally NAME_SESSION_TOKEN.
func s3Credentials(ref string, conf configmap.Simple) error {
	if ref == RefDefault {
		conf["env_auth"] = "true"
		return nil
	}
	name, err := envRef(ref)
	if err != nil {
		return err
	}
	id, secret := os.Getenv(name+"_ACCESS_KEY_ID"), os.Getenv(name+"_SECRET_ACCESS_KEY")
	if id == "" || secret == "" {
		return fmt.Errorf("%w: %s_ACCESS_KEY_ID and %s_SECRET_ACCESS_KEY must be set", ErrBadRef, name, name)
	}
	conf["access_key_id"], conf["secret_access_key"] = id, secret
	if token := os.Getenv(name + "_SESSION_TOKEN"); token != "" {
		conf["session_token"] = token
	}
	return nil
}

// gcsCredentials resolves "default" or "env:NAME", which reads
// NAME_CREDENTIALS_FILE, the path of a service account key file.
func gcsCredentials(ref string, conf configmap.Simple) error {
	if ref == RefDefault {
		conf["env_auth"] = "true"
		return nil
	}
	name, err := envRef(ref)
	if err != nil {
		return err
	}
	file := os.Getenv(name + "_CREDENTIALS_FILE")
	if file == "" {
		return fmt.Errorf("%w: %s_CREDENTIALS_FILE must be set", ErrBadRef, name)
	}
	conf["service_account_file"] = file
	return nil
}

func envRef(ref string) (string, error) {
	name, ok := strings.CutPrefix(ref, "env:")
	if !ok {
		return "", fmt.Errorf("%w: %q: want %q or env:NAME", ErrBadRef, ref, RefDefault)
	}
	if !envName.MatchString(name) {
		return "", fmt.Errorf("%w: %q: NAME must be upper case letters, digits and underscores", ErrBadRef, ref)
	}
	return name, nil
}
