// Package copier moves objects between buckets with rclone, used as a
// library. It knows how to reach each kind of storage and how to copy, compare
// and count; when to do which is up to the engine.
package copier

import (
	"context"
	"fmt"

	_ "github.com/rclone/rclone/backend/googlecloudstorage" // registers "google cloud storage"
	_ "github.com/rclone/rclone/backend/s3"                 // registers "s3"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"

	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/creds"
	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/migration"
)

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

func s3Credentials(ref string, conf configmap.Simple) error {
	keys, err := creds.AWS(ref)
	if err != nil {
		return err
	}
	if keys == nil {
		conf["env_auth"] = "true"
		return nil
	}
	conf["access_key_id"], conf["secret_access_key"] = keys.AccessKeyID, keys.SecretAccessKey
	if keys.SessionToken != "" {
		conf["session_token"] = keys.SessionToken
	}
	return nil
}

func gcsCredentials(ref string, conf configmap.Simple) error {
	file, err := creds.GCSFile(ref)
	if err != nil {
		return err
	}
	if file == "" {
		conf["env_auth"] = "true"
	} else {
		conf["service_account_file"] = file
	}
	return nil
}
