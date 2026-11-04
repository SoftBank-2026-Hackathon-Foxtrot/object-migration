// Package creds resolves an endpoint's credentials reference. A reference
// says where the server finds credentials; requests never carry secrets.
package creds

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"google.golang.org/api/option"
)

// ErrBadRef means a reference cannot be resolved. Retrying does not help
// until the server's environment changes.
var ErrBadRef = errors.New("bad credentials ref")

// Default uses the platform's default credentials: the AWS SDK chain for s3
// and seaweedfs, Application Default Credentials for gcs.
const Default = "default"

var envName = regexp.MustCompile(`^[A-Z][A-Z0-9_]*$`)

// AWSKeys are static S3 credentials.
type AWSKeys struct {
	AccessKeyID, SecretAccessKey, SessionToken string
}

// AWS resolves a reference for s3 or seaweedfs. "default" returns nil keys,
// meaning the SDK default chain. "env:NAME" reads NAME_ACCESS_KEY_ID,
// NAME_SECRET_ACCESS_KEY and optionally NAME_SESSION_TOKEN.
func AWS(ref string) (*AWSKeys, error) {
	if ref == Default {
		return nil, nil
	}
	name, err := envRef(ref)
	if err != nil {
		return nil, err
	}
	k := AWSKeys{
		AccessKeyID:     os.Getenv(name + "_ACCESS_KEY_ID"),
		SecretAccessKey: os.Getenv(name + "_SECRET_ACCESS_KEY"),
		SessionToken:    os.Getenv(name + "_SESSION_TOKEN"),
	}
	if k.AccessKeyID == "" || k.SecretAccessKey == "" {
		return nil, fmt.Errorf("%w: %s_ACCESS_KEY_ID and %s_SECRET_ACCESS_KEY must be set", ErrBadRef, name, name)
	}
	return &k, nil
}

// AWSConfig returns an SDK config for a reference in a region.
func AWSConfig(ctx context.Context, ref, region string) (aws.Config, error) {
	keys, err := AWS(ref)
	if err != nil {
		return aws.Config{}, err
	}
	opts := []func(*config.LoadOptions) error{config.WithRegion(region)}
	if keys != nil {
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(keys.AccessKeyID, keys.SecretAccessKey, keys.SessionToken)))
	}
	return config.LoadDefaultConfig(ctx, opts...)
}

// GCSOptions returns client options for Google APIs from a gcs reference:
// a service account key file, or Application Default Credentials.
func GCSOptions(ref string) ([]option.ClientOption, error) {
	file, err := GCSFile(ref)
	if err != nil {
		return nil, err
	}
	if file == "" {
		return nil, nil
	}
	return []option.ClientOption{option.WithCredentialsFile(file)}, nil
}

// GCSFile resolves a reference for gcs. "default" returns "", meaning
// Application Default Credentials. "env:NAME" reads NAME_CREDENTIALS_FILE,
// the path of a service account key file.
func GCSFile(ref string) (string, error) {
	if ref == Default {
		return "", nil
	}
	name, err := envRef(ref)
	if err != nil {
		return "", err
	}
	file := os.Getenv(name + "_CREDENTIALS_FILE")
	if file == "" {
		return "", fmt.Errorf("%w: %s_CREDENTIALS_FILE must be set", ErrBadRef, name)
	}
	return file, nil
}

func envRef(ref string) (string, error) {
	name, ok := strings.CutPrefix(ref, "env:")
	if !ok {
		return "", fmt.Errorf("%w: %q: want %q or env:NAME", ErrBadRef, ref, Default)
	}
	if !envName.MatchString(name) {
		return "", fmt.Errorf("%w: %q: NAME must be upper case letters, digits and underscores", ErrBadRef, ref)
	}
	return name, nil
}
