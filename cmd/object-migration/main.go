// Command object-migration runs the object storage migration server: the
// common migration API and the worker that carries migrations out, in one
// process.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/api"
	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/copier"
	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/engine"
	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/migration"
	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/routing"
	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/store"
	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/worker"
)

func main() {
	level := slog.LevelInfo
	if os.Getenv("LOG_LEVEL") == "debug" {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	copier.SetLogger(logger.Handler())
	if err := run(logger); err != nil {
		logger.Error("exit", "err", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	addr := envOr("ADDR", ":8080")
	poll, err := durationEnv("POLL_INTERVAL", 2*time.Second)
	if err != nil {
		return err
	}
	eng, defaultSync, err := newEngine(logger)
	if err != nil {
		return err
	}
	syncInterval, err := durationEnv("SYNC_INTERVAL", defaultSync)
	if err != nil {
		return err
	}

	appTTL, err := durationEnv("APP_TTL", 10*time.Second)
	if err != nil {
		return err
	}
	confirm, err := durationEnv("CONFIRM_TIMEOUT", 30*time.Second)
	if err != nil {
		return err
	}

	resources, err := loadResources(os.Getenv("OBJECT_RESOURCES"))
	if err != nil {
		return err
	}
	st, deletes, err := openStore(logger)
	if err != nil {
		return err
	}
	apps := routing.NewApps(appTTL)
	var directions any
	if r, ok := eng.(*engine.Rclone); ok {
		r.Deletes = deletes
		if d, ok := st.(*store.Dynamo); ok {
			r.Positions = d
		}
		directions = r.Directions()
	}
	w := worker.New(st, eng, worker.Config{
		PollInterval: poll, RetryBackoff: poll, SyncInterval: syncInterval,
		Apps: apps, ConfirmTimeout: confirm,
	}, logger)

	srv := &http.Server{
		Addr: addr,
		Handler: api.NewHandler(api.Deps{
			Store:      st,
			Notifier:   w,
			Engine:     eng.Name(),
			Preflight:  eng,
			Apps:       apps,
			Deletes:    deletes,
			Directions: directions,
			Token:      os.Getenv("API_TOKEN"),
			Resources:  resources,
			Logger:     logger,
		}),
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	workerDone := make(chan struct{})
	go func() {
		_ = w.Run(ctx)
		close(workerDone)
	}()

	serveErr := make(chan error, 1)
	go func() {
		logger.Info("listening", "addr", addr, "engine", eng.Name(), "syncInterval", syncInterval.String())
		if os.Getenv("API_TOKEN") == "" {
			logger.Warn("API_TOKEN not set; control routes accept requests without a token")
		}
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
		}
		close(serveErr)
	}()

	select {
	case <-ctx.Done():
	case err := <-serveErr:
		stop() // stop the worker too
		<-workerDone
		return err
	}

	logger.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = srv.Shutdown(shutdownCtx)
	<-workerDone
	return err
}

// openStore keeps migrations in the service's DynamoDB table when
// DYNAMODB_TABLE is set, so a restarted server goes on with them, and in
// memory otherwise. The table is reached with the platform's own AWS
// credentials (the SDK's default chain), not an endpoint's.
func openStore(logger *slog.Logger) (store.Store, *routing.Deletes, error) {
	table := os.Getenv("DYNAMODB_TABLE")
	if table == "" {
		logger.Warn("DYNAMODB_TABLE not set; migrations are lost when the server stops")
		return store.NewMemory(), routing.NewDeletes(), nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(envOr("DYNAMODB_REGION", "ap-northeast-2")))
	if err != nil {
		return nil, nil, fmt.Errorf("DYNAMODB_TABLE: %w", err)
	}
	db := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		if ep := os.Getenv("DYNAMODB_ENDPOINT"); ep != "" {
			o.BaseEndpoint = aws.String(ep)
		}
	})
	d, err := store.OpenDynamo(ctx, db, table)
	if err != nil {
		return nil, nil, fmt.Errorf("DYNAMODB_TABLE: %w", err)
	}
	deletes, err := routing.LoadDeletes(ctx, d)
	if err != nil {
		return nil, nil, fmt.Errorf("load deleted keys: %w", err)
	}
	return d, deletes, nil
}

// newEngine picks the engine from ENGINE and returns it with its default
// wait between sync rounds. For sources without a change feed each rclone
// round lists both buckets, so rounds are spaced out; sources with a feed
// start the next round at once, and fake rounds cost nothing.
func newEngine(logger *slog.Logger) (engine.Engine, time.Duration, error) {
	switch name := envOr("ENGINE", "rclone"); name {
	case "rclone":
		transfers, err := intEnv("COPY_TRANSFERS", 16)
		if err != nil {
			return nil, 0, err
		}
		checkers, err := intEnv("COPY_CHECKERS", 16)
		if err != nil {
			return nil, 0, err
		}
		eng := engine.NewRclone(copier.Options{Transfers: transfers, Checkers: checkers})
		eng.Log = logger
		eng.GCPProject = os.Getenv("GCP_PROJECT")
		if eng.Stream.FullCopyInterval, err = durationEnv("FULL_COPY_INTERVAL", eng.Stream.FullCopyInterval); err != nil {
			return nil, 0, err
		}
		return eng, 30 * time.Second, nil
	case "fake":
		delay, err := durationEnv("FAKE_STEP_DELAY", 2*time.Second)
		if err != nil {
			return nil, 0, err
		}
		return engine.NewFake(delay), 2 * time.Second, nil
	default:
		return nil, 0, fmt.Errorf("ENGINE: want rclone or fake, got %q", name)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func intEnv(key string, def int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%s: want a positive integer, got %q", key, v)
	}
	return n, nil
}

func durationEnv(key string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%s: want a positive duration like 2s, got %q", key, v)
	}
	return d, nil
}

// loadResources reads the buckets common-API plans may name: a JSON object
// from name to endpoint, e.g. {"aws-receipts": {"kind": "s3", "bucket": ...}}.
func loadResources(path string) (map[string]migration.Endpoint, error) {
	if path == "" {
		return nil, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("OBJECT_RESOURCES: %w", err)
	}
	var out map[string]migration.Endpoint
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, fmt.Errorf("OBJECT_RESOURCES %s: %w", path, err)
	}
	for name, e := range out {
		if err := e.Validate(); err != nil {
			return nil, fmt.Errorf("OBJECT_RESOURCES %s: %s: %w", path, name, err)
		}
	}
	return out, nil
}
