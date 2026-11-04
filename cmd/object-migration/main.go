// Command object-migration runs the object storage migration server: the
// common migration API and the worker that carries migrations out, in one
// process.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/api"
	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/copier"
	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/engine"
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

	st, deletes, closeStore, err := openStore(logger)
	if err != nil {
		return err
	}
	defer closeStore()
	apps := routing.NewApps(appTTL)
	var directions any
	if r, ok := eng.(*engine.Rclone); ok {
		r.Deletes = deletes
		if pg, ok := st.(*store.Postgres); ok {
			r.Positions = pg
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

// openStore keeps migrations in PostgreSQL when DATABASE_URL is set, so a
// restarted server goes on with them, and in memory otherwise.
func openStore(logger *slog.Logger) (store.Store, *routing.Deletes, func(), error) {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		logger.Warn("DATABASE_URL not set; migrations are lost when the server stops")
		return store.NewMemory(), routing.NewDeletes(), func() {}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pg, err := store.OpenPostgres(ctx, url)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("DATABASE_URL: %w", err)
	}
	deletes, err := routing.LoadDeletes(ctx, pg)
	if err != nil {
		pg.Close()
		return nil, nil, nil, fmt.Errorf("load deleted keys: %w", err)
	}
	return pg, deletes, pg.Close, nil
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
