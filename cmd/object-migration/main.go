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
	"syscall"
	"time"

	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/api"
	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/engine"
	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/store"
	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/worker"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
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
	stepDelay, err := durationEnv("FAKE_STEP_DELAY", 2*time.Second)
	if err != nil {
		return err
	}

	st := store.NewMemory()
	eng := engine.NewFake(stepDelay)
	w := worker.New(st, eng, worker.Config{PollInterval: poll, RetryBackoff: poll}, logger)

	srv := &http.Server{
		Addr: addr,
		Handler: api.NewHandler(api.Deps{
			Store:    st,
			Notifier: w,
			Engine:   eng.Name(),
			Logger:   logger,
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
		logger.Info("listening", "addr", addr, "engine", eng.Name())
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

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
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
