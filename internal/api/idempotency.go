package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"net/http"
	"sync"

	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/store"
)

const idempotencyHeader = "Idempotency-Key"

// idempotency replays the first response for a repeated Idempotency-Key, so a
// retried request gets the same answer even after the worker has moved on.
// The responses are kept with the migrations, so a restart keeps them too.
type idempotency struct {
	saved responses
	mu    sync.Mutex
	locks map[string]*sync.Mutex
}

// responses keeps the saved responses; every store does.
type responses interface {
	SavedResponse(ctx context.Context, key string) (store.SavedResponse, bool, error)
	SaveResponse(ctx context.Context, key string, r store.SavedResponse) error
}

func newIdempotency(saved responses) *idempotency {
	return &idempotency{saved: saved, locks: make(map[string]*sync.Mutex)}
}

// wrap applies idempotency to next when the request carries a key.
func (i *idempotency) wrap(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get(idempotencyHeader)
		if key == "" {
			next(w, r)
			return
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
		if err != nil {
			writeError(w, badRequest("read body: "+err.Error()))
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		fp := sha256.Sum256(append([]byte(r.Method+" "+r.URL.Path+"\n"), body...))

		// One request per key at a time; a concurrent duplicate waits and
		// then gets the replay.
		lock := i.lockFor(key)
		lock.Lock()
		defer lock.Unlock()

		s, ok, err := i.saved.SavedResponse(r.Context(), key)
		if err != nil {
			writeError(w, err)
			return
		}
		if ok {
			if s.Fingerprint != fp {
				writeError(w, &apiError{
					Code:    "IDEMPOTENCY_KEY_REUSED",
					Message: "Idempotency-Key was already used for a different request",
					status:  http.StatusUnprocessableEntity,
				})
				return
			}
			if s.Location != "" {
				w.Header().Set("Location", s.Location)
			}
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Idempotent-Replayed", "true")
			w.WriteHeader(s.Status)
			_, _ = w.Write(s.Body)
			return
		}

		rec := &recorder{ResponseWriter: w, status: http.StatusOK}
		next(rec, r)
		if !retryable(rec.status, rec.body.Bytes()) {
			saved := store.SavedResponse{Fingerprint: fp, Status: rec.status, Location: w.Header().Get("Location"), Body: rec.body.Bytes()}
			// The request already took effect; a key that could not be
			// saved only means a retry is not replayed.
			_ = i.saved.SaveResponse(context.WithoutCancel(r.Context()), key, saved)
		}
	}
}

// retryable reports whether a response tells the client to try again. Such
// responses are not saved, so a retry with the same key gets a fresh answer.
func retryable(status int, body []byte) bool {
	if status >= http.StatusInternalServerError {
		return true
	}
	if status < http.StatusBadRequest {
		return false
	}
	var e struct {
		Retryable bool `json:"retryable"`
	}
	return json.Unmarshal(body, &e) == nil && e.Retryable
}

func (i *idempotency) lockFor(key string) *sync.Mutex {
	i.mu.Lock()
	defer i.mu.Unlock()
	l, ok := i.locks[key]
	if !ok {
		l = &sync.Mutex{}
		i.locks[key] = l
	}
	return l
}

// recorder passes the response through and keeps a copy.
type recorder struct {
	http.ResponseWriter
	status int
	body   bytes.Buffer
}

func (r *recorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *recorder) Write(b []byte) (int, error) {
	r.body.Write(b)
	return r.ResponseWriter.Write(b)
}
