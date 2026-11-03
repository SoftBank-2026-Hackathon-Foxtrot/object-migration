package api

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"io"
	"net/http"
	"sync"
)

const idempotencyHeader = "Idempotency-Key"

// idempotency replays the first response for a repeated Idempotency-Key, so a
// retried request gets the same answer even after the worker has moved on.
// Keys live in memory until the store becomes persistent.
type idempotency struct {
	mu    sync.Mutex
	locks map[string]*sync.Mutex
	saved map[string]savedResponse
}

type savedResponse struct {
	fingerprint [32]byte
	status      int
	location    string
	body        []byte
}

func newIdempotency() *idempotency {
	return &idempotency{locks: make(map[string]*sync.Mutex), saved: make(map[string]savedResponse)}
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

		if s, ok := i.get(key); ok {
			if s.fingerprint != fp {
				writeError(w, &apiError{
					Code:    "IDEMPOTENCY_KEY_REUSED",
					Message: "Idempotency-Key was already used for a different request",
					status:  http.StatusUnprocessableEntity,
				})
				return
			}
			if s.location != "" {
				w.Header().Set("Location", s.location)
			}
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Idempotent-Replayed", "true")
			w.WriteHeader(s.status)
			_, _ = w.Write(s.body)
			return
		}

		rec := &recorder{ResponseWriter: w, status: http.StatusOK}
		next(rec, r)
		if !retryable(rec.status, rec.body.Bytes()) {
			i.put(key, savedResponse{fingerprint: fp, status: rec.status, location: w.Header().Get("Location"), body: rec.body.Bytes()})
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

func (i *idempotency) get(key string) (savedResponse, bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	s, ok := i.saved[key]
	return s, ok
}

func (i *idempotency) put(key string, s savedResponse) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.saved[key] = s
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
