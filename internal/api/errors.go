package api

import (
	"errors"
	"net/http"

	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/migration"
	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/store"
)

// apiError is the error body of the team's common API: code, message,
// operationId and blockers. retryable is this server's addition; a client
// that retries with the same Idempotency-Key gets a fresh answer for it.
type apiError struct {
	Code        string   `json:"code"`
	Message     string   `json:"message"`
	OperationID *string  `json:"operationId"`
	Blockers    []string `json:"blockers"`
	Retryable   bool     `json:"retryable"`

	status int
}

func (e *apiError) Error() string { return e.Message }

func newError(status int, code, msg string, blockers ...string) *apiError {
	if blockers == nil {
		blockers = []string{}
	}
	return &apiError{Code: code, Message: msg, Blockers: blockers, status: status}
}

func badRequest(msg string) error {
	return newError(http.StatusBadRequest, "INVALID_REQUEST", msg)
}

func notFound(msg string) error {
	return newError(http.StatusNotFound, "NOT_FOUND", msg)
}

func invalidState(msg string) error {
	return newError(http.StatusConflict, "INVALID_STATE", msg)
}

func writeError(w http.ResponseWriter, err error) {
	e := toAPIError(err)
	writeJSON(w, e.status, e)
}

func toAPIError(err error) *apiError {
	var ae *apiError
	var e *apiError
	switch {
	case errors.As(err, &ae):
		return ae
	case errors.Is(err, store.ErrNotFound):
		e = newError(http.StatusNotFound, "NOT_FOUND", err.Error())
	case errors.Is(err, migration.ErrInvalidPhase):
		e = newError(http.StatusConflict, "INVALID_STATE", err.Error())
	case errors.Is(err, migration.ErrCommandPending):
		e = newError(http.StatusConflict, "RESOURCE_BUSY", err.Error())
		e.Retryable = true
	case errors.Is(err, migration.ErrVersionMismatch):
		e = newError(http.StatusConflict, "REVISION_CONFLICT", err.Error())
	case errors.Is(err, migration.ErrPrecondition):
		e = newError(http.StatusUnprocessableEntity, "PRECONDITION_FAILED", err.Error())
		e.Retryable = true
	default:
		e = newError(http.StatusInternalServerError, "INTERNAL", "internal error")
		e.Retryable = true
	}
	return e
}
