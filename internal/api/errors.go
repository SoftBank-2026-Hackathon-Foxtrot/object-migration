package api

import (
	"errors"
	"net/http"

	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/migration"
	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/store"
)

// apiError is the error body shared by all migration servers.
type apiError struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`

	status int
}

func (e *apiError) Error() string { return e.Message }

func badRequest(msg string) error {
	return &apiError{Code: "INVALID_ARGUMENT", Message: msg, status: http.StatusBadRequest}
}

func notFound(msg string) error {
	return &apiError{Code: "NOT_FOUND", Message: msg, status: http.StatusNotFound}
}

func writeError(w http.ResponseWriter, err error) {
	writeJSON(w, toAPIError(err).status, toAPIError(err))
}

func toAPIError(err error) *apiError {
	var ae *apiError
	switch {
	case errors.As(err, &ae):
		return ae
	case errors.Is(err, store.ErrNotFound):
		return &apiError{Code: "NOT_FOUND", Message: err.Error(), status: http.StatusNotFound}
	case errors.Is(err, migration.ErrInvalidPhase):
		return &apiError{Code: "INVALID_PHASE", Message: err.Error(), status: http.StatusConflict}
	case errors.Is(err, migration.ErrCommandPending):
		return &apiError{Code: "COMMAND_PENDING", Message: err.Error(), Retryable: true, status: http.StatusConflict}
	case errors.Is(err, migration.ErrVersionMismatch):
		return &apiError{Code: "VERSION_CONFLICT", Message: err.Error(), status: http.StatusConflict}
	case errors.Is(err, migration.ErrPrecondition):
		return &apiError{Code: "PRECONDITION_FAILED", Message: err.Error(), Retryable: true, status: http.StatusConflict}
	}
	return &apiError{Code: "INTERNAL", Message: "internal error", Retryable: true, status: http.StatusInternalServerError}
}
