package gql

import (
	"errors"
)

// Codes (align with Apollo/GraphQL conventions)
const (
	CodeInternalServerError = "INTERNAL_SERVER_ERROR"
	CodeUnauthenticated     = "UNAUTHENTICATED" // 401
	CodeForbidden           = "FORBIDDEN"       // 403
	CodeBadUserInput        = "BAD_USER_INPUT"  // 400 (use instead of BAD_REQUEST in GraphQL land)
	CodeNotFound            = "NOT_FOUND"
	CodeConflict            = "CONFLICT"
	CodeTooManyRequests     = "TOO_MANY_REQUESTS"    // 429
	CodeUnprocessable       = "UNPROCESSABLE_ENTITY" // 422
	CodeNotImplemented      = "NOT_IMPLEMENTED"      // 501
	CodeBadGateway          = "BAD_GATEWAY"          // 502
)

// Error is a GraphQL-friendly error with code + extensions.
// It wraps an underlying cause for logs/telemetry.
type Error struct {
	Message    string
	Code       string
	Extensions map[string]any
	cause      error
}

func (e *Error) Error() string {
	if e.Message != "" {
		return e.Message
	}
	if e.cause != nil {
		return e.cause.Error()
	}
	return e.Code
}

func (e *Error) Unwrap() error { return e.cause }

// WithMeta adds a single k/v to extensions (copy-on-write).
func (e *Error) WithMeta(k string, v any) *Error {
	cp := *e
	cp.Extensions = copyExt(e.Extensions)
	cp.Extensions[k] = v
	return &cp
}

// WithExtensions merges a map into extensions (copy-on-write).
func (e *Error) WithExtensions(m map[string]any) *Error {
	if len(m) == 0 {
		return e
	}
	cp := *e
	cp.Extensions = copyExt(e.Extensions)
	for k, v := range m {
		cp.Extensions[k] = v
	}
	return &cp
}

// New builds a GraphQL error with code/message, wrapping an optional cause.
// NOTE: we intentionally DO NOT include the cause message in Extensions.
func New(code, message string, cause error, ext map[string]any) *Error {
	return &Error{
		Message:    message,
		Code:       code,
		Extensions: copyExt(ext),
		cause:      cause,
	}
}

// CodeOf extracts the GraphQL error code, if present.
func CodeOf(err error) (string, bool) {
	var ge *Error
	if errors.As(err, &ge) {
		return ge.Code, true
	}
	return "", false
}

// Convenience constructors

func InternalServerError(err error) error {
	return New(CodeInternalServerError, "internal server error", err, nil)
}

func Unauthenticated(err error) error {
	return New(CodeUnauthenticated, "authentication required for this action", err, nil)
}

func Forbidden(err error) error {
	return New(CodeForbidden, "forbidden", err, nil)
}

func NotFound(err error) error {
	return New(CodeNotFound, "not found", err, nil)
}

func BadUserInput(err error) error {
	return New(CodeBadUserInput, "bad request", err, nil)
}

func Conflict(err error) error {
	return New(CodeConflict, "conflict", err, nil)
}

func TooManyRequests(err error) error {
	return New(CodeTooManyRequests, "too many requests", err, nil)
}

func UnprocessableEntity(err error) error {
	return New(CodeUnprocessable, "unprocessable entity", err, nil)
}

func NotImplemented(err error) error {
	return New(CodeNotImplemented, "not implemented", err, nil)
}

func BadGateway(err error) error {
	return New(CodeBadGateway, "bad gateway", err, nil)
}

// --- helpers ---

func copyExt(in map[string]any) map[string]any {
	if len(in) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
