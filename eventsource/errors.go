package eventsource

import (
	"net/http"

	"github.com/golly-go/golly/errors"
)

var (
	ErrorConflict = errors.Error{
		Key:    "ERROR.UPDATE_CONFLICT",
		Status: http.StatusConflict,
	}

	ErrorInvalidRecord = errors.Error{
		Key:    "ERROR.INVALID_RECORD",
		Status: http.StatusNotFound,
	}
)
