package tape9

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// HTTPError is a non-retryable HTTP response returned by the server.
type HTTPError struct {
	// StatusCode is the HTTP response status code.
	StatusCode int

	// Body is the response body text, capped to a small diagnostic size.
	Body string
}

// Error implements error.
func (e *HTTPError) Error() string {
	return fmt.Sprintf("http %d: %s", e.StatusCode, e.Body)
}

// RetryableError is an HTTP response that can be retried after backpressure.
type RetryableError struct {
	// StatusCode is the HTTP response status code.
	StatusCode int

	// RetryAfter is the parsed Retry-After delay, or zero when absent/invalid.
	RetryAfter time.Duration

	// Body is the response body text, capped to a small diagnostic size.
	Body string
}

// Error implements error.
func (e *RetryableError) Error() string {
	if e.RetryAfter > 0 {
		return fmt.Sprintf("http %d (retry after %s): %s", e.StatusCode, e.RetryAfter, e.Body)
	}
	return fmt.Sprintf("http %d: %s", e.StatusCode, e.Body)
}

func httpAPIErrorMessage(err error, statusCode int) (string, bool) {
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.StatusCode != statusCode {
		return "", false
	}
	return apiErrorMessage(httpErr.Body), true
}

func apiErrorMessage(body string) string {
	var payload struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal([]byte(body), &payload); err == nil && strings.TrimSpace(payload.Error) != "" {
		return strings.TrimSpace(payload.Error)
	}
	return strings.TrimSpace(body)
}
