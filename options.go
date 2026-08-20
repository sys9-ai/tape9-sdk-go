package tape9

import (
	"net/http"
	"time"
)

type clientOptions struct {
	secret       string
	httpClient   *http.Client
	maxRetryTime time.Duration
}

// Option configures a Client.
type Option func(*clientOptions)

// WithSecret attaches a space secret to API requests.
func WithSecret(secret string) Option {
	return func(o *clientOptions) {
		o.secret = secret
	}
}

// WithHTTPClient supplies the HTTP client used for API requests.
//
// The SDK clones the provided client so it can still strip the space secret
// from redirect requests.
func WithHTTPClient(httpClient *http.Client) Option {
	return func(o *clientOptions) {
		o.httpClient = httpClient
	}
}

// WithMaxRetryTime configures the maximum continuous retry time for one
// retryable request.
func WithMaxRetryTime(d time.Duration) Option {
	return func(o *clientOptions) {
		o.maxRetryTime = d
	}
}
