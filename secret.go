package tape9

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
)

// AddSecret lets newSecret open a secured space alongside the client's secret.
//
// A space keeps at most two secrets. To rotate, add the new secret, move every
// caller to it, then revoke the old one with a client that uses the new secret.
// Adding a secret that already opens the space succeeds without change.
func (c *Client) AddSecret(ctx context.Context, spaceID string, newSecret string) error {
	return c.changeSecret(ctx, spaceID, newSecret, false)
}

// RevokeSecret stops oldSecret from opening a secured space. The client's own
// secret must stay valid, so it cannot revoke itself. Revoking a secret that
// no longer opens the space succeeds without change.
func (c *Client) RevokeSecret(ctx context.Context, spaceID string, oldSecret string) error {
	return c.changeSecret(ctx, spaceID, oldSecret, true)
}

func (c *Client) changeSecret(ctx context.Context, spaceID, secret string, revoke bool) error {
	if !IsValidID(spaceID) {
		return fmt.Errorf("invalid space_id: %q", spaceID)
	}
	path := []string{"v1", "spaces", url.PathEscape(spaceID), "secrets"}
	if revoke {
		path = append(path, "revoke")
	}
	body, err := json.Marshal(struct {
		Secret string `json:"secret"`
	}{secret})
	if err != nil {
		return err
	}

	return c.doWithRetry(ctx, func(attemptCtx context.Context) error {
		attemptCtx, cancel := context.WithTimeout(attemptCtx, c.requestTimeout)
		defer cancel()

		req, err := http.NewRequestWithContext(
			attemptCtx,
			http.MethodPost,
			c.endpointURL(path...),
			bytes.NewReader(body),
		)
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		c.applySpaceSecret(req)

		resp, err := c.http.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusNoContent {
			return readResponseError(resp)
		}
		return nil
	})
}
