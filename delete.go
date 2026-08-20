package tape9

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
)

// Delete removes the current visible tape incarnation.
//
// The operation is idempotent for an existing live tape and a missing tape
// within an existing space.
func (c *Client) Delete(ctx context.Context, spaceID string, tapeID string) error {
	if !IsValidID(spaceID) {
		return fmt.Errorf("invalid space_id: %q", spaceID)
	}
	if !IsValidID(tapeID) {
		return fmt.Errorf("invalid tape_id: %q", tapeID)
	}

	return c.doWithRetry(ctx, func(attemptCtx context.Context) error {
		attemptCtx, cancel := context.WithTimeout(attemptCtx, c.requestTimeout)
		defer cancel()

		req, err := http.NewRequestWithContext(
			attemptCtx,
			http.MethodDelete,
			c.endpointURL("v1", "spaces", url.PathEscape(spaceID), "tapes", url.PathEscape(tapeID)),
			nil,
		)
		if err != nil {
			return err
		}
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
