package demo

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// maxBody bounds what the demo reads of any answer.
const maxBody = 1 << 20

// apiClient is the part of the REST API the demo uses.
type apiClient struct {
	base string
	http *http.Client
}

// createRequest is the body of POST /v1/notifications and an item of the
// batch request.
type createRequest struct {
	Channel   string `json:"channel"`
	Recipient string `json:"recipient"`
	Content   string `json:"content"`
	Priority  string `json:"priority"`
}

// notificationDTO is the members of a notification the demo reads. Ids stay
// strings here and are parsed with domain.ParseID by the caller.
type notificationDTO struct {
	ID            string  `json:"id"`
	Channel       string  `json:"channel"`
	Priority      string  `json:"priority"`
	Status        string  `json:"status"`
	Version       int64   `json:"version"`
	AttemptCount  int     `json:"attempt_count"`
	FailureReason *string `json:"failure_reason"`
}

type batchDTO struct {
	BatchID  string `json:"batch_id"`
	Accepted []struct {
		Index int    `json:"index"`
		ID    string `json:"id"`
	} `json:"accepted"`
	Rejected []struct {
		Index int `json:"index"`
	} `json:"rejected"`
}

// summaryDTO is the part of GET /v1/metrics/summary the demo prints.
type summaryDTO struct {
	GeneratedAt string `json:"generated_at"`
	Window      string `json:"window"`
	Channels    []struct {
		Channel     string           `json:"channel"`
		Attempts    map[string]int64 `json:"attempts"`
		SuccessRate *float64         `json:"success_rate"`
	} `json:"channels"`
}

// do sends one request and returns the status, the Idempotent-Replayed flag
// and the body. A status other than want is an error that carries the
// problem's title and detail, which never hold a recipient or content.
func (c *apiClient) do(ctx context.Context, method, path, idemKey string, body any, want int, out any) (replayed bool, err error) {
	var rd io.Reader = http.NoBody
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return false, err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(c.base, "/")+path, rd)
	if err != nil {
		return false, err
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if idemKey != "" {
		req.Header.Set("Idempotency-Key", idemKey)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return false, err
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	if err != nil {
		return false, err
	}
	if resp.StatusCode != want {
		var p struct {
			Title  string `json:"title"`
			Detail string `json:"detail"`
		}
		_ = json.Unmarshal(data, &p)
		return false, fmt.Errorf("%s %s answered %d %s %s", method, path, resp.StatusCode, p.Title, p.Detail)
	}
	if out != nil {
		if err := json.Unmarshal(data, out); err != nil {
			return false, fmt.Errorf("%s %s: unreadable answer: %w", method, path, err)
		}
	}
	return resp.Header.Get("Idempotent-Replayed") == "true", nil
}

func (c *apiClient) create(ctx context.Context, key string, r createRequest) (notificationDTO, bool, error) {
	var n notificationDTO
	replayed, err := c.do(ctx, http.MethodPost, "/v1/notifications", key, r, http.StatusAccepted, &n)
	return n, replayed, err
}

func (c *apiClient) createBatch(ctx context.Context, key string, items []createRequest) (batchDTO, bool, error) {
	var b batchDTO
	replayed, err := c.do(ctx, http.MethodPost, "/v1/notifications/batch", key,
		map[string]any{"notifications": items}, http.StatusAccepted, &b)
	return b, replayed, err
}

func (c *apiClient) get(ctx context.Context, id string) (notificationDTO, error) {
	var n notificationDTO
	_, err := c.do(ctx, http.MethodGet, "/v1/notifications/"+id, "", nil, http.StatusOK, &n)
	return n, err
}

func (c *apiClient) summary(ctx context.Context) (summaryDTO, error) {
	var s summaryDTO
	_, err := c.do(ctx, http.MethodGet, "/v1/metrics/summary", "", nil, http.StatusOK, &s)
	return s, err
}
