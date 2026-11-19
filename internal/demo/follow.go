package demo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/coder/websocket"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
)

// item is one notification the demo created and follows.
type item struct {
	id        string
	channel   string
	priority  string
	recipient domain.Recipient
	// status and version are the newest the gateway reported; version 0
	// means nothing has arrived yet.
	status  string
	version int64
}

func (it *item) final() bool { return it.status == "sent" || it.status == "failed" }

// shortID is the last 8 characters of an id: the first ones are a timestamp
// and the same for ids created together.
func shortID(id string) string {
	if len(id) < 8 {
		return id
	}
	return id[len(id)-8:]
}

// wsMessage is a message of the gateway (ADR-0006 §10).
type wsMessage struct {
	Type          string  `json:"type"`
	ID            string  `json:"id"`
	Status        string  `json:"status"`
	Version       int64   `json:"version"`
	FailureReason *string `json:"failure_reason"`
	Code          string  `json:"code"`
	Message       string  `json:"message"`
}

// errTimeout is returned by follow when the notifications were not all final
// in time.
var errTimeout = errors.New("timeout")

// follow subscribes to items on the gateway and prints each status change
// that is newer than the one it holds, until every item is final or ctx
// ends (errTimeout).
func follow(ctx context.Context, gatewayURL string, items []*item, w io.Writer, now func() time.Time) error {
	conn, resp, err := websocket.Dial(ctx, gatewayURL, nil)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if err != nil {
		if ctx.Err() != nil {
			return errTimeout
		}
		return fmt.Errorf("the gateway at %s cannot be reached: %w", gatewayURL, err)
	}
	defer func() { _ = conn.CloseNow() }()

	byID := make(map[string]*item, len(items))
	ids := make([]string, 0, len(items))
	for _, it := range items {
		byID[it.id] = it
		ids = append(ids, it.id)
	}
	req, err := json.Marshal(map[string]any{"action": "subscribe", "notification_ids": ids})
	if err != nil {
		return err
	}
	if err := conn.Write(ctx, websocket.MessageText, req); err != nil {
		return fmt.Errorf("subscribe on the gateway: %w", err)
	}

	for !allFinal(items) {
		_, data, err := conn.Read(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return errTimeout
			}
			return fmt.Errorf("the gateway connection ended: %w", err)
		}
		var m wsMessage
		if json.Unmarshal(data, &m) != nil {
			continue
		}
		switch m.Type {
		case "error":
			_, _ = fmt.Fprintf(w, "gateway error %s: %s\n", m.Code, m.Message)
			if m.Code != "snapshot_unavailable" {
				return fmt.Errorf("the gateway refused the subscription (%s)", m.Code)
			}
		case "status_changed":
			it, ok := byID[m.ID]
			// The client rule: a version that is not newer is ignored.
			if !ok || m.Version <= it.version {
				continue
			}
			it.status, it.version = m.Status, m.Version
			line := fmt.Sprintf("%s  %s  %-5s %-6s %-10s v%d", now().Format("15:04:05.000"),
				shortID(it.id), it.channel, it.priority, it.status, it.version)
			if m.FailureReason != nil {
				line += "  (" + *m.FailureReason + ")"
			}
			_, _ = fmt.Fprintln(w, line)
		}
	}
	return nil
}

func allFinal(items []*item) bool {
	for _, it := range items {
		if !it.final() {
			return false
		}
	}
	return true
}

// pending describes the items that are not final, for the timeout message.
func pending(items []*item) string {
	var parts []string
	for _, it := range items {
		if it.final() {
			continue
		}
		st := it.status
		if st == "" {
			st = "no status yet"
		}
		parts = append(parts, fmt.Sprintf("%s (%s)", shortID(it.id), st))
	}
	return strings.Join(parts, ", ")
}
