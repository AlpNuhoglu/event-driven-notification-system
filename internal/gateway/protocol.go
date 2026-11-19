package gateway

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
)

// The WebSocket protocol of ADR-0006 §10.

// Actions of a client request.
const (
	actionSubscribe   = "subscribe"
	actionUnsubscribe = "unsubscribe"
)

// Codes of an error message. The connection stays open after each.
const (
	// codeInvalidMessage: not a JSON object, an unknown action, a member of
	// the wrong type, neither notification_ids nor batch_id, a binary frame.
	codeInvalidMessage = "invalid_message"
	// codeInvalidID: an id is not a UUID.
	codeInvalidID = "invalid_id"
	// codeTooManySubscriptions: the request would take the connection over
	// WS_MAX_SUBSCRIPTIONS; nothing of it was registered.
	codeTooManySubscriptions = "too_many_subscriptions"
	// codeSnapshotUnavailable: the current state could not be read; the
	// subscription is registered and live events arrive.
	codeSnapshotUnavailable = "snapshot_unavailable"
)

// request is a parsed, valid client request.
type request struct {
	action string
	// ids are the distinct notification ids, in request order.
	ids []uuid.UUID
	// batch is the batch id, or nil.
	batch *uuid.UUID
}

// protocolError is the answer to a request that cannot be served. Its
// message never contains a value the client sent; it may name a member and
// an index.
type protocolError struct {
	code    string
	message string
}

func invalidMessage(format string, args ...any) *protocolError {
	return &protocolError{code: codeInvalidMessage, message: fmt.Sprintf(format, args...)}
}

// parseRequest parses one text message of a client.
//
// Both notification_ids and batch_id are optional, and a request names at
// least one id or a batch; a null member counts as absent. Members this
// version does not know are ignored. An id is accepted only in the
// canonical form of 36 characters (hexadecimal digits in either case).
func parseRequest(data []byte) (request, *protocolError) {
	var members map[string]json.RawMessage
	if err := json.Unmarshal(data, &members); err != nil || members == nil {
		return request{}, invalidMessage("the message is not a JSON object")
	}
	var r request
	if err := unmarshalMember(members, "action", &r.action); err != nil {
		return request{}, err
	}
	if r.action != actionSubscribe && r.action != actionUnsubscribe {
		return request{}, invalidMessage("action must be %q or %q", actionSubscribe, actionUnsubscribe)
	}

	var rawIDs []json.RawMessage
	if err := unmarshalMember(members, "notification_ids", &rawIDs); err != nil {
		return request{}, err
	}
	seen := make(map[uuid.UUID]bool, len(rawIDs))
	for i, raw := range rawIDs {
		var s string
		if json.Unmarshal(raw, &s) != nil {
			return request{}, invalidMessage("notification_ids[%d] must be a string", i)
		}
		id, err := domain.ParseID(s)
		if err != nil {
			return request{}, &protocolError{code: codeInvalidID, message: fmt.Sprintf("notification_ids[%d] is not a UUID", i)}
		}
		if !seen[id] {
			seen[id] = true
			r.ids = append(r.ids, id)
		}
	}

	var batch *string
	if err := unmarshalMember(members, "batch_id", &batch); err != nil {
		return request{}, err
	}
	if batch != nil {
		id, err := domain.ParseID(*batch)
		if err != nil {
			return request{}, &protocolError{code: codeInvalidID, message: "batch_id is not a UUID"}
		}
		r.batch = &id
	}

	if len(r.ids) == 0 && r.batch == nil {
		return request{}, invalidMessage("the request needs notification_ids or batch_id")
	}
	return r, nil
}

// unmarshalMember decodes the member name into v when it is present. A
// member of the wrong type is an invalid message.
func unmarshalMember(members map[string]json.RawMessage, name string, v any) *protocolError {
	raw, ok := members[name]
	if !ok {
		return nil
	}
	if json.Unmarshal(raw, v) != nil {
		return invalidMessage("%s has the wrong type", name)
	}
	return nil
}

// statusMessage is the server's status_changed message, for a snapshot and
// a live event alike. Every member is always present; batch_id and
// failure_reason are null when there is none.
type statusMessage struct {
	Type          string     `json:"type"`
	ID            uuid.UUID  `json:"id"`
	BatchID       *uuid.UUID `json:"batch_id"`
	Status        string     `json:"status"`
	Version       int64      `json:"version"`
	FailureReason *string    `json:"failure_reason"`
	UpdatedAt     time.Time  `json:"updated_at"`
}

// encodeStatus returns the status_changed message of e. It fails only for
// an updated_at outside the years 0 to 9999.
func encodeStatus(e domain.StatusChanged) ([]byte, error) {
	m := statusMessage{
		Type:      "status_changed",
		ID:        e.ID,
		BatchID:   e.BatchID,
		Status:    string(e.Status),
		Version:   e.Version,
		UpdatedAt: e.UpdatedAt.UTC(),
	}
	if e.FailureReason != nil {
		r := string(*e.FailureReason)
		m.FailureReason = &r
	}
	b, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("encode status message: %w", err)
	}
	return b, nil
}

// errorMessage is the server's error message.
type errorMessage struct {
	Type    string `json:"type"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

// encodeError returns the error message of e. Marshalling three strings
// cannot fail.
func encodeError(e *protocolError) []byte {
	b, _ := json.Marshal(errorMessage{Type: "error", Code: e.code, Message: e.message}) //nolint:errchkjson // three strings
	return b
}
