package api

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
)

// The Idempotency-Key header and its limits (ADR-0006 §2).
const (
	headerIdempotencyKey = "Idempotency-Key"
	maxIdempotencyKey    = 255
	// headerIdempotentReplayed marks a replayed response.
	headerIdempotentReplayed = "Idempotent-Replayed"
)

// idempotencyKey reads the Idempotency-Key header. An absent header is not an
// error. A key is valid when the header is sent exactly once, holds 1 to 255
// bytes and every byte is in 0x21 to 0x7E; anything else is a 400 problem.
// The key is never written into a message or a log.
func idempotencyKey(h http.Header) (key string, present bool, p *problem) {
	values := h.Values(headerIdempotencyKey)
	if len(values) == 0 {
		return "", false, nil
	}
	if len(values) != 1 || !validIdempotencyKey(values[0]) {
		return "", false, invalidIdempotencyKeyProblem()
	}
	return values[0], true, nil
}

// invalidIdempotencyKeyProblem is the 400 for an Idempotency-Key header that
// cannot be used; it never contains the key.
func invalidIdempotencyKeyProblem() *problem {
	return newProblem(kindMalformed, "The Idempotency-Key header is invalid.",
		fieldError(headerIdempotencyKey, codeInvalidIdempotencyKey,
			"Idempotency-Key must be 1 to 255 visible ASCII characters and must be sent once"))
}

// idempotencyKeyReusedProblem is the 422 for a key that is live for a
// different request; it never contains the key.
func idempotencyKeyReusedProblem() *problem {
	return newProblem(kindIdempotencyKeyReused, "",
		fieldError(headerIdempotencyKey, codeIdempotencyKeyReused,
			"this Idempotency-Key was already used with a different request"))
}

func validIdempotencyKey(v string) bool {
	if len(v) < 1 || len(v) > maxIdempotencyKey {
		return false
	}
	for i := 0; i < len(v); i++ {
		if v[i] < 0x21 || v[i] > 0x7E {
			return false
		}
	}
	return true
}

// requestHash returns the lower-case hex SHA-256 of
// method + "\n" + path + "\n" + canonical, where canonical is the body
// decoded as generic JSON and encoded again: object keys come out sorted and
// whitespace is gone, so two bodies that differ only in key order or
// whitespace give the same hash. Numbers keep their literal text (the
// decoder uses json.Number), so 1 and 1.0 give different hashes; that is
// acceptable, because it can only turn a retry into a conflict, never merge
// two different requests. The body has already passed validation, so an
// error is unexpected.
func requestHash(method, path string, body []byte) (string, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return "", fmt.Errorf("decode body: %w", err)
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return "", fmt.Errorf("encode body: %w", err)
	}
	canonical := bytes.TrimSuffix(buf.Bytes(), []byte("\n"))

	sum := sha256.New()
	sum.Write([]byte(method + "\n" + path + "\n"))
	sum.Write(canonical)
	return hex.EncodeToString(sum.Sum(nil)), nil
}
