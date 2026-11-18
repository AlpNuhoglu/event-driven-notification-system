//go:build integration

// E6, part 1: which Go value types the broker accepts for the quorum queue
// argument x-consumer-timeout, and what a redeclaration with a different value,
// a different integer type or no argument at all does.
//
// Run from experiments/e6:
//
//	go test -tags=integration -count=1 -v -run TestDeclare ./...
package e6

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

const day = 86400000 // 24 h in ms

// declare declares a durable quorum queue on a fresh channel and returns the
// AMQP error code (0 on success).
func declare(t *testing.T, conn *amqp.Connection, name string, args amqp.Table) (int, string) {
	t.Helper()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("channel: %v", err)
	}
	defer ch.Close()
	_, err = ch.QueueDeclare(name, true, false, false, false, args)
	var aerr *amqp.Error
	if errors.As(err, &aerr) {
		return aerr.Code, aerr.Reason
	}
	if err != nil {
		t.Fatalf("declare %s: unexpected non-AMQP error %v", name, err)
	}
	return 0, ""
}

func TestDeclareArgumentTypes(t *testing.T) {
	conn := dial(t)
	cases := []struct {
		name  string
		value any
		want  int // expected AMQP reply code, 0 = accepted
	}{
		{"int", int(day), 0},       // amqp091-go encodes Go int as 'I' (signed 32-bit)
		{"int32", int32(day), 0},   // 'I'
		{"int64", int64(day), 0},   // 'l'
		{"uint32", uint32(day), 0}, // 'i'
		{"string", fmt.Sprint(day), 406},
		{"float64", float64(day), 406},
		{"negative", int32(-1), 406},
		// amqp091-go writes a Go int as a 32-bit 'I' field: larger values wrap.
		{"int-5e9", int(5_000_000_000), 0},   // arrives as 705032704
		{"int-3e9", int(3_000_000_000), 406}, // arrives negative
	}
	for _, c := range cases {
		code, reason := declare(t, conn, "e6-type-"+c.name,
			quorumArgs(amqp.Table{"x-consumer-timeout": c.value}))
		logf(t, "declare x-consumer-timeout as %-8s (%T) -> code=%d %s", c.name, c.value, code, reason)
		if code != c.want {
			t.Errorf("%s: got code %d, want %d", c.name, code, c.want)
		}
	}
	out := rabbitmqctl(t, "list_queues", "name", "type", "arguments")
	logf(t, "rabbitmqctl list_queues name type arguments:\n%s", out)
	if !strings.Contains(out, `{"x-consumer-timeout",705032704}`) {
		t.Errorf("want int(5e9) silently truncated to 705032704")
	}
}

func TestDeclareRedeclaration(t *testing.T) {
	conn := dial(t)
	const q = "e6-redeclare"
	if code, reason := declare(t, conn, q, quorumArgs(amqp.Table{"x-consumer-timeout": int64(day)})); code != 0 {
		t.Fatalf("first declare: %d %s", code, reason)
	}
	cases := []struct {
		name string
		args amqp.Table
		want int
	}{
		{"same value, same type (int64)", quorumArgs(amqp.Table{"x-consumer-timeout": int64(day)}), 0},
		{"same value, other type (int32)", quorumArgs(amqp.Table{"x-consumer-timeout": int32(day)}), 0},
		{"different value", quorumArgs(amqp.Table{"x-consumer-timeout": int64(60000)}), 406},
		{"argument omitted", quorumArgs(nil), 406},
	}
	for _, c := range cases {
		code, reason := declare(t, conn, q, c.args)
		logf(t, "redeclare %-32s -> code=%d %s", c.name, code, reason)
		if code != c.want {
			t.Errorf("%s: got code %d, want %d", c.name, code, c.want)
		}
	}
}
