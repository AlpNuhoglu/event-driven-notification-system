package testutil

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"slices"
	"sync"
)

// AMQPTap reads the AMQP 0-9-1 frames that pass through a Proxy and records,
// per channel session, the deliveries the broker sent (basic.deliver with its
// body) and the answers the client sent (basic.ack, basic.nack,
// basic.reject). A channel session begins with the client's channel.open on
// that channel of that TCP connection; delivery tags are numbered per
// session. It shows on the wire which delivery each answer names.
type AMQPTap struct {
	mu         sync.Mutex
	conns      int
	sessions   int
	current    map[chanKey]int    // the session of each open channel
	deliveries []TapDelivery      // in arrival order
	byTag      map[tagKey]int     // index into deliveries
	answers    []TapAnswer        // in sending order
	violations []string           // answers that name no unanswered delivery of their session
	closes     []string           // channel.close and connection.close sent by the broker
	pending    map[chanKey]*frame // a basic.deliver waiting for its body
}

// TapDelivery is one basic.deliver seen on the wire.
type TapDelivery struct {
	Conn    int    // the proxied TCP connection, from 1
	Session int    // the channel session, from 1
	Tag     uint64 // the delivery tag within the session
	Body    string // the message body (the notification id)
	Answer  string // "ack", "nack", "reject" or "" (unanswered)
}

// TapAnswer is one basic.ack, basic.nack or basic.reject seen on the wire.
type TapAnswer struct {
	Conn    int
	Session int
	Tag     uint64
	Kind    string
	Body    string // the body of the delivery the tag names in the session; "" if none
}

type chanKey struct {
	conn    int
	channel uint16
}

type tagKey struct {
	session int
	tag     uint64
}

// frame is a basic.deliver whose content is being assembled.
type frame struct {
	tag  uint64
	size uint64
	body []byte
}

// Tap starts recording the AMQP traffic of the connections the proxy accepts
// from now on and returns the record.
func (p *Proxy) Tap() *AMQPTap {
	t := &AMQPTap{
		current: map[chanKey]int{},
		byTag:   map[tagKey]int{},
		pending: map[chanKey]*frame{},
	}
	p.mu.Lock()
	p.tap = t
	p.mu.Unlock()
	return t
}

// Deliveries returns every delivery seen so far, with its answer.
func (t *AMQPTap) Deliveries() []TapDelivery {
	t.mu.Lock()
	defer t.mu.Unlock()
	return slices.Clone(t.deliveries)
}

// Answers returns every answer seen so far.
func (t *AMQPTap) Answers() []TapAnswer {
	t.mu.Lock()
	defer t.mu.Unlock()
	return slices.Clone(t.answers)
}

// Violations returns the answers that did not name an unanswered delivery
// of their own session: a tag never delivered on that session, or one
// answered before.
func (t *AMQPTap) Violations() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return slices.Clone(t.violations)
}

// BrokerCloses returns the channel.close and connection.close methods the
// broker sent, with their reply code and text.
func (t *AMQPTap) BrokerCloses() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return slices.Clone(t.closes)
}

// Conns returns the number of TCP connections seen.
func (t *AMQPTap) Conns() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.conns
}

func (t *AMQPTap) newConn() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.conns++
	return t.conns
}

// stream parses one direction of one connection.
type stream struct {
	tap        *AMQPTap
	conn       int
	fromClient bool
	buf        []byte
	header     bool // the client's protocol header has been skipped
}

func (t *AMQPTap) stream(conn int, fromClient bool) *stream {
	return &stream{tap: t, conn: conn, fromClient: fromClient, header: !fromClient}
}

// write takes the next bytes of the direction and records every complete
// frame. Unparseable input stops the parsing of this direction.
func (s *stream) write(b []byte) {
	if s == nil {
		return
	}
	s.buf = append(s.buf, b...)
	if !s.header {
		if len(s.buf) < 8 {
			return
		}
		s.buf = s.buf[8:] // "AMQP" 0 0 9 1
		s.header = true
	}
	for len(s.buf) >= 7 {
		typ := s.buf[0]
		channel := binary.BigEndian.Uint16(s.buf[1:3])
		size := int(binary.BigEndian.Uint32(s.buf[3:7]))
		if len(s.buf) < 7+size+1 {
			return
		}
		payload := s.buf[7 : 7+size]
		s.tap.frame(s.conn, s.fromClient, typ, channel, payload)
		s.buf = s.buf[7+size+1:]
	}
}

const (
	frameMethod = 1
	frameHeader = 2
	frameBody   = 3
)

func (t *AMQPTap) frame(conn int, fromClient bool, typ uint8, channel uint16, p []byte) {
	t.mu.Lock()
	defer t.mu.Unlock()
	key := chanKey{conn, channel}
	switch typ {
	case frameMethod:
		if len(p) < 4 {
			return
		}
		class, method := binary.BigEndian.Uint16(p[0:2]), binary.BigEndian.Uint16(p[2:4])
		args := p[4:]
		switch {
		case fromClient && class == 20 && method == 10: // channel.open
			t.sessions++
			t.current[key] = t.sessions
		case !fromClient && ((class == 20 && method == 40) || (class == 10 && method == 50)) && len(args) >= 3:
			// channel.close, connection.close: reply-code, reply-text
			code := binary.BigEndian.Uint16(args[0:2])
			n := int(args[2])
			text := ""
			if len(args) >= 3+n {
				text = string(args[3 : 3+n])
			}
			what := map[uint16]string{20: "channel", 10: "connection"}[class]
			t.closes = append(t.closes, fmt.Sprintf("connection %d, %s %d closed by the broker: %d %s", conn, what, channel, code, text))
		case !fromClient && class == 60 && method == 60: // basic.deliver
			if tag, ok := deliverTag(args); ok {
				t.pending[key] = &frame{tag: tag}
			}
		case fromClient && class == 60 && (method == 80 || method == 90 || method == 120) && len(args) >= 8:
			kind := map[uint16]string{80: "ack", 90: "reject", 120: "nack"}[method]
			t.answer(conn, key, binary.BigEndian.Uint64(args[0:8]), kind)
		}
	case frameHeader:
		f := t.pending[key]
		if f == nil || fromClient || len(p) < 12 {
			return
		}
		f.size = binary.BigEndian.Uint64(p[4:12])
		if f.size == 0 {
			t.delivered(conn, key, f)
		}
	case frameBody:
		f := t.pending[key]
		if f == nil || fromClient {
			return
		}
		f.body = append(f.body, p...)
		if uint64(len(f.body)) >= f.size {
			t.delivered(conn, key, f)
		}
	}
}

// deliverTag reads the delivery tag of basic.deliver: consumer-tag
// (shortstr), delivery-tag (longlong), ...
func deliverTag(args []byte) (uint64, bool) {
	if len(args) < 1 {
		return 0, false
	}
	n := int(args[0])
	if len(args) < 1+n+8 {
		return 0, false
	}
	return binary.BigEndian.Uint64(args[1+n : 1+n+8]), true
}

func (t *AMQPTap) delivered(conn int, key chanKey, f *frame) {
	delete(t.pending, key)
	session := t.current[key]
	t.byTag[tagKey{session, f.tag}] = len(t.deliveries)
	t.deliveries = append(t.deliveries, TapDelivery{
		Conn: conn, Session: session, Tag: f.tag, Body: string(bytes.Clone(f.body)),
	})
}

func (t *AMQPTap) answer(conn int, key chanKey, tag uint64, kind string) {
	session := t.current[key]
	a := TapAnswer{Conn: conn, Session: session, Tag: tag, Kind: kind}
	i, ok := t.byTag[tagKey{session, tag}]
	switch {
	case !ok:
		t.violations = append(t.violations, fmt.Sprintf(
			"%s of tag %d on connection %d, session %d: no delivery with that tag on that session", kind, tag, conn, session))
	case t.deliveries[i].Answer != "":
		a.Body = t.deliveries[i].Body
		t.violations = append(t.violations, fmt.Sprintf(
			"%s of tag %d on connection %d, session %d (%s): already answered with %s",
			kind, tag, conn, session, a.Body, t.deliveries[i].Answer))
	default:
		a.Body = t.deliveries[i].Body
		t.deliveries[i].Answer = kind
	}
	t.answers = append(t.answers, a)
}
