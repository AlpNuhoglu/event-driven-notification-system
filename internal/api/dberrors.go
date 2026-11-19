package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	htmltemplate "html/template"
	"io"
	"net"
	"text/template"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/puddle/v2"
)

// dbUnavailable reports whether err means that Postgres cannot be used right
// now (answered with 503), as opposed to an unexpected error (500): the
// connection cannot be made or was lost, the pool is closed, the time budget
// ran out, or the server reports a connection, resource, operator or
// catalog problem (SQLSTATE classes 08, 53, 57, 3D and 28).
func dbUnavailable(err error) bool {
	if err == nil {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if len(pgErr.Code) < 2 {
			return false
		}
		switch pgErr.Code[:2] {
		case "08", "53", "57", "3D", "28":
			return true
		}
		return false
	}
	var connectErr *pgconn.ConnectError
	var netErr net.Error
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled),
		errors.Is(err, puddle.ErrClosedPool),
		errors.As(err, &connectErr), errors.As(err, &netErr),
		errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF),
		pgconn.Timeout(err), pgconn.SafeToRetry(err):
		return true
	}
	return false
}

// storeProblem maps an error of the store to its problem: 503 when Postgres
// is unavailable, 500 otherwise. The cause is kept for the log line only.
func storeProblem(err error) *problem {
	p := newProblem(kindInternal, "")
	if dbUnavailable(err) {
		p = newProblem(kindUnavailable, "")
	}
	p.cause = err
	return p
}

// errorClass describes an error for the log by its class, never by its
// text: the text of a Postgres error, a JSON decoder's or a template's can
// quote an input value (a request body, a template variable), so no error
// text reaches the log line. A Postgres error is reduced to its SQLSTATE;
// an error of no known class is named by the type of the innermost error
// of its chain.
func errorClass(err error) string {
	var (
		pgErr      *pgconn.PgError
		connectErr *pgconn.ConnectError
		netErr     net.Error
		syntaxErr  *json.SyntaxError
		typeErr    *json.UnmarshalTypeError
		valueErr   *json.UnsupportedValueError
		marshalErr *json.MarshalerError
		execErr    template.ExecError
		htmlErr    *htmltemplate.Error
	)
	switch {
	case errors.As(err, &pgErr):
		return fmt.Sprintf("postgres error, SQLSTATE %s", pgErr.Code)
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline exceeded"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, puddle.ErrClosedPool):
		return "postgres pool closed"
	case errors.As(err, &connectErr):
		return "postgres connect error"
	case errors.As(err, &netErr), errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return "connection error"
	case errors.As(err, &syntaxErr), errors.As(err, &typeErr), errors.As(err, &valueErr), errors.As(err, &marshalErr):
		return "json error"
	case errors.As(err, &execErr):
		return "template execution error"
	case errors.As(err, &htmlErr):
		return "template escaping error"
	}
	root := err
	for {
		next := errors.Unwrap(root)
		if next == nil {
			break
		}
		root = next
	}
	return fmt.Sprintf("error of type %T", root)
}
