package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"

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

// errorClass describes an error for the log without risking personal data:
// a Postgres error is reduced to its SQLSTATE (its message can quote an
// input value), every other error keeps its text, which the store builds
// from fixed strings and driver errors.
func errorClass(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return fmt.Sprintf("postgres error, SQLSTATE %s", pgErr.Code)
	}
	return err.Error()
}
