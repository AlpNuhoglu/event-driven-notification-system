package relay

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/bootstrap"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/config"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
)

// DefaultRequeueOlderThan is the threshold of requeue-stale without
// --older-than (D64).
const DefaultRequeueOlderThan = 5 * time.Minute

// Main is the relay binary: the relay role, its healthcheck, and the
// requeue-stale command. It returns the process exit code.
func Main(ctx context.Context, args []string, lookup func(string) (string, bool), stdout, stderr io.Writer) int {
	return bootstrap.RunCommands(ctx, "relay", args, lookup, stdout, stderr, Service, RequeueStaleCommand())
}

// RequeueStaleCommand is "relay requeue-stale [--older-than=5m] [--all]"
// (ADR-0003 §3, D64), run as "docker-compose run --rm relay requeue-stale":
// transition 8 on demand, for the queued rows that reconciliation does not
// cover (a message dead-lettered, or lost while the connection stayed up).
// It connects to Postgres only, so it runs next to a live relay, returns the
// queued rows older than the threshold (every queued row with --all) to
// pending, and prints how many. The normal cycle of the running relay
// publishes them again.
//
// Exit codes: 0 done; 1 a configuration or database error; 2 a usage error
// (an unknown flag or argument, a negative or unparsable duration, both
// flags).
func RequeueStaleCommand() bootstrap.Command {
	return bootstrap.Command{
		Name:  "requeue-stale",
		Usage: "requeue-stale [--older-than=5m] [--all]",
		Parse: func(args []string) (bootstrap.Action, error) {
			a, err := parseRequeueStale(args)
			if err != nil {
				return nil, err
			}
			return a.run, nil
		},
	}
}

// requeueStale is a parsed requeue-stale command.
type requeueStale struct {
	olderThan time.Duration
	all       bool
}

func parseRequeueStale(args []string) (requeueStale, error) {
	fs := flag.NewFlagSet("requeue-stale", flag.ContinueOnError)
	fs.SetOutput(io.Discard) // the caller prints the error and the usage
	a := requeueStale{}
	fs.DurationVar(&a.olderThan, "older-than", DefaultRequeueOlderThan, "")
	fs.BoolVar(&a.all, "all", false, "")
	if err := fs.Parse(args); err != nil {
		return requeueStale{}, err // the flag package's message names the flag
	}
	if fs.NArg() > 0 {
		return requeueStale{}, fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}
	set := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { set[f.Name] = true })
	if set["all"] && set["older-than"] {
		return requeueStale{}, errors.New("--all and --older-than cannot be used together")
	}
	if a.olderThan < 0 {
		return requeueStale{}, fmt.Errorf("--older-than must not be negative, got %s", a.olderThan)
	}
	return a, nil
}

// run applies the command on the database of cfg. The events of the rows go
// to the role's event publisher, as for every transition.
func (a requeueStale) run(ctx context.Context, cfg config.Config, stdout, stderr io.Writer) int {
	pool, err := openPool(ctx, cfg)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "requeue-stale: postgres: %v\n", err)
		return 1
	}
	defer pool.Close()
	st := store.New(pool, store.ProfilesFrom(cfg.Retry))
	var evs []domain.StatusChanged
	if a.all {
		evs, err = st.ReconcileQueued(ctx)
	} else {
		evs, err = st.RequeueStale(ctx, a.olderThan)
	}
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "requeue-stale: %v\n", err)
		return 1
	}
	ev := eventPublisher()
	for _, e := range evs {
		ev.Publish(e)
	}
	_, _ = fmt.Fprintf(stdout, "requeue-stale: %d notifications returned to pending\n", len(evs))
	return 0
}
