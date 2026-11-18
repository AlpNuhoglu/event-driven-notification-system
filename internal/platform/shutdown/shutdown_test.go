package shutdown

import (
	"context"
	"testing"
)

func TestParentCancelCancelsChild(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	ctx, stop := Context(parent)
	defer stop()

	cancel()
	<-ctx.Done()
	if ctx.Err() == nil {
		t.Fatal("child context not cancelled")
	}
}
