package domain

import "testing"

func TestRoutingKey(t *testing.T) {
	e := StatusChanged{Channel: ChannelSMS, Status: StatusQueued}
	if got := e.RoutingKey(); got != "status.sms.queued" {
		t.Errorf("RoutingKey = %q", got)
	}
}
