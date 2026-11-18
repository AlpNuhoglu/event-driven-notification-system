// Package e4 holds experiment E4: connection and topology recovery in
// github.com/rabbitmq/amqp091-go against a RabbitMQ broker that is stopped,
// replaced by an empty one, or kept down until recovery gives up.
//
// All evidence lives in the integration tests of this package:
//
//	go test -tags=integration -count=1 -v -timeout 20m ./...
package e4
