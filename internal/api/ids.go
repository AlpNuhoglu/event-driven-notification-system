package api

import (
	"net/http"

	"github.com/google/uuid"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/api/apigen"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
)

// The id path parameters (NotificationID, BatchID) and the batch_id query
// parameter are bound by the generated wrapper as strings (x-go-type in
// api/openapi.yaml): bound as UUIDs, they would go through the UUID
// library's parser, which also accepts urn:uuid:, braces, the 32 digits
// without hyphens and a UUID wrapped in two characters. Every id from a
// request is parsed with domain.ParseID instead, the canonical form only.

// paramID is the name of the id path parameter.
const paramID = "id"

// invalidPathIDProblem is the 400 for a path parameter that is not an id in
// the canonical form.
func invalidPathIDProblem(name string) *problem {
	return newProblem(kindMalformed, "A path parameter is invalid.",
		fieldError(name, codeInvalidUUID, name+" must be a UUID"))
}

// pathID parses the id path parameter.
func pathID(s string) (uuid.UUID, *problem) {
	id, err := domain.ParseID(s)
	if err != nil {
		return uuid.UUID{}, invalidPathIDProblem(paramID)
	}
	return id, nil
}

// requirePathID answers 400 and returns false when the id path parameter is
// not an id. createGuard checks it before anything else of the request, as
// the generated wrapper did when it bound the parameter as a UUID; the
// handler parses it again with pathID.
func requirePathID(w http.ResponseWriter, r *http.Request, s string) bool {
	if _, p := pathID(s); p != nil {
		writeProblem(w, r, p)
		return false
	}
	return true
}

// GetNotification refuses an id that is not one (400), then hands the
// request on to the strict handler.
func (g createGuard) GetNotification(w http.ResponseWriter, r *http.Request, id string, params apigen.GetNotificationParams) {
	if !requirePathID(w, r, id) {
		return
	}
	g.ServerInterface.GetNotification(w, r, id, params)
}

// ListNotificationAttempts refuses an id that is not one (400), then hands
// the request on to the strict handler.
func (g createGuard) ListNotificationAttempts(w http.ResponseWriter, r *http.Request, id apigen.NotificationID, params apigen.ListNotificationAttemptsParams) {
	if !requirePathID(w, r, id) {
		return
	}
	g.ServerInterface.ListNotificationAttempts(w, r, id, params)
}

// GetBatch refuses an id that is not one (400), then hands the request on
// to the strict handler.
func (g createGuard) GetBatch(w http.ResponseWriter, r *http.Request, id string, params apigen.GetBatchParams) {
	if !requirePathID(w, r, id) {
		return
	}
	g.ServerInterface.GetBatch(w, r, id, params)
}
