// Package main: minimal strict-server implementation for experiment E2.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	openapi_types "github.com/oapi-codegen/runtime/types"

	"github.com/AlpNuhoglu/event-driven-notification-system/experiments/e2/api"
)

const (
	knownID   = "11111111-1111-1111-1111-111111111111"
	unknownID = "00000000-0000-0000-0000-000000000404"
)

type server struct{}

func (server) CreateThing(_ context.Context, req api.CreateThingRequestObject) (api.CreateThingResponseObject, error) {
	if req.Body.Name == "boom" {
		return nil, errors.New("boom: handler failed")
	}
	return api.CreateThing201JSONResponse{Id: openapi_types.UUID{}, Name: req.Body.Name}, nil
}

func (server) GetThing(_ context.Context, req api.GetThingRequestObject) (api.GetThingResponseObject, error) {
	if req.Id.String() == unknownID {
		d := "no such thing"
		return api.GetThing404ApplicationProblemPlusJSONResponse{
			Type: "about:blank", Title: "Not Found", Status: http.StatusNotFound, Detail: &d,
		}, nil
	}
	return api.GetThing200JSONResponse{Id: req.Id, Name: "thing"}, nil
}

func writeProblem(w http.ResponseWriter, status int, detail string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(api.Problem{
		Type: "about:blank", Title: http.StatusText(status), Status: status, Detail: &detail,
	})
}

// newHandler wires strict handler -> generated std-http handler on a ServeMux.
func newHandler() http.Handler {
	strict := api.NewStrictHandlerWithOptions(server{}, nil, api.StrictHTTPServerOptions{
		RequestErrorHandlerFunc: func(w http.ResponseWriter, _ *http.Request, err error) {
			writeProblem(w, http.StatusBadRequest, err.Error())
		},
		ResponseErrorHandlerFunc: func(w http.ResponseWriter, _ *http.Request, _ error) {
			writeProblem(w, http.StatusInternalServerError, "internal error")
		},
	})
	return api.HandlerWithOptions(strict, api.StdHTTPServerOptions{
		BaseRouter: http.NewServeMux(),
		ErrorHandlerFunc: func(w http.ResponseWriter, _ *http.Request, err error) {
			writeProblem(w, http.StatusBadRequest, err.Error())
		},
	})
}

func main() {
	srv := &http.Server{Addr: ":8080", Handler: newHandler()}
	_ = srv.ListenAndServe()
}
