# Pinned tool and image versions (each defined once).
GO_IMAGE              = golang:1.27.4-trixie
GOLANGCI_LINT_VERSION = v2.15.0
SQLC_VERSION          = v1.31.1
OAPI_CODEGEN_VERSION  = v2.8.2
MIGRATE_VERSION       = v4.20.1
# The Prometheus image of docker-compose.yml; promtool comes from it.
PROMETHEUS_IMAGE      = prom/prometheus:v3.16.0

GOLANGCI_LINT = go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
SQLC          = go run github.com/sqlc-dev/sqlc/cmd/sqlc@$(SQLC_VERSION)
OAPI_CODEGEN  = go run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@$(OAPI_CODEGEN_VERSION)
MIGRATE       = go run -tags postgres github.com/golang-migrate/migrate/v4/cmd/migrate@$(MIGRATE_VERSION)

# The commit the images are built from: the version in every log line and
# span. docker-compose reads it as a build argument (docker-compose.yml).
export GIT_SHA ?= $(shell git rev-parse HEAD 2>/dev/null || echo dev)

COMPOSE_MOCK = docker-compose -f docker-compose.yml -f docker-compose.mock.yml

# make test-e2e runs the stack under its own Compose project, so it never
# reuses or removes the stack of make up-mock. The host ports can be changed
# when the defaults are taken. The tests receive E2E_PROJECT too: the chaos
# scenarios stop, kill and start the stack's services and read their
# /metrics through docker-compose. E2E_OVERRIDE shortens the durations the
# scenarios wait for (the circuit's open period after a provider_fatal
# answer); it is used here only, never by make up-mock.
#
# make test-e2e has two passes, each on a stack of its own that is always
# removed afterwards: the whole suite on the default layout, then the one
# scenario of the per-channel worker layout (E2E_LAYOUT_FILE) under the
# project E2E_LAYOUT_PROJECT. Both passes run; the target fails when either
# fails, and a failed pass leaves its service logs in e2e-logs/ (the second
# in e2e-logs/worker-per-channel/).
E2E_PROJECT        ?= edns-e2e
E2E_TIMEOUT        ?= 10m
E2E_LAYOUT_PROJECT ?= $(E2E_PROJECT)-per-channel
E2E_LAYOUT_TIMEOUT ?= 5m
API_PORT           ?= 8080
PROMETHEUS_PORT    ?= 9090
GATEWAY_PORT       ?= 8082
MOCKPROVIDER_PORT  ?= 8090
JAEGER_UI_PORT     ?= 16686
GRAFANA_PORT       ?= 3000
E2E_OVERRIDE        = test/e2e/docker-compose.e2e.yml
E2E_LAYOUT_FILE     = docker-compose.override.worker-per-channel.yml
E2E_PORTS           = API_PORT=$(API_PORT) GATEWAY_PORT=$(GATEWAY_PORT) PROMETHEUS_PORT=$(PROMETHEUS_PORT) MOCKPROVIDER_PORT=$(MOCKPROVIDER_PORT) JAEGER_UI_PORT=$(JAEGER_UI_PORT) GRAFANA_PORT=$(GRAFANA_PORT)
E2E_COMPOSE         = $(E2E_PORTS) $(COMPOSE_MOCK) -f $(E2E_OVERRIDE) -p $(E2E_PROJECT)
E2E_LAYOUT_COMPOSE  = $(E2E_PORTS) $(COMPOSE_MOCK) -f $(E2E_OVERRIDE) -f $(E2E_LAYOUT_FILE) -p $(E2E_LAYOUT_PROJECT)
E2E_ENV             = E2E_API_URL=http://localhost:$(API_PORT) E2E_GATEWAY_URL=ws://localhost:$(GATEWAY_PORT)/v1/ws E2E_MOCK_URL=http://localhost:$(MOCKPROVIDER_PORT) E2E_PROMETHEUS_URL=http://localhost:$(PROMETHEUS_PORT) E2E_JAEGER_URL=http://localhost:$(JAEGER_UI_PORT)
E2E_GO_TEST         = go test -tags e2e -count=1 -v

# One pass of make test-e2e, in a subshell: start the stack with the Compose
# command $(1), run the tests command $(2) against it, write every service's
# log to the directory $(3) when the pass fails, and always remove the stack
# and its volumes. $(4) names the pass in the failure message. The subshell
# exits with the pass's status.
define e2e_pass
( trap '$(1) down -v --remove-orphans' EXIT; \
  trap 'exit 130' INT TERM; \
  status=0; \
  $(1) up -d --build --wait || status=$$?; \
  if [ $$status -eq 0 ]; then \
    $(E2E_ENV) $(2) || status=$$?; \
  fi; \
  if [ $$status -ne 0 ]; then \
    mkdir -p $(3); \
    $(1) ps -a > $(3)/ps.log 2>&1; \
    for svc in $$($(1) config --services); do \
      $(1) logs --no-color --timestamps $$svc > $(3)/$$svc.log 2>&1; \
    done; \
    echo "test-e2e: $(4) failed (exit $$status); service logs are in $(3)/" >&2; \
  fi; \
  exit $$status )
endef

TEST_CMD = go test -race -tags=integration -count=1 ./...

.DEFAULT_GOAL := help

.PHONY: help build generate lint fmt test test-docker tools image \
	test-e2e load demo up-mock down verify-suite fuzz check-rules

help: ## Show this help
	@echo "Targets:"
	@grep -E '^[a-z0-9-]+:.*## ' $(MAKEFILE_LIST) | \
		sed -e 's/:.*## /|/' | \
		awk -F'|' '{ printf "  %-14s %s\n", $$1, $$2 }'

build: ## Build all packages
	go build ./...

generate: ## Run sqlc and oapi-codegen
	$(SQLC) generate
	$(OAPI_CODEGEN) --config api/oapi-codegen.yaml api/openapi.yaml

lint: ## Run golangci-lint (also checks formatting)
	$(GOLANGCI_LINT) run ./...

fmt: ## Format code with golangci-lint fmt
	$(GOLANGCI_LINT) fmt

test: ## Run all tests with the race detector and integration tag
	$(TEST_CMD)

test-docker: ## Run the tests inside the Go container (cgo, Docker socket)
	docker run --rm \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-v "$(CURDIR)":"$(CURDIR)" -w "$(CURDIR)" \
		--add-host=host.docker.internal:host-gateway \
		-e TESTCONTAINERS_HOST_OVERRIDE=host.docker.internal \
		-e TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock \
		-v edns-gomod:/go/pkg/mod \
		-v edns-gobuild:/root/.cache/go-build \
		$(GO_IMAGE) $(TEST_CMD)

tools: ## Print the version of each pinned tool
	$(GOLANGCI_LINT) version
	$(SQLC) version
	$(OAPI_CODEGEN) --version
	$(MIGRATE) -version

image: ## Build the container image notifications:dev
	docker build --build-arg GIT_SHA=$(GIT_SHA) -t notifications:dev .

test-e2e: ## Compose stack with the mock override: e2e and chaos scenarios, load-smoke, then the per-channel worker layout (needs Go and Docker)
	@rm -rf e2e-logs; \
	trap 'exit 130' INT TERM; \
	status=0; \
	$(call e2e_pass,$(E2E_COMPOSE),E2E_PROJECT=$(E2E_PROJECT) $(E2E_GO_TEST) -timeout $(E2E_TIMEOUT) ./test/e2e/...,e2e-logs,the default layout) || status=$$?; \
	$(call e2e_pass,$(E2E_LAYOUT_COMPOSE),E2E_PROJECT=$(E2E_LAYOUT_PROJECT) E2E_LAYOUT_FILE=$(E2E_LAYOUT_FILE) $(E2E_GO_TEST) -timeout $(E2E_LAYOUT_TIMEOUT) -run '^TestComposeE2E_WorkerPerChannel$$' ./test/e2e/...,e2e-logs/worker-per-channel,the per-channel worker layout) || status=$$?; \
	exit $$status

check-rules: ## Validate and unit-test the Prometheus alert rules (needs Docker)
	docker run --rm --entrypoint promtool -v "$(CURDIR)/deploy/prometheus":/etc/prometheus:ro -w /etc/prometheus $(PROMETHEUS_IMAGE) check rules alerts.yml
	docker run --rm --entrypoint promtool -v "$(CURDIR)/deploy/prometheus":/etc/prometheus:ro -w /etc/prometheus $(PROMETHEUS_IMAGE) test rules alerts_test.yml

load: ## Not implemented yet
	@echo "load: not implemented yet; see docs/plan/implementation-plan.md" >&2; exit 1

demo: ## Send at most 10 notifications through the running stack and follow them (DEMO_ARGS="--count 3")
	@if [ -z "$$(docker-compose ps --status running -q worker 2>/dev/null)" ]; then \
		echo "demo: the stack is not running; start it with docker-compose up -d --wait, or make up-mock for the mock provider" >&2; exit 1; \
	fi
	@docker-compose exec -T worker demo $(DEMO_ARGS)

up-mock: ## Start the stack with mockprovider as the provider
	$(COMPOSE_MOCK) up -d --build --wait

down: ## Stop the stack and remove its volumes
	$(COMPOSE_MOCK) down -v

verify-suite: ## Not implemented yet
	@echo "verify-suite: not implemented yet; see docs/plan/implementation-plan.md" >&2; exit 1

# Time box of each fuzz target. The minimisation of a new input is bounded
# as well: by default the engine may spend up to 60s on each one, and with
# the long inputs of the SMS counter that used up the whole time box.
FUZZTIME     ?= 30s
FUZZMINIMIZE ?= 1000x

fuzz: ## Run each fuzz target for FUZZTIME (default 30s)
	@for t in FuzzCountSMS FuzzDecodeCursor FuzzCursorRoundTrip FuzzParseRetryAfter FuzzCompileTemplate; do \
		echo "fuzz: $$t for $(FUZZTIME)"; \
		go test -run '^$$' -fuzz "^$$t$$" -fuzztime $(FUZZTIME) -fuzzminimizetime $(FUZZMINIMIZE) ./internal/domain/ || exit 1; \
	done
