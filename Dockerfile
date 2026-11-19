FROM golang:1.27.4-alpine3.24 AS build
WORKDIR /src
ENV CGO_ENABLED=0
# go.su[m] copies go.sum only when it exists.
COPY go.mod go.su[m] ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-s -w" -o /out/ \
      ./cmd/api ./cmd/relay ./cmd/worker ./cmd/gateway ./cmd/migrate ./cmd/mockprovider ./cmd/demo

FROM scratch
LABEL org.opencontainers.image.title="event-driven-notification-system" \
      org.opencontainers.image.source="https://github.com/AlpNuhoglu/event-driven-notification-system"
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/ /usr/local/bin/
ENV PATH=/usr/local/bin
USER 65532:65532
EXPOSE 8080 8081 8082
