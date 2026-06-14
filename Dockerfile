# syntax=docker/dockerfile:1.4
# Build stage. Контекст — каталог dataflow (docker build из dataflow/ или -f dataflow/Dockerfile dataflow).
FROM golang:1.25-alpine AS builder

WORKDIR /workspace

RUN apk add --no-cache git

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

ARG VERSION=dev
ARG TARGETARCH=amd64
# -trimpath убирает пути из бинарника, -ldflags -s -w уменьшает размер
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux GOARCH=$TARGETARCH go build -trimpath -ldflags "-s -w -X github.com/dataflow-operator/dataflow/internal/version.Version=${VERSION}" \
    -o manager main.go && \
    CGO_ENABLED=0 GOOS=linux GOARCH=$TARGETARCH go build -trimpath -ldflags "-s -w" \
    -o processor ./cmd/processor
# Final stage
FROM alpine:3.19

WORKDIR /

RUN apk --no-cache add ca-certificates

COPY --from=builder /workspace/manager .
COPY --from=builder /workspace/processor .

ENTRYPOINT ["/manager"]
