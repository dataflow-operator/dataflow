# Build stage. Контекст — каталог dataflow (docker build из dataflow/ или -f dataflow/Dockerfile dataflow).
FROM golang:1.25-alpine AS builder

WORKDIR /workspace

RUN apk add --no-cache git make

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

ARG VERSION=dev
ARG TARGETARCH=amd64
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux GOARCH=$TARGETARCH sh -c "\
    go build -o manager -ldflags \"-X github.com/dataflow-operator/dataflow/internal/version.Version=${VERSION}\" main.go && \
    go build -o processor cmd/processor/main.go"
# Final stage
FROM alpine:3.19

WORKDIR /

RUN apk --no-cache add ca-certificates

COPY --from=builder /workspace/manager .
COPY --from=builder /workspace/processor .

ENTRYPOINT ["/manager"]
