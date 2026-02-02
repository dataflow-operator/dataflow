# Build stage
FROM golang:1.25-alpine AS builder

WORKDIR /workspace

# Install dependencies
RUN apk add --no-cache git make

# Copy go mod files
COPY go.mod go.mod
COPY go.sum go.sum

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build manager (operator)
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -o manager main.go

# Build processor
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -o processor cmd/processor/main.go

# Build GUI server
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -o gui-server cmd/gui-server/main.go

# Final stage
FROM alpine:latest

WORKDIR /

RUN apk --no-cache add ca-certificates

COPY --from=builder /workspace/manager .
COPY --from=builder /workspace/processor .
COPY --from=builder /workspace/gui-server .
# Статика для GUI (gui-server обслуживает ./web/static при WORKDIR /)
COPY --from=builder /workspace/web /web

ENTRYPOINT ["/manager"]



