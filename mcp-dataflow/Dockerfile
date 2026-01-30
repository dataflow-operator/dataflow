# Build stage
FROM golang:1.25-alpine AS builder

WORKDIR /workspace

# Install dependencies
RUN apk add --no-cache git make

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build MCP server
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -o mcp-dataflow cmd/server/main.go

# Final stage
FROM alpine:latest

WORKDIR /

RUN apk --no-cache add ca-certificates

COPY --from=builder /workspace/mcp-dataflow .

ENTRYPOINT ["/mcp-dataflow"]
