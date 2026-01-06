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

# Build
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -o manager main.go

# Final stage
FROM alpine:latest

WORKDIR /

RUN apk --no-cache add ca-certificates

COPY --from=builder /workspace/manager .

ENTRYPOINT ["/manager"]



