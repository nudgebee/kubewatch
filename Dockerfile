# Use Golang Alpine as the builder stage
FROM golang:alpine AS builder

# Install dependencies
RUN apk add --no-cache build-base

# Set up working directory
WORKDIR /src/kubewatch

# Copy source code
COPY . .

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -ldflags="-s -w" -o /kubewatch

# Use Alpine as the final image
FROM alpine:latest

# Copy the built binary
COPY --from=builder /kubewatch /bin/kubewatch

# Set configuration directory
ENV KW_CONFIG=/opt/bitnami/kubewatch

# Set entrypoint
ENTRYPOINT ["/bin/kubewatch"]
