# Pinned to >= 1.26.2 to pick up Go stdlib CVE patches (CVE-2026-32280/32281/32283/33810).
# Alpine variant kept for small image size and multi-arch builds; binary is statically
# linked (CGO_ENABLED=0) so glibc CVEs in other base images do not apply here.
FROM golang:1.26.4-alpine AS builder
ARG TARGETARCH
ARG VERSION=dev
ARG GIT_COMMIT=unknown
ARG BUILD_DATE=unknown
RUN apk add --no-cache build-base

WORKDIR /src/kubewatch

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=$TARGETARCH go build -a \
    -ldflags="-s -w \
      -X github.com/bitnami-labs/kubewatch/cmd.version=${VERSION} \
      -X github.com/bitnami-labs/kubewatch/cmd.gitCommit=${GIT_COMMIT} \
      -X github.com/bitnami-labs/kubewatch/cmd.buildDate=${BUILD_DATE}" \
    -o /kubewatch

FROM alpine:latest

COPY --from=builder /kubewatch /bin/kubewatch

ENV KW_CONFIG=/opt/bitnami/kubewatch

ENTRYPOINT ["/bin/kubewatch"]
