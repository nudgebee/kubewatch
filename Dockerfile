# Pinned to a current Go patch to pick up stdlib CVE fixes (1.26.5 clears
# CVE-2026-42505/42504/42507/39822/27145 that 1.26.3 carries).
# Alpine variant kept for small image size and multi-arch builds; binary is statically
# linked (CGO_ENABLED=0) so glibc CVEs in other base images do not apply here.
FROM golang:1.27.3-alpine AS builder
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

# Patch the base's OS packages (e.g. libcrypto3/libssl3) to the latest Alpine
# release — the alpine:latest tag snapshot lags, so without this the image ships
# stale openssl CVEs even though the binary is static.
RUN apk upgrade --no-cache

COPY --from=builder /kubewatch /bin/kubewatch

ENV KW_CONFIG=/opt/bitnami/kubewatch

ENTRYPOINT ["/bin/kubewatch"]
