# Pinned to a current Go patch to pick up stdlib CVE fixes (1.27.3 clears the
# stdlib CVEs that 1.26.5 carries).
# The builder stays on Alpine (small, multi-arch); the binary is statically linked
# (CGO_ENABLED=0), so only the UBI runtime stage below ships in the image.
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

# --- certs: the only thing the runtime needs on top of ubi-micro ---
# Only files are copied out of this stage, nothing is installed over ubi-micro: installing
# packages there rewrites files the base owns, which fails Red Hat's HasModifiedFiles check.
# Bumping OS_PKG_EPOCH changes this layer's cache key so the registry cache can't serve a
# stale CA bundle.
FROM registry.access.redhat.com/ubi9 AS certs
ARG OS_PKG_EPOCH=2026-W48
RUN echo "os-pkg-epoch: ${OS_PKG_EPOCH}" \
 && dnf upgrade -y ca-certificates \
 && mkdir /kubewatch-config

# Runtime on Red Hat UBI 9 micro (Red Hat container certification). The binary is static,
# so the CA bundle (TLS to the Kubernetes API and notification endpoints) is all it needs.
FROM registry.access.redhat.com/ubi9/ubi-micro
ARG IMAGE_VERSION=0.0.0
ARG IMAGE_RELEASE=1
LABEL name="nudgebee-kubewatch" \
      maintainer="nudgebee" \
      vendor="nudgebee" \
      version="${IMAGE_VERSION}" \
      release="${IMAGE_RELEASE}" \
      summary="Nudgebee kubewatch" \
      description="Kubernetes event watcher that forwards resource changes to the Nudgebee agent."
COPY --from=certs /etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem /etc/pki/tls/certs/ca-bundle.crt

# KW_CONFIG dir: group 0 and group-writable, so an arbitrary OpenShift uid (always gid 0)
# can use it if no ConfigMap is mounted over it.
COPY --from=certs --chown=1000:0 --chmod=0775 /kubewatch-config /opt/bitnami/kubewatch

# Red Hat certification (HasLicense): license text must be under /licenses.
COPY LICENSE /licenses/LICENSE

# Root-owned, world-readable/executable: the runtime uid can run but not overwrite the
# binary (container immutability). --chmod avoids a second layer from a RUN chmod.
COPY --chmod=0755 --from=builder /kubewatch /bin/kubewatch

ENV KW_CONFIG=/opt/bitnami/kubewatch

# Numeric uid so runAsNonRoot can be verified without an /etc/passwd entry.
USER 1000

ENTRYPOINT ["/bin/kubewatch"]
