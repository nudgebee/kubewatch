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

# --- rootfs: the only OS packages the runtime needs ---
# ubi-micro has no package manager, so the packages are installed into /rootfs here and
# copied over. ca-certificates: TLS to the Kubernetes API and notification endpoints;
# tzdata: time zones. Bumping OS_PKG_EPOCH changes this layer's cache key so the
# registry cache can't serve a stale package layer.
FROM registry.access.redhat.com/ubi9 AS rootfs-builder
ARG OS_PKG_EPOCH=2026-W44
RUN echo "os-pkg-epoch: ${OS_PKG_EPOCH}" \
 && dnf install -y --installroot /rootfs --releasever 9 --setopt=install_weak_deps=false --nodocs \
      ca-certificates tzdata \
 && dnf --installroot /rootfs clean all && rm -rf /rootfs/var/cache/*
# KW_CONFIG dir: group 0 and group-writable, so an arbitrary OpenShift uid (always gid 0)
# can use it if no ConfigMap is mounted over it.
RUN mkdir -p /rootfs/opt/bitnami/kubewatch \
 && chown -R 1000:0 /rootfs/opt/bitnami/kubewatch && chmod -R g=u /rootfs/opt/bitnami/kubewatch

# Runtime on Red Hat UBI 9 micro (Red Hat container certification). The binary is static,
# so the CA bundle and tzdata from the rootfs stage are all it needs from the OS.
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
COPY --from=rootfs-builder /rootfs/ /

# Red Hat certification (HasLicense): license text must be under /licenses.
COPY LICENSE /licenses/LICENSE

# Root-owned, world-readable/executable: the runtime uid can run but not overwrite the
# binary (container immutability). --chmod avoids a second layer from a RUN chmod.
COPY --chmod=0755 --from=builder /kubewatch /bin/kubewatch

ENV KW_CONFIG=/opt/bitnami/kubewatch

# Numeric uid so runAsNonRoot can be verified without an /etc/passwd entry.
USER 1000

ENTRYPOINT ["/bin/kubewatch"]
