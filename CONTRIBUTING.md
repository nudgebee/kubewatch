# Contributing

Thanks for your interest in contributing. This repository is a fork of
[`robusta-dev/kubewatch`](https://github.com/robusta-dev/kubewatch); see
the README for what differs in this fork.

## Building locally

Requires Go matching the version in `go.mod` (currently 1.26.3).

```bash
go build ./...
go test ./...
```

To build the container image:

```bash
docker build -t kubewatch:dev .
```

The image is statically linked (`CGO_ENABLED=0`) and supports `linux/amd64`
and `linux/arm64`. CI builds both via `docker buildx`.

## Pull request process

1. Open a PR against `master`.
2. Fill out the PR template. The `pr-description-check` workflow requires
   a `## Tests performed` section with at least one bullet.
3. CI must pass (`go build`, `go test`).
4. A code owner reviews and merges.

## Releases

Releases are cut by pushing a semver tag:

```bash
git tag v1.2.3
git push origin v1.2.3
```

This triggers `build.yaml`, which:

- Publishes a multi-arch image to `ghcr.io/nudgebee/kubewatch:v1.2.3`,
  `:1.2`, and `:latest` (or only the version tag for pre-releases like
  `v1.2.3-rc1`).
- Auto-creates a GitHub Release with notes generated from the PRs merged
  since the previous tag.

The `version`, `gitCommit`, and `buildDate` are injected into the binary
via `-ldflags`; run `kubewatch version` to inspect.

## Tracking upstream

We periodically merge `robusta-dev/kubewatch` master into this fork. Some
upstream files are intentionally not present in this fork (advanced
filtering, the metrics package, certain CI workflows). When merging, those
will reappear from the upstream side and need to be re-removed as part of
the conflict resolution. Document anything non-obvious in the merge
commit message.

## Reporting issues

For bugs and feature requests, open a GitHub issue. For security
vulnerabilities, see [SECURITY.md](./SECURITY.md).
