.PHONY: default build docker-image test stop clean-images clean

BINARY = kubewatch

VERSION=
BUILD=

PKG            = github.com/bitnami-labs/kubewatch
GIT_COMMIT    ?= `git describe --tags`
VERSION       ?= dev
GOCMD          = go
BUILD_DATE     = `date +%FT%T%z`
GOFLAGS       ?= $(GOFLAGS:)
LDFLAGS       := "-X '$(PKG)/cmd.version=$(VERSION)' \
		          -X '$(PKG)/cmd.gitCommit=$(GIT_COMMIT)' \
		          -X '$(PKG)/cmd.buildDate=$(BUILD_DATE)'"

default: build test

build:
	"$(GOCMD)" build ${GOFLAGS} -ldflags ${LDFLAGS} -o "${BINARY}"

docker-image:
	@docker build -t "${BINARY}" .

test:
	"$(GOCMD)" test -race -v ./...

stop:
	@docker stop "${BINARY}"

clean-images: stop
	@docker rmi "${BUILDER}" "${BINARY}"

clean:
	"$(GOCMD)" clean -i
