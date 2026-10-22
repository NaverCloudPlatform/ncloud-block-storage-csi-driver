# Ncloud Block Storage CSI Driver
# Copyright (c) 2026-present NAVER Cloud Corp.
# Apache-2.0
#
# Derived from the digitalocean/csi-digitalocean Makefile (Apache-2.0),
# forked at v0.4.1 and merged up to v1.1.2, Copyright 2018 DigitalOcean,
# through the in-house nks-csi fork, and modified by NAVER Cloud Corp.

BINDIR := bin
APP_NAME=ncloud-block-storage-csi-driver
OS ?= linux

ifeq ($(strip $(shell git status --porcelain 2>/dev/null)),)
  GIT_TREE_STATE=clean
else
  GIT_TREE_STATE=dirty
endif

# Evaluate once so compile, image, and push all use the same value.
# Override for dev builds: make image VERSION=v1.0.0-dev1 REGISTRY=...
ifndef VERSION
VERSION := $(shell cat VERSION)
endif
COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null; if [ $$? -ne 0 ]; then echo unknown; fi)
BRANCH ?= $(shell git rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)
LDFLAGS ?= -X github.com/NaverCloudPlatform/ncloud-block-storage-csi-driver/pkg/driver.version=${VERSION} -X github.com/NaverCloudPlatform/ncloud-block-storage-csi-driver/pkg/driver.commit=${COMMIT} -X github.com/NaverCloudPlatform/ncloud-block-storage-csi-driver/pkg/driver.gitTreeState=${GIT_TREE_STATE}

REGISTRY ?=

all: test

publish: compile image push clean

CHART_YAML := deploy/chart/$(APP_NAME)/Chart.yaml

# Bumps VERSION, the Helm chart version/appVersion, and CHANGELOG.md
# together. All preconditions are checked before any file is written so a
# failed run leaves the tree untouched.
.PHONY: bump-version
bump-version:
	@[ "${NEW_VERSION}" ] || ( echo "NEW_VERSION must be set (ex. make NEW_VERSION=v1.2.3 bump-version)"; exit 1 )
	@echo "${NEW_VERSION}" | grep -Eq '^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z][0-9A-Za-z.-]*)?$$' \
		|| ( echo "NEW_VERSION must be a semver with a 'v' prefix (ex. v1.2.3, v1.2.3-rc.1)"; exit 1 )
	@grep -q '^## unreleased$$' CHANGELOG.md || ( echo "CHANGELOG.md must contain an '## unreleased' heading"; exit 1 )
	@grep -q '^version:' $(CHART_YAML) || ( echo "$(CHART_YAML) must contain a 'version:' field"; exit 1 )
	@grep -q '^appVersion:' $(CHART_YAML) || ( echo "$(CHART_YAML) must contain an 'appVersion:' field"; exit 1 )
	$(eval NEW_VERSION_NO_V := $(patsubst v%,%,$(NEW_VERSION)))
	$(eval NEW_DATE = $(shell date +%Y.%m.%d))
	@echo "Bumping version from $(VERSION) to $(NEW_VERSION)"
	@sed -e 's/^## unreleased$$/## ${NEW_VERSION} - ${NEW_DATE}/' CHANGELOG.md \
		| awk 'NR==1{print; print ""; print "## unreleased"; next} {print}' > CHANGELOG.md.tmp
	@sed -e 's/^version:.*/version: $(NEW_VERSION_NO_V)/' \
		-e 's/^appVersion:.*/appVersion: "$(NEW_VERSION_NO_V)"/' $(CHART_YAML) > $(CHART_YAML).tmp
	@mv CHANGELOG.md.tmp CHANGELOG.md
	@mv $(CHART_YAML).tmp $(CHART_YAML)
	@echo $(NEW_VERSION) > VERSION

.PHONY: compile
compile:
	GOOS=linux GOARCH=amd64 \
	CGO_ENABLED=0 GO111MODULE=on \
	go build -ldflags "$(LDFLAGS)" \
	-o $(BINDIR)/$(APP_NAME) ./cmd

.PHONY: test
test:
	@echo "==> Testing all packages"
	@go test -v ./pkg/...

.PHONY: image
image:
ifndef REGISTRY
	$(error REGISTRY is not set. Usage: make REGISTRY=your.registry.com image)
endif
	@echo "==> Building the docker image"
	@docker buildx build --platform linux/amd64 \
		--build-arg VERSION=$(VERSION) \
		--build-arg COMMIT=$(COMMIT) \
		--build-arg GIT_TREE_STATE=$(GIT_TREE_STATE) \
		-t $(REGISTRY)/$(APP_NAME):$(VERSION) \
		--load \
		-f Dockerfile .

.PHONY: push
push:
ifndef REGISTRY
	$(error REGISTRY is not set. Usage: make REGISTRY=your.registry.com push)
endif
	@echo "==> Publishing $(REGISTRY)/$(APP_NAME):$(VERSION)"
	@docker push $(REGISTRY)/$(APP_NAME):$(VERSION)
	@echo "==> Your image is now available at $(REGISTRY)/$(APP_NAME):$(VERSION)"

.PHONY: clean
clean:
	@echo "==> Cleaning releases"
	@GOOS=${OS} go clean -i -x ./...
