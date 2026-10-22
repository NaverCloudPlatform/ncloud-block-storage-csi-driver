# Ncloud Block Storage CSI Driver
# Copyright (c) 2026-present NAVER Cloud Corp.
# Apache-2.0
#
# Derived from the digitalocean/csi-digitalocean Dockerfile (Apache-2.0),
# Copyright 2018 DigitalOcean, through the in-house nks-csi fork, and
# rewritten as a multi-stage build by NAVER Cloud Corp.

# Builder pinned by digest like the runtime base: a floating tag that
# resolves below go.mod's toolchain would silently pull the toolchain
# from the network (or fail offline). GOTOOLCHAIN=local makes any such
# drift fail loudly instead.
FROM --platform=$BUILDPLATFORM golang:1.26.10@sha256:672c944cfd07599b5f1bb179869270cdc94c6254e41db4d431259693cb286b82 AS builder
ENV GOTOOLCHAIN=local

WORKDIR /go/src/github.com/NaverCloudPlatform/ncloud-block-storage-csi-driver/

# The build context is filtered by .dockerignore (no hack/, .git/, docs),
# and copying the tree keeps one Dockerfile working for both layouts:
# vendor/ is used automatically when present, and a tree without it
# resolves modules from the network instead.
COPY . .

ARG VERSION=dev
ARG COMMIT=unknown
ARG GIT_TREE_STATE=clean
ARG TARGETOS=linux
ARG TARGETARCH=amd64

RUN GOOS=${TARGETOS} GOARCH=${TARGETARCH} CGO_ENABLED=0 GO111MODULE=on \
    go build \
    -ldflags "-X github.com/NaverCloudPlatform/ncloud-block-storage-csi-driver/pkg/driver.version=${VERSION} -X github.com/NaverCloudPlatform/ncloud-block-storage-csi-driver/pkg/driver.commit=${COMMIT} -X github.com/NaverCloudPlatform/ncloud-block-storage-csi-driver/pkg/driver.gitTreeState=${GIT_TREE_STATE}" \
    -o bin/ncloud-block-storage-csi-driver ./cmd

# Runtime base pinned by digest (alpine:3.24, supported until 2028-05).
# Bump the tag+digest together; EOL'd bases stop receiving security fixes.
FROM alpine:3.24@sha256:28bd5fe8b56d1bd048e5babf5b10710ebe0bae67db86916198a6eec434943f8b
# Build args are per stage; restate the two the labels use.
ARG VERSION=dev
ARG COMMIT=unknown
LABEL org.opencontainers.image.title="ncloud-block-storage-csi-driver" \
      org.opencontainers.image.source="https://github.com/NaverCloudPlatform/ncloud-block-storage-csi-driver" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${COMMIT}"
# mount/umount must be the util-linux implementations: busybox mount does
# not apply the read-only remount for bind mounts, so a readOnly publish
# would silently stay writable.
RUN apk add --no-cache ca-certificates \
                       e2fsprogs \
                       e2fsprogs-extra \
                       findmnt \
                       blkid \
                       mount \
                       umount \
                       xfsprogs \
                       xfsprogs-extra

COPY --from=builder /go/src/github.com/NaverCloudPlatform/ncloud-block-storage-csi-driver/bin/ncloud-block-storage-csi-driver \
    /bin/

ENTRYPOINT ["/bin/ncloud-block-storage-csi-driver"]
