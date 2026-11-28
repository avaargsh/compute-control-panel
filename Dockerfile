# Synthetic ccp-demo image.
# Builder pin matches GitHub Go validation (Go 1.27.4, GOTOOLCHAIN=local).
# go.mod requires Go 1.27.0 or newer.
# The runtime process is the loopback-only synthetic demo. It is not a
# production listener and it does not report physical GPU readiness.

FROM golang:1.27.4-bookworm@sha256:ce59b41415e2524ae99926fe1f37ce48ba8c194dcbd2a56a65436cdcefa2bd34 AS build

WORKDIR /src
ENV CGO_ENABLED=0 \
    GOTOOLCHAIN=local

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN mkdir -p /out \
    && go build -trimpath -o /out/ccp-demo ./cmd/ccp-demo \
    && go build -trimpath -o /out/compute-control-panel ./cmd/compute-control-panel \
    && go build -trimpath -o /out/migrate ./cmd/migrate \
    && go build -trimpath -o /out/kueue-observe ./cmd/kueue-observe \
    && go build -trimpath -o /out/kueue-provider-smoke ./cmd/kueue-provider-smoke \
    && go build -trimpath -o /out/pd-jobset-provider-smoke ./cmd/pd-jobset-provider-smoke

FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab

ARG CCP_SOURCE_REVISION=unknown
LABEL org.opencontainers.image.revision="${CCP_SOURCE_REVISION}"

COPY --from=build --chown=65532:65532 /out/ /usr/local/bin/

USER 65532:65532
WORKDIR /home/nonroot
ENTRYPOINT ["/usr/local/bin/ccp-demo"]
CMD ["-addr", "127.0.0.1:8081"]
