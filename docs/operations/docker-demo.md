# Synthetic Demo with only Docker

This runs the same `ccp-demo` process as the [Demo Quickstart](../product/DEMO-CONSOLE-QUICKSTART.md), on a machine that has Docker Engine and does not have a Go toolchain.

The process serves an in-memory Dashboard, Clusters, and GPUs preview. The data is synthetic. This is not scheduling, not a PostgreSQL or Kubernetes client, not an authenticated Console, and not physical GPU readiness. Do not publish it or put it behind a reverse proxy.

## Listen address

`ccp-demo` accepts only `-addr 127.0.0.1:<port>`. The default is `127.0.0.1:8081`. It rejects `0.0.0.0`, `localhost`, wildcards, and LAN addresses. This image does not change that check.

Docker port publishing (`-p` / `--publish`) delivers connections to the container's non-loopback interface. Nothing in this image listens there, so a published port cannot reach the demo. Do not pass `-addr 0.0.0.0:8081`; the process exits with `demo preview must bind explicitly to 127.0.0.1`.

On Docker Engine for Linux, `--network host` runs the process in the host network namespace. It binds that machine's `127.0.0.1:8081` and still does not listen on other interfaces. Open `http://127.0.0.1:8081` on the same machine.

Docker Desktop on macOS and Windows runs the engine inside a Linux VM. `--network host` binds the VM loopback, which is not the macOS or Windows loopback, so the command below does not open the demo in the desktop browser. Use the Go quickstart on those systems.

## Build

From the repository root:

```bash
docker build -t ccp-demo:local .
```

The builder is the official `golang:1.27.4-bookworm` image, the Go version pinned by GitHub Go validation, with `CGO_ENABLED=0` and `GOTOOLCHAIN=local`. `go.mod` requires Go 1.27.0 or newer. The runtime image is `gcr.io/distroless/static-debian12:nonroot` and runs as uid 65532. The build does not push an image to a registry.

## Run

```bash
docker run -d --name ccp-demo --network host ccp-demo:local
curl -fsS http://127.0.0.1:8081/api/v1/observations
```

A usable response has `"data_mode":"demo"` and every item `"source":"synthetic/demo"`. Then open `http://127.0.0.1:8081` on the same machine.

The image has no shell. `docker logs ccp-demo` shows the startup line `CCP Demo — synthetic observations only`.

If `127.0.0.1:8081` is already in use, choose another loopback port. This replaces the image command:

```bash
docker run -d --name ccp-demo --network host ccp-demo:local -addr 127.0.0.1:18081
```

Open `http://127.0.0.1:18081` on the same machine.

## Stop

```bash
docker stop ccp-demo
docker rm ccp-demo
```

`docker stop` sends SIGTERM. The process shuts down on SIGTERM and SIGINT.

## What the image contains

The entrypoint is `/usr/local/bin/ccp-demo` with `-addr 127.0.0.1:8081`.

The builder also compiles every other `./cmd` program with `CGO_ENABLED=0` and copies those static binaries into the image. The run command above does not start them. They are not a production listener, a migration job, a scheduler, or a GPU check. `ccp-demo` loads no credentials and does not mount workload, provider, host-session, or receipt handlers. It does not need PostgreSQL or Kubernetes.
