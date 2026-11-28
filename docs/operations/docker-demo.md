# Synthetic Demo with only Docker

This runs the same `ccp-demo` process as the [Demo Quickstart](../product/DEMO-CONSOLE-QUICKSTART.md), on a machine that has Docker Engine and does not have a Go toolchain.

The process serves an in-memory Dashboard, Clusters, and GPUs preview. The data is synthetic. This is not scheduling, not a PostgreSQL or Kubernetes client, not an authenticated Console, and not physical GPU readiness. Do not publish it or put it behind a reverse proxy.

## Listen address

`ccp-demo` accepts only `-addr 127.0.0.1:<port>`. The default is `127.0.0.1:8081`. It rejects `0.0.0.0`, `localhost`, wildcards, and LAN addresses. This image does not change that check.

Docker port publishing (`-p` / `--publish`) delivers connections to the container's non-loopback interface. Nothing in this image listens there, so a published port cannot reach the demo. Do not pass `-addr 0.0.0.0:8081`; the process exits with `demo preview must bind explicitly to 127.0.0.1`.

On Linux with a local, standard (rootful) Docker Engine daemon, `--network host` runs the process in the host network namespace. It binds that machine's `127.0.0.1:8081` and still does not listen on other interfaces. Open `http://127.0.0.1:8081` on the machine running the Docker daemon, not on a client using a remote Docker context. Do not expose the port to reach it remotely.

This Docker procedure was verified on Linux with the standard daemon. It does not require the container process to run as root.

- **Rootless Docker Engine on Linux:** Before 29.5, [host networking stays inside RootlessKit](https://docs.docker.com/engine/security/rootless/troubleshoot/#nethost-doesnt-listen-ports-on-the-host-network-namespace), so the host browser cannot reach the Demo. This image has not been verified on rootless Engine 29.5 or later. Use the [Go quickstart](../product/DEMO-CONSOLE-QUICKSTART.md) instead of changing your daemon settings.
- **Docker Desktop on macOS or Windows:** Version 4.34 and later offers [opt-in host networking for Linux containers](https://docs.docker.com/engine/network/drivers/host/#docker-desktop). This Demo has not been verified there for desktop-browser access or LAN isolation. Use the [Go quickstart](../product/DEMO-CONSOLE-QUICKSTART.md).

## Build

From the repository root:

```bash
docker build -t ccp-demo:local .
```

The builder is the official `golang:1.27.4-bookworm` image, the Go version pinned by GitHub Go validation, with `CGO_ENABLED=0` and `GOTOOLCHAIN=local`. `go.mod` requires Go 1.27.0 or newer. The runtime image is `gcr.io/distroless/static-debian12:nonroot` and runs as uid 65532. The build does not push an image to a registry.

## Run

```bash
docker run -d --name ccp-demo --network host ccp-demo:local
curl -fsS --retry 10 --retry-connrefused --retry-delay 1 --max-time 2 \
  http://127.0.0.1:8081/api/v1/observations
```

A usable response has `"data_mode":"demo"` and every item `"source":"synthetic/demo"`. Then open `http://127.0.0.1:8081` on the same machine.

The image has no shell. `docker logs ccp-demo` shows the startup line `CCP Demo — synthetic observations only`.

If the startup check fails, run `docker logs ccp-demo` and check for a bind error. Docker may keep the stopped container even when the process could not start. If it is the failed container from the command above, remove it with `docker rm ccp-demo`; do not remove an unrelated container with the same name.

If `127.0.0.1:8081` is already in use, choose another loopback port and use that same port for all three steps: start, probe, and open.

```bash
docker run -d --name ccp-demo --network host ccp-demo:local -addr 127.0.0.1:18081
curl -fsS --retry 10 --retry-connrefused --retry-delay 1 --max-time 2 \
  http://127.0.0.1:18081/api/v1/observations
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
