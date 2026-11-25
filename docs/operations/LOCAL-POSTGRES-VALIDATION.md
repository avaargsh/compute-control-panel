# Local PostgreSQL validation

This procedure reproduces the PostgreSQL-backed steps of the GitHub Actions
workflow [`.github/workflows/go.yml`](../../.github/workflows/go.yml), job
`verify`. `make test` runs `go test -race -count=1 ./...` and skips tests that
need `DATABASE_URL` when that variable is unset. The `verify` job does not
skip those tests: it starts PostgreSQL 16, provisions roles, applies
migrations twice, runs `migrations/tests/0001_compute_core_constraints.sql`,
runs the race suite with `DATABASE_URL` set, and repeats the SIGKILL recovery
test.

The commands below were executed on 2026-11-25 against commit
`f69714346419ba73cd375514876956527bb4d37d`, before this file existed. Output
blocks are from that run. A later `postgres:16-alpine` pull can be a newer
16.x minor; this run's server was PostgreSQL 16.16. Limits of that run are in
[What this run did not do](#what-this-run-did-not-do).

## Prerequisites

Install:

- Go 1.27.4, the toolchain the workflow installs. `go.mod` allows 1.27.0 or
  newer. The workflow sets `GOTOOLCHAIN=local` so Go does not download a
  different toolchain.
- Docker. This run used Docker client and server 29.1.3.
- A `psql` client. This run used `psql (PostgreSQL) 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1)`.
- A C compiler, required by `go test -race`. This run used
  `gcc (Ubuntu 13.3.0-6ubuntu2~24.04.1) 13.3.0`.

Recorded checks:

```text
$ go version
go version go1.27.4 linux/amd64

$ psql --version
psql (PostgreSQL) 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1)

$ docker version --format 'Client {{.Client.Version}} Server {{.Server.Version}}'
Client 29.1.3 Server 29.1.3
```

Run every command from the repository root. `cmd/migrate` reads `MIGRATIONS_DIR`
and, when that variable is unset, uses the relative directory `migrations`.
The workflow does not set `MIGRATIONS_DIR`. Leave it unset and stay in the
repository root.

The role passwords below are the disposable values already published in the
workflow. They are for this throwaway database only.

## Start disposable PostgreSQL 16

The `verify` job uses a GitHub Actions service container:

- image `postgres:16-alpine`
- `POSTGRES_USER=postgres`
- `POSTGRES_PASSWORD=compute-ci`
- `POSTGRES_DB=postgres`
- container port 5432
- health command `pg_isready -U postgres -d postgres`

The service publishes `5432:5432` on the runner. The local command below
publishes that port on `127.0.0.1` only. Clients still use `127.0.0.1:5432`,
which is the host in the workflow's `DATABASE_URL`.

```bash
docker run -d --name ccp-postgres-ci \
  -e POSTGRES_PASSWORD=compute-ci \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_DB=postgres \
  -p 127.0.0.1:5432:5432 \
  postgres:16-alpine
```

Recorded start of the container used for the successful run:

```text
d03c9e989873f20cbd5f406462a632e18ad832558f4136e21995be26252bc834
```

The image pulled for this run was `postgres:16-alpine` with digest
`sha256:f2f98726820d0342055351c6e9d36e705f5bfffb3dd53ff556325114b08afb6d`.
`docker inspect --format '{{.Config.Image}}' ccp-postgres-ci` printed
`postgres:16-alpine`.

Wait until the server accepts connections:

```bash
pg_isready --host=127.0.0.1 --username=postgres --dbname=postgres
```

```text
127.0.0.1:5432 - accepting connections
```

On this run `pg_isready` succeeded on the second attempt, one second after the first.

Confirm major version 16:

```bash
docker exec ccp-postgres-ci postgres --version
PGPASSWORD=compute-ci psql --host=127.0.0.1 --username=postgres --dbname=postgres \
  --set=ON_ERROR_STOP=1 --command='SHOW server_version;'
```

```text
postgres (PostgreSQL) 16.16
 server_version
----------------
 16.16
(1 row)
```

## Provision the workflow roles

Migrations `0013`, `0015`, `0017`, `0022`, `0023`, and `0024` refuse to apply
unless specific roles already exist. The `verify` job creates those roles with
`psql` in the step "Provision dedicated database roles in disposable CI
database". There is no separate role script for that job. The statements below
are copied from that step. `scripts/kueue_kind_smoke.sh` repeats the same six
`CREATE ROLE` statements for the kind smoke; do not run that script for this
procedure.

```bash
PGPASSWORD=compute-ci psql --host=127.0.0.1 --username=postgres --dbname=postgres \
  --set=ON_ERROR_STOP=1 \
  --command="CREATE ROLE ccp_provider_audit_owner NOLOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS" \
  --command="CREATE ROLE ccp_host_identity_guard_owner NOLOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS" \
  --command="CREATE ROLE ccp_host_session_guard_owner NOLOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS" \
  --command="CREATE ROLE ccp_host_session_writer LOGIN PASSWORD 'host-session-ci-only' NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS" \
  --command="CREATE ROLE ccp_kueue_job_binding LOGIN PASSWORD 'kueue-binding-ci-only' NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS" \
  --command="CREATE ROLE ccp_dra_receipt_ingest LOGIN PASSWORD 'dra-ingest-ci-only' NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS"
```

```text
CREATE ROLE
CREATE ROLE
CREATE ROLE
CREATE ROLE
CREATE ROLE
CREATE ROLE
```

## Set `DATABASE_URL` and apply migrations

The workflow's database URL is:

```bash
export GOTOOLCHAIN=local
export DATABASE_URL='postgres://postgres:compute-ci@127.0.0.1:5432/postgres?sslmode=disable'
```

Apply migrations twice. The second run is the workflow's idempotency check.
`go run ./cmd/migrate` is the command the workflow uses.

```bash
go run ./cmd/migrate
go run ./cmd/migrate
```

```text
2026/11/25 13:11:07 INFO migrations complete applied="[1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25]"
2026/11/25 13:11:07 INFO migrations complete applied=[]
```

`applied=[]` on the second run means the runner found every migration already
recorded and applied none again.

## Check schema constraints

The workflow then runs the constraint file as the `postgres` user:

```bash
PGPASSWORD=compute-ci psql --host=127.0.0.1 --username=postgres --dbname=postgres \
  --set=ON_ERROR_STOP=1 \
  --file=migrations/tests/0001_compute_core_constraints.sql
```

This run exited 0 and printed:

```text
INSERT 0 1
INSERT 0 1
UPDATE 1
INSERT 0 1
INSERT 0 1
DO
INSERT 0 1
INSERT 0 1
INSERT 0 1
INSERT 0 1
INSERT 0 1
DO
DO
DO
DO
DO
DO
DO
INSERT 0 1
DO
INSERT 0 1
INSERT 0 1
INSERT 0 1
DO
DO
```

The file's header comment says to run it after `0001_compute_core.sql` on an
empty database. The workflow runs it after the full migration set, and that is
the order used here. The successful `INSERT` statements leave rows committed.
The following tests were run on that same database, which is also what the
workflow does.

## Run the database-backed tests

Same environment as above (`GOTOOLCHAIN=local` and `DATABASE_URL`).

```bash
go test -race -count=1 ./...
```

Exit code 0. `go list ./...` listed 27 packages, and the test output named all
27 (`ok`, or `?` when the package has no test files). No package printed
`FAIL`. The database packages from this run:

```text
ok  	github.com/avaargsh/compute-control-panel/internal/provider	26.311s
ok  	github.com/avaargsh/compute-control-panel/internal/recovery	2.434s
ok  	github.com/avaargsh/compute-control-panel/internal/store/postgres	54.189s
```

`go test` without `-v` does not print individual skips. Packages that skip
unless a disposable kind harness is configured still skip under this command,
as they do in the `verify` job. This run did not add `-v`, so those skip lines
were not captured.

Repeat the SIGKILL recovery test the way the workflow does:

```bash
go test -race -count=3 -run '^TestSIGKILLRecoveryLeaseIsFencedBeforeInspection$' ./internal/recovery
```

```text
ok  	github.com/avaargsh/compute-control-panel/internal/recovery	2.879s
```

Exit code 0. The parent test SIGKILLs a helper process. The helper test itself
skips unless `COMPUTE_RECOVERY_KILL_HELPER=1`, which the parent sets for that
subprocess. That skip is part of the test, and the workflow uses the same
`-run` filter.

## Tear down

```bash
docker rm -f ccp-postgres-ci
```

```text
ccp-postgres-ci
```

A following `docker ps -a --filter name=ccp-postgres` printed only the column
header. The container and its data were removed.

## Troubleshooting

### Port already in use

A second `docker run` with `-p 127.0.0.1:5432:5432` while `ccp-postgres-ci` was
still published on that port exited 125:

```text
docker: Error response from daemon: failed to set up container networking: driver failed programming external connectivity on endpoint ccp-postgres-ci-port-conflict (787ed5fb33d006f38bbd318af84b638dd0f0e97802c5b07e220db7953d13fecd): Bind for 127.0.0.1:5432 failed: port is already allocated
```

`docker ps -a --filter name=ccp-postgres` afterward still showed only
`ccp-postgres-ci`. Stop the process that owns host port 5432, or remove the
container that publishes it, then start one container. This procedure was run
on port 5432. An alternate host port was not used.

### Wrong PostgreSQL major version

The workflow image is `postgres:16-alpine`. This run's `SHOW server_version`
was `16.16`, and `postgres --version` inside the container was
`postgres (PostgreSQL) 16.16`. A non-16 server was not started, so this session
has no error transcript from a wrong major version. If `SHOW server_version`
does not start with `16.`, remove the container and start `postgres:16-alpine`
again:

```bash
docker rm -f ccp-postgres-ci
```

That remove-and-start sequence was run for the stale-schema case below. The
replacement container's `SHOW server_version` was again `16.16`.

### Missing role

`go run ./cmd/migrate` before the `CREATE ROLE` statements failed. Migrations
1 through 12 were recorded, then 0013 stopped:

```text
2026/11/25 13:10:54 ERROR apply migrations error="apply 0013_provider_audit_writer_boundary.sql: ERROR: Provider audit owner must be independently provisioned with restricted NOLOGIN attributes (SQLSTATE P0001)" applied="[1 2 3 4 5 6 7 8 9 10 11 12]"
exit status 1
```

`SELECT version, name FROM schema_migrations ORDER BY version` then showed
versions 1 through 12 only. Later migrations check the other roles the same
way. Create the six roles from the workflow step, on a fresh database, before
migrating. Do not invent a different set of grants.

### Stale or partial schema

The failed migrate left `schema_migrations` at version 12. The disposable fix
used here was to remove the container and start a new one, which drops the
data directory:

```bash
docker rm -f ccp-postgres-ci
docker run -d --name ccp-postgres-ci \
  -e POSTGRES_PASSWORD=compute-ci \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_DB=postgres \
  -p 127.0.0.1:5432:5432 \
  postgres:16-alpine
```

After `pg_isready` succeeded, the new database had no migration table:

```text
ERROR:  relation "schema_migrations" does not exist
LINE 1: SELECT version, name FROM schema_migrations ORDER BY version...
                                  ^
```

Roles are cluster-local to that container. The new container required the
`CREATE ROLE` statements again. The migrate commands in the successful
procedure above were then run on that fresh database.

## What this run did not do

- It did not run `scripts/kueue_kind_smoke.sh` or any kind workflow.
- It did not run the `release-fence-source` job. That job uses the same image,
  roles, `DATABASE_URL`, double migrate, and constraint file, then runs
  narrower `go test -count=1 -v -run ...` commands and fails if those tests
  skip. Those filtered commands were not executed in this session. Their text
  is in `.github/workflows/go.yml` under `release-fence-source`.
- It did not run `gofmt`, `go vet`, `go mod verify`, or the workflow's Python
  migration-policy unittest. Those steps do not need PostgreSQL.
- It did not build or run `bin/migrate`. The workflow uses `go run ./cmd/migrate`.
- Passing these tests does not show a live Provider, a mounted production HTTP
  API, Kueue placement, DRA release, or a physical GPU.
