# Agent Workyard

A reusable engineering workflow foundation with portable instructions and a
harness-neutral Go coordinator for independently managed workers.

M1 is verified. The first M2 slice adds a durable local coordinator, a structured
Codex App Server adapter, separate worker conversations, status/result retrieval,
cancellation and conservative recovery. Codex is the only R0 worker adapter; the
supervisor can use any compatible harness. Messaging and coding collaboration
remain later work. See [the M2 runbook and remaining gaps](docs/m2.md).

## Build and check

Requires Go 1.27 or later. Durable state uses pure Go SQLite.
`go test ./...` and `go test -race ./...` are entirely offline: deterministic
fakes and a subprocess protocol peer never invoke Codex or a model. Live smoke
testing is a separate, explicit operation in the [M2 runbook](docs/m2.md).

```sh
go test ./...
go vet ./...
go build -o bin/workyard ./cmd/workyard
bin/workyard config check --bundle configs/default
bin/workyard config render --bundle configs/default --profile supervisor --skill workyard-plan
```

On Windows, build `bin/workyard.exe` and invoke that path. A Windows build can be
cross-compiled from Linux; that alone does not verify Windows runtime behavior.

## Use in another repository

Build once, then run the binary with an explicit shared bundle directory and the
target project directory. No sample application or copied instruction tree is
needed:

```sh
/path/to/workyard config check --bundle /path/to/agent-workyard/configs/default --project /path/to/my-project
/path/to/workyard config render --bundle /path/to/agent-workyard/configs/default --project /path/to/my-project --profile supervisor --skill workyard-plan
```

Ask your supervisor harness to read the rendered instructions alongside the
project's existing instructions and your request. Only selected skills are
included. Rendering instructions does not start an agent or enforce permissions.

Available profiles: `supervisor`, `general-worker`, `implementer`, `reviewer`.
Available skills: `workyard-plan`, `workyard-handoff`, `workyard-verify`.

- [Configuration and Codex use](docs/configuration.md)
- [Architecture and implementation decisions](docs/architecture.md)
- [M1 acceptance and verification](docs/m1.md)
- [M2 execution and recovery](docs/m2.md)

## Layout

| Location | Purpose |
| --- | --- |
| `cmd/workyard` | Configuration and local coordinator CLI |
| `internal/config` | Shared bundle and project additions |
| `internal/core` | Domain, capabilities, and resource vocabulary |
| `internal/worker` | Worker adapter contract |
| `internal/coordinator` | Durable lifecycle and authenticated local control |
| `internal/adapters/codex` | Structured Codex App Server worker adapter |
| `internal/store` | SQLite records, atomic events and process ownership |
| `configs/default` | Reusable instructions, profiles, and skills |
| `docs` | Implementation documentation and milestone evidence |

Private planning sources are intentionally excluded from this repository.
