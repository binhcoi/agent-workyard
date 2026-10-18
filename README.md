# Agent Workyard

A reusable engineering workflow foundation with portable instructions and a
harness-neutral Go contract for future coordinator-managed workers.

R0/M1 supplies configuration loading and rendering, domain and adapter contracts,
and initial profiles and skills. It cannot launch, persist, message, or cancel
workers yet. Codex is the only worker adapter required for R0; the supervisor
can use any compatible harness. No native subagent setup is installed.

## Build and check

Requires Go 1.27 or later. There are no third-party Go dependencies.

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

## Layout

| Location | Purpose |
| --- | --- |
| `cmd/workyard` | Read-only configuration CLI |
| `internal/config` | Shared bundle and project additions |
| `internal/core` | Domain, capabilities, and resource vocabulary |
| `internal/worker` | Worker adapter contract |
| `internal/coordinator` | Supervisor control contract, future deterministic service |
| `internal/adapters/codex` | Reserved R0 adapter boundary, no implementation yet |
| `configs/default` | Reusable instructions, profiles, and skills |
| `docs` | Implementation documentation and milestone evidence |

Private planning sources are intentionally excluded from this repository.
