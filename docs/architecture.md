# Architecture and decisions

This document describes the implemented foundation and its boundaries. Private
planning documents remain external authority and are not distributed here.

R0 targets Linux only. Preserve Linux/Windows portability at the architectural
boundaries; native Windows support, implementation and runtime validation are
post-MVP work and do not gate R0.

## Responsibility boundaries

The supervisor supplies engineering judgment and accepts integrated outcomes.
The coordinator stores execution state, allocates bounded local capacity and
exposes worker control independently of supervisor sessions. Assignment
administration has no model component. [The M2 runbook](m2.md) describes the
implemented slice and its recovery limits.

`core` contains vocabulary only. Projects identify repository roots and default
machines. Assignments retain both coordinator ownership and supervisor management.
Tasks hold executable briefs and profile references; Attempts identify individual
runs. Acceptance is separate from a worker's reported completion. Domain types
remain separate from the SQLite record/event implementation.

Profiles contain reusable instructions. A worker instance identifies a Task,
Attempt, conversation, runtime, and resource selection. The adapter owns native
handles. Two workers can share a runtime but must retain different conversations
and independent controls. Runtime and session/process layouts are adapter choices.

`worker.Adapter` separates conversation preparation, turn launch, recovery,
inspection and cancellation. Opaque handles are checkpointed before launch.
A runtime update stream carries structured observations. Optional messaging and
resumption report unsupported behavior in this slice. Request context
cancellation only ends an API call; worker cancellation is explicit. The update
stream is not durable storage or proof of completion. The coordinator journals
snapshots and reconciles disconnects. Delivery receipts distinguish queueing, adapter
acceptance, confirmation, unsupported delivery, failure, and ambiguous outcomes.
Confirmation requires evidence, as does terminal cancellation.

Capabilities have three states: available, unavailable, and unknown (the zero
value). Optional operations must expose unsupported behavior rather than quietly
degrading. Unknown capabilities must not be treated as available. Direct worker
history and messages stay separate from the supervisor conversation. Later collaboration work must arbitrate active direct interaction and conflicting
instructions; the current slice does not implement that arbitration.

Resources retain machine, runtime/harness, provider/account, and model identity.
Selections record the supervisor's rationale and fallback authorization. Catalog
entries expose capabilities and observed capacity. Missing counters are pointers
with nil values; they do not mean zero usage or infinite capacity. Usage identifies
the Attempt, resource, source (observed/estimated/unknown), and observation time.
Limit signals carry scope, observation evidence, and an optional retry time.
M2 enforces one configured runtime concurrency bound. Quota discovery, rate-limit
backoff and fallback remain later resource-policy work.

## Implementation choices

| Decision | Reason and limit |
| --- | --- |
| Go stack with pure Go SQLite | Native Linux builds for R0 and durable transactions without a framework or C compiler; long-term Linux/Windows portability remains a goal. Module path is local until a public import location is chosen. |
| Internal packages | Contracts can evolve during R0 without presenting an unstable external SDK. |
| Small CLI first | A harness can consume rendered instructions through local control without a second MCP interface. |
| Versioned JSON manifests and Markdown instructions | Parse configuration deterministically while keeping profiles and skills directly readable. |
| Explicit bundle path, additive project configuration | Reuse a shared checkout without overwriting project or harness instructions. |
| No assumed model access or quota | The operator supplies a local model/catalog entry. Observed usage is recorded; unknown capacity signals are never invented. |
| Shared Codex App Server | Isolated threads and targeted interruption use structured RPC; the protocol tests exercise independent control. |

SQLite now stores durable records and events. The local coordinator runs as a
separate process with a loopback control API and private bearer token. Git
isolation, coding collaboration, resource policy and messaging remain later work.
No dashboard, remote routing or second adapter is introduced.

## Codex boundary

The [official App Server documentation](https://developers.openai.com/codex/app-server/)
describes thread creation/resumption, structured events, steering, and interruption.
This is the implemented integration seam for M2. Each logical worker uses a
separate thread within one coordinator-owned App Server. The adapter uses native
completion, interruption, history and usage data. It never uses terminal text or
process exit as evidence of success. [M2 evidence and limits](m2.md) distinguish
protocol tests from real harness verification.

Permission enforcement and restrictions on unmanaged delegation must be verified
at the actual runtime boundary. These profiles provide instructions only. They
cannot supply enforcement guarantees.
