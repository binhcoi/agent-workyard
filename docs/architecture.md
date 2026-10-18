# M1 architecture and decisions

This document describes the implemented foundation and its boundaries. Private
planning documents remain external authority and are not distributed here.

## Responsibility boundaries

The supervisor supplies engineering judgment and accepts integrated outcomes.
The future coordinator will store execution state, allocate resources, and expose
worker control independently of supervisor sessions. The `coordinator.Control`
interface is the initial supervisor-facing seam; its methods have no running
implementation in M1. Assignment administration has no model component.

`core` contains vocabulary only. Projects identify repository roots and default
machines. Assignments retain both coordinator ownership and supervisor management.
Tasks hold executable briefs and profile references; Attempts identify individual
runs. Acceptance is separate from a worker's reported completion. No state
transition engine, persistence schema, or retry policy is implied by these types.

Profiles contain reusable instructions. A worker instance identifies a Task,
Attempt, conversation, runtime, and resource selection. The adapter owns native
handles. Two workers can share a runtime but must retain different conversations
and independent controls. Runtime and session/process layouts are adapter choices.

`worker.Adapter` defines creation, inspection, separate conversation history,
messages, cancellation, resumption, and observation subscriptions. Request context
cancellation only ends an API call; worker cancellation is explicit. A subscription
is a stream of observations, not durable storage or proof of completion. Consumers
must reconcile disconnects. Delivery receipts distinguish queueing, adapter
acceptance, confirmation, unsupported delivery, failure, and ambiguous outcomes.
Confirmation requires evidence, as does terminal cancellation.

Capabilities have three states: available, unavailable, and unknown (the zero
value). Optional operations must expose unsupported behavior rather than quietly
degrading. Unknown capabilities must not be treated as available. Direct worker
history and messages stay separate from the supervisor conversation. The later
coordinator must arbitrate active direct interaction and conflicting instructions;
M1's prompts and contracts do not implement that arbitration.

Resources retain machine, runtime/harness, provider/account, and model identity.
Selections record the supervisor's rationale and fallback authorization. Catalog
entries expose capabilities and observed capacity. Missing counters are pointers
with nil values; they do not mean zero usage or infinite capacity. Usage identifies
the Attempt, resource, source (observed/estimated/unknown), and observation time.
Limit signals carry scope, observation evidence, and an optional retry time.
No scheduler, quota discovery, rate-limit loop, or fallback executes in M1.

## Implementation choices

| Decision | Reason and limit |
| --- | --- |
| Provisional Go stack, standard library only | Native Linux/Windows builds and a small foundation without framework dependencies. Module path is local until a public import location is chosen. |
| Internal packages | Contracts can evolve during R0 without presenting an unstable external SDK. |
| Small CLI first | A harness can consume rendered instructions without a second MCP interface or background service. |
| Versioned JSON manifests and Markdown instructions | Parse configuration deterministically while keeping profiles and skills directly readable. |
| Explicit bundle path, additive project configuration | Reuse a shared checkout without overwriting project or harness instructions. |
| No model IDs or quota defaults | Available models and account capacity must come from real configuration or observations in later milestones. |
| Codex adapter directory only | Establish the boundary without claiming worker execution or prematurely implementing M2. |

SQLite, Git isolation, durable lifecycle, and real worker launches remain later
implementation work. No database dependency, daemon, installer, network routing,
dashboard, or second worker adapter is introduced here.

## Codex boundary

The [official App Server documentation](https://developers.openai.com/codex/app-server/)
describes thread creation/resumption, structured events, steering, and interruption.
This is the candidate integration seam for M2. That protocol documentation does
not prove our future adapter's recovery, isolated control, direct user access, or
safe runtime sharing. Those need implementation-specific tests. No capability is
advertised by the reserved package, and no parent-controlled native subagent is
used as a managed worker.

Permission enforcement and restrictions on unmanaged delegation must be verified
at the actual runtime boundary. These profiles provide instructions only. They
cannot supply enforcement guarantees.
