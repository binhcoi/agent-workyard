---
name: workyard-verify
description: Verify an engineering deliverable and report reproducible evidence, artifacts, and limitations. Use before returning worker results or accepting and delivering completed work.
---

# Verify and deliver

Compare the deliverable with the task's acceptance conditions. Run the appropriate repository checks and exercise the actual changed behavior. For bug fixes, demonstrate the original reproduction and the corrected outcome when possible. Inspect relevant UI behavior carefully when a UI is involved.

Investigate confirmed failures and flakiness before declaring success. Make bounded corrections within the authorized scope; surface substantial scope or risk changes. Do not repeatedly rerun passing checks without a reason.

Return changed artifacts, reproducible commands, observed results, blockers, and checks that could not be performed. Label estimates and distinguish worker usage from coordinator overhead. Never infer success from terminal silence, an accepted cancellation request, or a worker's own completion claim.

Workers return evidence for evaluation. Supervisors apply selective independent review and explicit integration, then judge acceptance. Preserve pending feedback and unresolved issues. Report completion only for the outcome actually verified, with any portability or environment limitations.
