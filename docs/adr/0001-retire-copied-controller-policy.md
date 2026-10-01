# ADR-0001: Retire copied controller policy

- Status: Accepted
- Date: 2026-09-30

The managed agent block and its controller files came from `pekth/ai-skills`
revision `97d487263399c8fba08edb28848dcd15cf7f22dd`. That source removed the
policy artifacts in `fea5968d15f88bd1177e1d43c6264c1213b954a0`.
The retained copies require delegation and handoffs for routine tasks, which
conflicts with the current global policy.

Remove the managed block and the generated `CONTROLLER.md` and `NO-LOOPS.md`.
Use the active global `AGENTS.md` for authority, delegation, and continuity.
Keep product instructions and repository-specific approval holds in `AGENTS.md`.

Verify the remaining agent references and preserve the product instruction body.
This decision records source guidance; installed runtime and deployment are unverified.
