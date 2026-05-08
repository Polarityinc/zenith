<!--
Thanks for sending a pull request! Please fill out the sections below.
For non-trivial changes, please open an issue first so we can align on direction.
-->

## Summary

<!-- One or two sentences. What does this change do, and why? -->

## Type of change

<!-- Mark with [x] -->

- [ ] `feat` — new feature
- [ ] `fix` — bug fix
- [ ] `perf` — performance improvement (please include numbers below)
- [ ] `refactor` — non-behavior-changing restructure
- [ ] `docs` — docs only
- [ ] `test` — tests only
- [ ] `ci` — CI / build only
- [ ] `chore` — dep bumps, tooling

## Related issues

<!-- e.g. Fixes #123, Refs #456. Required for non-trivial PRs. -->

## What changed

<!--
Bullet the important changes. Call out anything that touches:
- the on-disk format (zen_format, zen_wal)
- the query / planner contract (zen_query, zen_ql)
- the wire protocol (zen_proto, zen_server)
- public APIs of any crate
-->

## How was this tested

<!--
- [ ] `cargo test --workspace --all-features`
- [ ] `cargo clippy --workspace --all-targets -- -D warnings`
- [ ] `cargo fmt --all --check`
- [ ] `cargo deny check`
- [ ] Manual run-through (describe below)
- [ ] Bench results attached (required for `perf:` PRs)
-->

## Performance impact

<!--
Required for `perf:` PRs. Include a before/after on the relevant bench suite,
e.g. `cargo run --release -p zen_cli -- bench compare`. Otherwise: "n/a".
-->

## Breaking changes / migration notes

<!--
Anything users or operators need to do when upgrading? On-disk format
changes, config keys removed/renamed, default behavior changes.
Otherwise: "none".
-->

## Checklist

- [ ] PR title follows `<type>: <imperative summary>` (under 72 chars).
- [ ] Commits are signed off (`git commit -s`) per [DCO](../CONTRIBUTING.md#sign-off-dco).
- [ ] `CHANGELOG.md` updated under `## [Unreleased]` for user-visible changes.
- [ ] Docs updated where relevant (`docs/`, public API doc-comments).
- [ ] No new clippy warnings.
- [ ] No secrets, credentials, or proprietary data in the diff.
