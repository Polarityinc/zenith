# Security Policy

The ZenithDB team takes security seriously. We appreciate responsible disclosure of vulnerabilities and will work with reporters to triage and fix issues quickly.

## Supported versions

ZenithDB is currently in **alpha**. Until `1.0`, only the latest minor release receives security fixes.

| Version  | Supported          |
|----------|--------------------|
| `0.1.x`  | :white_check_mark: |
| `< 0.1`  | :x:                |

Once we reach `1.0`, we will publish a longer support window in this document.

## Reporting a vulnerability

**Please do not report security issues as public GitHub issues, in pull requests, or in our discussions.**

Instead, use one of:

1. **GitHub Private Vulnerability Reporting** — preferred. Use the **Security** tab on the repository or this link: [https://github.com/Polarityinc/zenith/security/advisories/new](https://github.com/Polarityinc/zenith/security/advisories/new). This keeps the report private and tracked.
2. **Email** — [security@polarity.cc](mailto:security@polarity.cc). For sensitive details, request our PGP key in your first message and we'll send it back before you share specifics.

Please include, to the extent you can:

- A description of the vulnerability and its impact.
- ZenithDB version (or git SHA) and configuration.
- Steps to reproduce, or a proof-of-concept.
- Any known mitigations or workarounds.
- Whether you intend to publish details, and on what timeline.

You'll receive an acknowledgement within **2 business days**. We aim to provide an initial assessment within **5 business days** and a fix or remediation plan within **30 days** for high-severity issues. Timelines for low-severity issues will be discussed case-by-case.

## Disclosure policy

- We follow **coordinated disclosure**. We'll work with you on a public-disclosure timeline that gives users a reasonable window to upgrade.
- We'll credit you in the advisory unless you ask to remain anonymous.
- We do not currently run a paid bug-bounty program.
- We may request a CVE on your behalf and will publish a [GitHub Security Advisory](https://github.com/Polarityinc/zenith/security/advisories) when the fix ships.

## Scope

In scope:

- The ZenithDB engine and CLI (`zen`, `zenithdb`).
- The 18 crates under [`crates/`](crates).
- Default deployment artifacts under [`deploy/`](deploy) (Docker, Helm, Terraform).
- Authentication, authorization, encryption, and isolation of tenant data.
- WAL durability and recoverability.
- TLS configuration as shipped.

Out of scope:

- Vulnerabilities in third-party dependencies that we have not yet had a reasonable chance to upgrade. (We watch [`cargo audit`](https://github.com/rustsec/rustsec/) in CI.) If you find one in a critical dependency, please still let us know.
- Issues that require a privileged attacker on the same host (root, kernel-level), unless ZenithDB itself enables the escalation.
- DoS via unbounded resource use when running with the default `examples/zenithdb.dev.toml` profile (which is explicitly a dev profile with no rate limits).
- Self-XSS or social-engineering attacks against operators.
- Findings from automated scanners without a working PoC against ZenithDB code.

## Hardening defaults

The `prod`-profile deployment artifacts in [`deploy/`](deploy) ship with:

- JWT authentication on customer routes; HMAC on inter-node routes.
- TLS termination via `rustls` + `aws-lc-rs`.
- WAL fsync **on** by default.
- Per-tenant rate limits and a global concurrency cap.
- AES-256-GCM envelope encryption with a pluggable KMS root key.
- A NetworkPolicy and PodDisruptionBudget in the Helm chart.

If you're running ZenithDB in production, follow [`docs/RUNBOOK.md`](docs/RUNBOOK.md) and avoid the `ZEN_UNSAFE_FAST=1` flag, which disables fsync. Audit your deployment against the runbook before exposing it to untrusted traffic.

## Questions

If you're unsure whether something is a security issue, err on the side of reporting privately and we'll triage it together.
