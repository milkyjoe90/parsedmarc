# Cybaa parsedmarc distribution

This fork preserves upstream ancestry. `11.3.0+cybaa.1` is based on upstream
`11.3.0`, commit `70a0ea0470d04649e33e6b09ce20da991d424f72`.
It is a GitHub-only distribution, not an upstream PyPI release.

The native change builds aggregate record interval datetimes directly from the
original epochs with `timezone.utc`, rather than reparsing local naive metadata.
During a daylight-saving fallback, that round trip loses the repeated hour's
identity: a valid interval can become inverted, and a 25-hour report can allocate
daily counts incorrectly. The original epoch is authoritative under
[RFC 9990 section 3.1.1.4](https://www.rfc-editor.org/rfc/rfc9990.html#section-3.1.1.4).
See also Python's
[`datetime.fromtimestamp` timezone contract](https://docs.python.org/3/library/datetime.html#datetime.datetime.fromtimestamp).

Report-level metadata and the public timestamp utility retain their existing
host-local, timezone-free format. Output backends deliberately understand that
format; changing it would require a separate compatibility change. Cybaa retains
child `TZ=UTC` to select UTC metadata and preserve its historical output contract.
Native interval/count correctness no longer depends on that setting.

The older ingestion-audit branch is not the release base. In particular, its
metadata-wide UTC patch is not cherry-picked: it changes backend assumptions.
Its report-ID, inferred-envelope and lenient-record changes are also outside this
release. They affect upstream compatibility and Cybaa's stored identities or
evidence, and need their own migration and replay analysis.

Cybaa retains application ownership of receiver/source authentication, current
tenant grants, policy-domain checks, field allow-lists, retention, MIME/XML and
resource admission, process isolation/timeouts/output bounds, deduplication,
TLS full-source correspondence, canonical-presence semantics and immutable
history/replay. No installed-package source rewrite is required or performed.

## Publication and validation

The `Cybaa Release` workflow accepts the distinct PEP 440 local-version tag,
runs the full reusable upstream CI (Python 3.11–3.15, lint, formatting, types,
documentation/package builds, tests and Elasticsearch-backed sample CLI checks),
and verifies installed distributions outside the checkout before GitHub publication.
The release includes a wheel, sdist and SHA256SUMS. There is no PyPI, GHCR or
documentation deployment. Inherited publishing workflows are repository-gated.

Before tagging, inspect the feature-branch CI and the final diff, including
workflows. A tag must match the distribution version. Consumers must install the
release wheel using its SHA-256 URL fragment and verify its exact metadata/CLI
version. The source distribution receives the same installed-artifact checks.

On 2026-11-13 the fork's code-scanning endpoint returned HTTP 404, "no analysis
found"; Dependabot returned HTTP 403, alerts disabled. Neither establishes a clean
scan. The secret-scanning API returned an empty list of open alerts. Public
[upstream advisories](https://github.com/domainaware/parsedmarc/security/advisories)
were inspected; their published patched versions extend through the inherited
11.3.0 baseline. This is not a dependency audit or an executed CodeQL scan.
Do not describe unavailable checks as passing or bypass a newly available
blocking alert without investigation.
