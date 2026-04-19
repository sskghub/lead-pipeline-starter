# Changelog

## [1.1.0] - 2026-04-19

### Security

- HTTP endpoint requires `Authorization: Bearer <PIPELINE_AUTH_TOKEN>` (stored in Modal secret `lead-pipeline-secrets`).
- Email verification: Reoon API/network errors now **fail closed** (lead discarded) instead of treating unknown errors as valid.

### Changed

- Split `execute_pipeline` (test via `modal run`) from `run_pipeline` (HTTP with auth).

## [1.0.0] - prior

- Initial pipeline: raw tab → dedupe → verify → Main → clear raw.
