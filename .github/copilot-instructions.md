# Copilot Instructions for tap-mongodb

## Objective

Maintain and evolve this Singer tap for MongoDB without breaking compatibility for Meltano users.

## Project Context

- Package: `tap-mongodb`
- Runtime: Python `>=3.10,<4.0`
- Framework: `singer-sdk`
- Entry point: `tap_mongodb.tap:TapMongoDB.cli`
- Main domains: connection handling, schema inference, incremental replication, type conversion.

## Working Rules

- Preserve backward compatibility for existing config keys whenever possible.
- Prefer minimal, targeted changes over refactors.
- Keep current behavior for these core features unless explicitly requested:
  - standalone and replica set support
  - `connection_string` and host/port authentication modes
  - schema strategies: `raw`, `flexible`, `strict`
  - per-stream `stream_configs` (filters, projection, replication overrides)
- Never hardcode credentials, hosts, or environment-specific values.
- If adding/changing settings in `tap_mongodb/tap.py`, also update docs in `README.md`.
- If changing stream behavior in `tap_mongodb/streams.py`, update or add tests accordingly.

## Code Style

- Follow existing code style and naming conventions.
- Add type hints where practical, consistent with surrounding code.
- Keep comments short and only for non-obvious logic.
- Avoid introducing new dependencies unless clearly justified.

## Testing and Validation

Before considering a task done, run:

```bash
conda run -n tap-mongodb pytest -q
```

For Hub-related or metadata-related changes, also run:

```bash
conda run -n tap-mongodb tap-mongodb --about --format json
```

## PR/Change Checklist

- Code compiles and tests pass.
- No unrelated file changes.
- README reflects any config or behavior changes.
- Changes remain compatible with Meltano Hub publication expectations.
