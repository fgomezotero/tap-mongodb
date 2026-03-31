# AGENTS Guide for tap-mongodb

## Purpose

This file defines execution standards for AI or human-assisted iterations in this repository.
It complements .github/copilot-instructions.md and focuses on action-level workflow.

## Scope

Applies to all tasks that modify source code, tests, documentation, or release metadata.

## Core Principles

- Keep backward compatibility for existing user configurations.
- Prefer small, targeted changes over broad refactors.
- Preserve existing behavior unless the task explicitly requests behavior changes.
- Never introduce secrets, credentials, or environment-specific constants.

## Required Checks Before Completion

1. Run tests:

   conda run -n tap-mongodb pytest -q

2. For metadata or Hub-related work, validate about output:

   conda run -n tap-mongodb tap-mongodb --about --format json

3. Confirm docs alignment:

- If settings changed in tap_mongodb/tap.py, update README.md.
- If stream behavior changed in tap_mongodb/streams.py, update/add tests.

## Change Review Rules

- Reject changes that remove support for standalone or replica set without explicit request.
- Reject changes that break connection_string or host/port auth modes.
- Reject changes that alter raw/flexible/strict schema strategy semantics without tests and docs.
- Reject changes that modify stream_configs behavior without coverage updates.

## Pull Request Readiness Checklist

- Code and tests pass.
- No unrelated file edits included.
- README reflects user-facing behavior and config changes.
- Meltano Hub compatibility remains intact.

## Hub Publishing Notes

When preparing Hub submissions for this repository:

- Verify installability and about metadata before opening/refreshing a Hub issue.
- Avoid duplicate Hub issues for the same variant.
- Clearly state whether pip source should track main or a tagged version.
