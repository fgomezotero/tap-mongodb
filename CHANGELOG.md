# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog, and this project follows Semantic Versioning.

## [Unreleased]

### Added

- Added a LICENSE file with Apache License 2.0 text.
- Added this CHANGELOG file to track future releases.
- Added `max_record_per_run` stream configuration to cap per-stream extraction volume.

## [2.0.0] - 2026-03-31

### Added

- Singer tap implementation for MongoDB using singer-sdk.
- Support for standalone and replica set deployments.
- Multiple schema strategies: raw, flexible, and strict.
- Stream-level configuration overrides (filters, projection, replication settings).

### Changed

- Modernized project metadata and packaging for Python 3.10+.
