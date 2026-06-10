# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog, and this project follows Semantic Versioning.

## [Unreleased]

## [2.3.0] - 2026-06-10

### Changed

- Schema sampling now uses `_id` descending index (`find().sort("_id", -1).limit(N)`) instead of applying stream filters, eliminating collection scans on unindexed fields.
- Default `infer_schema_max_docs` reduced from 1000 to 100.
- Schema is now cached after first inference, preventing redundant queries during discovery and sync phases.
- `flexible` strategy now declares all discovered fields as `string` type for maximum compatibility with MongoDB's dynamic typing across documents.
- `_convert_value` now stringifies primitive types (`int`, `float`, `bool`) when using `flexible` strategy to prevent schema validation errors.

## [2.1.2] - 2026-06-09

### Fixed

- Schema inference now applies stream filters during sampling, ensuring all fields present in the filtered dataset are discovered and declared in the schema.

## [2.1.1] - 2026-06-08

### Fixed

- Fixed `InvalidRecord` error when a field contains both `dict` and `list` values across documents.
- `_flexible_schema` now forces `dict` and `list` fields to `string` type to avoid schema/data mismatches.
- `_convert_value` serializes `list` and `dict` values as JSON strings with proper BSON type handling.
- `_python_to_json_type` maps `dict` and `list` to `string` for consistent schema generation.

## [2.1.0]

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
