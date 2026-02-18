# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## 5.9.2 - 2026-01-18

### Other
- ⏪️ Revert "🐛 zv: Don't impl Type for dicts with non-basic keys".

## 5.9.1 - 2026-01-10

### Other
- 🤖 release-plz: Fix formatting of CHANGELOG files.
- 🤖 release-plz: Use the default header in changelog.

## 5.9.0 - 2026-01-09

### Added
- ✨ Implement `TryFrom<&Value>` for tuples.
- ✨ Add signature! macro for compile-time validation. #984

### Changed
- 🎨 Format all files (rust 1.85).
- ♻️ Use signature! macro in tests.

### Dependencies
- ⬆️ Update endi to v1.1.1 (#1583).

### Fixed
- 🐛 Don't impl Type for dicts with non-basic keys. #1637

### Other
- 🧱 Fix all clippy warnings (rust 1.85).
- 🧑‍💻 Bump rust version to 1.85.
- 🚸 Implement `to_string_lossy` for `FilePath`.

### Testing
- ✅ Remove unused imports from tests.
