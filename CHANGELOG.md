# Changelog

All notable changes are documented here. The project follows semantic versioning after 1.0;
pre-1.0 versions may include documented source-incompatible changes.

## Unreleased

### Added

- A root `ags.Client` and canonical `ags.Sandbox` resource.
- Environment-backed `sandbox.Create`, `Connect`, `Get`, and `List` shortcuts.
- Pause, Resume, WaitFor, Update, Delete, Cloud Monitor Metrics, PTY, Watch, Commands List/Connect,
  streaming files, and generation-bound Code contexts.
- Explicit runtime-user selection for process listing, while retaining the zero-options default.
- Stable SDK-owned errors and models, bounded streams and aggregates, protocol provenance, and
  offline verification gates.

### Changed

- Cloud actions use the official Tencent Cloud Go SDK behind internal adapters.
- Generated filesystem/process bindings and data-plane credentials are confined to internal
  adapters instead of becoming accidental public packages.
- Files, Commands, PTY, Watch, and Code now cross the internal data-plane boundary through private
  semantic DTOs; root production code no longer constructs protobuf/Connect requests or parses
  wire events. Public API and behavior are unchanged.
- Sandbox services are obtained through methods and share one private data-plane generation.
- Create and Resume accept whole-second lifetimes from 30 seconds; Update uses a 300-second
  minimum; Connect selects the rule after reading state. Valid values are sent unchanged and
  deployed Cloud policy rejections are preserved.
- Code recognizes the runtime `end_of_execution` terminator while continuing to reject unknown
  or post-termination events.

### Removed

- Public generated Cloud models, mutable connection configuration, independent tool clients,
  duplicate sandbox implementations, and the `Kill` synonym.

See [MIGRATION.md](MIGRATION.md) for source changes.
