# Transport package

Follow the workspace AGENTS.md when working in gwz-dev.
Work TDD-first. Transport payloads are taut-defined; regenerate, never hand-edit
generated.rs or the vendored cbor.rs. Keep this crate independent of GWZ core/CLI.
All control-flow bodies must be braced. Conditional compilation belongs inside
cfg_if blocks or enclosing platform modules, never on isolated declarations.
