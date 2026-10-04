# Transport package

Follow the workspace AGENTS.md when working in gwz-dev.
Work TDD-first. Transport payloads are taut-defined; regenerate, never hand-edit
generated.rs or the vendored cbor.rs. Keep this crate independent of GWZ core/CLI.
All control-flow bodies must be braced. Conditional compilation belongs inside
cfg_if blocks or enclosing platform modules, never on isolated declarations.

<!-- gearu:agents:start -->
## Releases

- This repository uses [Gearu](https://owebeeone.github.io/gearu/) for release
  preparation.
- Read `RELEASE.md` before planning or performing a release.
- `gearu plan VERSION` and `gearu plan --bump LEVEL` are read-only. Do not run
  `gearu release`, push a release tag, or create a GitHub Release unless the
  user explicitly requests it.
- Never move or reuse a release tag. Correct released content with a new version.
- Never publish directly to PyPI, crates.io, or npm from a local checkout.
  Registry publication belongs in the repository's release workflow.
<!-- gearu:agents:end -->
