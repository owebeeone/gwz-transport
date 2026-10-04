# Release Process

<!-- gearu:release:start -->
## Gearu Release Process

Gearu prepares and verifies the repository, creates an immutable tag, and can
create the GitHub Release that starts this repository's publication workflow.
It does not publish directly to package registries.

Full documentation: <https://owebeeone.github.io/gearu/>

### Install

Install the released tool with:

```sh
uv tool install gearu
```

Upgrade an existing installation with:

```sh
uv tool upgrade gearu
```

To test the unreleased `main` branch, install it directly from its repository:

```sh
uv tool install git+https://github.com/owebeeone/gearu.git
```

Verify the installation with `gearu --version`.

### Preconditions

- Read `gearu.toml` and this repository's release workflow.
- Choose an explicit release version or an explicit major, minor, or patch bump.
  Gearu does not infer release intent from commits.
- Use a clean checkout on the branch configured by `project.branch`.
- Synchronize configured release and source branches with their remote.
- Release required cross-repository dependencies first.
- Install and authenticate `gh` before requesting GitHub Release creation.

### Plan

Always inspect the read-only plan first:

```sh
gearu plan VERSION
```

Or ask Gearu to select the next version:

```sh
gearu plan --bump patch
gearu plan --bump minor
gearu plan --bump major
```

Gearu compares configured package versions with valid local and remote release
tags, then bumps the highest version. It reads remote tags directly and does not
fetch or create local tags while planning.

For a release candidate, use a numbered version such as `1.2.3-rc.1`.

Override a configured dependency tag only when the release intentionally uses a
different version:

```sh
gearu plan VERSION --dependency-tag DEPENDENCY=TAG
```

### Prepare the Local Release

After reviewing the plan:

```sh
gearu release VERSION
```

The release command can select the version itself:

```sh
gearu release --bump minor
```

This recalculates the next version at release time. To lock the version reviewed
in a prior bump plan, pass that plan's reported `VERSION` explicitly.

Gearu builds and tests in a temporary worktree. Only a successful candidate is
applied to the local release branch and tagged. This step does not change a
remote repository.

### Push and Create the GitHub Release

Push the exact release commit and tag atomically:

```sh
gearu release VERSION --push
```

Create the GitHub Release after that push:

```sh
gearu release VERSION --push --github-release
```

The final command starts workflows listening for `release.published`, including
package publication and documentation deployment where configured.

### Recovery

- If candidate checks fail, fix the problem and rerun; the normal checkout is
  left unchanged.
- If local preparation succeeds, rerun the same version with `--push`.
- If the push succeeds but GitHub Release creation fails, rerun with
  `--push --github-release`.
- If released contents must change, use a new patch or release-candidate version.
  Never move or replace the existing tag.
- If only a publication workflow fails, repair and rerun that workflow for the
  same GitHub Release.
<!-- gearu:release:end -->

## gwz-transport release gate

gwz-transport releases with Gearu, as gwz-sspi does. gwz-core, gwz-cli and gwz-py
keep their own `scripts/release.py`, which checks the cross-repository dependency
pins that Gearu does not.

- **The gate.** Cargo.toml has `publish = false`, and `scripts/release_checks.py`
  refuses while it stands. gwz-transport's first real release, 0.1.0, is step 2 of
  1.1.0's release batch (the transport release plan's Phase 10), whose reviewed
  change lifts the guard.
- **crates.io.** The name exists as the placeholder `0.0.0-bootstrap.1`
  (2026-09-23, `bootstrap-crate.yml`). Its trusted publisher must be owner
  `owebeeone`, repository `gwz-transport`, workflow `release.yml`, environment
  `crates-io`, configured before the release (TR3.3). `release.yml` has no token
  fallback.
- **The release interpreter, once per machine.** Gearu's checks run
  `{repo}/.release-venv/bin/python`. It must hold the taut-proto release that
  `protocol/generator.json` pins in its own site directories, since
  `scripts/regen.py` refuses any other; the checks also need Rust 1.96.0's
  rustfmt and Rust 1.95.0:

  ```sh
  python3 -m venv .release-venv
  .release-venv/bin/python -m pip install taut-proto==0.10.0
  rustup toolchain install 1.96.0 --profile minimal --component rustfmt
  rustup toolchain install 1.95.0 --profile minimal
  ```

- **Stages.** Gearu's `checks` run the contracts job's gates on the uncommitted
  candidate: the script tests, the generated-artifact check, formatting and both
  test suites. Its `exact_checks` package the release commit, because
  `cargo package` refuses uncommitted files.
- **Releasing.** `gearu plan VERSION` is read-only. `gearu release VERSION` makes
  the commit and tag; `--push` and `--github-release` are separate steps. The
  GitHub Release starts `release.yml`, which reruns both stages on the tag and
  publishes through Trusted Publishing.
