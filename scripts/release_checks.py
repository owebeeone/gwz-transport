#!/usr/bin/env python3
"""Gearu's release check for gwz-transport.

It refuses before running anything until the release opens: gwz-transport
publishes only in 1.1.0's release batch (the transport release plan's Phase 10,
step 2), whose reviewed change lifts `publish = false`. Then it runs the
contracts job's gates (.github/workflows/contracts.yml), in its order, and stops
at the first that fails. Gearu checks its release candidate before committing it,
and `cargo package` refuses uncommitted files, so packaging is the exact stage
(`--exact`), run on the release commit.
"""
import argparse
import os
from pathlib import Path
import subprocess
import sys
import tomllib

# The generators' and `cargo fmt`'s pinned rustfmt build, as in the contracts job.
FORMATTER = {"RUSTUP_TOOLCHAIN": "1.96.0"}


def gates(python, exact):
    """Each gate's command and the environment it adds."""
    if exact:
        return [(["cargo", "+1.95.0", "package", "--locked"], {})]
    return [
        ([python, "-m", "unittest", "discover", "-s", "scripts", "-p", "test_*.py"],
         {**FORMATTER, "PYTHONPATH": "scripts"}),
        ([python, "scripts/regen.py", "--check"], FORMATTER),
        (["cargo", "fmt", "--all", "--", "--check"], FORMATTER),
        (["cargo", "+1.95.0", "test", "--locked"], {}),
        (["cargo", "+1.95.0", "test", "--locked", "--features", "unstable-sequenced"], {}),
    ]


def check(root, version, run=subprocess.run, exact=False):
    with (root / "Cargo.toml").open("rb") as source:
        package = tomllib.load(source)["package"]
    if package["version"] != version:
        print("release refused: Cargo version does not match requested version", file=sys.stderr)
        return 1
    if package.get("publish") is False:
        print("release refused: gwz-transport publishes only in 1.1.0's release batch "
              "(Phase 10, step 2), whose reviewed change lifts publish = false", file=sys.stderr)
        return 1
    for command, added in gates(sys.executable, exact):
        result = run(command, cwd=root, env={**os.environ, **added}, check=False)
        if result.returncode != 0:
            return result.returncode
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--version", required=True)
    parser.add_argument("--exact", action="store_true", help="the release commit's stage: package it")
    args = parser.parse_args()
    return check(Path(__file__).resolve().parents[1], args.version, exact=args.exact)


if __name__ == "__main__":
    raise SystemExit(main())
