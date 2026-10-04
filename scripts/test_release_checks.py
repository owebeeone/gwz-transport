"""Gearu's release check: it refuses before any gate until gwz-transport's release
opens, then runs the contracts job's gates in order and stops at the first that
fails. Packaging runs in the exact stage, on the release commit: gearu checks its
candidate before committing it, and `cargo package` refuses uncommitted files."""
from pathlib import Path
import subprocess
import tempfile
import unittest

import release_checks


def repository(directory: str, version: str, publish: str | None) -> Path:
    root = Path(directory)
    text = f'[package]\nname = "gwz-transport"\nversion = "{version}"\n'
    if publish is not None:
        text += f"publish = {publish}\n"
    (root / "Cargo.toml").write_text(text)
    return root


class Recorder:
    def __init__(self, failing: int | None = None):
        self.commands: list[tuple[list[str], dict[str, str]]] = []
        self.failing = failing

    def __call__(self, command, *, cwd, env, check):
        self.commands.append((command, env))
        code = 3 if len(self.commands) - 1 == self.failing else 0
        return subprocess.CompletedProcess(command, code)


class ReleaseCheckTests(unittest.TestCase):
    def test_a_version_other_than_the_manifests_is_refused_before_any_gate(self):
        with tempfile.TemporaryDirectory() as directory:
            run = Recorder()
            self.assertEqual(release_checks.check(repository(directory, "0.1.0", None), "0.1.1", run), 1)
            self.assertEqual(run.commands, [])

    def test_publish_false_refuses_before_any_gate(self):
        with tempfile.TemporaryDirectory() as directory:
            run = Recorder()
            self.assertEqual(release_checks.check(repository(directory, "0.1.0", "false"), "0.1.0", run), 1)
            self.assertEqual(run.commands, [])

    def test_an_open_release_runs_the_contracts_gates_in_order(self):
        with tempfile.TemporaryDirectory() as directory:
            run = Recorder()
            self.assertEqual(release_checks.check(repository(directory, "0.1.0", None), "0.1.0", run), 0)
        commands = [command for command, _ in run.commands]
        self.assertEqual(commands[0][1:], ["-m", "unittest", "discover", "-s", "scripts", "-p", "test_*.py"])
        self.assertEqual(run.commands[0][1]["PYTHONPATH"], "scripts")
        self.assertEqual(commands[1][1:], ["scripts/regen.py", "--check"])
        self.assertEqual(commands[2], ["cargo", "fmt", "--all", "--", "--check"])
        for _, environment in run.commands[:3]:
            # The generators' and the format check's pinned rustfmt build.
            self.assertEqual(environment["RUSTUP_TOOLCHAIN"], "1.96.0")
        self.assertEqual(commands[3:], [
            ["cargo", "+1.95.0", "test", "--locked"],
            ["cargo", "+1.95.0", "test", "--locked", "--features", "unstable-sequenced"],
        ])

    def test_the_exact_stage_packages_the_release_commit(self):
        with tempfile.TemporaryDirectory() as directory:
            run = Recorder()
            root = repository(directory, "0.1.0", None)
            self.assertEqual(release_checks.check(root, "0.1.0", run, exact=True), 0)
        self.assertEqual([command for command, _ in run.commands], [["cargo", "+1.95.0", "package", "--locked"]])

    def test_the_exact_stage_refuses_while_publish_is_false(self):
        with tempfile.TemporaryDirectory() as directory:
            run = Recorder()
            root = repository(directory, "0.1.0", "false")
            self.assertEqual(release_checks.check(root, "0.1.0", run, exact=True), 1)
            self.assertEqual(run.commands, [])

    def test_the_first_failing_gate_ends_the_check_with_its_status(self):
        with tempfile.TemporaryDirectory() as directory:
            run = Recorder(failing=1)
            self.assertEqual(release_checks.check(repository(directory, "0.1.0", None), "0.1.0", run), 3)
        self.assertEqual(len(run.commands), 2)


if __name__ == "__main__":
    unittest.main()
