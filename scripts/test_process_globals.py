"""New process-global state in gwz-transport fails here (GwzCoreSessionDesign O9).

The checker belongs to gwz-core. CI checks gwz-core out inside this repository
and names it in GWZ_CORE_CHECKOUT; in a GWZ workspace it sits beside this
checkout. The allowlist of existing items lives here.
"""
import os
from pathlib import Path
import subprocess
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
ALLOWLIST = ROOT / 'scripts' / 'process_globals_allowlist.json'


def checker_path():
    configured = os.environ.get('GWZ_CORE_CHECKOUT')
    core = ROOT / configured if configured else ROOT.parent / 'gwz-core'
    return core / 'scripts' / 'checks' / 'check_process_globals.py'


class ProcessGlobalsTests(unittest.TestCase):
    def test_transport_adds_no_process_global_state(self):
        checker = checker_path()
        self.assertTrue(checker.is_file(), f'gwz-core checker not found at {checker}')
        result = subprocess.run(
            [sys.executable, '-B', str(checker), '--repo', str(ROOT), '--allowlist', str(ALLOWLIST)],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == '__main__':
    unittest.main()
