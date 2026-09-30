"""Regeneration prerequisites must fail before any generated output is written."""
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import regen


class FormatterPinTests(unittest.TestCase):
    def test_exact_formatter_is_accepted(self):
        pin = {"rustfmt": "rustfmt test-build"}
        with patch.object(regen.subprocess, "check_output", return_value="rustfmt test-build\n"):
            regen.verify_formatter(pin)

    def test_different_formatter_is_refused(self):
        pin = {"rustfmt": "rustfmt expected-build"}
        with patch.object(regen.subprocess, "check_output", return_value="rustfmt other-build\n"):
            with self.assertRaisesRegex(SystemExit, "Expected rustfmt"):
                regen.verify_formatter(pin)


class ReleasePinTests(unittest.TestCase):
    def test_retained_v1_reader_is_the_immutable_baseline(self):
        fixture = regen.ROOT / 'tests/fixtures/retained_protocol_v1.rs'
        self.assertEqual(
            hashlib.sha256(fixture.read_bytes()).hexdigest(),
            '89142ac8a8e000707dfc78e7b76af295033e6f0d45194da8bf9866220a960a6e',
        )

    def test_a_different_taut_proto_release_is_refused(self):
        with self.assertRaisesRegex(SystemExit, 'taut-proto pin mismatch'):
            regen.released_taut({'taut-proto': '0.0.0'})

    def test_a_taut_that_shadows_the_release_is_refused(self):
        # A checkout on PYTHONPATH can carry the pinned version string; only the
        # installed release's own files count.
        with tempfile.TemporaryDirectory() as raw:
            shadow = Path(raw) / 'taut'
            shadow.mkdir()
            (shadow / '__init__.py').write_text("__version__ = '0.10.0'\n")
            result = regen.subprocess.run(
                [regen.sys.executable, str(Path(__file__).with_name('regen.py')), '--check'],
                capture_output=True,
                text=True,
                env={**os.environ, 'PYTHONPATH': raw},
            )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('is not the installed taut-proto release', result.stderr)

    def test_a_taut_carrying_its_own_release_metadata_is_refused(self):
        # Metadata that claims the pinned release does not make a copy on
        # PYTHONPATH the installed release; only this interpreter's site
        # directories hold that.
        version = json.loads((regen.ROOT / 'protocol/generator.json').read_text())['taut-proto']
        with tempfile.TemporaryDirectory() as raw:
            metadata = Path(raw) / f'taut_proto-{version}.dist-info'
            metadata.mkdir()
            (metadata / 'METADATA').write_text(
                f'Metadata-Version: 2.1\nName: taut-proto\nVersion: {version}\n'
            )
            shadow = Path(raw) / 'taut'
            shadow.mkdir()
            (shadow / '__init__.py').write_text(f"__version__ = '{version}'\n")
            result = regen.subprocess.run(
                [regen.sys.executable, str(Path(__file__).with_name('regen.py')), '--check'],
                capture_output=True,
                text=True,
                env={**os.environ, 'PYTHONPATH': raw},
            )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('is not the installed taut-proto release', result.stderr)


if __name__ == "__main__":
    unittest.main()
