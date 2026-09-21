"""Regeneration prerequisites must fail before any generated output is written."""
import hashlib
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


class SourcePinTests(unittest.TestCase):
    def test_retained_v1_reader_is_the_immutable_baseline(self):
        fixture = regen.ROOT / 'tests/fixtures/retained_protocol_v1.rs'
        self.assertEqual(
            hashlib.sha256(fixture.read_bytes()).hexdigest(),
            '89142ac8a8e000707dfc78e7b76af295033e6f0d45194da8bf9866220a960a6e',
        )

    def test_pinned_source_requires_explicit_checkout(self):
        result = regen.subprocess.run(
            [regen.sys.executable, str(regen.Path(__file__).with_name('regen.py')), '--check'],
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('pinned local taut source', result.stderr)


if __name__ == "__main__":
    unittest.main()
