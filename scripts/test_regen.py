"""Regeneration prerequisites must fail before any generated output is written."""
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


if __name__ == "__main__":
    unittest.main()
