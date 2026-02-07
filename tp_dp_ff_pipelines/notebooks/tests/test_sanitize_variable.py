import unittest
from unittest.mock import patch
from src.tp_utils.common import sanitize_variable

class TestSanitizeVariable(unittest.TestCase):

    def test_valid_inputs(self):
        valid_inputs = [
            "valid_name",
            "name123",
            "name.with.dots",
            "name-with-hyphen",
            "name_with_underscore"
        ]

        with patch("src.tp_utils.common.re.match", return_value=True):
            for val in valid_inputs:
                self.assertEqual(sanitize_variable(val), val)

    def test_invalid_inputs(self):
        invalid_inputs = [
            "name with space",
            "name@domain",
            "name$",
            "name#123",
            "name/123"
        ]

        with patch("src.tp_utils.common.re.match", return_value=False):
            for val in invalid_inputs:
                with self.subTest(val=val):
                    with self.assertRaises(ValueError):
                        sanitize_variable(val)
