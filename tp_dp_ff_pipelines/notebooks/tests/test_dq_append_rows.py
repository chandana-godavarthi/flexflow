import unittest
from pyspark.sql import Row
from src.tp_utils.common import dq_append_rows  # Adjust the import path as needed

class TestDQAppendRows(unittest.TestCase):

    def test_grouping_multiple_types(self):
        results = [
            ("Check1", "Pass", "OK", "TypeA"),
            ("Check2", "Fail", "Issue", "TypeA"),
            ("Check3", "Pass", "Fine", "TypeB"),
            ("Check4", "Fail", "Error", "TypeC"),
        ]
        output_rows = []

        expected = [
            Row(Validation_Type="", Validation="TypeA", Result="", Details=""),
            Row(Validation_Type="", Validation="Check1", Result="Pass", Details="OK"),
            Row(Validation_Type="", Validation="Check2", Result="Fail", Details="Issue"),
            Row(Validation_Type="", Validation="TypeB", Result="", Details=""),
            Row(Validation_Type="", Validation="Check3", Result="Pass", Details="Fine"),
            Row(Validation_Type="", Validation="TypeC", Result="", Details=""),
            Row(Validation_Type="", Validation="Check4", Result="Fail", Details="Error"),
        ]

        result = dq_append_rows(results, output_rows)
        self.assertEqual(result, expected)

    def test_single_type_grouping(self):
        results = [
            ("Check1", "Pass", "OK", "TypeX"),
            ("Check2", "Fail", "Error", "TypeX"),
        ]
        output_rows = []

        expected = [
            Row(Validation_Type="", Validation="TypeX", Result="", Details=""),
            Row(Validation_Type="", Validation="Check1", Result="Pass", Details="OK"),
            Row(Validation_Type="", Validation="Check2", Result="Fail", Details="Error"),
        ]

        result = dq_append_rows(results, output_rows)
        self.assertEqual(result, expected)

    def test_empty_input(self):
        results = []
        output_rows = []
        result = dq_append_rows(results, output_rows)
        self.assertEqual(result, [])

    def test_prepopulated_output_rows(self):
        results = [
            ("Check1", "Pass", "OK", "TypeA"),
        ]
        output_rows = [Row(Validation_Type="Pre", Validation="Existing", Result="Data", Details="Here")]

        expected = [
            Row(Validation_Type="Pre", Validation="Existing", Result="Data", Details="Here"),
            Row(Validation_Type="", Validation="TypeA", Result="", Details=""),
            Row(Validation_Type="", Validation="Check1", Result="Pass", Details="OK"),
        ]

        result = dq_append_rows(results, output_rows)
        self.assertEqual(result, expected)

    def test_validation_type_fluctuation(self):
        results = [
            ("Check1", "Pass", "OK", "TypeA"),
            ("Check2", "Fail", "Issue", "TypeB"),
            ("Check3", "Pass", "Fine", "TypeA"),  # TypeA again
        ]
        output_rows = []

        expected = [
            Row(Validation_Type="", Validation="TypeA", Result="", Details=""),
            Row(Validation_Type="", Validation="Check1", Result="Pass", Details="OK"),
            Row(Validation_Type="", Validation="TypeB", Result="", Details=""),
            Row(Validation_Type="", Validation="Check2", Result="Fail", Details="Issue"),
            Row(Validation_Type="", Validation="TypeA", Result="", Details=""),
            Row(Validation_Type="", Validation="Check3", Result="Pass", Details="Fine"),
        ]

        result = dq_append_rows(results, output_rows)
        self.assertEqual(result, expected)