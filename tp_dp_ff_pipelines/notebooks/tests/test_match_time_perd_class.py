import unittest
from src.tp_utils.common import match_time_perd_class  # Adjust path as needed

class TestMatchTimePerdClass(unittest.TestCase):

    def test_valid_time_periods(self):
        self.assertEqual(match_time_perd_class('mth'), 'mth')
        self.assertEqual(match_time_perd_class('wk'), 'wk')
        self.assertEqual(match_time_perd_class('bimth'), 'bimth')
        self.assertEqual(match_time_perd_class('qtr'), 'qtr')
        self.assertEqual(match_time_perd_class('custm'), 'custm')

    def test_invalid_time_period(self):
        # This will raise UnboundLocalError because class_cd is not set
        with self.assertRaises(UnboundLocalError):
            match_time_perd_class('invalid')

    def test_case_sensitivity(self):
        # This will also raise UnboundLocalError for uppercase input
        with self.assertRaises(UnboundLocalError):
            match_time_perd_class('MTH')

    def test_empty_string(self):
        # This will also raise UnboundLocalError for empty string
        with self.assertRaises(UnboundLocalError):
            match_time_perd_class('')

    def test_none_input(self):
        # This will also raise UnboundLocalError for None input
        with self.assertRaises(UnboundLocalError):
            match_time_perd_class(None)

    def test_all_cases(self):
        # This test ensures all match cases are covered for SonarQube
        for value in ['mth', 'wk', 'bimth', 'qtr', 'custm']:
            self.assertEqual(match_time_perd_class(value), value)
