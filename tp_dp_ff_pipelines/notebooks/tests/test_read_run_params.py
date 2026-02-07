import pytest
import sys
from unittest.mock import patch
from src.tp_utils.common import read_run_params

def test_all_arguments_provided():
    test_args = ["script_name", "--FILE_NAME", "file.csv", "--CNTRT_ID", "123", "--RUN_ID", "abc123"]
    with patch.object(sys, 'argv', test_args):
        args = read_run_params()
        assert args.FILE_NAME == "file.csv"
        assert args.CNTRT_ID == "123"
        assert args.RUN_ID == "abc123"

def test_only_file_name_provided():
    test_args = ["script_name", "--FILE_NAME", "file.csv"]
    with patch.object(sys, 'argv', test_args):
        args = read_run_params()
        assert args.FILE_NAME == "file.csv"
        assert args.CNTRT_ID is None
        assert args.RUN_ID is None

def test_no_arguments_provided():
    test_args = ["script_name"]
    with patch.object(sys, 'argv', test_args):
        args = read_run_params()
        assert args.FILE_NAME is None
        assert args.CNTRT_ID is None
        assert args.RUN_ID is None

def test_empty_string_arguments():
    test_args = ["script_name", "--FILE_NAME", "", "--CNTRT_ID", "", "--RUN_ID", ""]
    with patch.object(sys, 'argv', test_args):
        args = read_run_params()
        assert args.FILE_NAME == ""
        assert args.CNTRT_ID == ""
        assert args.RUN_ID == ""

def test_invalid_argument_should_fail():
    test_args = ["script_name", "--INVALID_ARG", "value"]
    with patch.object(sys, 'argv', test_args):
        with pytest.raises(SystemExit):  # argparse throws SystemExit on unknown args
            read_run_params()
