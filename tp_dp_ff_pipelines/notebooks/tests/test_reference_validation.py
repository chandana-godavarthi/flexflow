# import pytest
# from unittest.mock import patch, MagicMock
# from contextlib import ExitStack
# from src.tp_utils.common import reference_validation

# @pytest.fixture
# def mock_inputs():
#     return {
#         "materialised_path": "test_path",
#         "validation_name": "CHK_DQ16",
#         "file_name": "test_file",
#         "cntrt_id": 123,
#         "run_id": "test_run",
#         "spark": MagicMock(),
#         "ref_db_jdbc_url": "jdbc:test",
#         "ref_db_name": "test_db",
#         "ref_db_user": "user",
#         "ref_db_pwd": "pwd",
#         "postgres_schema": "test_schema",
#         "catalog_name": "test_catalog"
#     }

# def patch_all():
#     return [
#         patch("src.tp_utils.common.get_logger", return_value=MagicMock()),
#         patch("src.tp_utils.common.reference_data_load_variables", return_value=("code", 456, "type", MagicMock(), MagicMock())),
#         patch("src.tp_utils.common.read_query_from_postgres"),
#         patch("src.tp_utils.common.dq_query_retrieval"),
#         patch("src.tp_utils.common.validation", return_value="validation_result"),
#         patch("src.tp_utils.common.merge_into_tp_data_vldtn_rprt_tbl"),
#         patch("src.tp_utils.common.generate_report"),
#         patch("src.tp_utils.common.generate_formatting_report")
#     ]

# def run_test_with_patches(test_func, mock_inputs):
#     with ExitStack() as stack:
#         mocks = [stack.enter_context(p) for p in patch_all()]
#         test_func(mock_inputs, *mocks)

# # Scenario 1: CHK_DQ16 + CHK_DQ18
# def test_chk_dq16_only(mock_inputs):
#     def test_logic(mock_inputs, _, __, mock_read_pg, mock_dq_query, mock_validation, *rest):
#         mock_read_pg.return_value.select.return_value.collect.return_value = [("CHK_DQ16",)]
#         mock_dq_query.return_value.collect.return_value = [{"queries": "SELECT 1"}]
#         mock_inputs["spark"].sql.return_value = MagicMock()

#         reference_validation(**mock_inputs)

#         assert mock_dq_query.call_count == 2  # CHK_DQ16 + CHK_DQ18
#         assert mock_validation.call_count == 2

#     run_test_with_patches(test_logic, mock_inputs)

# # Scenario 2: CHK_DQ16, CHK_DQ17 + CHK_DQ18
# def test_chk_dq16_and_dq17(mock_inputs):
#     def test_logic(mock_inputs, _, __, mock_read_pg, mock_dq_query, mock_validation, *rest):
#         mock_read_pg.return_value.select.return_value.collect.return_value = [("CHK_DQ16",), ("CHK_DQ17",)]
#         mock_dq_query.return_value.collect.return_value = [{"queries": "SELECT 1"}]
#         mock_inputs["spark"].sql.return_value = MagicMock()

#         reference_validation(**mock_inputs)

#         assert mock_dq_query.call_count == 3
#         assert mock_validation.call_count == 3

#     run_test_with_patches(test_logic, mock_inputs)

# # Scenario 3: CHK_DQ18 only
# def test_chk_dq18_always_runs(mock_inputs):
#     mock_inputs["validation_name"] = "CHK_DQ18"

#     def test_logic(mock_inputs, _, __, mock_read_pg, mock_dq_query, mock_validation, *rest):
#         mock_read_pg.return_value.select.return_value.collect.return_value = []
#         mock_dq_query.return_value.collect.return_value = [{"queries": "SELECT 1"}]

#         reference_validation(**mock_inputs)

#         assert mock_dq_query.call_count == 1
#         assert mock_validation.call_count == 1

#     run_test_with_patches(test_logic, mock_inputs)

# # Scenario 4: CHK_DQ19 disabled
# def test_chk_dq19_disabled(mock_inputs):
#     def test_logic(mock_inputs, _, __, mock_read_pg, mock_dq_query, *rest):
#         mock_read_pg.return_value.select.return_value.collect.return_value = [("CHK_DQ16",)]
#         mock_dq_query.return_value.collect.return_value = [{"queries": "SELECT 1"}]
#         mock_inputs["spark"].sql.return_value = MagicMock()

#         reference_validation(**mock_inputs)

#         assert mock_dq_query.call_count == 2  # CHK_DQ16 + CHK_DQ18

#     run_test_with_patches(test_logic, mock_inputs)

# # Scenario 5: CHK_DQ16, CHK_DQ17, CHK_DQ19 (disabled), CHK_DQ18
# def test_all_validations_enabled(mock_inputs):
#     def test_logic(mock_inputs, _, __, mock_read_pg, mock_dq_query, mock_validation, *rest):
#         mock_read_pg.return_value.select.return_value.collect.return_value = [
#             ("CHK_DQ16",), ("CHK_DQ17",), ("CHK_DQ19",)
#         ]
#         mock_dq_query.return_value.collect.return_value = [{"queries": "SELECT 1"}]
#         mock_inputs["spark"].sql.return_value = MagicMock()

#         reference_validation(**mock_inputs)

#         assert mock_dq_query.call_count == 3  # CHK_DQ16, CHK_DQ17, CHK_DQ18
#         assert mock_validation.call_count == 3

#     run_test_with_patches(test_logic, mock_inputs)

# # Scenario 6: dq_query_retrieval raises exception
# def test_dq_query_retrieval_exception(mock_inputs):
#     def test_logic(mock_inputs, _, __, mock_read_pg, mock_dq_query, mock_validation, *rest):
#         mock_read_pg.return_value.select.return_value.collect.return_value = [("CHK_DQ16",)]
#         mock_dq_query.side_effect = Exception("Simulated error")

#         try:
#             reference_validation(**mock_inputs)
#         except Exception as e:
#             assert str(e) == "Simulated error"

#         mock_validation.assert_not_called()

#     run_test_with_patches(test_logic, mock_inputs)

# # Scenario 7: No validations except CHK_DQ18
# def test_no_validations_except_dq18(mock_inputs):
#     def test_logic(mock_inputs, _, __, mock_read_pg, mock_dq_query, mock_validation, *rest):
#         mock_read_pg.return_value.select.return_value.collect.return_value = []
#         mock_dq_query.return_value.collect.return_value = [{"queries": "SELECT 1"}]

#         reference_validation(**mock_inputs)

#         assert mock_dq_query.call_count == 1
#         assert mock_validation.call_count == 1

#     run_test_with_patches(test_logic, mock_inputs)
