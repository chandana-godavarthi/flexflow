import pytest
from unittest.mock import MagicMock , patch
from src.tp_utils.common import tier1_file_structure_validation
#logic
@pytest.fixture(autouse=True)
def mock_dbutils(monkeypatch):
    import pyspark.dbutils
    monkeypatch.setattr(pyspark.dbutils, 'DBUtils', lambda spark: MagicMock(name="DBUtils"))

@pytest.fixture
def setup_context():
    return {
        "spark": MagicMock(),
        "dbutils": MagicMock(),
        "run_id": "R001",
        "cntrt_id": "C001",
        "catalog_name": "test_catalog",
        "postgres_schema": "ref_schema",
        "file_name": "source_file.csv",
        "notebook_name": "tier1_file_structure_validation",
        "validation_name": "Tier1 File Structure Validation",
        "jdbc_url": "jdbc:postgresql://localhost:5432/refdb",
        "db_name": "refdb",
        "db_user": "user",
        "db_pwd": "pwd"
    }

def mock_contract_row():
    row = MagicMock()
    row.srce_sys_id = "SYS001"
    row.time_perd_type_code = "MTH"
    row.cntry_name = "India"
    row.vendr_id = "V001"
    return row
@patch("src.tp_utils.common.F.regexp_replace", return_value=MagicMock())
@patch("src.tp_utils.common.send_emails")
@patch("src.tp_utils.common.generate_formatting_report")
@patch("src.tp_utils.common.generate_report")
@patch("src.tp_utils.common.merge_into_tp_data_vldtn_rprt_tbl")
@patch("src.tp_utils.common.validation", return_value="validation_result")
@patch("src.tp_utils.common.dq_query_retrieval")
@patch("src.tp_utils.common.read_query_from_postgres", return_value=MagicMock())
@patch("src.tp_utils.common.load_cntrt_lkp")
@patch("src.tp_utils.common.get_logger")
def test_successful_execution(
    mock_get_logger, mock_load_cntrt_lkp, mock_read_query,
    mock_dq_query, mock_validation, mock_merge, mock_generate_report,
    mock_generate_formatting, mock_send_email,mock_regexp_replace, setup_context
):
    mock_cntrt_df = MagicMock()
    mock_cntrt_df.collect.return_value = [mock_contract_row()]
    mock_load_cntrt_lkp.return_value = mock_cntrt_df

    mock_query_df = MagicMock()
    mock_query_df.collect.return_value = [{'qry_txt': "SELECT 1"}]
    mock_dq_query.return_value = mock_query_df
    setup_context["spark"].sql.return_value = MagicMock()
    setup_context["spark"].read.parquet.return_value = MagicMock()

    tier1_file_structure_validation( setup_context["validation_name"], setup_context["file_name"],
        setup_context["cntrt_id"], setup_context["run_id"], setup_context["spark"],
        setup_context["jdbc_url"], setup_context["db_name"], setup_context["db_user"], setup_context["db_pwd"],
        setup_context["postgres_schema"], setup_context["catalog_name"]
    )

    assert mock_merge.called
    assert mock_generate_report.called
    assert mock_generate_formatting.called
    assert mock_validation.call_count == 19

@patch("src.tp_utils.common.get_logger")
@patch("src.tp_utils.common.load_cntrt_lkp", side_effect=Exception("Contract lookup failed"))
def test_contract_lookup_failure(mock_load_cntrt_lkp, mock_get_logger, setup_context):
    with pytest.raises(Exception) as excinfo:
        tier1_file_structure_validation( setup_context["validation_name"], setup_context["file_name"],
            setup_context["cntrt_id"], setup_context["run_id"], setup_context["spark"],
            setup_context["jdbc_url"], setup_context["db_name"], setup_context["db_user"], setup_context["db_pwd"],
            setup_context["postgres_schema"], setup_context["catalog_name"]
        )
    assert "Contract lookup failed" in str(excinfo.value)

@patch("src.tp_utils.common.get_logger")
@patch("src.tp_utils.common.load_cntrt_lkp")
@patch("src.tp_utils.common.read_query_from_postgres", side_effect=Exception("Reference table read failed"))
def test_reference_table_read_failure(mock_read_query, mock_load_cntrt_lkp, mock_get_logger, setup_context):
    mock_cntrt_df = MagicMock()
    mock_cntrt_df.collect.return_value = [mock_contract_row()]
    mock_load_cntrt_lkp.return_value = mock_cntrt_df

    with pytest.raises(Exception) as excinfo:
        tier1_file_structure_validation( setup_context["validation_name"], setup_context["file_name"],
            setup_context["cntrt_id"], setup_context["run_id"], setup_context["spark"],
            setup_context["jdbc_url"], setup_context["db_name"], setup_context["db_user"], setup_context["db_pwd"],
            setup_context["postgres_schema"], setup_context["catalog_name"]
        )
    assert "Reference table read failed" in str(excinfo.value)

@patch("src.tp_utils.common.F.regexp_replace", return_value=MagicMock())
@patch("src.tp_utils.common.get_logger")
@patch("src.tp_utils.common.load_cntrt_lkp")
@patch("src.tp_utils.common.read_query_from_postgres")
@patch("src.tp_utils.common.dq_query_retrieval")
@patch("src.tp_utils.common.validation", side_effect=Exception("Validation failed"))
def test_validation_failure(
    mock_validation, mock_dq_query, mock_read_query,
    mock_load_cntrt_lkp, mock_get_logger, mock_regexp_replace, setup_context
):
    mock_cntrt_df = MagicMock()
    mock_cntrt_df.collect.return_value = [mock_contract_row()]
    mock_load_cntrt_lkp.return_value = mock_cntrt_df

    mock_query_df = MagicMock()
    mock_query_df.collect.return_value = [{'qry_txt': "SELECT 1"}]
    mock_dq_query.return_value = mock_query_df

    setup_context["spark"].sql.return_value = MagicMock()
    setup_context["spark"].read.parquet.return_value = MagicMock()

    with pytest.raises(Exception) as excinfo:
        tier1_file_structure_validation(setup_context["validation_name"], setup_context["file_name"],
            setup_context["cntrt_id"], setup_context["run_id"], setup_context["spark"],
            setup_context["jdbc_url"], setup_context["db_name"], setup_context["db_user"], setup_context["db_pwd"],
            setup_context["postgres_schema"], setup_context["catalog_name"]
        )
    assert "Validation failed" in str(excinfo.value)
