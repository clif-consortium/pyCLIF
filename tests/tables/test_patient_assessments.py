"""
Tests for the patient_assessments table module.
"""
import os
import pytest
import pandas as pd
import json
from datetime import datetime
from clifpy.tables.patient_assessments import PatientAssessments as patient_assessments

# --- Mock Schema ---
@pytest.fixture
def mock_assessment_schema_content():
    """Provides the content for a mock Patient_assessmentsModel.json."""
    return {
        "table_name": "patient_assessments",
        "columns": [
            {"name": "hospitalization_id", "data_type": "VARCHAR", "required": True},
            {"name": "recorded_dttm", "data_type": "DATETIME", "required": True},
            {"name": "assessment_id", "data_type": "VARCHAR", "required": True},
            {"name": "assessment_category", "data_type": "VARCHAR", "required": True, "is_category_column": True, "permissible_values": ["GCS", "RASS"]},
            {"name": "assessment_group", "data_type": "VARCHAR", "required": False},
            {"name": "assessment_tool", "data_type": "VARCHAR", "required": False},
            {"name": "numerical_value", "data_type": "DOUBLE", "required": False},
            {"name": "string_value", "data_type": "VARCHAR", "required": False}
        ],
        "assessment_category_to_group_mapping": {
            "GCS": "Neurological",
            "RASS": "Sedation/Agitation"
        },
        "assessment_score_ranges": {
            "GCS": {"min": 3, "max": 15},
            "RASS": {"min": -5, "max": 4}
        }
    }

@pytest.fixture
def mock_mcide_dir(tmp_path):
    """Creates a temporary mCIDE directory."""
    mcide_path = tmp_path / "mCIDE"
    mcide_path.mkdir()
    return mcide_path

@pytest.fixture
def mock_assessments_model_json(mock_mcide_dir, mock_assessment_schema_content):
    """Creates a mock Patient_assessmentsModel.json file."""
    schema_file_path = mock_mcide_dir / "Patient_assessmentsModel.json"
    with open(schema_file_path, 'w') as f:
        json.dump(mock_assessment_schema_content, f)
    return schema_file_path

@pytest.fixture
def patch_assessment_schema_path(monkeypatch, mock_assessments_model_json):
    """Patches the path to Patient_assessmentsModel.json for the patient_assessments class."""
    original_dirname = os.path.dirname
    original_join = os.path.join
    original_abspath = os.path.abspath

    def mock_dirname(path):
        if '__file__' in path:
            return str(mock_assessments_model_json.parent.parent / "tables")
        return original_dirname(path)

    def mock_abspath(path):
        if '__file__' in path:
            return str(mock_assessments_model_json.parent.parent / "tables" / "dummy_assessments.py")
        return original_abspath(path)

    def mock_join(*args):
        if len(args) > 1 and 'Patient_assessmentsModel.json' in args[-1]:
            return str(mock_assessments_model_json)
        return original_join(*args)

    monkeypatch.setattr(os.path, 'dirname', mock_dirname)
    monkeypatch.setattr(os.path, 'abspath', mock_abspath)
    monkeypatch.setattr(os.path, 'join', mock_join)

# --- Data Fixtures ---
@pytest.fixture
def sample_valid_assessments_data():
    """Create a valid patient_assessments DataFrame for testing."""
    return pd.DataFrame({
        'hospitalization_id': ['H001', 'H001', 'H002'],
        'recorded_dttm': pd.to_datetime(['2023-01-01 10:00', '2023-01-01 11:00', '2023-01-02 09:00']),
        'assessment_id': ['A001', 'A002', 'A003'],
        'assessment_category': ['gcs_total', 'rass', 'gcs_total'],
        'assessment_group': ['neurological', 'sedation_or_agitation', 'neurological'],
        'numerical_value': [14.0, -2.0, 15.0],
        'string_value': ['E4V5M5', '-2', 'E4V5M6'],
        'categorical_value': [None, None, None],
        'text_value': ['E4V5M5', '-2', 'E4V5M6']
    })

@pytest.fixture
def sample_invalid_assessments_data_schema():
    """Create a patient_assessments DataFrame with schema violations."""
    return pd.DataFrame({
        'hospitalization_id': ['H001'],
        # Missing recorded_dttm
        'assessment_id': ['A001'],
        'assessment_category': ['INVALID_CAT'], # Not in permissible_values
        'numerical_value': ['not-a-number'] # Invalid data type
    })

@pytest.fixture
def mock_assessments_file(tmp_path, sample_valid_assessments_data):
    """Create a mock patient_assessments parquet file for testing."""
    test_dir = tmp_path / "test_data"
    test_dir.mkdir()
    file_path = test_dir / "clif_patient_assessments.parquet"
    sample_valid_assessments_data.to_parquet(file_path)
    return str(test_dir)

# --- Tests for patient_assessments class ---

# Initialization and Schema Loading
@pytest.mark.usefixtures("patch_assessment_schema_path")
def test_assessments_init_with_valid_data(sample_valid_assessments_data):
    """Test patient_assessments initialization with valid data."""
    pa_obj = patient_assessments(data=sample_valid_assessments_data)
    assert pa_obj.df is not None
    pa_obj.validate()
    assert pa_obj.isvalid() is True
    assert not pa_obj.errors
    assert "gcs_total" in pa_obj.assessment_score_ranges # Check schema loaded

@pytest.mark.usefixtures("patch_assessment_schema_path")
def test_assessments_init_with_invalid_schema_data(sample_invalid_assessments_data_schema):
    """Test patient_assessments initialization with schema-invalid data."""
    pa_obj = patient_assessments(data=sample_invalid_assessments_data_schema)
    assert pa_obj.df is not None
    pa_obj.validate()
    assert pa_obj.isvalid() is False
    assert len(pa_obj.errors) > 0
    error_types = {e['type'] for e in pa_obj.errors}
    # The DQA pipeline reports missing required columns alongside other errors;
    # it no longer suppresses them when a dtype mismatch is also present.
    assert 'Missing Required Columns' in error_types
    assert 'Data Type Mismatch' in error_types
    assert 'Invalid Categorical Values' in error_types

@pytest.mark.usefixtures("patch_assessment_schema_path")
def test_assessments_init_without_data():
    """Test patient_assessments initialization without data."""
    pa_obj = patient_assessments()
    assert pa_obj.df is None
    # No data means validation never ran, so the table is not yet known to be valid.
    assert pa_obj.isvalid() is False
    assert not pa_obj.errors
    assert "gcs_total" in pa_obj.assessment_score_ranges
@pytest.mark.usefixtures("patch_assessment_schema_path")
def test_assessments_from_file(mock_assessments_file, sample_valid_assessments_data):
    """Test loading data from a parquet file."""
    pa_obj = patient_assessments.from_file(mock_assessments_file, filetype="parquet", timezone="UTC")
    assert pa_obj.df is not None
    pd.testing.assert_frame_equal(pa_obj.df.reset_index(drop=True), sample_valid_assessments_data.reset_index(drop=True), check_dtype=False)
    pa_obj.validate()
    assert pa_obj.isvalid() is True

@pytest.mark.usefixtures("patch_assessment_schema_path")
def test_assessments_from_file_nonexistent(tmp_path):
    """Test loading from a nonexistent file."""
    non_existent_path = str(tmp_path / "nonexistent_dir")
    with pytest.raises(FileNotFoundError):
        patient_assessments.from_file(non_existent_path, filetype="parquet", timezone="UTC")

# isvalid method
@pytest.mark.usefixtures("patch_assessment_schema_path")
def test_assessments_isvalid(sample_valid_assessments_data, sample_invalid_assessments_data_schema):
    """Test isvalid method."""
    valid_pa = patient_assessments(data=sample_valid_assessments_data)
    valid_pa.validate()
    assert valid_pa.isvalid() is True

    invalid_pa = patient_assessments(data=sample_invalid_assessments_data_schema)
    invalid_pa.validate()
    assert invalid_pa.isvalid() is False

# validate method
@pytest.mark.usefixtures("patch_assessment_schema_path")
def test_assessments_validate_output(sample_valid_assessments_data, sample_invalid_assessments_data_schema, capsys):
    """Test validate method output messages."""
    patient_assessments(data=sample_valid_assessments_data).validate()
    captured = capsys.readouterr()
    assert "Validation completed successfully." in captured.out

    patient_assessments(data=sample_invalid_assessments_data_schema).validate()
    captured = capsys.readouterr()
    assert "Validation completed with" in captured.out
    assert "error(s)" in captured.out

    pa_no_data = patient_assessments()
    pa_no_data.validate()
    captured = capsys.readouterr()
    assert "No dataframe to validate." in captured.out

# --- Helper Method Tests ---

@pytest.mark.usefixtures("patch_assessment_schema_path")
def test_get_assessment_categories(sample_valid_assessments_data):
    """Test get_assessment_categories method."""
    pa_obj = patient_assessments(data=sample_valid_assessments_data)
    categories = pa_obj.get_assessment_categories()
    assert isinstance(categories, list)
    assert set(categories) == {'gcs_total', 'rass'}

    pa_empty = patient_assessments()
    assert pa_empty.get_assessment_categories() == []

@pytest.mark.usefixtures("patch_assessment_schema_path")
def test_filter_by_assessment_category(sample_valid_assessments_data):
    """Test filter_by_assessment_category method."""
    pa_obj = patient_assessments(data=sample_valid_assessments_data)
    gcs_df = pa_obj.filter_by_assessment_category('gcs_total')
    assert len(gcs_df) == 2
    assert all(gcs_df['assessment_category'] == 'gcs_total')

    non_existent_df = pa_obj.filter_by_assessment_category('NonExistent')
    assert non_existent_df.empty

    pa_empty = patient_assessments()
    assert pa_empty.filter_by_assessment_category('gcs_total').empty

@pytest.mark.usefixtures("patch_assessment_schema_path")
def test_get_summary_stats(sample_valid_assessments_data):
    """Test get_summary_stats method."""
    pa_obj = patient_assessments(data=sample_valid_assessments_data)
    stats = pa_obj.get_summary_stats()

    assert stats['total_records'] == 3
    assert stats['unique_hospitalizations'] == 2
    assert stats['assessment_category_counts'] == {'gcs_total': 2, 'rass': 1}
    assert 'numerical_value_stats' in stats
    assert 'gcs_total' in stats['numerical_value_stats']
    assert stats['numerical_value_stats']['gcs_total']['mean'] == 14.5

    pa_empty = patient_assessments()
    assert pa_empty.get_summary_stats() == {}

# Retired: test_load_*_schema covered a JSON `*Model.json` loader that no
# longer exists. Schemas are versioned YAML loaded by BaseTable via
# clifpy.schemas.load_schema; these tests monkeypatched os.path.join for a
# file the package never opens.
