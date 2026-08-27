"""
Tests for the vitals table module.
"""
import os
import pytest
import pandas as pd
import json
from datetime import datetime
from clifpy.tables.vitals import Vitals as vitals

# --- Data Fixtures ---
@pytest.fixture
def sample_valid_vitals_data():
    """Create a valid vitals DataFrame for testing."""
    return pd.DataFrame({
        'hospitalization_id': ['H001', 'H001', 'H002'],
        'recorded_dttm': pd.to_datetime(['2023-01-01 10:00', '2023-01-01 11:00', '2023-01-02 09:00']),
        'vital_category': ['heart_rate', 'temp_c', 'sbp'],
        'vital_value': [75.0, 36.5, 120.0],
        'value_source_concept_id': ['SNOMED_HR', 'SNOMED_TEMP', 'SNOMED_SBP']
    })

@pytest.fixture
def sample_invalid_vitals_data_schema():
    """Create a vitals DataFrame with schema violations (specifically invalid type for vital_value)."""
    return pd.DataFrame({
        'hospitalization_id': ['H001'],
        'recorded_dttm': pd.to_datetime(['2023-01-01 10:00']), # Added required column
        'vital_category': ['heart_rate'],
        'vital_value': ['high'] # Invalid data type for vital_value, schema expects DOUBLE
    })

@pytest.fixture
def sample_invalid_vitals_data_range():
    """Create a vitals DataFrame with values outside defined ranges."""
    return pd.DataFrame({
        'hospitalization_id': ['H001', 'H001'],
        'recorded_dttm': pd.to_datetime(['2023-01-01 10:00', '2023-01-01 11:00']),
        'vital_category': ['heart_rate', 'temp_c'],
        'vital_value': [400.0, 10.0], # heart_rate too high, temp_c too low
    })

@pytest.fixture
def sample_vitals_data_unknown_category():
    """Create a vitals DataFrame with an unknown vital category."""
    return pd.DataFrame({
        'hospitalization_id': ['H001'],
        'recorded_dttm': pd.to_datetime(['2023-01-01 10:00']),
        'vital_category': ['blood_sugar'], # Not in mock_vitals_schema_content
        'vital_value': [150.0]
    })

@pytest.fixture
def mock_vitals_file(tmp_path, sample_valid_vitals_data):
    """Create a mock vitals parquet file for testing."""
    test_dir = tmp_path / "test_data"
    test_dir.mkdir()
    file_path = test_dir / "clif_vitals.parquet"
    sample_valid_vitals_data.to_parquet(file_path)
    return str(test_dir) # from_file expects directory path

# --- Mock Schema ---
@pytest.fixture
def mock_vitals_schema_content():
    """Provides the content for a mock VitalsModel.json."""
    return {
        "columns": [
            {"name": "hospitalization_id", "data_type": "VARCHAR", "required": True},
            {"name": "recorded_dttm", "data_type": "DATETIME", "required": True},
            {"name": "vital_category", "data_type": "VARCHAR", "required": True, "is_category_column": True, "permissible_values": ["heart_rate", "respiratory_rate", "temp_c", "sbp", "dbp"]},
            {"name": "vital_value", "data_type": "DOUBLE", "required": True},
            {"name": "value_source_concept_id", "data_type": "VARCHAR", "required": False}
        ],
        "vital_units": {
            "heart_rate": "bpm",
            "respiratory_rate": "breaths/min",
            "temp_c": "°C",
            "sbp": "mmHg",
            "dbp": "mmHg"
        },
        "vital_ranges": {
            "heart_rate": {"min": 40, "max": 180},
            "respiratory_rate": {"min": 8, "max": 30},
            "temp_c": {"min": 35.0, "max": 41.0},
            "sbp": {"min": 70, "max": 200},
            "dbp": {"min": 40, "max": 120}
        }
    }

@pytest.fixture
def mock_mcide_dir(tmp_path):
    """Creates a temporary mCIDE directory."""
    mcide_path = tmp_path / "mCIDE"
    mcide_path.mkdir()
    return mcide_path

@pytest.fixture
def mock_vitals_model_json(mock_mcide_dir, mock_vitals_schema_content):
    """Creates a mock VitalsModel.json file in the temporary mCIDE directory."""
    schema_file_path = mock_mcide_dir / "VitalsModel.json"
    with open(schema_file_path, 'w') as f:
        json.dump(mock_vitals_schema_content, f)
    return schema_file_path

@pytest.fixture
def patch_vitals_schema_path(monkeypatch, mock_vitals_model_json):
    """Patches the path to VitalsModel.json for the vitals class."""
    original_dirname = os.path.dirname
    original_join = os.path.join
    original_abspath = os.path.abspath

    def mock_dirname(path):
        if '__file__' in path: 
            return str(mock_vitals_model_json.parent.parent / "tables")
        return original_dirname(path)

    def mock_abspath(path):
        if '__file__' in path:
            return str(mock_vitals_model_json.parent.parent / "tables" / "dummy_vitals.py")
        return original_abspath(path)

    def mock_join(*args):
        if len(args) > 1 and args[1] == '..' and args[2] == 'mCIDE' and args[3] == 'VitalsModel.json':
            return str(mock_vitals_model_json)
        return original_join(*args)

    monkeypatch.setattr(os.path, 'dirname', mock_dirname)
    monkeypatch.setattr(os.path, 'abspath', mock_abspath)
    monkeypatch.setattr(os.path, 'join', mock_join)

# --- Tests for vitals class --- 

# Initialization and Schema Loading
@pytest.mark.usefixtures("patch_vitals_schema_path")
def test_vitals_init_with_valid_data(sample_valid_vitals_data, mock_vitals_schema_content):
    """Test vitals initialization with valid data and mocked schema."""
    vital_obj = vitals(data=sample_valid_vitals_data)
    assert vital_obj.df is not None
    # Validate is called in __init__
    if not vital_obj.isvalid():
        print("DEBUG vital_obj.errors:", vital_obj.errors)
        print("DEBUG vital_obj.range_validation_errors:", vital_obj.range_validation_errors)
    assert vital_obj.isvalid() is True
    assert not vital_obj.errors
    assert not vital_obj.range_validation_errors
    assert "heart_rate" in vital_obj.vital_ranges # Check schema loaded
    assert "temp_c" in vital_obj.vital_units

@pytest.mark.usefixtures("patch_vitals_schema_path")
def test_vitals_init_with_invalid_schema_data(sample_invalid_vitals_data_schema):
    """Test vitals initialization with schema-invalid data."""
    vital_obj = vitals(data=sample_invalid_vitals_data_schema)
    assert vital_obj.df is not None
    vital_obj.validate()
    assert vital_obj.isvalid() is False
    assert len(vital_obj.errors) > 0
    error_types = [e['type'] for e in vital_obj.errors]
    # validate_table (used by _validate_schema) reports 'Data Type Mismatch'
    assert 'Missing Required Columns' in error_types or 'Data Type Mismatch' in error_types

@pytest.mark.usefixtures("patch_vitals_schema_path")
def test_vitals_init_with_invalid_range_data(sample_invalid_vitals_data_range):
    """Test vitals initialization with out-of-range data."""
    vital_obj = vitals(data=sample_invalid_vitals_data_range)
    assert vital_obj.df is not None
    vital_obj.validate()
    assert vital_obj.isvalid() is False
    assert not vital_obj.errors # Schema might be fine
    assert len(vital_obj.range_validation_errors) > 0
    assert any(e['error_type'] == 'values_out_of_range' for e in vital_obj.range_validation_errors)

@pytest.mark.usefixtures("patch_vitals_schema_path")
def test_vitals_init_without_data():
    """Test vitals initialization without data."""
    vital_obj = vitals()
    assert vital_obj.df is None
    assert vital_obj.isvalid() is True # No data, so no data errors
    assert not vital_obj.errors
    assert not vital_obj.range_validation_errors
    assert "heart_rate" in vital_obj.vital_ranges # Schema should still load
@pytest.mark.usefixtures("patch_vitals_schema_path")
def test_vitals_from_file(mock_vitals_file, sample_valid_vitals_data):
    """Test loading vitals data from a parquet file."""
    vital_obj = vitals.from_file(mock_vitals_file, filetype="parquet", timezone="UTC")
    assert vital_obj.df is not None
    # Standardize DataFrames before comparison (e.g. reset index, sort)
    expected_df = sample_valid_vitals_data.reset_index(drop=True)
    loaded_df = vital_obj.df.reset_index(drop=True)
    pd.testing.assert_frame_equal(loaded_df, expected_df, check_dtype=False)
    assert vital_obj.isvalid() is True # Validation is called in init

@pytest.mark.usefixtures("patch_vitals_schema_path")
def test_vitals_from_file_nonexistent(tmp_path):
    """Test loading vitals data from a nonexistent file."""
    non_existent_path = str(tmp_path / "nonexistent_dir")
    with pytest.raises(FileNotFoundError):
        vitals.from_file(non_existent_path, filetype="parquet", timezone="UTC")

# isvalid method
@pytest.mark.usefixtures("patch_vitals_schema_path")
def test_vitals_isvalid(sample_valid_vitals_data, sample_invalid_vitals_data_range):
    """Test isvalid method."""
    valid_vital = vitals(data=sample_valid_vitals_data)
    valid_vital.validate()
    assert valid_vital.isvalid() is True

    invalid_vital = vitals(data=sample_invalid_vitals_data_range)
    invalid_vital.validate()
    # isvalid() reflects the state after the last validate() call
    assert invalid_vital.isvalid() is False 

# validate method
@pytest.mark.usefixtures("patch_vitals_schema_path")
def test_vitals_validate_output(sample_valid_vitals_data, sample_invalid_vitals_data_range, capsys):
    """Test validate method output messages."""
    # Valid data
    vitals(data=sample_valid_vitals_data).validate()
    captured = capsys.readouterr()
    assert "Validation completed successfully." in captured.out

    # Invalid range data
    vitals(data=sample_invalid_vitals_data_range).validate()
    captured = capsys.readouterr()
    assert "Validation completed with" in captured.out
    assert "range validation error(s)" in captured.out

    # No data
    vital_obj_no_data = vitals()
    vital_obj_no_data.validate() # Explicit call as init with no data might not print this
    captured = capsys.readouterr()
    assert "No dataframe to validate." in captured.out

# Schema Properties Access
@pytest.mark.usefixtures("patch_vitals_schema_path")
def test_vitals_properties_access():
    """Test access to vital_units and vital_ranges properties."""
    from clifpy.schemas import load_schema
    vital_obj = vitals()
    schema = load_schema('vitals', vital_obj.clif_version)
    # Test vital_units
    units = vital_obj.vital_units
    assert units == schema['vital_units']
    assert id(units) != id(vital_obj._vital_units) # Ensure it's a copy
    # Modify the returned dict and check original is not affected
    units['new_key'] = 'new_value'
    assert 'new_key' not in vital_obj.vital_units

    # Test vital_ranges
    ranges = vital_obj.vital_ranges
    assert ranges == schema['vital_ranges']
    assert id(ranges) != id(vital_obj._vital_ranges) # Ensure it's a copy
    # Modify the returned dict and check original is not affected
    ranges['new_key'] = {'min': 0, 'max': 1}
    assert 'new_key' not in vital_obj.vital_ranges

    # Test properties when schema is not loaded
    vital_obj_no_schema = vitals()
    vital_obj_no_schema._vital_units = None
    vital_obj_no_schema._vital_ranges = None
    assert vital_obj_no_schema.vital_units == {}
    assert vital_obj_no_schema.vital_ranges == {}

# validate_vital_ranges method
@pytest.mark.usefixtures("patch_vitals_schema_path")
def test_validate_vital_ranges_valid(sample_valid_vitals_data):
    """Test validate_vital_ranges with valid data."""
    vital_obj = vitals(data=sample_valid_vitals_data)
    vital_obj.validate_vital_ranges()
    assert not vital_obj.range_validation_errors
    assert vital_obj.isvalid() is True

@pytest.mark.usefixtures("patch_vitals_schema_path")
def test_validate_vital_ranges_out_of_range(sample_invalid_vitals_data_range):
    """Test validate_vital_ranges with out-of-range values."""
    vital_obj = vitals(data=sample_invalid_vitals_data_range)
    vital_obj.validate_vital_ranges()
    assert len(vital_obj.range_validation_errors) > 0
    assert any(e['error_type'] == 'values_out_of_range' for e in vital_obj.range_validation_errors)
    # Check specific error details for one of the categories
    hr_error = next((e for e in vital_obj.range_validation_errors if e['vital_category'] == 'heart_rate'), None)
    assert hr_error is not None
    assert hr_error['min_value'] == 400.0 # In this fixture, only one HR value is provided, so min=max
    assert hr_error['max_value'] == 400.0
    assert 'maximum value 400.0 above expected 300' in hr_error['issues']
    assert vital_obj.isvalid() is False

@pytest.mark.usefixtures("patch_vitals_schema_path")
def test_validate_vital_ranges_unknown_category(sample_vitals_data_unknown_category):
    """Test validate_vital_ranges with an unknown vital category."""
    vital_obj = vitals(data=sample_vitals_data_unknown_category)
    vital_obj.validate_vital_ranges()
    assert len(vital_obj.range_validation_errors) > 0
    assert any(e['error_type'] == 'unknown_vital_category' for e in vital_obj.range_validation_errors)
    unknown_cat_error = next((e for e in vital_obj.range_validation_errors if e['error_type'] == 'unknown_vital_category'), None)
    assert unknown_cat_error is not None
    assert unknown_cat_error['vital_category'] == 'blood_sugar'
    assert vital_obj.isvalid() is False

@pytest.mark.usefixtures("patch_vitals_schema_path")
def test_validate_vital_ranges_missing_columns():
    """Test validate_vital_ranges with missing required columns."""
    # Missing vital_value
    data_missing_value = pd.DataFrame({
        'hospitalization_id': ['H001'],
        'recorded_dttm': pd.to_datetime(['2023-01-01 10:00']),
        'vital_category': ['heart_rate']
    })
    vital_obj_missing_value = vitals(data=data_missing_value)
    vital_obj_missing_value.validate_vital_ranges()
    # Here we explicitly check the range_validation_errors
    assert any(e['error_type'] == 'missing_columns_for_range_validation' for e in vital_obj_missing_value.range_validation_errors)
    assert 'vital_value' in vital_obj_missing_value.range_validation_errors[0]['message']
    
    # Missing vital_category
    data_missing_category = pd.DataFrame({
        'hospitalization_id': ['H001'],
        'recorded_dttm': pd.to_datetime(['2023-01-01 10:00']),
        'vital_value': [75.0]
    })
    vital_obj_missing_category = vitals(data=data_missing_category)
    vital_obj_missing_category.validate_vital_ranges()
    assert any(e['error_type'] == 'missing_columns_for_range_validation' for e in vital_obj_missing_category.range_validation_errors)
    assert 'vital_category' in vital_obj_missing_category.range_validation_errors[0]['message']

@pytest.mark.usefixtures("patch_vitals_schema_path")
def test_validate_vital_ranges_no_data_or_schema(capsys):
    """Test validate_vital_ranges with no data or no schema ranges."""
    # No DataFrame
    vital_obj_no_df = vitals()
    vital_obj_no_df.validate_vital_ranges() # Explicitly call
    assert not vital_obj_no_df.range_validation_errors

    # Empty DataFrame
    vital_obj_empty_df = vitals(data=pd.DataFrame(columns=['hospitalization_id', 'recorded_dttm', 'vital_category', 'vital_value']))
    vital_obj_empty_df.validate_vital_ranges()
    assert not vital_obj_empty_df.range_validation_errors

    # No vital_ranges in schema (e.g., schema loaded but _vital_ranges is empty)
    vital_obj_no_schema_ranges = vitals(data=pd.DataFrame({'vital_category': ['hr'], 'vital_value': [70]}))
    vital_obj_no_schema_ranges._vital_ranges = {} # Manually clear ranges after init
    vital_obj_no_schema_ranges.validate_vital_ranges()
    assert not vital_obj_no_schema_ranges.range_validation_errors # Should not attempt validation if no ranges defined

# --- Helper Method Tests ---

@pytest.mark.usefixtures("patch_vitals_schema_path")
def test_get_vital_categories(sample_valid_vitals_data):
    """Test get_vital_categories method."""
    # With data
    vital_obj = vitals(data=sample_valid_vitals_data)
    categories = vital_obj.get_vital_categories()
    assert isinstance(categories, list)
    assert set(categories) == set(['heart_rate', 'temp_c', 'sbp'])

    # With data missing vital_category column
    data_no_cat = sample_valid_vitals_data.drop(columns=['vital_category'])
    vital_obj_no_cat = vitals(data=data_no_cat)
    assert vital_obj_no_cat.get_vital_categories() == []

    # No data
    vital_obj_no_data = vitals()
    assert vital_obj_no_data.get_vital_categories() == []

@pytest.mark.usefixtures("patch_vitals_schema_path")
def test_filter_by_hospitalization(sample_valid_vitals_data):
    """Test filter_by_hospitalization method."""
    vital_obj = vitals(data=sample_valid_vitals_data)
    
    # Existing hospitalization_id
    filtered_df_h001 = vital_obj.filter_by_hospitalization('H001')
    assert len(filtered_df_h001) == 2
    assert all(filtered_df_h001['hospitalization_id'] == 'H001')

    # Non-existing hospitalization_id
    filtered_df_h003 = vital_obj.filter_by_hospitalization('H003')
    assert filtered_df_h003.empty

    # No data
    vital_obj_no_data = vitals()
    assert vital_obj_no_data.filter_by_hospitalization('H001').empty

    # Data missing hospitalization_id column
    data_no_hosp_id = sample_valid_vitals_data.drop(columns=['hospitalization_id'])
    vital_obj_no_hosp_id = vitals(data=data_no_hosp_id)
    # This will result in an empty DataFrame due to the key error during filtering, which is expected.
    assert vital_obj_no_hosp_id.filter_by_hospitalization('H001').empty

@pytest.mark.usefixtures("patch_vitals_schema_path")
def test_filter_by_vital_category(sample_valid_vitals_data):
    """Test filter_by_vital_category method."""
    vital_obj = vitals(data=sample_valid_vitals_data)

    # Existing vital_category
    filtered_df_hr = vital_obj.filter_by_vital_category('heart_rate')
    assert len(filtered_df_hr) == 1
    assert filtered_df_hr['vital_category'].iloc[0] == 'heart_rate'

    # Non-existing vital_category
    filtered_df_unknown = vital_obj.filter_by_vital_category('unknown_cat')
    assert filtered_df_unknown.empty

    # No data
    vital_obj_no_data = vitals()
    assert vital_obj_no_data.filter_by_vital_category('heart_rate').empty

    # Data missing vital_category column
    data_no_cat = sample_valid_vitals_data.drop(columns=['vital_category'])
    vital_obj_no_cat = vitals(data=data_no_cat)
    assert vital_obj_no_cat.filter_by_vital_category('heart_rate').empty

@pytest.mark.usefixtures("patch_vitals_schema_path")
def test_filter_by_date_range(sample_valid_vitals_data):
    """Test filter_by_date_range method."""
    vital_obj = vitals(data=sample_valid_vitals_data)
    start_date = datetime(2023, 1, 1, 10, 30)
    end_date = datetime(2023, 1, 2, 8, 59) # Before the last record

    # Filter within range
    filtered_df = vital_obj.filter_by_date_range(start_date, end_date)
    assert len(filtered_df) == 1
    assert filtered_df['hospitalization_id'].iloc[0] == 'H001' # The second H001 record
    assert filtered_df['recorded_dttm'].iloc[0] == pd.to_datetime('2023-01-01 11:00')

    # Filter all records
    filtered_all_df = vital_obj.filter_by_date_range(datetime(2023,1,1,0,0), datetime(2023,1,3,0,0))
    assert len(filtered_all_df) == 3

    # Filter no records (range outside)
    filtered_none_df = vital_obj.filter_by_date_range(datetime(2024,1,1,0,0), datetime(2024,1,3,0,0))
    assert filtered_none_df.empty

    # No data
    vital_obj_no_data = vitals()
    assert vital_obj_no_data.filter_by_date_range(start_date, end_date).empty

    # Data missing recorded_dttm column
    data_no_dttm = sample_valid_vitals_data.drop(columns=['recorded_dttm'])
    vital_obj_no_dttm = vitals(data=data_no_dttm)
    assert vital_obj_no_dttm.filter_by_date_range(start_date, end_date).empty

@pytest.mark.usefixtures("patch_vitals_schema_path")
def test_get_summary_stats(sample_valid_vitals_data):
    """Test get_summary_stats method."""
    # With data
    vital_obj = vitals(data=sample_valid_vitals_data)
    stats = vital_obj.get_summary_stats()
    assert stats['total_records'] == 3
    assert stats['unique_hospitalizations'] == 2
    assert stats['vital_category_counts']['heart_rate'] == 1
    assert 'vital_value_stats' in stats
    assert 'heart_rate' in stats['vital_value_stats']
    assert stats['vital_value_stats']['heart_rate']['mean'] == 75.0

    # No data
    vital_obj_no_data = vitals()
    stats_no_data = vital_obj_no_data.get_summary_stats()
    assert stats_no_data == {}

@pytest.mark.usefixtures("patch_vitals_schema_path")
def test_get_range_validation_report(sample_invalid_vitals_data_range, sample_valid_vitals_data):
    """Test get_range_validation_report method."""
    # With range errors
    vital_obj_errors = vitals(data=sample_invalid_vitals_data_range)
    vital_obj_errors.validate_vital_ranges()
    report_errors = vital_obj_errors.get_range_validation_report()
    assert isinstance(report_errors, pd.DataFrame)
    assert not report_errors.empty
    assert 'values_out_of_range' in report_errors['error_type'].tolist()

    # No range errors
    vital_obj_no_errors = vitals(data=sample_valid_vitals_data)
    vital_obj_no_errors.validate_vital_ranges()
    report_no_errors = vital_obj_no_errors.get_range_validation_report()
    assert isinstance(report_no_errors, pd.DataFrame)
    assert report_no_errors.empty # Or check for specific columns if an empty df with columns is returned
    # Based on current impl, it returns an empty df with columns if errors list is empty
    expected_cols = ['error_type', 'vital_category', 'affected_rows', 'message']
    if report_no_errors.empty and not vital_obj_no_errors.range_validation_errors:
         # If truly empty (no columns), this is fine. If it has columns, check them.
        pass 
    else:
        assert all(col in report_no_errors.columns for col in expected_cols)

# Retired: test_load_*_schema covered a JSON `*Model.json` loader that no
# longer exists. Schemas are versioned YAML loaded by BaseTable via
# clifpy.schemas.load_schema; these tests monkeypatched os.path.join for a
# file the package never opens.
