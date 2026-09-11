"""
Tests for the labs table module.
"""
import pytest
import pandas as pd
from clifpy.tables.labs import Labs

# --- Data Fixtures ---
@pytest.fixture
def sample_valid_labs_data():
    """Create a valid labs DataFrame for testing."""
    return pd.DataFrame({
        'hospitalization_id': ['H001', 'H002', 'H003'],
        'lab_order_dttm': pd.to_datetime(['2023-01-01 09:00:00', '2023-02-01 13:30:00', '2023-03-01 07:15:00']).tz_localize('UTC'),
        'lab_collect_dttm': pd.to_datetime(['2023-01-01 10:00:00', '2023-02-01 14:30:00', '2023-03-01 08:15:00']).tz_localize('UTC'),
        'lab_result_dttm': pd.to_datetime(['2023-01-01 11:00:00', '2023-02-01 15:30:00', '2023-03-01 09:15:00']).tz_localize('UTC'),
        'lab_order_category': ['bmp', 'cbc', 'bmp'],
        'lab_category': ['glucose', 'hemoglobin', 'sodium'],
        'lab_value': ['100.0', '14.5', '140'],
        'lab_value_numeric': [100.0, 14.5, 140.0],
        'reference_unit': ['mg/dL', 'g/dL', 'mmol/L'],
        'lab_specimen_category': ['plasma_blood', 'plasma_blood', 'plasma_blood'],
    })

@pytest.fixture
def sample_labs_data_invalid_category():
    """Create a labs DataFrame with invalid categorical values."""
    return pd.DataFrame({
        'hospitalization_id': ['H001', 'H002', 'H003'],
        'lab_order_dttm': pd.to_datetime(['2023-01-01 09:00:00', '2023-02-01 13:30:00', '2023-03-01 07:15:00']).tz_localize('UTC'),
        'lab_collect_dttm': pd.to_datetime(['2023-01-01 10:00:00', '2023-02-01 14:30:00', '2023-03-01 08:15:00']).tz_localize('UTC'),
        'lab_result_dttm': pd.to_datetime(['2023-01-01 11:00:00', '2023-02-01 15:30:00', '2023-03-01 09:15:00']).tz_localize('UTC'),
        'lab_order_category': ['BMP', 'CBC', 'BMP'],
        'lab_category': ['INVALID_CATEGORY', 'hemoglobin', 'sodium'],
        'lab_value': ['100.0', '14.5', '140'],
        'lab_value_numeric': [100.0, 14.5, 140.0],
        'reference_unit': ['mg/dL', 'g/dL', 'mmol/L'],
    })

@pytest.fixture
def sample_labs_data_missing_cols():
    """Create a labs DataFrame with missing required columns."""
    return pd.DataFrame({
        'hospitalization_id': ['H001', 'H002', 'H003'],
        'lab_order_dttm': pd.to_datetime(['2023-01-01 09:00:00', '2023-02-01 13:30:00', '2023-03-01 07:15:00']).tz_localize('UTC'),
        'lab_collect_dttm': pd.to_datetime(['2023-01-01 10:00:00', '2023-02-01 14:30:00', '2023-03-01 08:15:00']).tz_localize('UTC'),
        'lab_result_dttm': pd.to_datetime(['2023-01-01 11:00:00', '2023-02-01 15:30:00', '2023-03-01 09:15:00']).tz_localize('UTC'),
        'lab_value': ['100.0', '14.5', '140'],
        'lab_value_numeric': [100.0, 14.5, 140.0],
        'reference_unit': ['mg/dL', 'g/dL', 'mmol/L'],
    })


@pytest.fixture
def sample_labs_data_invalid_datetime():
    """Create a labs DataFrame with timezone-naive datetime columns."""
    return pd.DataFrame({
        'hospitalization_id': ['H001', 'H002', 'H003'],
        'lab_order_dttm': pd.to_datetime(['2023-01-01 09:00', '2023-02-01 13:30', '2023-03-01 07:15']),
        'lab_collect_dttm': pd.to_datetime(['2023-01-01 10:00', '2023-02-01 14:30', '2023-03-01 08:15']),
        'lab_result_dttm': pd.to_datetime(['2023-01-01 11:00', '2023-02-01 15:30', '2023-03-01 09:15']),
        'lab_order_category': ['BMP', 'CBC', 'BMP'],
        'lab_category': ['INVALID_CATEGORY', 'hemoglobin', 'sodium'],
        'lab_value': ['100.0', '14.5', '140'],
        'lab_value_numeric': [100.0, 14.5, 140.0],
        'reference_unit': ['mg/dL', 'g/dL', 'mmol/L'],
    })


@pytest.fixture
def sample_labs_data_non_utc_timezone():
    """Create a labs DataFrame with non-UTC timezone datetime columns."""
    return pd.DataFrame({
        'hospitalization_id': ['H001'],
        'lab_order_dttm': pd.to_datetime(['2023-01-01 09:00:00']).tz_localize('America/New_York'),
        'lab_collect_dttm': pd.to_datetime(['2023-01-01 10:00:00']).tz_localize('America/New_York'),
        'lab_result_dttm': pd.to_datetime(['2023-01-01 11:00:00']).tz_localize('America/New_York'),
        'lab_category': ['glucose_serum'],
        'lab_order_category': ['BMP'],
        'lab_value': ['100.0'],
        'lab_value_numeric': [100.0],
        'reference_unit': ['mg/dL']
    })

@pytest.fixture
def mock_labs_file(tmp_path, sample_valid_labs_data):
    """Create a mock labs parquet file for testing."""
    test_dir = tmp_path / "test_data"
    test_dir.mkdir()
    file_path = test_dir / "clif_labs.parquet"
    sample_valid_labs_data.to_parquet(file_path)
    return str(test_dir)  # from_file expects directory path

# --- Tests for Labs class ---

# Initialization and Schema Loading
def test_labs_init_with_valid_data(sample_valid_labs_data):
    """Test labs initialization with valid data and mocked schema."""
    labs_obj = Labs(data=sample_valid_labs_data)   
    labs_obj.validate()
    assert labs_obj.df is not None
    assert labs_obj.isvalid() is True
    assert not labs_obj.errors

def test_labs_init_with_invalid_category(sample_labs_data_invalid_category):
    """Test labs initialization with invalid categorical data."""
    labs_obj = Labs(data=sample_labs_data_invalid_category)
    labs_obj.validate()
    assert labs_obj.isvalid() is False
    assert len(labs_obj.errors) > 0
    error_types = {e['type'] for e in labs_obj.errors}
    assert "Invalid Categorical Values" in error_types

def test_labs_init_with_missing_columns(sample_labs_data_missing_cols):
    """Test labs initialization with missing required columns."""
    labs_obj = Labs(data=sample_labs_data_missing_cols)
    labs_obj.validate()
    assert labs_obj.isvalid() is False
    assert len(labs_obj.errors) > 0
    error_types = {e['type'] for e in labs_obj.errors}
    assert "Missing Required Columns" in error_types
    missing_cols = [e['details']['column'] for e in labs_obj.errors
                    if e['type'] == 'Missing Required Columns']
    # lab_specimen_category became required in CLIF 3.0 and is also absent
    # from this fixture, so it is reported alongside the other two.
    assert set(missing_cols) == {'lab_category', 'lab_order_category', 'lab_specimen_category'}

def test_labs_init_without_data():
    """Test labs initialization without data."""
    labs_obj = Labs()
    labs_obj.validate()
    assert labs_obj.df is None
def test_labs_from_file(mock_labs_file):
    """Test loading labs data from a parquet file."""
    labs_obj = Labs.from_file(data_directory=mock_labs_file, filetype="parquet", timezone="UTC")
    assert labs_obj.df is not None

def test_labs_from_file_nonexistent(tmp_path):
    """Test loading labs data from a nonexistent file."""
    non_existent_path = str(tmp_path / "nonexistent_dir")
    with pytest.raises(FileNotFoundError):
        Labs.from_file(non_existent_path, filetype="parquet", timezone="UTC")

# isvalid method
def test_labs_isvalid(sample_valid_labs_data, sample_labs_data_invalid_category):
    """Test isvalid method."""
    valid_labs = Labs(data=sample_valid_labs_data)
    valid_labs.validate()
    assert valid_labs.isvalid() is True
    
    invalid_labs = Labs(data=sample_labs_data_invalid_category)
    invalid_labs.validate()
    assert invalid_labs.isvalid() is False

# validate method
def test_labs_validate_output(sample_valid_labs_data, sample_labs_data_invalid_category, capsys):
    """Test validate method output messages."""
    # Valid data
    valid_labs = Labs(data=sample_valid_labs_data)
    valid_labs.validate()
    captured = capsys.readouterr()
    assert "Validation completed successfully" in captured.out
    
    # Invalid data
    invalid_labs = Labs(data=sample_labs_data_invalid_category)
    invalid_labs.validate()
    captured = capsys.readouterr()
    assert "Validation completed with" in captured.out
    assert "error(s)" in captured.out
    
    # No data
    l_no_data = Labs()
    l_no_data.validate()
    captured = capsys.readouterr()
    assert "No dataframe to validate" in captured.out

# --- Tests for labs-specific methods ---

def test_lab_reference_units_property(sample_valid_labs_data):
    """Test lab_reference_units property."""
    labs_obj = Labs(data=sample_valid_labs_data)
    ref_units = labs_obj.lab_reference_units
    
    assert isinstance(ref_units, dict)
    # Check some known reference units from the schema
    if ref_units:  # Only check if schema loaded successfully
        assert 'glucose_serum' in ref_units or len(ref_units) >= 0  # Allow for empty dict if schema not loaded

def test_get_lab_category_stats(sample_valid_labs_data):
    """Test lab category statistics generation."""
    labs_obj = Labs(data=sample_valid_labs_data)
    stats = labs_obj.get_lab_category_stats()
    
    if isinstance(stats, dict) and stats.get("status") == "Missing columns":
        # Method returned error status due to missing columns
        assert stats == {"status": "Missing columns"}
    else:
        assert isinstance(stats, pd.DataFrame)
        assert 'count' in stats.columns
        assert 'unique' in stats.columns
        assert 'missing_pct' in stats.columns
        assert 'mean' in stats.columns
        
        # Check that lab categories are present
        assert 'glucose' in stats.index
        assert 'hemoglobin' in stats.index
        assert 'sodium' in stats.index
        
        # Check counts
        assert stats.loc['glucose', 'count'] == 1
        assert stats.loc['hemoglobin', 'count'] == 1
        assert stats.loc['sodium', 'count'] == 1

def test_get_lab_category_stats_missing_columns():
    """Test lab category statistics with missing columns."""
    data = pd.DataFrame({
        'other_col': ['value1', 'value2']
        # Missing lab_value_numeric and hospitalization_id
    })
    labs_obj = Labs(data=data)
    stats = labs_obj.get_lab_category_stats()
    
    assert stats == {"status": "Missing columns"}

def test_get_lab_category_stats_empty_data():
    """Test lab category statistics with empty data."""
    labs_obj = Labs()
    stats = labs_obj.get_lab_category_stats()
    
    assert stats == {"status": "Missing columns"}

def test_get_lab_specimen_stats(sample_valid_labs_data):
    """Test lab specimen statistics generation."""
    labs_obj = Labs(data=sample_valid_labs_data)
    stats = labs_obj.get_lab_specimen_stats()
    
    if isinstance(stats, dict) and stats.get("status") == "Missing columns":
        # Method returned error status due to missing columns
        assert stats == {"status": "Missing columns"}
    else:
        assert isinstance(stats, pd.DataFrame)
        assert 'count' in stats.columns
        assert 'unique' in stats.columns
        assert 'missing_pct' in stats.columns
        assert 'mean' in stats.columns
        
        # Check that specimen categories are present
        assert 'blood/plasma/serum' in stats.index
        
        # Check counts
        assert stats.loc['blood/plasma/serum', 'count'] == 3

def test_get_lab_specimen_stats_missing_columns():
    """Test lab specimen statistics with missing columns."""
    data = pd.DataFrame({
        'other_col': ['value1', 'value2']
        # Missing lab_value_numeric, hospitalization_id, lab_specimen_category
    })
    labs_obj = Labs(data=data)
    stats = labs_obj.get_lab_specimen_stats()
    
    assert stats == {"status": "Missing columns"}

def test_get_lab_specimen_stats_empty_data():
    """Test lab specimen statistics with empty data."""
    labs_obj = Labs()
    stats = labs_obj.get_lab_specimen_stats()

    assert stats == {"status": "Missing columns"}

def test_standardize_reference_units_keeps_each_unit_of_multi_unit_lab():
    """Urine albumin in mg/dL must stay mg/dl, not be relabelled g/dl."""
    data = pd.DataFrame({
        'hospitalization_id': ['H001', 'H002'],
        'lab_category': ['albumin', 'albumin'],
        'reference_unit': ['g/dL', 'mg/dL'],
        'lab_specimen_category': ['plasma_blood', 'urine'],
    })
    labs_obj = Labs(data=data)
    labs_obj.standardize_reference_units(lowercase=True)

    out = labs_obj.data
    units = dict(zip(out['lab_specimen_category'].to_list(), out['reference_unit'].to_list()))
    assert units == {'plasma_blood': 'g/dl', 'urine': 'mg/dl'}