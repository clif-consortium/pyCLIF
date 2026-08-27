"""
Tests for the hospitalization table module.
"""
import pytest
import pandas as pd
from clifpy.tables.hospitalization import Hospitalization

# --- Data Fixtures ---
@pytest.fixture
def sample_valid_hospitalization_data():
    """Create a valid hospitalization DataFrame for testing."""
    return pd.DataFrame({
        'patient_id': ['P001', 'P002', 'P003'],
        'hospitalization_id': ['H001', 'H002', 'H003'],
        'admission_dttm': pd.to_datetime(['2023-01-01 10:00:00', '2023-02-01 14:30:00', '2023-03-01 08:15:00']).tz_localize('UTC'),
        'discharge_dttm': pd.to_datetime(['2023-01-05 16:00:00', '2023-02-08 12:00:00', '2023-03-10 09:30:00']).tz_localize('UTC'),
        'age_at_admission': [65, 45, 72],
        'admission_type_name': ['emergency', 'elective', 'urgent'],
        'admission_type_category': ['ed', 'elective', 'direct'],
        'discharge_name': ['home', 'snf', 'expired'],
        'discharge_category': ['home', 'snf', 'expired'],
        'zipcode_nine_digit': ['12345-6789', '23456-7890', '34567-8901'],
        'zipcode_five_digit': ['12345', '23456', '34567']
    })

@pytest.fixture
def sample_hospitalization_data_invalid_category():
    """Create a hospitalization DataFrame with invalid categorical values."""
    return pd.DataFrame({
        'patient_id': ['P001'],
        'hospitalization_id': ['H001'],
        'admission_dttm': pd.to_datetime(['2023-01-01 10:00:00']).tz_localize('UTC'),
        'discharge_dttm': pd.to_datetime(['2023-01-05 16:00:00']).tz_localize('UTC'),
        'age_at_admission': [65],
        'admission_type_name': ['emergency'],
        'admission_type_category': ['Emergency'],
        'discharge_name': ['home'],
        'discharge_category': ['INVALID_DISCHARGE'],  # Invalid value
        'zipcode_nine_digit': ['12345-6789'],
        'zipcode_five_digit': ['12345']
    })

@pytest.fixture
def sample_hospitalization_data_missing_cols():
    """Create a hospitalization DataFrame with missing required columns."""
    return pd.DataFrame({
        'patient_id': ['P001'],
        'hospitalization_id': ['H001'],
        'admission_dttm': pd.to_datetime(['2023-01-01 10:00:00']).tz_localize('UTC'),
        'discharge_dttm': pd.to_datetime(['2023-01-05 16:00:00']).tz_localize('UTC'),
        'age_at_admission': [65],
        'admission_type_name': ['emergency'],
        'admission_type_category': ['Emergency'],
        'discharge_name': ['home'],
        'zipcode_nine_digit': ['12345-6789'],
        'zipcode_five_digit': ['12345']
    })

@pytest.fixture
def sample_hospitalization_data_invalid_datetime():
    """Create a hospitalization DataFrame with timezone-naive datetime columns."""
    return pd.DataFrame({
        'patient_id': ['P001'],
        'hospitalization_id': ['H001'],
        'admission_dttm': pd.to_datetime(['2023-01-01 10:00:00']),
        'discharge_dttm': pd.to_datetime(['2023-01-05 16:00:00']),
        'age_at_admission': [65],
        'admission_type_name': ['emergency'],
        'admission_type_category': ['Emergency'],
        'discharge_name': ['home'],
        'discharge_category': ['Home'],  # Invalid value
        'zipcode_nine_digit': ['12345-6789'],
        'zipcode_five_digit': ['12345']
    })

@pytest.fixture
def sample_hospitalization_data_non_utc_timezone():
    """Create a hospitalization DataFrame with non-UTC timezone datetime columns."""
    return pd.DataFrame({
        'patient_id': ['P001'],
        'hospitalization_id': ['H001'],
        'admission_dttm': pd.to_datetime(['2023-01-01 10:00:00']).tz_localize('America/New_York'),
        'discharge_dttm': pd.to_datetime(['2023-01-05 16:00:00']).tz_localize('UTC'),
        'age_at_admission': [65],
        'admission_type_name': ['emergency'],
        'admission_type_category': ['Emergency'],
        'discharge_name': ['home'],
        'discharge_category': ['Home'],  # Invalid value
        'zipcode_nine_digit': ['12345-6789'],
        'zipcode_five_digit': ['12345']
    })

@pytest.fixture
def sample_hospitalization_data_inccorect_5zip():
    """Create a hospitalization DataFrame with incorrect 5-digit zip codes (too short)."""
    return pd.DataFrame({
        'patient_id': ['P001', 'P002', 'P003'],
        'hospitalization_id': ['H001', 'H002', 'H003'],
        'admission_dttm': pd.to_datetime(['2023-01-01 10:00:00', '2023-02-01 14:30:00', '2023-03-01 08:15:00']).tz_localize('UTC'),
        'discharge_dttm': pd.to_datetime(['2023-01-05 16:00:00', '2023-02-08 12:00:00', '2023-03-10 09:30:00']).tz_localize('UTC'),
        'age_at_admission': [65, 45, 72],
        'admission_type_name': ['emergency', 'elective', 'urgent'],
        'admission_type_category': ['Emergency', 'Elective', 'Urgent'],
        'discharge_name': ['home', 'snf', 'expired'],
        'discharge_category': ['Home', 'Skilled Nursing Facility (SNF)', 'Expired'],
        'zipcode_nine_digit': ['12345-6789', '23456-7890', '34567-8901'],
        'zipcode_five_digit': ['123', '234', '345']
    })

@pytest.fixture
def sample_hospitalization_data_inccorect_9zip():
    """Create a hospitalization DataFrame with incorrect 9-digit zip codes (missing dash or too short)."""
    return pd.DataFrame({
        'patient_id': ['P001', 'P002', 'P003'],
        'hospitalization_id': ['H001', 'H002', 'H003'],
        'admission_dttm': pd.to_datetime(['2023-01-01 10:00:00', '2023-02-01 14:30:00', '2023-03-01 08:15:00']).tz_localize('UTC'),
        'discharge_dttm': pd.to_datetime(['2023-01-05 16:00:00', '2023-02-08 12:00:00', '2023-03-10 09:30:00']).tz_localize('UTC'),
        'age_at_admission': [65, 45, 72],
        'admission_type_name': ['emergency', 'elective', 'urgent'],
        'admission_type_category': ['Emergency', 'Elective', 'Urgent'],
        'discharge_name': ['home', 'snf', 'expired'],
        'discharge_category': ['Home', 'Skilled Nursing Facility (SNF)', 'Expired'],
        'zipcode_nine_digit': ['123456789', '23456-7890', '34567-8901'],
        'zipcode_five_digit': ['12345', '23456', '34567']
    })

@pytest.fixture
def mock_hospitalization_file(tmp_path, sample_valid_hospitalization_data):
    """Create a mock hospitalization parquet file for testing."""
    test_dir = tmp_path / "test_data"
    test_dir.mkdir()
    file_path = test_dir / "clif_hospitalization.parquet"
    sample_valid_hospitalization_data.to_parquet(file_path)
    return str(test_dir)  # from_file expects directory path

# --- Tests for Hospitalization class ---

# Initialization and Schema Loading
def test_hospitalization_init_with_valid_data(sample_valid_hospitalization_data):
    """Test hospitalization initialization with valid data and mocked schema."""
    hosp_obj = Hospitalization(data=sample_valid_hospitalization_data)   
    hosp_obj.validate()
    assert hosp_obj.df is not None
    assert hosp_obj.isvalid() is True
    assert not hosp_obj.errors

def test_hospitalization_init_with_invalid_category(sample_hospitalization_data_invalid_category):
    """Test hospitalization initialization with invalid categorical data."""
    hosp_obj = Hospitalization(data=sample_hospitalization_data_invalid_category)
    hosp_obj.validate()
    assert hosp_obj.isvalid() is False
    assert len(hosp_obj.errors) > 0
    error_types = {e['type'] for e in hosp_obj.errors}
    assert "Invalid Categorical Values" in error_types

def test_hospitalization_init_with_missing_columns(sample_hospitalization_data_missing_cols):
    """Test hospitalization initialization with missing required columns."""
    hosp_obj = Hospitalization(data=sample_hospitalization_data_missing_cols)
    hosp_obj.validate()
    assert hosp_obj.isvalid() is False
    assert len(hosp_obj.errors) > 0
    error_types = {e['type'] for e in hosp_obj.errors}
    assert "Missing Required Columns" in error_types
    missing_cols = [e['details']['column'] for e in hosp_obj.errors
                    if e['type'] == 'Missing Required Columns']
    assert set(missing_cols) == {'discharge_category'}

def test_hospitalization_init_without_data():
    """Test hospitalization initialization without data."""
    hosp_obj = Hospitalization()
    hosp_obj.validate()
    assert hosp_obj.df is None
def test_hospitalization_from_file(mock_hospitalization_file):
    """Test loading hospitalization data from a parquet file."""
    hosp_obj = Hospitalization.from_file(data_directory=mock_hospitalization_file, filetype="parquet", timezone="UTC")
    assert hosp_obj.df is not None

def test_hospitalization_from_file_nonexistent(tmp_path):
    """Test loading hospitalization data from a nonexistent file."""
    non_existent_path = str(tmp_path / "nonexistent_dir")
    with pytest.raises(FileNotFoundError):
        Hospitalization.from_file(non_existent_path, filetype="parquet", timezone="UTC")

# isvalid method
def test_hospitalization_isvalid(sample_valid_hospitalization_data, sample_hospitalization_data_invalid_category):
    """Test isvalid method."""
    valid_hosp = Hospitalization(data=sample_valid_hospitalization_data)
    valid_hosp.validate()
    assert valid_hosp.isvalid() is True
    
    invalid_hosp = Hospitalization(data=sample_hospitalization_data_invalid_category)
    invalid_hosp.validate()
    assert invalid_hosp.isvalid() is False

# validate method
def test_hospitalization_validate_output(sample_valid_hospitalization_data, sample_hospitalization_data_invalid_category, capsys):
    """Test validate method output messages."""
    # Valid data
    valid_hosp = Hospitalization(data=sample_valid_hospitalization_data)
    valid_hosp.validate()
    captured = capsys.readouterr()
    assert "Validation completed successfully" in captured.out
    
    # Invalid data
    invalid_hosp = Hospitalization(data=sample_hospitalization_data_invalid_category)
    invalid_hosp.validate()
    captured = capsys.readouterr()
    assert "Validation completed with" in captured.out
    assert "error(s)" in captured.out
    
    # No data
    h_no_data = Hospitalization()
    h_no_data.validate()
    captured = capsys.readouterr()
    assert "No dataframe to validate" in captured.out

# # --- Tests for incorrect zip code fixtures ---

# def test_hospitalization_incorrect_5zip(sample_hospitalization_data_inccorect_5zip):
#     """Test that hospitalization data with incorrect 5-digit zip codes is detected as invalid."""
#     hosp_obj = Hospitalization(data=sample_hospitalization_data_inccorect_5zip)
#     hosp_obj.validate()
#     assert not hosp_obj.isvalid()
#     # Check for zip code error in errors
#     zip_errors = [e for e in hosp_obj.errors if 'zipcode_five_digit' in str(e.get('columns', '')) or e.get('column', '') == 'zipcode_five_digit']
#     assert zip_errors, "Should have error(s) related to zipcode_five_digit"

# def test_hospitalization_incorrect_9zip(sample_hospitalization_data_inccorect_9zip):
#     """Test that hospitalization data with incorrect 9-digit zip codes is detected as invalid."""
#     hosp_obj = Hospitalization(data=sample_hospitalization_data_inccorect_9zip)
#     hosp_obj.validate()
#     assert not hosp_obj.isvalid()
#     # Check for zip code error in errors
#     zip_errors = [e for e in hosp_obj.errors if 'zipcode_nine_digit' in str(e.get('columns', '')) or e.get('column', '') == 'zipcode_nine_digit']
#     assert zip_errors, "Should have error(s) related to zipcode_nine_digit"
