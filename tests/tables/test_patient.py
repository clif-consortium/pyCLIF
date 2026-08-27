"""
Tests for the patient table module.
"""
import pytest
import pandas as pd
from clifpy.tables.patient import Patient

# --- Data Fixtures ---
@pytest.fixture
def sample_valid_patient_data():
    """Create a valid patient DataFrame for testing."""
    return pd.DataFrame({
        'patient_id': ['P001', 'P002', 'P003'],
        'birth_date': pd.to_datetime(['1980-01-01', '1990-02-02', '2000-03-03']).date,
        'death_dttm': pd.to_datetime(['2024-12-01 08:15:00+00:00', '2024-12-01 08:15:00+00:00', '2024-12-01 08:15:00+00:00']),
        'race_name': ['white', 'black or african american', 'asian'],
        'race_category': ['white', 'black_or_african_american', 'asian'],
        'ethnicity_name': ['hispanic', 'non-hispanic', 'non-hispanic'],
        'ethnicity_category': ['non_hispanic', 'hispanic', 'non_hispanic'],
        'sex_name': ['male', 'female', 'male'],
        'sex_category': ['male', 'female', 'male'],
        'language_name': ['english', 'spanish', 'english'],
        'language_category': ['english', 'spanish', 'english']
    })

@pytest.fixture
def sample_patient_data_invalid_category():
    """Create a patient DataFrame with invalid categorical values."""
    return pd.DataFrame({
        'patient_id': ['P001'],
        'birth_date': pd.to_datetime(['1980-01-01']).date,
        'death_dttm': pd.to_datetime(['2024-12-01 08:15:00+00:00']),
        'race_name': ['white'],
        'ethnicity_name': ['hispanic'],
        'sex_name': ['male'],
        'language_name': ['english'],
        'race_category': ['INVALID_RACE'],  # Invalid value
        'ethnicity_category': ['Non-Hispanic'],
        'sex_category': ['Male'],
        'language_category': ['English']
    })

@pytest.fixture
def sample_patient_data_missing_cols():
    """Create a patient DataFrame with missing required columns."""
    return pd.DataFrame({
        'patient_id': ['P001'],
        'birth_date': pd.to_datetime(['1980-01-01']).date,
        'death_dttm': pd.to_datetime(['2024-12-01 08:15:00+00:00']),
        'race_name': ['white'],
        'ethnicity_name': ['hispanic'],
        'sex_name': ['male'],
        'language_name': ['english'],
        # Missing race_category, ethnicity_category, sex_category
    })

@pytest.fixture
def sample_patient_data_non_utc_timezone():
    """Create a patient DataFrame with invalid categorical values."""
    return pd.DataFrame({
        'patient_id': ['P001'],
        'birth_date': pd.to_datetime(['1980-01-01']).date,
        'death_dttm': pd.to_datetime(['2024-12-01 08:15:00 EST']),
        'race_name': ['white'],
        'ethnicity_name': ['hispanic'],
        'sex_name': ['male'],
        'language_name': ['english'],
        'race_category': ['INVALID_RACE'],  # Invalid value
        'ethnicity_category': ['Non-Hispanic'],
        'sex_category': ['Male'],
        'language_category': ['English']
    })

@pytest.fixture
def mock_patient_file(tmp_path, sample_valid_patient_data):
    """Create a mock patient parquet file for testing."""
    test_dir = tmp_path / "test_data"
    test_dir.mkdir()
    file_path = test_dir / "clif_patient.parquet"
    sample_valid_patient_data.to_parquet(file_path)
    return str(test_dir) # from_file expects directory path

# --- Tests for patient class ---

# Initialization and Schema Loading
def test_patient_init_with_valid_data(sample_valid_patient_data):
    """Test patient initialization with valid data and mocked schema."""
    patient_obj = Patient(data=sample_valid_patient_data)   
    patient_obj.validate()
    assert patient_obj.df is not None
    assert patient_obj.isvalid() is True
    assert not patient_obj.errors

def test_patient_init_with_invalid_category(sample_patient_data_invalid_category):
    """Test patient initialization with invalid categorical data."""
    patient_obj = Patient(data=sample_patient_data_invalid_category)
    patient_obj.validate()
    assert patient_obj.isvalid() is False
    assert len(patient_obj.errors) > 0
    error_types = {e['type'] for e in patient_obj.errors}
    assert "Invalid Categorical Values" in error_types
    assert "Missing Required Columns" not in error_types

def test_patient_init_with_missing_columns(sample_patient_data_missing_cols):
    """Test patient initialization with missing required columns."""
    patient_obj = Patient(data=sample_patient_data_missing_cols)
    patient_obj.validate()
    assert patient_obj.isvalid() is False
    assert len(patient_obj.errors) > 0
    error_types = {e['type'] for e in patient_obj.errors}
    assert "Missing Required Columns" in error_types
    missing_cols = [e['details']['column'] for e in patient_obj.errors
                    if e['type'] == 'Missing Required Columns']
    assert set(missing_cols) == {'race_category', 'ethnicity_category', 'sex_category', 'language_category'}


def test_patient_init_without_data():
    """Test patient initialization without data."""
    patient_obj = Patient()
    patient_obj.validate()
    assert patient_obj.df is None
    assert patient_obj.isvalid() is False # isvalid is True because no errors were generated
    assert not patient_obj.errors

def test_timezone_validation_non_utc_datetime(sample_patient_data_non_utc_timezone):
    """Test that non-UTC datetime columns fail timezone validation."""
    patient_obj = Patient(data=sample_patient_data_non_utc_timezone)
    patient_obj.validate()
    
    # Should fail due to non-UTC timezone
    assert patient_obj.isvalid() is False
    

# from_file constructor
def test_patient_from_file(mock_patient_file):
    """Test loading patient data from a parquet file."""
    patient_obj = Patient.from_file(data_directory=mock_patient_file, filetype="parquet", timezone="UTC")
    assert patient_obj.df is not None

def test_patient_from_file_nonexistent(tmp_path):
    """Test loading patient data from a nonexistent file."""
    non_existent_path = str(tmp_path / "nonexistent_dir")
    with pytest.raises(FileNotFoundError):
        Patient.from_file(non_existent_path, filetype="parquet", timezone="UTC")

# isvalid method
def test_patient_isvalid(sample_valid_patient_data, sample_patient_data_invalid_category):
    """Test isvalid method."""
    valid_patient = Patient(data=sample_valid_patient_data)
    valid_patient.validate()
    assert valid_patient.isvalid() is True
    
    invalid_patient = Patient(data=sample_patient_data_invalid_category)
    invalid_patient.validate()
    assert invalid_patient.isvalid() is False

# validate method
def test_patient_validate_output(sample_patient_data_invalid_category, capsys):
    """Test validate method output messages."""    
    # Invalid data
    invalid_patient = Patient(data=sample_patient_data_invalid_category)
    invalid_patient.validate()
    captured = capsys.readouterr()
    assert "Validation completed with 2 error(s)" in captured.out
    
    # No data
    p_no_data = Patient()
    p_no_data.validate() # Explicitly call validate
    captured = capsys.readouterr()
    assert "No dataframe to validate" in captured.out
