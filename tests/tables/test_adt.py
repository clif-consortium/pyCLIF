"""
Tests for the ADT (Admission, Discharge, Transfer) table module.
"""
import pytest
import pandas as pd
from clifpy.tables.adt import Adt
# from datetime import datetime
# from typing import Union, Any
# from clifpy.tables.adt import adt
# import clifpy.utils.validator # To patch its os module

# --- Data Fixtures ---
@pytest.fixture
def sample_valid_adt_data():
    """Create a valid ADT DataFrame for testing."""
    return pd.DataFrame({
        'hospitalization_id': ['H001', 'H001', 'H002', 'H003'],
        'hospital_id': ['HOSP_A', 'HOSP_A', 'HOSP_B', 'HOSP_A'],
        'patient_id': ['P001', 'P001', 'P002', 'P003'],
        'in_dttm': pd.to_datetime(['2023-01-01 10:00:00+00:00', '2023-01-01 14:00:00+00:00', '2023-01-05 09:00:00+00:00', '2023-01-10 12:00:00+00:00']),
        'out_dttm': pd.to_datetime(['2023-01-01 13:59:00+00:00', '2023-01-03 18:00:00+00:00', '2023-01-08 11:00:00+00:00', '2023-01-12 15:00:00+00:00']),
        'location_name': ['B06F', 'B06T', 'T09F', 'N23E'],
        'location_category': ['ed', 'icu', 'ward', 'icu'],
        'hospital_type': ['academic', 'academic', 'academic', 'academic'],
        'location_type': ['general_icu', 'medical_icu', 'general_icu', 'general_icu'],
        'room_id': ['R101', 'R202', 'R303', 'R404']
    })

@pytest.fixture
def sample_adt_data_missing_cols():
    """Create an ADT DataFrame with schema violations."""
    return pd.DataFrame({
        'hospitalization_id': ['H001', 'H002'],
        'in_dttm': ['2023-01-01 10:00:00+00:00', '2023-01-05 09:00:00+00:00'], # Invalid datetime format for direct use / wrong type
        'out_dttm': pd.to_datetime(['2023-01-01 13:59:00+00:00', '2023-01-08 11:00:00+00:00']),
        'location_category': ['ed', 'ward']
    })


@pytest.fixture
def sample_adt_data_invalid_category():
    """Create an ADT DataFrame with invalid category values."""
    return pd.DataFrame({
        'hospitalization_id': ['H001', 'H002'],
        'hospital_id': ['HOSP_A', 'HOSP_B'],
        'patient_id': ['P001', 'P002'],
        'in_dttm': pd.to_datetime(['2023-01-01 10:00:00+00:00', '2023-01-05 09:00:00+00:00']),
        'out_dttm': pd.to_datetime(['2023-01-01 13:59:00+00:00', '2023-01-08 11:00:00+00:00']),
        'location_name': ['B06F', 'B06T'],
        'room_id': ['R001', 'R002'],  # required from CLIF 3.0
        'location_category': ['INVALID_LOCATION', 'ICU'], # Invalid location
        'hospital_type': ['ACADEMIC', 'INVALID_HOSP_TYPE'], # Invalid hospital type
        'location_type': ['INVALID_icu', 'medical_icu'] # Invalid location type
    })

@pytest.fixture
def sample_adt_data_invalid_datetime():
    """Create an ADT DataFrame with invalid category values."""
    return pd.DataFrame({
        'hospitalization_id': ['H001', 'H002'],
        'hospital_id': ['HOSP_A', 'HOSP_B'],
        'patient_id': ['P001', 'P002'],
        'in_dttm': pd.to_datetime(['2023-01-01 13:59:00', '2023-01-01 13:59:00']),
        'out_dttm': pd.to_datetime(['2023-01-01 13:59:00+00:00', '2023-01-08 11:00:00+00:00']),
        'location_name': ['B06F', 'B06T'],
        'room_id': ['R001', 'R002'],  # required from CLIF 3.0
        'location_category': ['INVALID_LOCATION', 'ICU'], # Invalid location
        'hospital_type': ['ACADEMIC', 'INVALID_HOSP_TYPE'], # Invalid hospital type
        'location_type': ['INVALID_icu', 'medical_icu'] # Invalid location type
    })

@pytest.fixture
def mock_adt_file(tmp_path, sample_valid_adt_data):
    """Create a mock patient parquet file for testing."""
    test_dir = tmp_path / "test_data"
    test_dir.mkdir()
    file_path = test_dir / "clif_adt.parquet"
    sample_valid_adt_data.to_parquet(file_path)
    return str(test_dir) # from_file expects directory path

# --- Tests for adt class --- 

# Initialization and Schema Loading
def test_adt_init_with_valid_data(sample_valid_adt_data):
    """Test adt initialization with valid data and mocked schema."""
    adt_obj = Adt(data=sample_valid_adt_data)   
    adt_obj.validate()
    assert adt_obj.df is not None
    assert adt_obj.isvalid() is True
    assert not adt_obj.errors

def test_adt_init_with_invalid_category(sample_adt_data_invalid_category):
    """Test adt initialization with invalid categorical data."""
    adt_obj = Adt(data=sample_adt_data_invalid_category)
    adt_obj.validate()
    assert adt_obj.isvalid() is False
    assert len(adt_obj.errors) > 0
    error_types = {e['type'] for e in adt_obj.errors}
    assert "Invalid Categorical Values" in error_types
    assert "Missing Required Columns" not in error_types

def test_adt_init_with_missing_columns(sample_adt_data_missing_cols):
    """Test adt initialization with missing required columns."""
    adt_obj = Adt(data=sample_adt_data_missing_cols)
    adt_obj.validate()
    assert adt_obj.isvalid() is False
    assert len(adt_obj.errors) > 0
    error_types = {e['type'] for e in adt_obj.errors}
    assert "Missing Required Columns" in error_types
    missing_cols = [e['details']['column'] for e in adt_obj.errors
                    if e['type'] == 'Missing Required Columns']
    assert set(missing_cols) == {'hospital_id', 'hospital_type', 'location_type', 'room_id'}

def test_adt_init_without_data():
    """Test adt initialization without data."""
    adt_obj = Adt()
    adt_obj.validate()
    assert adt_obj.df is None
    assert adt_obj.isvalid() is False # isvalid is True because no errors were generated
    assert not adt_obj.errors

def test_timezone_validation_non_utc_datetime(sample_adt_data_invalid_datetime):
    """Test that non-UTC datetime columns fail timezone validation."""
    adt_obj = Adt(data=sample_adt_data_invalid_datetime)
    adt_obj.validate()
    
    # Should fail due to non-UTC timezone
    assert adt_obj.isvalid() is False

# from_file constructor
def test_adt_from_file(mock_adt_file):
    """Test loading adt data from a parquet file."""
    adt_obj = Adt.from_file(data_directory=mock_adt_file, filetype="parquet", timezone="UTC")
    assert adt_obj.df is not None

def test_adt_from_file_nonexistent(tmp_path):
    """Test loading adt data from a nonexistent file."""
    non_existent_path = str(tmp_path / "nonexistent_dir")
    with pytest.raises(FileNotFoundError):
        Adt.from_file(non_existent_path, filetype="parquet", timezone="UTC")

# isvalid method
def test_adt_isvalid(sample_valid_adt_data, sample_adt_data_invalid_category):
    """Test isvalid method."""
    valid_adt = Adt(data=sample_valid_adt_data)
    valid_adt.validate()
    assert valid_adt.isvalid() is True
    
    invalid_adt = Adt(data=sample_adt_data_invalid_category)
    invalid_adt.validate()
    assert invalid_adt.isvalid() is False

# validate method
def test_adt_validate_output(sample_adt_data_invalid_category, capsys):
    """Test validate method output messages."""
    # Invalid data
    invalid_adt = Adt(data=sample_adt_data_invalid_category)
    invalid_adt.validate()
    captured = capsys.readouterr()
    assert f"Validation completed with {len(invalid_adt.errors)} error(s)" in captured.out
    
    # No data
    adt_no_data = Adt()
    adt_no_data.validate() # Explicitly call validate
    captured = capsys.readouterr()
    assert "No dataframe to validate" in captured.out
