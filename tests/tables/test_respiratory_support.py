"""
Tests for the respiratory_support table module.
"""
import os
import pytest
import pandas as pd
import json
from datetime import datetime
from clifpy.tables.respiratory_support import RespiratorySupport

# --- Mock Schema ---
@pytest.fixture
def mock_rs_schema_content():
    """Provides the content for a mock Respiratory_supportModel.json."""
    return {
        "columns": [
            {"name": "hospitalization_id", "data_type": "VARCHAR", "required": True},
            {"name": "recorded_dttm", "data_type": "DATETIME", "required": True},
            {"name": "device_category", "data_type": "VARCHAR", "is_category_column": True, "permissible_values": ["imv", "nippv", "room air", "trach collar"]},
            {"name": "device_name", "data_type": "VARCHAR"},
            {"name": "mode_category", "data_type": "VARCHAR", "is_category_column": True, "permissible_values": ["assist control-volume control", "simv", "pressure control", "pressure support/cpap"]},
            {"name": "mode_name", "data_type": "VARCHAR"},
            {"name": "tracheostomy", "data_type": "INTEGER"},
            {"name": "fio2_set", "data_type": "DOUBLE"},
            {"name": "lpm_set", "data_type": "DOUBLE"},
            {"name": "peep_set", "data_type": "DOUBLE"},
            {"name": "tidal_volume_set", "data_type": "DOUBLE"},
            {"name": "resp_rate_set", "data_type": "DOUBLE"},
            {"name": "resp_rate_obs", "data_type": "DOUBLE"},
            {"name": "pressure_support_set", "data_type": "DOUBLE"},
            {"name": "peak_inspiratory_pressure_set", "data_type": "DOUBLE"}
        ],
        "required_columns": ["hospitalization_id", "recorded_dttm"]
    }

@pytest.fixture
def mock_mcide_dir(tmp_path):
    """Creates a temporary mCIDE directory."""
    mcide_path = tmp_path / "mCIDE"
    mcide_path.mkdir()
    return mcide_path

@pytest.fixture
def mock_rs_model_json(mock_mcide_dir, mock_rs_schema_content):
    """Creates a mock Respiratory_supportModel.json file."""
    schema_file_path = mock_mcide_dir / "Respiratory_supportModel.json"
    with open(schema_file_path, 'w') as f:
        json.dump(mock_rs_schema_content, f)
    return schema_file_path


# --- Data Fixtures ---
@pytest.fixture
def sample_valid_rs_data():
    """Create a valid respiratory_support DataFrame for testing."""
    return pd.DataFrame({
        'hospitalization_id': ['H001', 'H001'], # Ensure string type
        'recorded_dttm': pd.to_datetime(['2023-01-01 10:00', '2023-01-01 11:00']).tz_localize('UTC'),
        'device_category': ['imv', 'imv'],
        'device_name': ['ventilator1', 'ventilator1'],
        'mode_category': ['acvc', 'acvc'],
        'mode_name': ['AC/VC', 'AC/VC'],  # Added required column
        'fio2_set': [0.5, 0.6],
        'lpm_set': [None, None],  # Added optional columns
        'peep_set': [5.0, 5.0],
        'tidal_volume_set': [500.0, 500.0],
        'resp_rate_set': [12.0, 12.0],
        'resp_rate_obs': [12.0, 13.0],
        'pressure_support_set': [10.0, 10.0],
        'peak_inspiratory_pressure_set': [20.0, 22.0],
        'pressure_control_set': [None, None],
        'flow_rate_set': [None, None],
        'inspiratory_time_set': [1.0, 1.0],
        'tidal_volume_obs': [480.0, 495.0],
        'plateau_pressure_obs': [18.0, 19.0],
        'peak_inspiratory_pressure_obs': [21.0, 23.0],
        'peep_obs': [5.0, 5.0],
        'minute_vent_obs': [6.0, 6.4],
        'mean_airway_pressure_obs': [12.0, 12.5],
        'tracheostomy': [0, 0]
    }).astype({'hospitalization_id': 'str'})

@pytest.fixture
def sample_invalid_rs_data_schema():
    """Create a respiratory_support DataFrame with schema violations."""
    return pd.DataFrame({
        'hospitalization_id': ['H001'],
        # Missing recorded_dttm
        'device_category': ['invalid_device'], # Invalid category
        'fio2_set': ['not_a_number'] # Invalid data type
    })

@pytest.fixture
def mock_rs_file(tmp_path, sample_valid_rs_data):
    """Create a mock respiratory_support parquet file for testing."""
    test_dir = tmp_path / "test_data"
    test_dir.mkdir()
    file_path = test_dir / "clif_respiratory_support.parquet"
    sample_valid_rs_data.to_parquet(file_path)
    return str(test_dir) # from_file expects directory path

# --- Tests for respiratory_support class ---

# Initialization and Schema Loading
def test_rs_init_with_valid_data(sample_valid_rs_data):
    """Test initialization with valid data."""
    rs_obj = RespiratorySupport(data=sample_valid_rs_data)
    assert rs_obj.df is not None
    rs_obj.validate()  # Run validation
    assert rs_obj.isvalid() is True
    assert not rs_obj.errors

def test_rs_init_with_invalid_schema_data(sample_invalid_rs_data_schema):
    """Test initialization with schema-invalid data."""
    rs_obj = RespiratorySupport(data=sample_invalid_rs_data_schema)
    rs_obj.validate()  # Run validation
    assert rs_obj.isvalid() is False
    assert len(rs_obj.errors) > 0
    error_types = {e['type'] for e in rs_obj.errors}
    assert 'Missing Required Columns' in error_types
    assert 'Invalid Categorical Values' in error_types
    assert 'Data Type Mismatch' in error_types

def test_rs_init_without_data():
    """Test initialization without data."""
    rs_obj = RespiratorySupport()
    assert rs_obj.df is None
    # No validation needed when no data
    # isvalid returns False until validation is run
    assert rs_obj._validated is False

# from_file constructor
def test_rs_from_file(mock_rs_file, sample_valid_rs_data):
    """Test loading data from a parquet file."""
    rs_obj = RespiratorySupport.from_file(mock_rs_file, filetype="parquet", timezone="UTC")
    assert rs_obj.df is not None
    pd.testing.assert_frame_equal(rs_obj.df.reset_index(drop=True), sample_valid_rs_data.reset_index(drop=True), check_dtype=False)
    rs_obj.validate()  # Run validation
    assert rs_obj.isvalid() is True

def test_rs_from_file_nonexistent(tmp_path):
    """Test loading from a nonexistent file."""
    with pytest.raises(FileNotFoundError):
        RespiratorySupport.from_file(str(tmp_path), filetype="parquet", timezone="UTC")

# isvalid and validate methods
def test_rs_isvalid_and_validate(sample_valid_rs_data, sample_invalid_rs_data_schema, capsys):
    """Test isvalid and validate methods and their output."""
    # Valid
    valid_obj = RespiratorySupport(data=sample_valid_rs_data)
    valid_obj.validate()  # Run validation
    captured = capsys.readouterr()
    assert valid_obj.isvalid() is True
    assert "Validation completed successfully." in captured.out

    # Invalid
    invalid_obj = RespiratorySupport(data=sample_invalid_rs_data_schema)
    invalid_obj.validate()  # Run validation
    captured = capsys.readouterr()
    assert invalid_obj.isvalid() is False
    assert "Validation completed with" in captured.out

    # No data
    no_data_obj = RespiratorySupport()
    no_data_obj.validate()
    captured = capsys.readouterr()
    assert "No dataframe to validate." in captured.out

# --- Waterfall Method Tests ---

@pytest.fixture
def waterfall_input_data():
    """Create a DataFrame for testing the waterfall method."""
    return pd.DataFrame({
        'hospitalization_id': ['H001', 'H001', 'H001', 'H001'],
        'recorded_dttm': pd.to_datetime(['2023-01-01 10:00', '2023-01-01 12:00', '2023-01-01 14:00', '2023-01-01 15:00']).tz_localize('UTC'),
        'device_category': ['imv', None, 'room air', 'imv'],
        'device_name': ['ventilator1', 'ventilator1', None, 'ventilator2'],
        'mode_category': ['assist control-volume control', None, None, 'simv'],
        'mode_name': ['AC/VC', None, None, 'SIMV-VC'],
        'fio2_set': [50.0, None, None, 60.0], # Note: FiO2 > 1 to test scaling
        'peep_set': [5.0, 6.0, None, 7.0],
        'resp_rate_set': [12.0, 14.0, None, 16.0],
        'tidal_volume_set': [500.0, 510.0, None, 520.0],
        'peak_inspiratory_pressure_set': [20.0, 22.0, None, 25.0],
        'pressure_support_set': [10.0, 12.0, None, 15.0],
        'tracheostomy': [None, None, None, None],      # Added missing column
        'lpm_set': [None, None, None, None],           # Added missing column
        'resp_rate_obs': [None, None, None, None]      # Added missing column
    })

def test_waterfall_processing(waterfall_input_data):
    """Test the full waterfall processing logic."""
    rs_obj = RespiratorySupport(data=waterfall_input_data)
    processed = rs_obj.waterfall(verbose=False)
    
    # Check that it returns a RespiratorySupport instance
    assert isinstance(processed, RespiratorySupport)
    
    # Get the processed DataFrame
    processed_df = processed.df

    # Check for hourly scaffold rows
    assert 'is_scaffold' in processed_df.columns
    assert processed_df['is_scaffold'].any()

    # Check FiO2 scaling (50.0 -> 0.5, 60.0 -> 0.6)
    assert processed_df['fio2_set'].max() <= 1.0

    # Check hierarchical ID generation
    for col in ['device_cat_id', 'device_id', 'mode_cat_id', 'mode_name_id']:
        assert col in processed_df.columns

    # Check filling logic (forward/backward fill within groups)
    # Example: Check if None in fio2_set at 12:00 is filled
    original_none_mask = waterfall_input_data['recorded_dttm'] == pd.to_datetime('2023-01-01 12:00').tz_localize('UTC')
    original_none_index = waterfall_input_data[original_none_mask].index[0]
    
    # Find the corresponding row in the processed_df (might not be at the same index)
    processed_row = processed_df[processed_df['recorded_dttm'] == pd.to_datetime('2023-01-01 12:00').tz_localize('UTC')]
    assert not processed_row.empty
    # The value should be filled from the 10:00 entry (0.5)
    assert pd.notna(processed_row['fio2_set'].iloc[0])
    assert processed_row['fio2_set'].iloc[0] == 0.5 

    # Check room air default FiO2
    room_air_row = processed_df[processed_df['device_category'] == 'room air']
    assert not room_air_row.empty
    # The value should be filled to 0.21
    assert room_air_row['fio2_set'].iloc[0] == 0.21
    
    # Test return_dataframe parameter
    df_result = rs_obj.waterfall(verbose=False, return_dataframe=True)
    assert isinstance(df_result, pd.DataFrame)
    pd.testing.assert_frame_equal(df_result, processed_df)

def test_waterfall_no_data():
    """Test waterfall with no data."""
    rs_obj = RespiratorySupport()
    with pytest.raises(ValueError, match="No data available"):
        rs_obj.waterfall()
