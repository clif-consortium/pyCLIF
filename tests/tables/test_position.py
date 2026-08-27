"""
Tests for the position table module.
"""
import pytest
import pandas as pd
from clifpy.tables.position import Position

# --- Data Fixtures ---
@pytest.fixture
def sample_valid_position_data():
    """Create a valid position DataFrame for testing."""
    return pd.DataFrame({
        'hospitalization_id': ['H001', 'H002', 'H003'],
        'recorded_dttm': pd.to_datetime(['2023-01-01 10:00:00', '2023-02-01 14:30:00', '2023-03-01 08:15:00']).tz_localize('UTC'),
        'position_name': ['prone position', 'supine position', 'not prone position'],
        'position_category': ['prone', 'not_prone', 'not_prone']
    })

@pytest.fixture
def sample_position_data_invalid_category():
    """Create a position DataFrame with invalid categorical values."""
    return pd.DataFrame({
        'hospitalization_id': ['H001'],
        'recorded_dttm': pd.to_datetime(['2023-01-01 10:00:00']).tz_localize('UTC'),
        'position_category': ['INVALID_POSITION']  # Invalid value
    })

@pytest.fixture
def sample_position_data_missing_cols():
    """Create a position DataFrame with missing required columns."""
    return pd.DataFrame({
        'hospitalization_id': ['H001'],
        'recorded_dttm': pd.to_datetime(['2023-01-01 10:00:00']).tz_localize('UTC'),
        # Missing required column: position_category
    })

@pytest.fixture
def sample_position_data_invalid_datetime():
    """Create a position DataFrame with timezone-naive datetime columns."""
    return pd.DataFrame({
        'hospitalization_id': ['H001'],
        'recorded_dttm': pd.to_datetime(['2023-01-01 10:00:00']),  # No timezone
        'position_category': ['prone']
    })

@pytest.fixture
def sample_position_data_non_utc_timezone():
    """Create a position DataFrame with non-UTC timezone datetime columns."""
    return pd.DataFrame({
        'hospitalization_id': ['H001'],
        'recorded_dttm': pd.to_datetime(['2023-01-01 10:00:00']).tz_localize('America/New_York'),
        'position_category': ['prone']
    })

@pytest.fixture
def mock_position_file(tmp_path, sample_valid_position_data):
    """Create a mock position parquet file for testing."""
    test_dir = tmp_path / "test_data"
    test_dir.mkdir()
    file_path = test_dir / "clif_position.parquet"
    sample_valid_position_data.to_parquet(file_path)
    return str(test_dir)  # from_file expects directory path

# --- Tests for position class --- 

# Initialization
def test_position_init_with_valid_data(sample_valid_position_data):
    """Test position initialization with valid data."""
    pos_obj = Position(data=sample_valid_position_data)
    pos_obj.validate()
    assert pos_obj.df is not None
    assert pos_obj.isvalid() is True
    assert not pos_obj.errors


def test_position_init_without_data():
    """Test position initialization without data."""
    pos_obj = Position()
    pos_obj.validate()
    assert pos_obj.df is None
    assert not pos_obj.errors

# from_file constructor
def test_position_from_file(mock_position_file, sample_valid_position_data):
    """Test loading position data from a parquet file."""
    pos_obj = Position.from_file(mock_position_file, filetype="parquet", timezone="UTC")
    pos_obj.validate()
    assert pos_obj.df is not None
    pd.testing.assert_frame_equal(pos_obj.df.reset_index(drop=True), sample_valid_position_data.reset_index(drop=True), check_dtype=False)
    assert pos_obj.isvalid() is True # Validation is called in init

def test_position_from_file_nonexistent(tmp_path):
    """Test loading position data from a nonexistent file."""
    non_existent_path = str(tmp_path / "nonexistent_dir")
    with pytest.raises(FileNotFoundError):
        Position.from_file(non_existent_path, filetype="parquet", timezone="UTC")

# isvalid method
def test_position_isvalid(sample_valid_position_data, sample_position_data_missing_cols):
    """Test isvalid method."""
    valid_pos = Position(data=sample_valid_position_data)
    valid_pos.validate()
    assert valid_pos.isvalid() is True
    
    invalid_pos = Position(data=sample_position_data_missing_cols)
    invalid_pos.validate()
    assert invalid_pos.isvalid() is False

# validate method
def test_position_validate_output(sample_valid_position_data, sample_position_data_missing_cols, capsys):
    """Test validate method output messages."""
    # Valid data - validation runs at init
    valid_position = Position(data=sample_valid_position_data)
    valid_position.validate()
    captured = capsys.readouterr()
    assert "Validation completed successfully." in captured.out

    # Invalid data - validation runs at init
    invalid_position = Position(data=sample_position_data_missing_cols)
    invalid_position.validate()
    captured = capsys.readouterr()
    # Expecting 2 errors, not 3, due to known issue with missing_columns reporting.
    assert "Validation completed with" in captured.out

    # No data
    pos_obj_no_data = Position()
    pos_obj_no_data.validate() # Explicit call
    captured = capsys.readouterr()
    assert "No dataframe to validate." in captured.out

# --- Tests for position-specific methods ---

def test_get_position_category_stats(sample_valid_position_data):
    """Test position category statistics generation."""
    pos_obj = Position(data=sample_valid_position_data)
    stats = pos_obj.get_position_category_stats()
    
    assert isinstance(stats, pd.DataFrame)
    assert 'count' in stats.columns
    assert 'unique' in stats.columns
    
    # Check that prone and not_prone categories are present
    assert 'prone' in stats.index
    assert 'not_prone' in stats.index
    
    # Check counts
    assert stats.loc['prone', 'count'] == 1
    assert stats.loc['not_prone', 'count'] == 2
    
    # Check unique hospitalization counts
    assert stats.loc['prone', 'unique'] == 1
    assert stats.loc['not_prone', 'unique'] == 2

def test_get_position_category_stats_missing_columns():
    """Test position category statistics with missing columns."""
    data = pd.DataFrame({
        'other_col': ['value1', 'value2']
        # Missing position_category and hospitalization_id
    })
    pos_obj = Position(data=data)
    stats = pos_obj.get_position_category_stats()
    
    assert stats == {"status": "Missing columns"}

def test_get_position_category_stats_empty_data():
    """Test position category statistics with empty data."""
    pos_obj = Position()
    stats = pos_obj.get_position_category_stats()
    
    assert stats == {"status": "Missing columns"}
