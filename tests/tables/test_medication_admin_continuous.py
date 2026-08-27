"""
Tests for the medication_admin_continuous table module.
"""
import os
import pytest
import pandas as pd
import json
from datetime import datetime
import numpy as np
from pathlib import Path
from clifpy.tables.medication_admin_continuous import MedicationAdminContinuous

# --- Helper Fixtures for CSV Loading ---
@pytest.fixture
def load_fixture_csv():
    """Load CSV fixture from tests/fixtures/medication_admin_continuous/"""
    def _load(filename):
        path = Path(__file__).parent.parent / 'fixtures' / 'medication_admin_continuous' / filename
        return pd.read_csv(path)
    return _load

# --- Mock Schema --- 
@pytest.fixture
def mock_med_admin_continuous_schema_content():
    """Provides the content for a mock Medication_admin_continuousModel.json."""
    return {
        "columns": [
            {"name": "hospitalization_id", "data_type": "VARCHAR", "required": True},
            {"name": "admin_dttm", "data_type": "DATETIME", "required": True},
            {"name": "med_category", "data_type": "VARCHAR", "required": False, "is_category_column": True, "permissible_values": ["Antibiotics", "Vasopressors"]},
            {"name": "med_group", "data_type": "VARCHAR", "required": False, "is_category_column": True, "permissible_values": ["Cephalosporins", "Catecholamines"]},
            {"name": "med_dose", "data_type": "DOUBLE", "required": False},
            {"name": "med_unit", "data_type": "VARCHAR", "required": False}
        ],
        "med_category_to_group_mapping": {
            "Antibiotics": "Antimicrobials",
            "Vasopressors": "Cardiovascular Agents"
        }
    }

@pytest.fixture
def mock_mcide_dir(tmp_path):
    """Creates a temporary mCIDE directory."""
    mcide_path = tmp_path / "mCIDE"
    mcide_path.mkdir()
    return mcide_path

@pytest.fixture
def mock_med_admin_continuous_model_json(mock_mcide_dir, mock_med_admin_continuous_schema_content):
    """Creates a mock Medication_admin_continuousModel.json file."""
    schema_file_path = mock_mcide_dir / "Medication_admin_continuousModel.json"
    with open(schema_file_path, 'w') as f:
        json.dump(mock_med_admin_continuous_schema_content, f)
    return schema_file_path

@pytest.fixture
def patch_med_admin_continuous_schema_path(monkeypatch, mock_med_admin_continuous_model_json):
    """Patches path resolution for Medication_admin_continuousModel.json for _load_medication_schema."""
    # This fixture specifically targets the _load_medication_schema method in medication_admin_continuous.py
    # It ensures that when this class tries to load its med_category_to_group_mapping, it uses the mock file.
    original_dirname = os.path.dirname
    original_join = os.path.join
    original_abspath = os.path.abspath

    import importlib
    # Explicitly import the module to ensure we get the module object, not potentially the class
    module_name = "clifpy.tables.medication_admin_continuous"
    module_obj = importlib.import_module(module_name)
    target_module_file_path = os.path.normpath(module_obj.__file__)

    # Create a fake directory structure that _load_medication_schema would expect
    # if it were located such that '../mCIDE/Medication_admin_continuousModel.json' points to our mock file.
    fake_module_dir = mock_med_admin_continuous_model_json.parent.parent / "tables_dummy_root_mac"
    fake_module_path = fake_module_dir / "medication_admin_continuous.py"

    def mock_abspath_for_class(path):
        if os.path.normpath(path) == target_module_file_path:
            return str(fake_module_path)
        return original_abspath(path)

    def mock_dirname_for_class(path):
        if os.path.normpath(path) == str(fake_module_path):
            return str(fake_module_dir)
        return original_dirname(path)

    def mock_join_for_class(*args):
        # This targets the specific join: os.path.join(current_dir, '..', 'mCIDE', 'Medication_admin_continuousModel.json')
        if (len(args) == 4 and 
            os.path.normpath(args[0]) == str(fake_module_dir) and 
            args[1] == '..' and args[2] == 'mCIDE' and 
            args[3] == 'Medication_admin_continuousModel.json'):
            return str(mock_med_admin_continuous_model_json.resolve())
        return original_join(*args)

    monkeypatch.setattr(os.path, 'abspath', mock_abspath_for_class)
    monkeypatch.setattr(os.path, 'dirname', mock_dirname_for_class)
    monkeypatch.setattr(os.path, 'join', mock_join_for_class)

# --- Data Fixtures --- 
@pytest.fixture
def sample_valid_med_admin_continuous_data():
    """Create a valid medication_admin_continuous DataFrame."""
    return pd.DataFrame({
        'hospitalization_id': ['H001', 'H001', 'H002'],
        'admin_dttm': pd.to_datetime(['2023-01-01 10:00', '2023-01-01 11:00', '2023-01-02 09:00']),
        'med_category': ['propofol', 'norepinephrine', 'propofol'],
        'med_group': ['sedation', 'vasoactives', 'sedation'],
        'med_dose': [100.0, 10.5, 150.0],
        'med_unit': ['mcg/kg/min', 'mcg/kg/min', 'mcg/kg/min'],
        'med_dose_unit': ['mcg/kg/min', 'mcg/kg/min', 'mcg/kg/min'],
        'med_route_category': ['iv', 'iv', 'iv'],
        'mar_action_category': ['start', 'start', 'start'],
        'mar_action_group': ['administered', 'administered', 'administered']
    })

@pytest.fixture
def sample_invalid_med_admin_continuous_data_schema():
    """Create a DataFrame with schema violations."""
    return pd.DataFrame({
        'hospitalization_id': ['H001'],
        # Missing admin_dttm (required)
        'med_category': ['UnknownCategory'], # Not in permissible_values if schema enforces
        'med_dose': ['not_a_number'] # Invalid type
    })

@pytest.fixture
def sample_med_admin_continuous_data_for_stats():
    """Create data suitable for testing get_summary_stats thoroughly."""
    return pd.DataFrame({
        'hospitalization_id': ['H001', 'H001', 'H002', 'H003', 'H003'],
        'admin_dttm': pd.to_datetime(['2023-01-01 10:00', '2023-01-01 11:00', '2023-01-02 09:00', '2023-01-03 14:00', '2023-01-03 15:00']),
        'med_category': ['Antibiotics', 'Vasopressors', 'Antibiotics', 'Vasopressors', 'Antibiotics'],
        'med_group': ['Cephalosporins', 'Catecholamines', 'Cephalosporins', 'OtherVaso', 'Penicillins'],
        'med_dose': [100.0, 10.5, 150.0, np.nan, 200.0],
        'med_unit': ['mg', 'mcg/kg/min', 'mg', 'mg', 'mg']
    })

@pytest.fixture
def mock_med_admin_continuous_file(tmp_path, sample_valid_med_admin_continuous_data):
    """Create a mock parquet file for testing."""
    test_dir = tmp_path / "test_data"
    test_dir.mkdir()
    file_path = test_dir / "clif_medication_admin_continuous.parquet"
    sample_valid_med_admin_continuous_data.to_parquet(file_path)
    return str(test_dir) # from_file expects directory path

# --- Tests for medication_admin_continuous class --- 

# Initialization and Schema Loading
def test_init_with_valid_data(sample_valid_med_admin_continuous_data):
    mac_obj = MedicationAdminContinuous(data=sample_valid_med_admin_continuous_data)
    assert mac_obj.df is not None
    mac_obj.validate()
    assert mac_obj.isvalid() is True
    assert not mac_obj.errors
    assert mac_obj.med_category_to_group_mapping['propofol'] == 'sedation'

def test_init_with_invalid_schema_data(sample_invalid_med_admin_continuous_data_schema):
    mac_obj = MedicationAdminContinuous(data=sample_invalid_med_admin_continuous_data_schema)
    assert mac_obj.df is not None
    mac_obj.validate() # Ensure validation is run if not fully in init or for clarity
    assert mac_obj.isvalid() is False
    assert len(mac_obj.errors) > 0
    error_types = [e['type'] for e in mac_obj.errors]

    # Check for the errors that are actually reported
    assert 'Invalid Categorical Values' in error_types
    assert 'Data Type Mismatch' in error_types

    # The DQA pipeline reports missing required columns alongside other errors;
    # it no longer suppresses them when a dtype mismatch is also present.
    assert 'Missing Required Columns' in error_types

    # Ensure no other unexpected errors are present
    assert set(error_types) == {
        'Invalid Categorical Values', 'Data Type Mismatch',
        'Missing Required Columns', 'Medication Dose Units',
    }

def test_init_without_data():
    mac_obj = MedicationAdminContinuous()
    assert mac_obj.df is None
    # No data means validation never ran, so the table is not yet known to be valid.
    assert mac_obj.isvalid() is False
    assert not mac_obj.errors
    assert mac_obj.med_category_to_group_mapping['propofol'] == 'sedation'
@pytest.mark.usefixtures("patch_med_admin_continuous_schema_path") # Only this patch needed as it tests _load_medication_schema directly
def test_med_category_to_group_mapping_property():
    from clifpy.schemas import load_schema
    mac_obj = MedicationAdminContinuous()
    expected_mapping = load_schema(
        'medication_admin_continuous', mac_obj.clif_version
    )['med_category_to_group_mapping']
    assert mac_obj.med_category_to_group_mapping == expected_mapping
    # Test it returns a copy
    mapping = mac_obj.med_category_to_group_mapping
    mapping['NewKey'] = 'NewValue'
    assert mac_obj.med_category_to_group_mapping == expected_mapping

# from_file constructor
def test_from_file(mock_med_admin_continuous_file, sample_valid_med_admin_continuous_data):
    mac_obj = MedicationAdminContinuous.from_file(mock_med_admin_continuous_file, filetype="parquet", timezone="UTC")
    assert mac_obj.df is not None
    pd.testing.assert_frame_equal(mac_obj.df.reset_index(drop=True), sample_valid_med_admin_continuous_data.reset_index(drop=True), check_dtype=False)
    mac_obj.validate()
    assert mac_obj.isvalid() is True

def test_from_file_nonexistent(tmp_path):
    non_existent_path = str(tmp_path / "nonexistent_dir")
    with pytest.raises(FileNotFoundError):
        MedicationAdminContinuous.from_file(non_existent_path, filetype="parquet", timezone="UTC")

# isvalid method
def test_isvalid(sample_valid_med_admin_continuous_data, sample_invalid_med_admin_continuous_data_schema):
    valid_mac = MedicationAdminContinuous(data=sample_valid_med_admin_continuous_data)
    valid_mac.validate()
    assert valid_mac.isvalid() is True

    invalid_mac = MedicationAdminContinuous(data=sample_invalid_med_admin_continuous_data_schema)
    invalid_mac.validate()
    assert invalid_mac.isvalid() is False 

# validate method
def test_validate_output(sample_valid_med_admin_continuous_data, sample_invalid_med_admin_continuous_data_schema, capsys):
    MedicationAdminContinuous(data=sample_valid_med_admin_continuous_data).validate()
    captured = capsys.readouterr()
    assert "Validation completed successfully." in captured.out

    MedicationAdminContinuous(data=sample_invalid_med_admin_continuous_data_schema).validate()
    captured = capsys.readouterr()
    assert f"Validation completed with" in captured.out # Checks for presence of error count

    mac_obj_no_data = MedicationAdminContinuous()
    mac_obj_no_data.validate() # Explicit call
    captured = capsys.readouterr()
    assert "No dataframe to validate." in captured.out

# Medication Admin Continuous Specific Methods
def test_get_med_categories(sample_valid_med_admin_continuous_data):
    mac_obj = MedicationAdminContinuous(data=sample_valid_med_admin_continuous_data)
    categories = mac_obj.get_med_categories()
    assert isinstance(categories, list)
    assert set(categories) == {'propofol', 'norepinephrine'}

    mac_obj_no_data = MedicationAdminContinuous()
    assert mac_obj_no_data.get_med_categories() == []

    mac_obj_no_col = MedicationAdminContinuous(data=pd.DataFrame({'hospitalization_id': [1]}))
    assert mac_obj_no_col.get_med_categories() == []

def test_get_med_groups(sample_valid_med_admin_continuous_data):
    mac_obj = MedicationAdminContinuous(data=sample_valid_med_admin_continuous_data)
    groups = mac_obj.get_med_groups()
    assert isinstance(groups, list)
    assert set(groups) == {'sedation', 'vasoactives'}

    mac_obj_no_data = MedicationAdminContinuous()
    assert mac_obj_no_data.get_med_groups() == []

    mac_obj_no_col = MedicationAdminContinuous(data=pd.DataFrame({'hospitalization_id': [1]}))
    assert mac_obj_no_col.get_med_groups() == []

def test_filter_by_med_group(sample_valid_med_admin_continuous_data):
    mac_obj = MedicationAdminContinuous(data=sample_valid_med_admin_continuous_data)
    filtered_df = mac_obj.filter_by_med_group('sedation')
    assert len(filtered_df) == 2
    assert all(filtered_df['med_group'] == 'sedation')

    filtered_df_non_existent = mac_obj.filter_by_med_group('NonExistentGroup')
    assert filtered_df_non_existent.empty

    mac_obj_no_data = MedicationAdminContinuous()
    assert mac_obj_no_data.filter_by_med_group('AnyGroup').empty

    mac_obj_no_col = MedicationAdminContinuous(data=pd.DataFrame({'hospitalization_id': [1]}))
    assert mac_obj_no_col.filter_by_med_group('AnyGroup').empty

def test_get_summary_stats(sample_med_admin_continuous_data_for_stats):
    mac_obj = MedicationAdminContinuous(data=sample_med_admin_continuous_data_for_stats)
    stats = mac_obj.get_summary_stats()

    assert stats['total_records'] == 5
    assert stats['unique_hospitalizations'] == 3
    assert stats['med_category_counts'] == {'Antibiotics': 3, 'Vasopressors': 2}
    assert stats['med_group_counts'] == {'Cephalosporins': 2, 'Catecholamines': 1, 'OtherVaso': 1, 'Penicillins': 1}
    assert stats['date_range']['earliest'] == pd.to_datetime('2023-01-01 10:00')
    assert stats['date_range']['latest'] == pd.to_datetime('2023-01-03 15:00')

    dose_stats = stats['dose_stats_by_group']
    assert 'Cephalosporins' in dose_stats
    assert dose_stats['Cephalosporins']['count'] == 2
    assert dose_stats['Cephalosporins']['mean_dose'] == pytest.approx(125.0)
    assert dose_stats['Cephalosporins']['min_dose'] == 100.0
    assert dose_stats['Cephalosporins']['max_dose'] == 150.0

    assert 'Catecholamines' in dose_stats
    assert dose_stats['Catecholamines']['count'] == 1
    assert dose_stats['Catecholamines']['mean_dose'] == pytest.approx(10.5)

    assert 'OtherVaso' not in dose_stats # Group with only NaN dose is not included in dose_stats

    assert 'Penicillins' in dose_stats
    assert dose_stats['Penicillins']['count'] == 1
    assert dose_stats['Penicillins']['mean_dose'] == pytest.approx(200.0)

def test_get_summary_stats_empty_df():
    mac_obj = MedicationAdminContinuous(data=pd.DataFrame(columns=['hospitalization_id', 'admin_dttm', 'med_category', 'med_group', 'med_dose']))
    stats = mac_obj.get_summary_stats()
    assert stats['total_records'] == 0
    assert stats['unique_hospitalizations'] == 0
    assert stats['med_category_counts'] == {}
    assert stats['med_group_counts'] == {}
    assert pd.isna(stats['date_range']['earliest'])
    assert pd.isna(stats['date_range']['latest'])
    assert 'dose_stats_by_group' not in stats or stats['dose_stats_by_group'] == {}

def test_get_summary_stats_no_df():
    mac_obj = MedicationAdminContinuous()
    stats = mac_obj.get_summary_stats()
    assert stats == {}

def test_get_summary_stats_missing_columns():
    # Test with missing optional columns like med_dose
    data_missing_dose = pd.DataFrame({
        'hospitalization_id': ['H001'],
        'admin_dttm': pd.to_datetime(['2023-01-01 10:00']),
        'med_category': ['Antibiotics'],
        'med_group': ['Cephalosporins']
        # med_dose is missing
    })
    mac_obj = MedicationAdminContinuous(data=data_missing_dose)
    stats = mac_obj.get_summary_stats()
    assert 'dose_stats_by_group' not in stats or stats['dose_stats_by_group'] == {}

    # Test with missing core columns like med_category or med_group
    data_missing_group = pd.DataFrame({
        'hospitalization_id': ['H001'],
        'admin_dttm': pd.to_datetime(['2023-01-01 10:00']),
        'med_category': ['Antibiotics'],
        'med_dose': [100.0]
        # med_group is missing
    })
    mac_obj_missing_group = MedicationAdminContinuous(data=data_missing_group)
    stats_missing_group = mac_obj_missing_group.get_summary_stats()
    assert stats_missing_group['med_group_counts'] == {}
    assert 'dose_stats_by_group' not in stats_missing_group or stats_missing_group['dose_stats_by_group'] == {}
# ===========================================
# Dose-unit conversion tests: RETIRED
# ===========================================
# The dose-unit logic these covered was moved off this table into
# clifpy/utils/unit_converter.py (770a90e, to de-duplicate between the
# continuous and intermittent med tables) and is covered by
# tests/utils/med_unit_converter/test_unit_converter.py. The methods they
# called (_normalize_dose_unit_pattern, standardize_dose_to_limited_units)
# and their fixture CSVs were deleted in that move, so these tests could
# only ever error. One of them was already marked "to be retired".

# Retired: test_load_*_schema covered a JSON `*Model.json` loader that no
# longer exists. Schemas are versioned YAML loaded by BaseTable via
# clifpy.schemas.load_schema; these tests monkeypatched os.path.join for a
# file the package never opens.
