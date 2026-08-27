"""
Tests for the microbiology_culture table module.
"""
import os
import pytest
import pandas as pd
import json
from datetime import datetime
from clifpy.tables.microbiology_culture import MicrobiologyCulture

# --- Data Fixtures ---
@pytest.fixture
def sample_valid_microbiology_data():
    """Create a valid microbiology culture DataFrame for testing."""
    return pd.DataFrame({
        'patient_id': ['12345', '12345', '67890'],
        'hospitalization_id': ['HOSP12345', 'HOSP12345', 'HOSP67890'],
        'order_id': ['ORD001', 'ORD001', 'ORD002'],
        'organism_id': ['ORG001', 'ORG002', 'ORG003'],
        'order_dttm': pd.to_datetime(['2025-06-05 08:15:00+00:00', '2025-06-05 08:15:00+00:00', '2025-06-10 14:10:00+00:00']),
        'collect_dttm': pd.to_datetime(['2025-06-05 08:45:00+00:00', '2025-06-05 08:45:00+00:00', '2025-06-10 14:35:00+00:00']),
        'result_dttm': pd.to_datetime(['2025-06-06 12:00:00+00:00', '2025-06-06 12:00:00+00:00', '2025-06-11 09:20:00+00:00']),
        'fluid_name': ['AFB/FUNGAL BLOOD CULTURE', 'AFB/FUNGAL BLOOD CULTURE', 'BRAIN BIOPSY CULTURE'],
        'fluid_category': ['blood_buffy', 'blood_buffy', 'brain'],
        'method_name': ['Blood culture', 'Blood culture', 'Tissue culture'],
        'method_category': ['culture', 'culture', 'culture'],
        'organism_name': ['Acinetobacter baumanii', 'Candida albicans', 'Aspergillus fumigatus'],
        'organism_category': ['acinetobacter_baumannii', 'candida_albicans', 'aspergillus_fumigatus'],
        'organism_group': ['acinetobacter', 'candida_albicans', 'aspergillus_fumigatus'],
        'lab_loinc_code': ['', '', '']
    })

@pytest.fixture
def sample_invalid_microbiology_data_schema():
    """Create a microbiology DataFrame with schema violations."""
    return pd.DataFrame({
        'patient_id': ['12345'],
        'hospitalization_id': ['HOSP12345'],
        'order_id': ['ORD001'],
        'organism_id': [123],  # Invalid data type for organism_id, schema expects VARCHAR
        'order_dttm': pd.to_datetime(['2025-06-05 08:15:00+00:00']),
        'collect_dttm': pd.to_datetime(['2025-06-05 08:45:00+00:00']),
        'result_dttm': pd.to_datetime(['2025-06-06 12:00:00+00:00']),
        'fluid_category': ['blood_buffy'],
        'method_category': ['culture'],
        'organism_category': ['acinetobacter_baumannii']
    })

@pytest.fixture
def sample_invalid_microbiology_data_organisms():
    """Create a microbiology DataFrame with unknown organism categories."""
    return pd.DataFrame({
        'patient_id': ['12345', '67890'],
        'hospitalization_id': ['HOSP12345', 'HOSP67890'],
        'order_id': ['ORD001', 'ORD002'],
        'organism_id': ['ORG001', 'ORG002'],
        'order_dttm': pd.to_datetime(['2025-06-05 08:15:00+00:00', '2025-06-10 14:10:00+00:00']),
        'collect_dttm': pd.to_datetime(['2025-06-05 08:45:00+00:00', '2025-06-10 14:35:00+00:00']),
        'result_dttm': pd.to_datetime(['2025-06-06 12:00:00+00:00', '2025-06-11 09:20:00+00:00']),
        'fluid_category': ['blood_buffy', 'brain'],
        'method_category': ['culture', 'culture'],  # valid method
        'organism_category': ['acinetobacter_baumannii', 'candida_albicans'],
        'organism_group': ['unknown_group', 'invalid_organism_group']  # invalid organism groups
    })

@pytest.fixture
def sample_microbiology_data_group_mismatch():
    """Create a microbiology DataFrame with organism group mismatches."""
    return pd.DataFrame({
        'patient_id': ['12345'],
        'hospitalization_id': ['HOSP12345'],
        'order_id': ['ORD001'],
        'organism_id': ['ORG001'],
        'order_dttm': pd.to_datetime(['2025-06-05 08:15:00+00:00']),
        'collect_dttm': pd.to_datetime(['2025-06-05 08:45:00+00:00']),
        'result_dttm': pd.to_datetime(['2025-06-06 12:00:00+00:00']),
        'fluid_category': ['blood_buffy'],
        'method_category': ['culture'],
        'organism_category': ['acinetobacter_baumannii'],  # unrestricted
        'organism_group': ['invalid_group']  # Invalid organism group
    })

@pytest.fixture
def sample_microbiology_data_invalid_timestamps():
    """Create a microbiology DataFrame with invalid timestamp order."""
    return pd.DataFrame({
        'patient_id': ['12345', '67890'],
        'hospitalization_id': ['HOSP12345', 'HOSP67890'],
        'order_id': ['ORD001', 'ORD002'],
        'organism_id': ['ORG001', 'ORG002'],
        'order_dttm': pd.to_datetime(['2025-06-05 10:00:00+00:00', '2025-06-10 16:00:00+00:00']),  # order after collect
        'collect_dttm': pd.to_datetime(['2025-06-05 08:45:00+00:00', '2025-06-10 14:35:00+00:00']),
        'result_dttm': pd.to_datetime(['2025-06-06 12:00:00+00:00', '2025-06-10 14:00:00+00:00']),  # result before collect
        'fluid_category': ['blood_buffy', 'brain'],
        'method_category': ['culture', 'culture'],
        'organism_category': ['acinetobacter_baumannii', 'aspergillus_fumigatus'],  # unrestricted
        'organism_group': ['acinetobacter', 'aspergillus_fumigatus']
    })

@pytest.fixture
def mock_microbiology_file(tmp_path, sample_valid_microbiology_data):
    """Create a mock microbiology parquet file for testing."""
    test_dir = tmp_path / "test_data"
    test_dir.mkdir()
    file_path = test_dir / "clif_microbiology_culture.parquet"
    sample_valid_microbiology_data.to_parquet(file_path)
    return str(test_dir) # from_file expects directory path

# --- Mock Schema ---
@pytest.fixture
def mock_microbiology_schema_content():
    """Provides the content for a mock microbiology_cultureModel.json."""
    return {
        "columns": [
            {"name": "patient_id", "data_type": "VARCHAR", "required": True, "is_category_column": False, "is_group_column": False},
            {"name": "hospitalization_id", "data_type": "VARCHAR", "required": True, "is_category_column": False, "is_group_column": False},
            {"name": "organism_id", "data_type": "VARCHAR", "required": True, "is_category_column": False, "is_group_column": False},
            {"name": "order_dttm", "data_type": "DATETIME", "required": True, "is_category_column": False, "is_group_column": False},
            {"name": "collect_dttm", "data_type": "DATETIME", "required": True, "is_category_column": False, "is_group_column": False},
            {"name": "result_dttm", "data_type": "DATETIME", "required": True, "is_category_column": False, "is_group_column": False},
            {"name": "fluid_name", "data_type": "VARCHAR", "required": False, "is_category_column": False, "is_group_column": False},
            {"name": "fluid_category", "data_type": "VARCHAR", "required": True, "is_category_column": True, "is_group_column": False},
            {"name": "method_name", "data_type": "VARCHAR", "required": False, "is_category_column": False, "is_group_column": False},
            {"name": "method_category", "data_type": "VARCHAR", "required": True, "is_category_column": True, "is_group_column": False},
            {"name": "organism_name", "data_type": "VARCHAR", "required": False, "is_category_column": False, "is_group_column": False},
            {"name": "organism_category", "data_type": "VARCHAR", "required": True, "is_category_column": True, "is_group_column": False},
            {"name": "organism_group", "data_type": "VARCHAR", "required": False, "is_category_column": False, "is_group_column": True},
            {"name": "lab_loinc_code", "data_type": "VARCHAR", "required": False, "is_category_column": False, "is_group_column": False},
        ],
        "fluid_category": [
                "brain",
                "central_nervous_system",
                "meninges_csf",
                "spinal_cord",
                "eyes",
                "ears",
                "sinuses",
                "nasopharynx_upperairway",
                "lips",
                "oropharynx_tongue_oralcavity",
                "larynx",
                "lymph_nodes",
                "catheter_tip",
                "cardiac",
                "respiratory_tract",
                "respiratory_tract_lower",
                "pleural_cavity_fluid",
                "joints",
                "bone_marrow",
                "bone_cortex",
                "blood_buffy",
                "esophagus",
                "gallbladder_billary_pancreas",
                "liver",
                "spleen",
                "stomatch",
                "small_intestine",
                "large_intestine",
                "gastrointestinal_tract",
                "kidneys_renal_pelvis_ureters_bladder",
                "genito_urinary_tract",
                "fallopians_uterus_cervix",
                "peritoneum",
                "feces_stool",
                "genital_area",
                "prostate",
                "testes",
                "vagina",
                "muscle",
                "skin_rash_pustules_abscesses",
                "skin_disseminated_multiple_sites",
                "woundsite",
                "skin_unspecified",
                "other_unspecified"
            ],
        "method_category": [
            "culture",
            "gram stain",
            "smear",
        ],
        "organism_group": [
            "acinetobacter",
            "adenovirus",
            "agrobacterium_radiobacter",
            "alcaligenes_xylosoxidans",
            "amebiasis",
            "anaerobes_wo_bacteroides_clostridium",
            "aspergillus_nos",
            "aspergillus_flavus",
            "aspergillus_fumigatus",
            "aspergillus_niger",
            "bacillus",
            "bacteria_other",
            "bacteroides",
            "borrelia",
            "branhamelia_moraxella_catarrhalis",
            "campylobacter",
            "candida_albicans",
            "candida_krusei",
            "candida_nos",
            "candida_parapsilosis",
            "candida_tropicalis",
            "chlamydia",
            "citrobacter",
            "clostridium_difficile",
            "clostridium_wo_difficile",
            "corynebacterium",
            "coxiella",
            "cryptococcus",
            "cryptosporidium",
            "cytomegalovirus",
            "echinoco_ocalcyst",
            "enterobacter",
            "enterococcus",
            "enterovirus",
            "epstein_barr_virus",
            "escherichia",
            "flavimonas_oryzihabitans",
            "flavobacterium",
            "fungus_other",
            "fusarium",
            "fusobacterium_nucleatum",
            "giardia",
            "gram_negative_diplococci",
            "gram_negative_rod",
            "gram_positive_cocci",
            "gram_positive_rod",
            "haemophilus",
            "helicobacter_pylori",
            "hepatitis_a",
            "hepatitis_b",
            "hepatitis_c",
            "herpes_simplex",
            "herpes_zoster",
            "hhv_6",
            "hiv_htlv",
            "influenza",
            "klebsiella",
            "lactobacillus",
            "legionella",
            "leptospira",
            "leptotrichia_buccalis",
            "leuconostoc",
            "listeria",
            "measles",
            "methylobacterium",
            "micrococcus",
            "mucormycosis_zygomycetes_rhizopus",
            "mumps",
            "mycobacteria_avium_bovium_haemophilum_intercelluare",
            "mycobacterium_other",
            "mycoplasma",
            "neisseria",
            "nocardia",
            "no_growth",
            "other_organism",
            "papovavirus",
            "parainfluenza",
            "pharyngeal_respiratory_flora",
            "pneumocystis",
            "polyomavirus",
            "propionbacterium",
            "protozoal_other",
            "pseudomonas_burkholderia_cepacia",
            "pseudomonas_stenotrophomonas_xanthomonas_maltophilia",
            "pseudomonas_wo_cepacia_maltophilia",
            "respiratory_syncytial_virus",
            "rhinovirus",
            "rhodococcus",
            "rickettsia",
            "rotavirus",
            "rubella",
            "salmonella",
            "serratia_marcescens",
            "shigella",
            "staphylococcus_coag_neg",
            "staphylococcus_coag_pos",
            "staphylococcus_nos",
            "stomatococcus_mucilaginosis",
            "streptococcus",
            "torulopsis_galbrata",
            "toxoplasma",
            "treponema",
            "trichomonas",
            "tuberculosis",
            "tuberculosis_nos_afb_kochbacillus",
            "vibrio",
            "viral_other",
            "yeast"
        ],
        "required_columns": [
            "hospitalization_id",
            "organism_id",
            "fluid_category",
            "method_category",
            "organism_category",
            "result_dttm"
        ],
        "category_columns": ["fluid_category", "method_category", "organism_category"],
        "group_columns": ["organism_group"],
    }


@pytest.fixture
def mock_mcide_dir(tmp_path):
    """Creates a temporary mCIDE directory."""
    mcide_path = tmp_path / "mCIDE"
    mcide_path.mkdir()
    return mcide_path

@pytest.fixture
def mock_microbiology_model_json(mock_mcide_dir, mock_microbiology_schema_content):
    """Creates a mock microbiology_cultureModel.json file in the temporary mCIDE directory."""
    schema_file_path = mock_mcide_dir / "microbiology_cultureModel.json"
    with open(schema_file_path, 'w') as f:
        json.dump(mock_microbiology_schema_content, f)
    return schema_file_path

@pytest.fixture
def patch_microbiology_schema_path(monkeypatch, mock_microbiology_model_json):
    """Patches the path to microbiology_cultureModel.json for the microbiology_culture class."""
    original_dirname = os.path.dirname
    original_join = os.path.join
    original_abspath = os.path.abspath

    def mock_dirname(path):
        if '__file__' in path: 
            return str(mock_microbiology_model_json.parent.parent / "tables")
        return original_dirname(path)

    def mock_abspath(path):
        if '__file__' in path:
            return str(mock_microbiology_model_json.parent.parent / "tables" / "dummy_microbiology_culture.py")
        return original_abspath(path)

    def mock_join(*args):
        if len(args) > 1 and args[1] == '..' and args[2] == 'mCIDE' and args[3] == 'microbiology_cultureModel.json':
            return str(mock_microbiology_model_json)
        return original_join(*args)

    monkeypatch.setattr(os.path, 'dirname', mock_dirname)
    monkeypatch.setattr(os.path, 'abspath', mock_abspath)
    monkeypatch.setattr(os.path, 'join', mock_join)

# --- Tests for microbiology_culture class --- 

# Initialization and Schema Loading
def test_microbiology_culture_init_with_valid_data(patch_microbiology_schema_path, sample_valid_microbiology_data):
    """Test microbiology culture initialization with valid data and mocked schema."""
    mc_obj = MicrobiologyCulture(data=sample_valid_microbiology_data)   
    mc_obj.validate()
    assert mc_obj.df is not None
    assert mc_obj.isvalid() is True
    assert not mc_obj.errors

def test_microbiology_culture_init_with_invalid_category(patch_microbiology_schema_path, sample_invalid_microbiology_data_organisms):
    """Test microbiology culture initialization with organism group values."""
    mc_obj = MicrobiologyCulture(data=sample_invalid_microbiology_data_organisms)
    mc_obj.validate()
    # organism_group values are not schema-validated; group checking lives elsewhere.
    assert mc_obj.isvalid() is True

def test_microbiology_culture_init_with_schema_violations(patch_microbiology_schema_path, sample_invalid_microbiology_data_schema):
    """Test microbiology culture initialization with schema violations."""
    mc_obj = MicrobiologyCulture(data=sample_invalid_microbiology_data_schema)
    mc_obj.validate()
    assert mc_obj.isvalid() is False
    assert len(mc_obj.errors) > 0
    # Should have datatype mismatch error since organism_id is integer instead of string
    error_types = {e['type'] for e in mc_obj.errors}
    assert "Data Type Mismatch" in error_types

def test_microbiology_culture_init_without_data(patch_microbiology_schema_path):
    """Test microbiology culture initialization without data."""
    mc_obj = MicrobiologyCulture()
    mc_obj.validate()
    assert mc_obj.df is None

def test_microbiology_culture_organism_group_mismatch(patch_microbiology_schema_path, sample_microbiology_data_group_mismatch):
    """Test microbiology culture with organism group values."""
    mc_obj = MicrobiologyCulture(data=sample_microbiology_data_group_mismatch)
    mc_obj.validate()
    assert mc_obj.isvalid() is True

# from_file constructor
def test_microbiology_culture_from_file(patch_microbiology_schema_path, mock_microbiology_file):
    """Test loading microbiology culture data from a parquet file."""
    mc_obj = MicrobiologyCulture.from_file(data_directory=mock_microbiology_file, filetype="parquet", timezone="UTC")
    assert mc_obj.df is not None

def test_microbiology_culture_from_file_nonexistent(patch_microbiology_schema_path, tmp_path):
    """Test loading microbiology culture data from a nonexistent file."""
    non_existent_path = str(tmp_path / "nonexistent_dir")
    with pytest.raises(FileNotFoundError):
        MicrobiologyCulture.from_file(non_existent_path, filetype="parquet", timezone="UTC")

# isvalid method
def test_microbiology_culture_isvalid(patch_microbiology_schema_path, sample_valid_microbiology_data, sample_invalid_microbiology_data_organisms):
    """Test isvalid method."""
    valid_mc = MicrobiologyCulture(data=sample_valid_microbiology_data)
    valid_mc.validate()
    assert valid_mc.isvalid() is True

    # Note: organism group validation is handled by other modules
    invalid_mc = MicrobiologyCulture(data=sample_invalid_microbiology_data_organisms)
    invalid_mc.validate()
    assert invalid_mc.isvalid() is True# validate method
def test_microbiology_culture_validate_output(patch_microbiology_schema_path, sample_valid_microbiology_data, sample_invalid_microbiology_data_organisms, capsys):
    """Test validate method output messages."""
    # Valid data
    valid_mc = MicrobiologyCulture(data=sample_valid_microbiology_data)
    valid_mc.validate()
    captured = capsys.readouterr()
    assert "Validation completed successfully" in captured.out

    # All data validates successfully at this level - group validation handled elsewhere
    invalid_mc = MicrobiologyCulture(data=sample_invalid_microbiology_data_organisms)
    invalid_mc.validate()
    captured = capsys.readouterr()
    assert "Validation completed successfully" in captured.out
    
    # No data
    mc_no_data = MicrobiologyCulture()
    mc_no_data.validate()
    captured = capsys.readouterr()
    assert "No dataframe to validate" in captured.out

# Timestamp order validation tests
def test_microbiology_culture_valid_timestamp_order(patch_microbiology_schema_path, sample_valid_microbiology_data):
    """Test that valid timestamp order passes validation."""
    mc_obj = MicrobiologyCulture(data=sample_valid_microbiology_data)
    mc_obj.validate()
    assert mc_obj.isvalid() is True
    assert not mc_obj.time_order_validation_errors

def test_microbiology_culture_invalid_timestamp_order(patch_microbiology_schema_path, sample_microbiology_data_invalid_timestamps):
    """Test that invalid timestamp order fails validation."""
    mc_obj = MicrobiologyCulture(data=sample_microbiology_data_invalid_timestamps)
    mc_obj.validate()
    assert mc_obj.isvalid() is False
    assert len(mc_obj.time_order_validation_errors) > 0
    
    # Check for specific time order validation errors
    time_errors = [e for e in mc_obj.errors if e.get('type') == 'time_order_validation']
    assert len(time_errors) > 0
    
    # Should have errors for both order > collect and collect > result
    error_rules = {e['rule'] for e in time_errors}
    assert any('order_dttm <= collect_dttm' in rule for rule in error_rules)

def test_microbiology_culture_timestamp_order_method_direct(patch_microbiology_schema_path, sample_microbiology_data_invalid_timestamps):
    """Test the validate_timestamp_order method directly."""
    mc_obj = MicrobiologyCulture(data=sample_microbiology_data_invalid_timestamps)
    
    # Call the method directly
    violating_rows = mc_obj.validate_timestamp_order()
    
    # Should return violating rows
    assert violating_rows is not None
    assert len(violating_rows) > 0
    
    # Should have timestamp columns
    expected_cols = ['order_dttm', 'collect_dttm', 'result_dttm']
    for col in expected_cols:
        assert col in violating_rows.columns
    
    # Should also retain the identifying columns. The versioned YAML schema has
    # no 'composite_keys'; validate_timestamp_order defines the set it keeps.
    key_cols = ["patient_id", "hospitalization_id", "organism_id"]
    for col in key_cols:
        if col in mc_obj.df.columns:
            assert col in violating_rows.columns


# --- Helper Method Tests ---
def test_microbiology_culture_organism_cat_name_map(patch_microbiology_schema_path, sample_valid_microbiology_data):
    """Test organism category to name mapping."""
    mc_obj = MicrobiologyCulture(data=sample_valid_microbiology_data)
    cat_name_map = mc_obj.organism_cat_name_map()
    
    assert isinstance(cat_name_map, dict)
    # Should have mapping for organism categories to organism names
    for category, names in cat_name_map.items():
        assert isinstance(names, list)
        for name in names:
            assert isinstance(name, str)

def test_microbiology_culture_organism_group_cat_name_map(patch_microbiology_schema_path, sample_valid_microbiology_data):
    """Test organism group to category to name mapping."""
    mc_obj = MicrobiologyCulture(data=sample_valid_microbiology_data)
    group_cat_name_map = mc_obj.organism_group_cat_name_map()
    
    assert isinstance(group_cat_name_map, dict)
    # Should have nested mapping: group -> category -> names
    for group, cat_map in group_cat_name_map.items():
        assert isinstance(cat_map, dict)
        for category, names in cat_map.items():
            assert isinstance(names, list)
            for name in names:
                assert isinstance(name, str)

def test_microbiology_culture_fluid_cat_name_map(patch_microbiology_schema_path, sample_valid_microbiology_data):
    """Test fluid category to name mapping."""
    mc_obj = MicrobiologyCulture(data=sample_valid_microbiology_data)
    fluid_cat_name_map = mc_obj.fluid_cat_name_map()
    
    assert isinstance(fluid_cat_name_map, dict)
    # Should have mapping for fluid categories to fluid names
    for category, names in fluid_cat_name_map.items():
        assert isinstance(names, list)
        for name in names:
            assert isinstance(name, str)

def test_microbiology_culture_cat_vs_name_map_with_counts(patch_microbiology_schema_path, sample_valid_microbiology_data):
    """Test category vs name mapping with counts."""
    mc_obj = MicrobiologyCulture(data=sample_valid_microbiology_data)
    cat_name_map = mc_obj.organism_cat_name_map(include_counts=True)
    
    assert isinstance(cat_name_map, dict)
    # Should have mapping with count information
    for category, names in cat_name_map.items():
        assert isinstance(names, list)
        for name_info in names:
            assert isinstance(name_info, dict)
            assert 'name' in name_info
            assert 'n' in name_info
            assert isinstance(name_info['name'], str)
            assert isinstance(name_info['n'], int)
