"""
Tests for encounter stitching functionality.
"""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from clifpy.utils.stitching_encounters import stitch_encounters
from clifpy.clif_orchestrator import ClifOrchestrator
from clifpy.tables.hospitalization import Hospitalization
from clifpy.tables.adt import Adt


class TestStitchingEncounters:
    """Test cases for the encounter stitching functionality."""
    
    @pytest.fixture
    def sample_hospitalization_data(self):
        """Create sample hospitalization data for testing."""
        return pd.DataFrame({
            'patient_id': ['P001', 'P001', 'P001', 'P002', 'P002'],
            'hospitalization_id': ['H001', 'H002', 'H003', 'H004', 'H005'],
            'admission_dttm': pd.to_datetime([
                '2023-01-01 10:00:00',  # P001 first admission
                '2023-01-01 20:00:00',  # P001 second admission (4 hours after first discharge)
                '2023-01-05 10:00:00',  # P001 third admission (not linked)
                '2023-01-01 08:00:00',  # P002 first admission
                '2023-01-10 10:00:00'   # P002 second admission (not linked)
            ]),
            'discharge_dttm': pd.to_datetime([
                '2023-01-01 16:00:00',  # P001 first discharge
                '2023-01-02 10:00:00',  # P001 second discharge
                '2023-01-06 10:00:00',  # P001 third discharge
                '2023-01-02 08:00:00',  # P002 first discharge
                '2023-01-11 10:00:00'   # P002 second discharge
            ]),
            'age_at_admission': [65, 65, 65, 45, 45],
            'admission_type_category': ['emergency', 'emergency', 'elective', 'emergency', 'elective'],
            'discharge_category': ['home', 'home', 'home', 'home', 'home']
        })
    
    @pytest.fixture
    def sample_adt_data(self):
        """Create sample ADT data for testing."""
        return pd.DataFrame({
            'hospitalization_id': ['H001', 'H001', 'H002', 'H003', 'H004', 'H005'],
            'in_dttm': pd.to_datetime([
                '2023-01-01 10:00:00',  # H001 first location
                '2023-01-01 12:00:00',  # H001 second location
                '2023-01-01 20:00:00',  # H002
                '2023-01-05 10:00:00',  # H003
                '2023-01-01 08:00:00',  # H004
                '2023-01-10 10:00:00'   # H005
            ]),
            'out_dttm': pd.to_datetime([
                '2023-01-01 12:00:00',  # H001 first location
                '2023-01-01 16:00:00',  # H001 second location
                '2023-01-02 10:00:00',  # H002
                '2023-01-06 10:00:00',  # H003
                '2023-01-02 08:00:00',  # H004
                '2023-01-11 10:00:00'   # H005
            ]),
            'location_category': ['ed', 'icu', 'ward', 'ward', 'icu', 'ward'],
            'hospital_id': ['HOSP1', 'HOSP1', 'HOSP1', 'HOSP1', 'HOSP2', 'HOSP2']
        })
    
    def test_basic_stitching(self, sample_hospitalization_data, sample_adt_data):
        """Test basic encounter stitching functionality."""
        hosp_stitched, adt_stitched, mapping = stitch_encounters(
            sample_hospitalization_data,
            sample_adt_data,
            time_interval=6
        )
        
        # Check that all DataFrames are returned
        assert isinstance(hosp_stitched, pd.DataFrame)
        assert isinstance(adt_stitched, pd.DataFrame)
        assert isinstance(mapping, pd.DataFrame)
        
        # Check that encounter_block column was added
        assert 'encounter_block' in hosp_stitched.columns
        assert 'encounter_block' in adt_stitched.columns
        
        # Check mapping structure
        assert set(mapping.columns) == {'hospitalization_id', 'encounter_block'}
        assert len(mapping) == 5  # Should have all 5 hospitalizations
        
        # Check that H001 and H002 are stitched together (4 hours apart)
        h001_block = mapping[mapping['hospitalization_id'] == 'H001']['encounter_block'].iloc[0]
        h002_block = mapping[mapping['hospitalization_id'] == 'H002']['encounter_block'].iloc[0]
        assert h001_block == h002_block
        
        # Check that H003 is not stitched with H001/H002 (>6 hours apart)
        h003_block = mapping[mapping['hospitalization_id'] == 'H003']['encounter_block'].iloc[0]
        assert h003_block != h001_block
        
        # Check that P002's hospitalizations are separate
        h004_block = mapping[mapping['hospitalization_id'] == 'H004']['encounter_block'].iloc[0]
        h005_block = mapping[mapping['hospitalization_id'] == 'H005']['encounter_block'].iloc[0]
        assert h004_block != h005_block
    
    def test_different_time_intervals(self, sample_hospitalization_data, sample_adt_data):
        """Test stitching with different time interval thresholds."""
        # Test with 2-hour window (should not stitch H001 and H002)
        hosp_stitched, _, mapping = stitch_encounters(
            sample_hospitalization_data,
            sample_adt_data,
            time_interval=2
        )
        
        h001_block = mapping[mapping['hospitalization_id'] == 'H001']['encounter_block'].iloc[0]
        h002_block = mapping[mapping['hospitalization_id'] == 'H002']['encounter_block'].iloc[0]
        assert h001_block != h002_block  # Should be different blocks
        
        # Test with 12-hour window (should stitch H001 and H002)
        hosp_stitched, _, mapping = stitch_encounters(
            sample_hospitalization_data,
            sample_adt_data,
            time_interval=12
        )
        
        h001_block = mapping[mapping['hospitalization_id'] == 'H001']['encounter_block'].iloc[0]
        h002_block = mapping[mapping['hospitalization_id'] == 'H002']['encounter_block'].iloc[0]
        assert h001_block == h002_block  # Should be same block
    
    def test_single_patient(self, sample_hospitalization_data, sample_adt_data):
        """Test stitching with a single patient."""
        # Filter to just patient P001
        hosp_p001 = sample_hospitalization_data[sample_hospitalization_data['patient_id'] == 'P001']
        adt_p001 = sample_adt_data[sample_adt_data['hospitalization_id'].isin(['H001', 'H002', 'H003'])]
        
        hosp_stitched, adt_stitched, mapping = stitch_encounters(
            hosp_p001,
            adt_p001,
            time_interval=6
        )
        
        # Should have 2 encounter blocks (H001+H002, H003)
        assert mapping['encounter_block'].nunique() == 2
    
    def test_no_stitching_needed(self):
        """Test when no encounters need stitching."""
        # Create data where all encounters are >6 hours apart
        hosp_data = pd.DataFrame({
            'patient_id': ['P001', 'P001'],
            'hospitalization_id': ['H001', 'H002'],
            'admission_dttm': pd.to_datetime(['2023-01-01 10:00', '2023-01-10 10:00']),
            'discharge_dttm': pd.to_datetime(['2023-01-02 10:00', '2023-01-11 10:00']),
            'age_at_admission': [65, 65],
            'admission_type_category': ['emergency', 'elective'],
            'discharge_category': ['home', 'home']
        })
        
        adt_data = pd.DataFrame({
            'hospitalization_id': ['H001', 'H002'],
            'in_dttm': pd.to_datetime(['2023-01-01 10:00', '2023-01-10 10:00']),
            'out_dttm': pd.to_datetime(['2023-01-02 10:00', '2023-01-11 10:00']),
            'location_category': ['ward', 'ward'],
            'hospital_id': ['HOSP1', 'HOSP1']
        })
        
        hosp_stitched, adt_stitched, mapping = stitch_encounters(
            hosp_data,
            adt_data,
            time_interval=6
        )
        
        # Each hospitalization should have its own encounter block
        assert mapping['encounter_block'].nunique() == 2
        assert len(mapping) == 2
    
    def test_missing_columns_error(self):
        """Test that missing columns raise appropriate errors."""
        # Missing required column in hospitalization data
        bad_hosp = pd.DataFrame({
            'patient_id': ['P001'],
            'hospitalization_id': ['H001']
            # Missing other required columns
        })
        
        good_adt = pd.DataFrame({
            'hospitalization_id': ['H001'],
            'in_dttm': pd.to_datetime(['2023-01-01']),
            'out_dttm': pd.to_datetime(['2023-01-02']),
            'location_category': ['ward'],
            'hospital_id': ['HOSP1']
        })
        
        with pytest.raises(ValueError) as excinfo:
            stitch_encounters(bad_hosp, good_adt)
        
        assert "Missing required columns" in str(excinfo.value)
    
    def test_empty_dataframes(self):
        """Test handling of empty DataFrames."""
        # Create empty DataFrames with correct columns
        empty_hosp = pd.DataFrame(columns=[
            'patient_id', 'hospitalization_id', 'admission_dttm',
            'discharge_dttm', 'age_at_admission', 'admission_type_category',
            'discharge_category'
        ])
        
        empty_adt = pd.DataFrame(columns=[
            'hospitalization_id', 'in_dttm', 'out_dttm',
            'location_category', 'hospital_id'
        ])
        
        hosp_stitched, adt_stitched, mapping = stitch_encounters(
            empty_hosp,
            empty_adt,
            time_interval=6
        )
        
        # Should return empty DataFrames with encounter_block column
        assert len(hosp_stitched) == 0
        assert len(adt_stitched) == 0
        assert len(mapping) == 0
        assert 'encounter_block' in hosp_stitched.columns
        assert 'encounter_block' in adt_stitched.columns
    
    def test_multiple_stitching_chains(self):
        """Test stitching of multiple consecutive encounters."""
        # Create chain of 3 encounters within time window
        hosp_data = pd.DataFrame({
            'patient_id': ['P001', 'P001', 'P001'],
            'hospitalization_id': ['H001', 'H002', 'H003'],
            'admission_dttm': pd.to_datetime([
                '2023-01-01 10:00:00',  # First admission
                '2023-01-01 20:00:00',  # 4 hours after first discharge
                '2023-01-02 14:00:00'   # 4 hours after second discharge
            ]),
            'discharge_dttm': pd.to_datetime([
                '2023-01-01 16:00:00',  # First discharge
                '2023-01-02 10:00:00',  # Second discharge
                '2023-01-03 10:00:00'   # Third discharge
            ]),
            'age_at_admission': [65, 65, 65],
            'admission_type_category': ['emergency', 'emergency', 'emergency'],
            'discharge_category': ['home', 'home', 'home']
        })
        
        adt_data = pd.DataFrame({
            'hospitalization_id': ['H001', 'H002', 'H003'],
            'in_dttm': hosp_data['admission_dttm'],
            'out_dttm': hosp_data['discharge_dttm'],
            'location_category': ['ed', 'ward', 'icu'],
            'hospital_id': ['HOSP1', 'HOSP1', 'HOSP1']
        })
        
        hosp_stitched, adt_stitched, mapping = stitch_encounters(
            hosp_data,
            adt_data,
            time_interval=6
        )
        
        # Check the encounter blocks
        h001_block = mapping[mapping['hospitalization_id'] == 'H001']['encounter_block'].iloc[0]
        h002_block = mapping[mapping['hospitalization_id'] == 'H002']['encounter_block'].iloc[0]
        h003_block = mapping[mapping['hospitalization_id'] == 'H003']['encounter_block'].iloc[0]
        
        # H001 and H002 should be stitched (4 hours apart)
        assert h001_block == h002_block
        # H002 and H003 should be stitched (4 hours apart)
        assert h002_block == h003_block
        # Therefore all three should be in the same block
        assert h001_block == h002_block == h003_block
        assert mapping['encounter_block'].nunique() == 1
    
    def test_multiple_patients_complex(self):
        """Test complex scenario with multiple patients and various stitching patterns."""
        hosp_data = pd.DataFrame({
            'patient_id': ['P001', 'P001', 'P002', 'P002', 'P003', 'P003'],
            'hospitalization_id': ['H001', 'H002', 'H003', 'H004', 'H005', 'H006'],
            'admission_dttm': pd.to_datetime([
                '2023-01-01 10:00:00',  # P001 first
                '2023-01-01 20:00:00',  # P001 second (should stitch)
                '2023-01-02 08:00:00',  # P002 first
                '2023-01-10 10:00:00',  # P002 second (should not stitch)
                '2023-01-03 14:00:00',  # P003 first
                '2023-01-03 22:00:00'   # P003 second (should stitch)
            ]),
            'discharge_dttm': pd.to_datetime([
                '2023-01-01 16:00:00',  # P001 first
                '2023-01-02 10:00:00',  # P001 second
                '2023-01-03 08:00:00',  # P002 first
                '2023-01-11 10:00:00',  # P002 second
                '2023-01-03 19:00:00',  # P003 first (3 hours before next admission)
                '2023-01-04 10:00:00'   # P003 second
            ]),
            'age_at_admission': [65, 65, 45, 45, 30, 30],
            'admission_type_category': ['emergency'] * 6,
            'discharge_category': ['home'] * 6
        })
        
        adt_data = pd.DataFrame({
            'hospitalization_id': hosp_data['hospitalization_id'],
            'in_dttm': hosp_data['admission_dttm'],
            'out_dttm': hosp_data['discharge_dttm'],
            'location_category': ['ward'] * 6,
            'hospital_id': ['HOSP1'] * 6
        })
        
        hosp_stitched, adt_stitched, mapping = stitch_encounters(
            hosp_data,
            adt_data,
            time_interval=6
        )
        
        # Expected: 4 encounter blocks
        # P001: H001 + H002 (1 block)
        # P002: H003 (1 block), H004 (1 block)
        # P003: H005 + H006 (1 block)
        assert mapping['encounter_block'].nunique() == 4
        
        # Check P001 stitching
        p001_blocks = mapping[mapping['hospitalization_id'].isin(['H001', 'H002'])]['encounter_block']
        assert p001_blocks.nunique() == 1
        
        # Check P002 no stitching
        p002_blocks = mapping[mapping['hospitalization_id'].isin(['H003', 'H004'])]['encounter_block']
        assert p002_blocks.nunique() == 2
        
        # Check P003 stitching
        p003_blocks = mapping[mapping['hospitalization_id'].isin(['H005', 'H006'])]['encounter_block']
        assert p003_blocks.nunique() == 1
    
    def test_exact_time_boundary(self):
        """Test stitching when time difference is exactly at the boundary."""
        hosp_data = pd.DataFrame({
            'patient_id': ['P001', 'P001', 'P001'],
            'hospitalization_id': ['H001', 'H002', 'H003'],
            'admission_dttm': pd.to_datetime([
                '2023-01-01 10:00:00',
                '2023-01-01 16:00:00',  # Exactly 6 hours after first discharge
                '2023-01-01 22:00:01'   # Just over 6 hours after second discharge
            ]),
            'discharge_dttm': pd.to_datetime([
                '2023-01-01 10:00:00',  # 0-duration stay
                '2023-01-01 16:00:00',  # 0-duration stay
                '2023-01-02 10:00:00'
            ]),
            'age_at_admission': [65, 65, 65],
            'admission_type_category': ['emergency'] * 3,
            'discharge_category': ['home'] * 3
        })
        
        adt_data = pd.DataFrame({
            'hospitalization_id': hosp_data['hospitalization_id'],
            'in_dttm': hosp_data['admission_dttm'],
            'out_dttm': hosp_data['discharge_dttm'],
            'location_category': ['ed'] * 3,
            'hospital_id': ['HOSP1'] * 3
        })
        
        hosp_stitched, adt_stitched, mapping = stitch_encounters(
            hosp_data,
            adt_data,
            time_interval=6
        )
        
        # H001 and H002 should be stitched (exactly 6 hours)
        # H003 should be separate (just over 6 hours)
        assert mapping['encounter_block'].nunique() == 2
        h001_block = mapping[mapping['hospitalization_id'] == 'H001']['encounter_block'].iloc[0]
        h002_block = mapping[mapping['hospitalization_id'] == 'H002']['encounter_block'].iloc[0]
        h003_block = mapping[mapping['hospitalization_id'] == 'H003']['encounter_block'].iloc[0]
        assert h001_block == h002_block
        assert h003_block != h002_block
    
    def test_multiple_adt_locations_per_hospitalization(self):
        """Test stitching with multiple ADT records per hospitalization."""
        hosp_data = pd.DataFrame({
            'patient_id': ['P001', 'P001'],
            'hospitalization_id': ['H001', 'H002'],
            'admission_dttm': pd.to_datetime([
                '2023-01-01 10:00:00',
                '2023-01-01 20:00:00'  # 4 hours after first discharge
            ]),
            'discharge_dttm': pd.to_datetime([
                '2023-01-01 16:00:00',
                '2023-01-02 10:00:00'
            ]),
            'age_at_admission': [65, 65],
            'admission_type_category': ['emergency', 'emergency'],
            'discharge_category': ['home', 'home']
        })
        
        # Multiple ADT records for each hospitalization
        adt_data = pd.DataFrame({
            'hospitalization_id': ['H001', 'H001', 'H001', 'H002', 'H002'],
            'in_dttm': pd.to_datetime([
                '2023-01-01 10:00:00',  # H001 ED
                '2023-01-01 12:00:00',  # H001 ICU
                '2023-01-01 14:00:00',  # H001 Ward
                '2023-01-01 20:00:00',  # H002 ED
                '2023-01-02 02:00:00'   # H002 Ward
            ]),
            'out_dttm': pd.to_datetime([
                '2023-01-01 12:00:00',  # H001 ED out
                '2023-01-01 14:00:00',  # H001 ICU out
                '2023-01-01 16:00:00',  # H001 Ward out
                '2023-01-02 02:00:00',  # H002 ED out
                '2023-01-02 10:00:00'   # H002 Ward out
            ]),
            'location_category': ['ed', 'icu', 'ward', 'ed', 'ward'],
            'hospital_id': ['HOSP1'] * 5
        })
        
        hosp_stitched, adt_stitched, mapping = stitch_encounters(
            hosp_data,
            adt_data,
            time_interval=6
        )
        
        # Hospitalizations should be stitched
        assert mapping['encounter_block'].nunique() == 1
        
        # ADT should have same encounter block for all records
        assert adt_stitched['encounter_block'].nunique() == 1
        assert len(adt_stitched) == 5  # All ADT records preserved
    
    def test_null_datetime_handling(self):
        """Test handling of null datetime values."""
        hosp_data = pd.DataFrame({
            'patient_id': ['P001', 'P001'],
            'hospitalization_id': ['H001', 'H002'],
            'admission_dttm': pd.to_datetime(['2023-01-01 10:00:00', '2023-01-01 20:00:00']),
            'discharge_dttm': pd.to_datetime(['2023-01-01 16:00:00', '2023-01-02 10:00:00']),
            'age_at_admission': [65, 65],
            'admission_type_category': ['emergency', 'emergency'],
            'discharge_category': ['home', 'home']
        })
        
        # ADT with some null out_dttm values
        adt_data = pd.DataFrame({
            'hospitalization_id': ['H001', 'H002'],
            'in_dttm': pd.to_datetime(['2023-01-01 10:00:00', '2023-01-01 20:00:00']),
            'out_dttm': [pd.to_datetime('2023-01-01 16:00:00'), pd.NaT],  # Second has null out time
            'location_category': ['ward', 'icu'],
            'hospital_id': ['HOSP1', 'HOSP1']
        })
        
        # Should not raise error
        hosp_stitched, adt_stitched, mapping = stitch_encounters(
            hosp_data,
            adt_data,
            time_interval=6
        )
        
        # Should still stitch the encounters
        assert mapping['encounter_block'].nunique() == 1


    def test_preserves_original_columns(self, sample_hospitalization_data, sample_adt_data):
        """Test that all original columns are preserved after stitching."""
        # Add some extra columns to test preservation
        sample_hospitalization_data['extra_hosp_col'] = 'test_value'
        sample_adt_data['extra_adt_col'] = 'test_adt_value'
        
        hosp_stitched, adt_stitched, mapping = stitch_encounters(
            sample_hospitalization_data,
            sample_adt_data,
            time_interval=6
        )
        
        # Check all original columns are preserved
        orig_hosp_cols = set(sample_hospitalization_data.columns)
        stitched_hosp_cols = set(hosp_stitched.columns)
        assert orig_hosp_cols.issubset(stitched_hosp_cols)
        assert 'extra_hosp_col' in hosp_stitched.columns
        
        orig_adt_cols = set(sample_adt_data.columns)
        stitched_adt_cols = set(adt_stitched.columns)
        assert orig_adt_cols.issubset(stitched_adt_cols)
        assert 'extra_adt_col' in adt_stitched.columns
        
        # Check encounter_block was added
        assert 'encounter_block' in hosp_stitched.columns
        assert 'encounter_block' in adt_stitched.columns
    
    def test_deterministic_encounter_blocks(self, sample_hospitalization_data, sample_adt_data):
        """Test that encounter block assignment is deterministic."""
        # Run stitching multiple times
        results = []
        for _ in range(3):
            _, _, mapping = stitch_encounters(
                sample_hospitalization_data.copy(),
                sample_adt_data.copy(),
                time_interval=6
            )
            results.append(mapping.sort_values('hospitalization_id')['encounter_block'].tolist())
        
        # All runs should produce identical results
        for i in range(1, len(results)):
            assert results[i] == results[0], "Encounter block assignment should be deterministic"


class TestClifOrchestratorStitching:
    """Test ClifOrchestrator's stitch_encounters method."""
    
    @pytest.fixture
    def sample_data_dir(self, tmp_path, sample_hospitalization_data, sample_adt_data):
        """Create temporary data directory with sample data files."""
        data_dir = tmp_path / "test_data"
        data_dir.mkdir()
        
        # Save sample data as parquet files
        sample_hospitalization_data.to_parquet(data_dir / "clif_hospitalization.parquet")
        sample_adt_data.to_parquet(data_dir / "clif_adt.parquet")
        
        return str(data_dir)
    
    @pytest.fixture
    def sample_hospitalization_data(self):
        """Create sample hospitalization data."""
        return pd.DataFrame({
            'patient_id': ['P001', 'P001'],
            'hospitalization_id': ['H001', 'H002'],
            'admission_dttm': pd.to_datetime(['2023-01-01 10:00', '2023-01-01 20:00']),
            'discharge_dttm': pd.to_datetime(['2023-01-01 16:00', '2023-01-02 10:00']),
            'age_at_admission': [65, 65],
            'admission_type_category': ['emergency', 'emergency'],
            'discharge_category': ['home', 'home']
        })
    
    @pytest.fixture
    def sample_adt_data(self):
        """Create sample ADT data."""
        return pd.DataFrame({
            'hospitalization_id': ['H001', 'H002'],
            'in_dttm': pd.to_datetime(['2023-01-01 10:00', '2023-01-01 20:00']),
            'out_dttm': pd.to_datetime(['2023-01-01 16:00', '2023-01-02 10:00']),
            'location_category': ['icu', 'ward'],
            'hospital_id': ['HOSP1', 'HOSP1']
        })
    
    def test_clif_orchestrator_stitch_encounters(self, sample_data_dir):
        """Test stitch_encounters through ClifOrchestrator."""
        # Create orchestrator with stitching enabled
        clif = ClifOrchestrator(
            data_directory=sample_data_dir,
            filetype='parquet',
            timezone='US/Eastern',
            stitch_encounter=True,
            stitch_time_interval=6
        )
        
        # Load required tables - stitching happens automatically
        clif.initialize(['hospitalization', 'adt'])
        
        # Check that encounter_mapping was created
        assert clif.encounter_mapping is not None
        
        # Check that encounter_block column was added
        assert 'encounter_block' in clif.hospitalization.df.columns
        assert 'encounter_block' in clif.adt.df.columns
        
        # Check mapping
        assert len(clif.encounter_mapping) == 2
        assert set(clif.encounter_mapping.columns) == {'hospitalization_id', 'encounter_block'}
    
    def test_stitch_encounters_auto_loads_required_tables(self, sample_data_dir):
        """run_stitch_encounters loads hospitalization/adt itself.

        It used to warn and no-op when they were missing; it now loads
        whichever is absent so that loading alone cannot short-circuit the
        stitch.
        """
        clif = ClifOrchestrator(
            data_directory=sample_data_dir,
            filetype='parquet',
            timezone='US/Eastern',
            stitch_encounter=True  # Enable stitching
        )

        clif.initialize([])  # Empty list: nothing explicitly requested

        assert clif.hospitalization is not None
        assert clif.adt is not None
        assert clif.encounter_mapping is not None
    
    def test_clif_orchestrator_auto_stitch_on_init(self, sample_data_dir):
        """Test automatic stitching when enabled in constructor."""
        # Create orchestrator with stitch_encounter=True
        clif = ClifOrchestrator(
            data_directory=sample_data_dir,
            filetype='parquet',
            timezone='US/Eastern',
            stitch_encounter=True,
            stitch_time_interval=12
        )
        
        # Load required tables - stitching should happen automatically
        clif.initialize(['hospitalization', 'adt'])
        
        # Check that stitching was performed
        assert clif.encounter_mapping is not None
        
        # Verify encounter_block column was added to the tables
        assert 'encounter_block' in clif.hospitalization.df.columns
        assert 'encounter_block' in clif.adt.df.columns
        
        # Check that the time interval was used correctly (12 hours)
        # H001 and H002 are 4 hours apart, so should be stitched
        mapping = clif.get_encounter_mapping()
        h001_block = mapping[mapping['hospitalization_id'] == 'H001']['encounter_block'].iloc[0]
        h002_block = mapping[mapping['hospitalization_id'] == 'H002']['encounter_block'].iloc[0]
        assert h001_block == h002_block
    
    def test_get_encounter_mapping_stitches_on_demand(self, sample_data_dir):
        """get_encounter_mapping stitches lazily instead of returning None."""
        clif = ClifOrchestrator(
            data_directory=sample_data_dir,
            filetype='parquet',
            timezone='US/Eastern'
        )

        # Nothing stitched yet, so the attribute itself is still unset
        assert clif.encounter_mapping is None

        # Asking for it triggers run_stitch_encounters()
        assert clif.get_encounter_mapping() is not None
        assert clif.encounter_mapping is not None

        # Explicit stitching keeps working
        clif.stitch_encounter = True
        clif.stitch_time_interval = 6
        clif.initialize(['hospitalization', 'adt'])
        assert clif.get_encounter_mapping() is not None

