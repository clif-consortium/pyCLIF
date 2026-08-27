"""
ClifOrchestrator class for managing multiple CLIF table objects.

This module provides a unified interface for loading and managing
all CLIF table objects with consistent configuration.
"""

import os
import logging
import pandas as pd
import psutil
from typing import Optional, List, Dict, Any, Union, Tuple

from .tables.patient import Patient
from .tables.hospitalization import Hospitalization
from .tables.adt import Adt
from .tables.labs import Labs
from .tables.vitals import Vitals
from .tables.medication_admin_continuous import MedicationAdminContinuous
from .tables.medication_admin_intermittent import MedicationAdminIntermittent
from .tables.patient_assessments import PatientAssessments
from .tables.respiratory_support import RespiratorySupport
from .tables.position import Position
from .tables.hospital_diagnosis import HospitalDiagnosis
from .tables.microbiology_culture import MicrobiologyCulture
from .tables.crrt_therapy import CrrtTherapy
from .tables.patient_procedures import PatientProcedures
from .tables.microbiology_susceptibility import MicrobiologySusceptibility
from .tables.ecmo_mcs import EcmoMcs
from .tables.microbiology_nonculture import MicrobiologyNonculture
from .tables.code_status import CodeStatus
# CLIF 3.0 tables (new in 3.0; mcs replaces ecmo_mcs)
from .tables.mcs import Mcs
from .tables.renal_replacement_therapy import RenalReplacementTherapy
from .tables.input import Input
from .tables.output import Output
from .tables.invasive_hemodynamics import InvasiveHemodynamics
from .tables.consult_orders import ConsultOrders
from .tables.misc_icu_orders import MiscIcuOrders
from .tables.medication_orders import MedicationOrders
from .tables.patient_diagnosis import PatientDiagnosis
from .tables.place_based_index import PlaceBasedIndex
from .tables.provider import Provider
from .tables.therapy_details import TherapyDetails
from .tables.transfusion import Transfusion
from .tables.clinical_trial import ClinicalTrial
from .tables.clinical_notes_facts import ClinicalNotesFacts
from .tables.validated_diagnosis import ValidatedDiagnosis
from .tables.model_registry import ModelRegistry
from .tables.scores import Scores
from .tables.radiology import Radiology
from .tables.line import Line
from .tables.drain import Drain
from .tables.airway import Airway
from .tables.patient_attributes import PatientAttributes
from .tables.ed_encounter import EdEncounter
from .utils.config import get_config_or_params
from .utils.stitching_encounters import stitch_encounters
from .utils.logging_config import setup_logging
from .schemas import DEFAULT_CLIF_VERSION


TABLE_CLASSES = {
    'patient': Patient,
    'hospitalization': Hospitalization,
    'adt': Adt,
    'labs': Labs,
    'vitals': Vitals,
    'medication_admin_continuous': MedicationAdminContinuous,
    'medication_admin_intermittent': MedicationAdminIntermittent,
    'patient_assessments': PatientAssessments,
    'respiratory_support': RespiratorySupport,
    'position': Position,
    'hospital_diagnosis': HospitalDiagnosis,
    'microbiology_culture': MicrobiologyCulture,
    'crrt_therapy': CrrtTherapy,
    'patient_procedures': PatientProcedures,
    'microbiology_susceptibility': MicrobiologySusceptibility,
    'ecmo_mcs': EcmoMcs,
    'microbiology_nonculture': MicrobiologyNonculture,
    'code_status': CodeStatus,
    # CLIF 3.0 tables (mcs replaces ecmo_mcs)
    'mcs': Mcs,
    'renal_replacement_therapy': RenalReplacementTherapy,
    'input': Input,
    'output': Output,
    'invasive_hemodynamics': InvasiveHemodynamics,
    'consult_orders': ConsultOrders,
    'misc_icu_orders': MiscIcuOrders,
    'medication_orders': MedicationOrders,
    'patient_diagnosis': PatientDiagnosis,
    'place_based_index': PlaceBasedIndex,
    'provider': Provider,
    'therapy_details': TherapyDetails,
    'transfusion': Transfusion,
    'clinical_trial': ClinicalTrial,
    'clinical_notes_facts': ClinicalNotesFacts,
    'validated_diagnosis': ValidatedDiagnosis,
    'model_registry': ModelRegistry,
    'scores': Scores,
    'radiology': Radiology,
    'line': Line,
    'drain': Drain,
    'airway': Airway,
    'patient_attributes': PatientAttributes,
    'ed_encounter': EdEncounter,
}


class ClifOrchestrator:
    """
    Orchestrator class for managing multiple CLIF table objects.
    
    This class provides a centralized interface for loading, managing,
    and validating multiple CLIF tables with consistent configuration.
    
    Attributes
    ----------
    config_path : str, optional
        Path to configuration JSON file
    data_directory : str
        Path to the directory containing data files
    filetype : str
        Type of data file (csv, parquet, etc.)
    timezone : str
        Timezone for datetime columns
    output_directory : str
        Directory for saving output files and logs
    stitch_encounter : bool
        Whether to stitch encounters within time interval
    stitch_time_interval : int
        Hours between discharge and next admission to consider encounters linked
    encounter_mapping : pd.DataFrame
        Mapping of hospitalization_id to encounter_block (after stitching)
    patient : Patient
        Patient table object
    hospitalization : Hospitalization
        Hospitalization table object
    adt : Adt
        ADT table object
    labs : Labs
        Labs table object
    vitals : Vitals
        Vitals table object
    medication_admin_continuous : MedicationAdminContinuous
        Medication administration continuous table object
    medication_admin_intermittent : MedicationAdminIntermittent
        Medication administration intermittent table object
    patient_assessments : PatientAssessments
        Patient assessments table object
    respiratory_support : RespiratorySupport
        Respiratory support table object
    position : Position
        Position table object
    hospital_diagnosis : HospitalDiagnosis
        Hospital diagnosis table object
    microbiology_culture : MicrobiologyCulture
        Microbiology culture table object
    crrt_therapy : CrrtTherapy
        CRRT therapy table object
    patient_procedures : PatientProcedures
        Patient procedures table object
    microbiology_susceptibility : MicrobiologySusceptibility
        Microbiology susceptibility table object
    ecmo_mcs : EcmoMcs
        ECMO/MCS table object
    microbiology_nonculture : MicrobiologyNonculture
        Microbiology non-culture table object
    code_status : CodeStatus
        Code status table object
    wide_df : pd.DataFrame
        Wide dataset with time-series data (populated by create_wide_dataset)
    """
    
    def __init__(
        self,
        config_path: Optional[str] = None,
        data_directory: Optional[str] = None,
        filetype: Optional[str] = None,
        timezone: Optional[str] = None,
        output_directory: Optional[str] = None,
        stitch_encounter: bool = False,
        stitch_time_interval: int = 6,
        clif_version: Optional[str] = None
    ):
        """
        Initialize the ClifOrchestrator.

        Parameters
        ----------
        config_path : str, optional
            Path to configuration JSON file
        data_directory : str, optional
            Path to the directory containing data files
        filetype : str, optional
            Type of data file (csv, parquet, etc.)
        timezone : str, optional
            Timezone for datetime columns
        output_directory : str, optional
            Directory for saving output files and logs.
            If not provided, creates an 'output' directory in the current working directory.
        stitch_encounter : bool, optional
            Whether to stitch encounters within time interval. Default False.
        stitch_time_interval : int, optional
            Hours between discharge and next admission to consider
            encounters linked. Default 6 hours.
        clif_version : str, optional
            CLIF schema version for all loaded tables (e.g. "2.1", "3.0").
            Overrides any ``clif_version`` in the config file. If neither is
            set, the package default (3.0) is used.
                
        Notes
        -----
        Loading priority:
        
        1. If all required params provided → use them
        2. If config_path provided → load from that path, allow param overrides
        3. If no params and no config_path → auto-detect config.json
        4. Parameters override config file values when both are provided
        """
        # Get configuration from config file or parameters
        config = get_config_or_params(
            config_path=config_path,
            data_directory=data_directory,
            filetype=filetype,
            timezone=timezone,
            output_directory=output_directory
        )
        
        self.data_directory = config['data_directory']
        self.filetype = config['filetype']
        self.timezone = config['timezone']

        # Resolve CLIF version: explicit param > config file > package default
        self.clif_version = clif_version or config.get('clif_version', DEFAULT_CLIF_VERSION)

        # Set output directory
        self.output_directory = config.get('output_directory')
        if self.output_directory is None:
            self.output_directory = os.path.join(os.getcwd(), 'output')
        os.makedirs(self.output_directory, exist_ok=True)

        # Initialize centralized logging
        setup_logging(output_directory=self.output_directory)

        # Get logger for orchestrator
        self.logger = logging.getLogger('clifpy.ClifOrchestrator')

        # Set stitching parameters
        self.stitch_encounter = stitch_encounter
        self.stitch_time_interval = stitch_time_interval
        self.encounter_mapping = None
        
        # Initialize all table attributes to None
        self.patient: Patient = None
        self.hospitalization: Hospitalization = None
        self.adt: Adt = None
        self.labs: Labs = None
        self.vitals: Vitals = None
        self.medication_admin_continuous: MedicationAdminContinuous = None
        self.medication_admin_intermittent: MedicationAdminIntermittent = None
        self.patient_assessments: PatientAssessments = None
        self.respiratory_support: RespiratorySupport = None
        self.position: Position = None
        self.hospital_diagnosis: HospitalDiagnosis = None
        self.microbiology_culture: MicrobiologyCulture = None
        self.crrt_therapy: CrrtTherapy = None
        self.patient_procedures: PatientProcedures = None
        self.microbiology_susceptibility: MicrobiologySusceptibility = None
        self.ecmo_mcs: EcmoMcs = None
        self.microbiology_nonculture: MicrobiologyNonculture = None
        self.code_status: CodeStatus = None
        # CLIF 3.0 tables (mcs replaces ecmo_mcs)
        self.mcs: Mcs = None
        self.renal_replacement_therapy: RenalReplacementTherapy = None
        self.input: Input = None
        self.output: Output = None
        self.invasive_hemodynamics: InvasiveHemodynamics = None
        self.consult_orders: ConsultOrders = None
        self.misc_icu_orders: MiscIcuOrders = None
        self.medication_orders: MedicationOrders = None
        self.patient_diagnosis: PatientDiagnosis = None
        self.place_based_index: PlaceBasedIndex = None
        self.provider: Provider = None
        self.therapy_details: TherapyDetails = None
        self.transfusion: Transfusion = None
        self.clinical_trial: ClinicalTrial = None
        self.clinical_notes_facts: ClinicalNotesFacts = None
        self.validated_diagnosis: ValidatedDiagnosis = None
        self.model_registry: ModelRegistry = None
        self.scores: Scores = None
        self.radiology: Radiology = None
        self.line: Line = None
        self.drain: Drain = None
        self.airway: Airway = None
        self.patient_attributes: PatientAttributes = None
        self.ed_encounter: EdEncounter = None

        # Initialize wide dataset property
        self.wide_df: Optional[pd.DataFrame] = None

        self.logger.info('ClifOrchestrator initialized')
    
    @classmethod
    def from_config(cls, config_path: str = "./config.json") -> 'ClifOrchestrator':
        """
        Create a ClifOrchestrator instance from a configuration file.
        
        Parameters
        ----------
        config_path : str
            Path to the configuration JSON file
            
        Returns
        -------
        ClifOrchestrator
            Configured instance
        """
        return cls(config_path=config_path)
    
    def load_table(
        self,
        table_name: str,
        sample_size: Optional[int] = None,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None
    ) -> Union[Patient, Hospitalization, Adt, Labs, Vitals, MedicationAdminContinuous, MedicationAdminIntermittent, PatientAssessments, RespiratorySupport, Position, HospitalDiagnosis, MicrobiologyCulture, CrrtTherapy, PatientProcedures, MicrobiologySusceptibility, EcmoMcs, MicrobiologyNonculture, CodeStatus]:
        """
        Load table data and create table object.
        
        Parameters
        ----------
        table_name : str
            Name of the table to load
        sample_size : int, optional
            Number of rows to load
        columns : List[str], optional
            Specific columns to load
        filters : Dict, optional
            Filters to apply when loading
            
        Returns
        -------
        Union[Patient, Hospitalization, Adt, Labs, Vitals, MedicationAdminContinuous, PatientAssessments, RespiratorySupport, Position]
            The loaded table object
        """
        if table_name not in TABLE_CLASSES:
            raise ValueError(f"Unknown table: {table_name}. Available tables: {list(TABLE_CLASSES.keys())}")
        
        table_class = TABLE_CLASSES[table_name]
        table_object = table_class.from_file(
            data_directory=self.data_directory,
            filetype=self.filetype,
            timezone=self.timezone,
            output_directory=self.output_directory,
            sample_size=sample_size,
            columns=columns,
            filters=filters,
            clif_version=self.clif_version
        )
        setattr(self, table_name, table_object)
        return table_object
    
    def initialize(
        self,
        tables: Optional[List[str]] = None,
        sample_size: Optional[int] = None,
        columns: Optional[Dict[str, List[str]]] = None,
        filters: Optional[Dict[str, Dict[str, Any]]] = None
    ):
        """
        Initialize specified tables with optional filtering and column selection.
        
        Parameters
        ----------
        tables : List[str], optional
            List of table names to load. Defaults to ['patient'].
        sample_size : int, optional
            Number of rows to load for each table.
        columns : Dict[str, List[str]], optional
            Dictionary mapping table names to lists of columns to load.
        filters : Dict[str, Dict], optional
            Dictionary mapping table names to filter dictionaries.
        """
        if tables is None:
            tables = ['patient']
        
        for table in tables:
            # Get table-specific columns and filters if provided
            table_columns = columns.get(table) if columns else None
            table_filters = filters.get(table) if filters else None
            
            try:
                self.load_table(table, sample_size, table_columns, table_filters)
            except ValueError as e:
                self.logger.warning(f"{e}")
        
        # Perform encounter stitching if enabled
        if self.stitch_encounter:
            self.run_stitch_encounters()
    
    def run_stitch_encounters(self):
        # automatically load whichever of hospitalization / adt is missing,
        # then stitch -- loading alone must not short-circuit the stitch
        if self.hospitalization is None:
            self.load_table('hospitalization')
        if self.adt is None:
            self.load_table('adt')

        self.logger.info(f"Performing encounter stitching with time interval of {self.stitch_time_interval} hours")
        try:
            hospitalization_stitched, adt_stitched, encounter_mapping = stitch_encounters(
                self.hospitalization.df,
                self.adt.df,
                time_interval=self.stitch_time_interval
            )

            # Update the dataframes in place
            self.hospitalization.df = hospitalization_stitched
            self.adt.df = adt_stitched
            self.encounter_mapping = encounter_mapping

            self.logger.info("Encounter stitching completed successfully")
        except Exception as e:
            self.logger.error(f"Error during encounter stitching: {e}")
            self.encounter_mapping = None
        
    def get_loaded_tables(self) -> List[str]:
        """
        Return list of currently loaded table names.
        
        Returns
        -------
        List[str]
            List of loaded table names
        """
        loaded = []
        for table_name in TABLE_CLASSES.keys():
            if getattr(self, table_name) is not None:
                loaded.append(table_name)
        return loaded
    
    def get_tables_obj_list(self) -> List:
        """
        Return list of loaded table objects.
        
        Returns
        -------
        List
            List of loaded table objects
        """
        table_objects = []
        for table_name in TABLE_CLASSES.keys():
            table_obj = getattr(self, table_name)
            if table_obj is not None:
                table_objects.append(table_obj)
        return table_objects
    
    def get_encounter_mapping(self) -> Optional[pd.DataFrame]:
        """
        Return the encounter mapping DataFrame if encounter stitching was performed.
        
        Returns
        -------
        pd.DataFrame or None
            Mapping of hospitalization_id to encounter_block if stitching was performed,
            None if stitching was not performed or failed.
        """
        if self.encounter_mapping is None:
            self.run_stitch_encounters()
        return self.encounter_mapping
    
    def validate_all(self):
        """
        Run validation on all loaded tables.

        This method runs the validate() method on each loaded table
        and reports the results.
        """
        loaded_tables = self.get_loaded_tables()

        if not loaded_tables:
            self.logger.info("No tables loaded to validate")
            return

        self.logger.info(f"Validating {len(loaded_tables)} table(s)")

        for table_name in loaded_tables:
            table_obj = getattr(self, table_name)
            self.logger.info(f"Validating {table_name}")
            table_obj.validate()
    
    def create_wide_dataset(
        self,
        tables_to_load: Optional[List[str]] = None,
        category_filters: Optional[Dict[str, List[str]]] = None,
        sample: bool = False,
        hospitalization_ids: Optional[List[str]] = None,
        encounter_blocks: Optional[List[int]] = None,
        cohort_df: Optional[pd.DataFrame] = None,
        output_format: str = 'dataframe',
        save_to_data_location: bool = False,
        output_filename: Optional[str] = None,
        return_dataframe: bool = True,
        batch_size: int = 1000,
        memory_limit: Optional[str] = None,
        threads: Optional[int] = None,
        show_progress: bool = True
    ) -> None:
        """
        Create wide time-series dataset using DuckDB for high performance.
        
        Parameters
        ----------
        tables_to_load : List[str], optional
            List of table names to include in the wide dataset (e.g., ['vitals', 'labs', 'respiratory_support']).
            If None, only base tables (patient, hospitalization, adt) are loaded.
        category_filters : Dict[str, List[str]], optional
            Dictionary mapping table names to lists for filtering/selection. Behavior differs
            by table type:

            **PIVOT TABLES** (narrow to wide - category values from schema):
            - Values are **category values** to filter and pivot into columns
            - Examples:
              * vitals: temp_c, heart_rate, sbp, dbp, spo2, respiratory_rate, map
              * labs: hemoglobin, wbc, sodium, potassium, creatinine, glucose_serum, lactate
              * medication_admin_continuous: norepinephrine, epinephrine, propofol, fentanyl
              * patient_assessments: RASS, gcs_total, cam_total, braden_total

            **WIDE TABLES** (already wide - column names from schema):
            - Values are **column names** to keep from the table
            - Examples:
              * respiratory_support: device_category, mode_category, fio2_set, peep_set

            Usage Example:
            ```python
            category_filters = {
                'vitals': ['heart_rate', 'sbp', 'spo2'],
                'labs': ['hemoglobin', 'sodium', 'creatinine'],
                'respiratory_support': ['device_category', 'fio2_set', 'peep_set']
            }
            ```

            **Supported Tables:**
            For complete list of supported tables and their types, see:
            clifpy/schemas/wide_tables_config.yaml

            **Category Values:**
            For complete lists of acceptable category values, see:
            - Table schemas: clifpy/schemas/<clif_version>/*_schema.yaml
            - Use `co.vitals.df['vital_category'].unique()` to see available values in your data
        sample : bool, default=False
            If True, randomly sample 20 hospitalizations for testing purposes.
        hospitalization_ids : List[str], optional
            List of specific hospitalization IDs to include. When provided, only data for these
            hospitalizations will be loaded, improving performance for large datasets.
        encounter_blocks : List[int], optional
            List of encounter block IDs to include when encounter stitching has been performed.
            Automatically converts encounter blocks to their corresponding hospitalization IDs.
            Only used when encounter stitching is enabled and encounter mapping exists.
        cohort_df : pd.DataFrame, optional
            DataFrame containing cohort definitions with columns:
            
            - 'patient_id': Patient identifier
            - 'start_time': Start of time window (datetime)
            - 'end_time': End of time window (datetime)
            
            When encounter stitching is enabled, can also include 'encounter_block' column.
            Used to filter data to specific time windows per patient.
        output_format : str, default='dataframe'
            Format for output data. Options: 'dataframe', 'csv', 'parquet'.
        save_to_data_location : bool, default=False
            If True, save output file to the data directory specified in orchestrator config.
        output_filename : str, optional
            Custom filename for saved output. If None, auto-generates filename with timestamp.
        return_dataframe : bool, default=True
            If True, return DataFrame even when saving to file. If False and saving,
            returns None to save memory.
        batch_size : int, default=1000
            Number of hospitalizations to process per batch. Lower values use less memory.
        memory_limit : str, optional
            DuckDB memory limit (e.g., '8GB', '16GB'). If None, uses DuckDB default.
        threads : int, optional
            Number of threads for DuckDB to use. If None, uses all available cores.
        show_progress : bool, default=True
            If True, display progress bars during processing.
            
        Returns
        -------
        None
            The wide dataset is stored in the `wide_df` property of the orchestrator instance.
            Access the result via `orchestrator.wide_df` after calling this method.
            
        Notes
        -----
        - When hospitalization_ids is provided, the function efficiently loads only the
          specified hospitalizations from all tables, significantly reducing memory usage
          and processing time for targeted analyses.
        - The wide dataset will have one row per hospitalization per time point, with
          columns for each category value specified in category_filters.
        """
        self.logger.info("=" * 50)
        self.logger.info("🚀 WIDE DATASET CREATION STARTED")
        self.logger.info("=" * 50)

        self.logger.info("Phase 1: Initialization")
        self.logger.debug("  1.1: Validating parameters")

        # Import the utility function
        from clifpy.utils.wide_dataset import create_wide_dataset as _create_wide

        # Handle encounter stitching scenarios
        if self.encounter_mapping is not None:
            self.logger.info("  1.2: Configuring encounter stitching (enabled)")
        else:
            self.logger.debug("  1.2: Encounter stitching (disabled)")

        self.logger.info("Phase 2: Encounter Processing")

        if self.encounter_mapping is not None:
            self.logger.info("  2.1: === SPECIAL: ENCOUNTER STITCHING ===")
            # Handle cohort_df with encounter_block column
            if cohort_df is not None:
                if 'encounter_block' in cohort_df.columns:
                    self.logger.info("       - Detected encounter_block column in cohort_df")
                    self.logger.debug("       - Mapping encounter blocks to hospitalization IDs")
                    # Merge cohort_df with encounter_mapping to get hospitalization_ids
                    cohort_df = pd.merge(
                        cohort_df,
                        self.encounter_mapping[['hospitalization_id', 'encounter_block']],
                        on='encounter_block',
                        how='inner',
                        suffixes=('_orig', '')
                    )
                    # If hospitalization_id_orig exists (cohort had both), use the mapping version
                    if 'hospitalization_id_orig' in cohort_df.columns:
                        cohort_df = cohort_df.drop(columns=['hospitalization_id_orig'])
                    self.logger.info(f"       - Processing {cohort_df['encounter_block'].nunique()} encounter blocks from cohort_df")
                elif 'hospitalization_id' in cohort_df.columns:
                    self.logger.info("Encounter stitching has been performed. Your cohort_df uses hospitalization_id. " +
                          "Consider using 'encounter_block' column instead for cleaner encounter-level filtering")
                else:
                    self.logger.warning("cohort_df must contain either 'hospitalization_id' or 'encounter_block' column")

            # Handle encounter_blocks parameter
            if encounter_blocks is not None:
                self.logger.debug("       - Processing encounter_blocks parameter")
                if len(encounter_blocks) == 0:
                    self.logger.warning("       - Empty encounter_blocks list provided. Processing all encounter blocks")
                    encounter_blocks = None
                else:
                    # Validate that provided encounter_blocks exist in mapping
                    invalid_blocks = [b for b in encounter_blocks if b not in self.encounter_mapping['encounter_block'].values]
                    if invalid_blocks:
                        self.logger.warning(f"       - Invalid encounter blocks found: {invalid_blocks}")
                        encounter_blocks = [b for b in encounter_blocks if b in self.encounter_mapping['encounter_block'].values]

                    if encounter_blocks:  # Only if valid blocks remain
                        hospitalization_ids = self.encounter_mapping[
                            self.encounter_mapping['encounter_block'].isin(encounter_blocks)
                        ]['hospitalization_id'].tolist()
                        self.logger.info(f"       - Converting {len(encounter_blocks)} encounter blocks to {len(hospitalization_ids)} hospitalizations")
                    else:
                        self.logger.warning("       - No valid encounter blocks found. Processing all data")
                        encounter_blocks = None

            # If no filters provided after stitching
            elif hospitalization_ids is None and cohort_df is None:
                self.logger.debug("       - No encounter_blocks provided - processing all encounter blocks")
        else:
            self.logger.debug("  2.1: No encounter stitching performed")

        filters = None
        if hospitalization_ids:
            filters = {'hospitalization_id': hospitalization_ids}

        self.logger.info("Phase 3: Table Loading")

        self.logger.info("  3.1: Auto-loading base tables")
        # Auto-load base tables if not loaded
        if self.patient is None:
            self.logger.info("       - Loading patient table")
            self.load_table('patient')  # Patient doesn't need filters
        if self.hospitalization is None:
            self.logger.info("       - Loading hospitalization table")
            self.load_table('hospitalization', filters=filters)
        if self.adt is None:
            self.logger.info("       - Loading adt table")
            self.load_table('adt', filters=filters)

        # Load optional tables only if not already loaded
        self.logger.info(f"  3.2: Loading optional tables: {tables_to_load or 'None'}")
        if tables_to_load:
            for table_name in tables_to_load:
                if getattr(self, table_name, None) is None:
                    self.logger.info(f"       - Loading {table_name} table")
                    try:
                        self.load_table(table_name)
                    except Exception as e:
                        self.logger.warning(f"       - Could not load {table_name}: {e}")
                else:
                    self.logger.debug(f"       - {table_name} table already loaded")

        # Check if patient_assessments needs assessment_value column
        if (tables_to_load and 'patient_assessments' in tables_to_load) or \
           (category_filters and 'patient_assessments' in category_filters):
            if self.patient_assessments is not None and hasattr(self.patient_assessments, 'df'):
                df = self.patient_assessments.df
                if 'numerical_value' in df.columns and 'categorical_value' in df.columns:
                    if 'assessment_value' not in df.columns:

                        self.logger.info("  === SPECIAL: PATIENT ASSESSMENTS PROCESSING ===")
                        self.logger.info("       - Merging numerical_value and categorical_value columns")
                        try:
                            import polars as pl
                            self.logger.debug("       - Using Polars for performance optimization")

                            # Convert to Polars for efficient processing
                            df_pl = pl.from_pandas(df)

                            # Check data integrity using Polars
                            both_filled = df_pl.filter(
                                (pl.col('numerical_value').is_not_null()) &
                                (pl.col('categorical_value').is_not_null())
                            )
                            both_filled_count = len(both_filled)

                            if both_filled_count > 0:
                                self.logger.warning(f"       - Found {both_filled_count} rows with both numerical and categorical values - numerical values will take precedence")

                            # Create assessment_value using Polars coalesce (much faster than pandas fillna)
                            df_pl = df_pl.with_columns(
                                pl.coalesce([
                                    pl.col('numerical_value'),
                                    pl.col('categorical_value')
                                ]).cast(pl.Utf8).alias('assessment_value')
                            )

                            # Calculate statistics efficiently with Polars
                            num_count = df_pl.select(pl.col('numerical_value').is_not_null().sum()).item()
                            cat_count = df_pl.select(pl.col('categorical_value').is_not_null().sum()).item()
                            total_count = df_pl.select(pl.col('assessment_value').is_not_null().sum()).item()

                            # Convert back to pandas for compatibility
                            self.patient_assessments.df = df_pl.to_pandas()

                            self.logger.info(f"       - Created assessment_value column: {num_count} numerical, {cat_count} categorical, {total_count} total non-null")
                            self.logger.debug(f"       -   Stored as string type for processing compatibility")

                        except ImportError:
                            self.logger.warning("       - Polars not installed. Using pandas (slower)")
                            # Fallback to pandas
                            both_filled = df[(df['numerical_value'].notna()) &
                                            (df['categorical_value'].notna())]
                            if len(both_filled) > 0:
                                self.logger.warning(f"       - Found {len(both_filled)} rows with both numerical and categorical values")

                            df['assessment_value'] = df['numerical_value'].fillna(df['categorical_value'])
                            df['assessment_value'] = df['assessment_value'].astype(str)

                            num_count = df['numerical_value'].notna().sum()
                            cat_count = df['categorical_value'].notna().sum()
                            total_count = df['assessment_value'].notna().sum()

                            self.logger.info(f"       - Created assessment_value column: {num_count} numerical, {cat_count} categorical, {total_count} total non-null")

        self.logger.info("Phase 4: Calling Wide Dataset Utility")

        self.logger.debug(f"  4.1: Passing to wide_dataset.create_wide_dataset()")
        self.logger.debug(f"       - Tables: {tables_to_load or 'None'}")
        self.logger.debug(f"       - Category filters: {list(category_filters.keys()) if category_filters else 'None'}")
        self.logger.debug(f"       - Batch size: {batch_size}")
        self.logger.debug(f"       - Memory limit: {memory_limit}")
        self.logger.debug(f"       - Show progress: {show_progress}")

        # Call utility function with self as clif_instance and store result in wide_df property
        self.wide_df = _create_wide(
            clif_instance=self,
            optional_tables=tables_to_load,
            category_filters=category_filters,
            sample=sample,
            hospitalization_ids=hospitalization_ids,
            cohort_df=cohort_df,
            output_format=output_format,
            save_to_data_location=save_to_data_location,
            output_filename=output_filename,
            return_dataframe=return_dataframe,
            batch_size=batch_size,
            memory_limit=memory_limit,
            threads=threads,
            show_progress=show_progress
        )

        self.logger.info("Phase 5: Post-Processing")

        # Add encounter_block column if encounter mapping exists and not already present
        if self.encounter_mapping is not None and self.wide_df is not None:

            self.logger.info("  5.1: === SPECIAL: ADDING ENCOUNTER BLOCKS ===")
            if 'encounter_block' not in self.wide_df.columns:
                self.logger.info("       - Adding encounter_block column from encounter mapping")
                self.wide_df = pd.merge(
                    self.wide_df,
                    self.encounter_mapping[['hospitalization_id', 'encounter_block']],
                    on='hospitalization_id',
                    how='left'
                )
                self.logger.info(f"       - Added encounter_block column - {self.wide_df['encounter_block'].nunique()} unique encounter blocks")
            else:
                self.logger.debug(f"       - Encounter_block column already present - {self.wide_df['encounter_block'].nunique()} unique encounter blocks")
        else:
            self.logger.debug("  5.1: No encounter block mapping to add")

        # Optimize data types for assessment columns using Polars for performance
        if self.wide_df is not None and ((tables_to_load and 'patient_assessments' in tables_to_load) or \
           (category_filters and 'patient_assessments' in category_filters)):

            self.logger.info("  5.2: === SPECIAL: ASSESSMENT TYPE OPTIMIZATION ===")
            try:
                import polars as pl
                self.logger.debug("       - Using Polars for performance optimization")

                # Determine which assessment columns to check
                assessment_columns = []

                if category_filters and 'patient_assessments' in category_filters:
                    # If specific categories were requested, use those
                    assessment_columns = [col for col in category_filters['patient_assessments']
                                         if col in self.wide_df.columns]
                else:
                    # Get all possible assessment categories from the schema
                    if self.patient_assessments and hasattr(self.patient_assessments, 'schema'):
                        schema = self.patient_assessments.schema
                        if 'columns' in schema:
                            for col_def in schema['columns']:
                                if col_def.get('name') == 'assessment_category':
                                    assessment_columns = col_def.get('permissible_values', [])
                                    break

                    # Filter to only columns that exist in wide_df
                    assessment_columns = [col for col in assessment_columns if col in self.wide_df.columns]

                if assessment_columns:
                    self.logger.info(f"       - Analyzing {len(assessment_columns)} assessment columns")

                    # Convert to Polars for efficient processing
                    df_pl = pl.from_pandas(self.wide_df)

                    numeric_conversions = []
                    string_kept = []

                    # Process all columns in one go for better performance
                    for col in assessment_columns:
                        try:
                            # Create a temporary column with numeric conversion attempt
                            temp_col = f"{col}_numeric_test"
                            df_pl = df_pl.with_columns(
                                pl.col(col).cast(pl.Float64, strict=False).alias(temp_col)
                            )

                            # Check conversion success rate
                            stats = df_pl.select([
                                pl.col(col).is_not_null().sum().alias('original_count'),
                                pl.col(temp_col).is_not_null().sum().alias('converted_count')
                            ]).row(0)

                            if stats[0] > 0:  # If there are non-null values
                                conversion_rate = stats[1] / stats[0]

                                if conversion_rate >= 0.95:  # 95% or more are numeric
                                    # Replace original with converted
                                    df_pl = df_pl.drop(col).rename({temp_col: col})
                                    numeric_conversions.append(col)
                                else:
                                    # Keep original, drop temp
                                    df_pl = df_pl.drop(temp_col)
                                    string_kept.append(col)
                            else:
                                # No data, just drop temp
                                df_pl = df_pl.drop(temp_col)

                        except Exception:
                            # Keep as string if any error, clean up temp column if it exists
                            if f"{col}_numeric_test" in df_pl.columns:
                                df_pl = df_pl.drop(f"{col}_numeric_test")
                            string_kept.append(col)

                    # Convert back to pandas
                    self.wide_df = df_pl.to_pandas()

                    # Report conversions
                    if numeric_conversions:
                        self.logger.info(f"       - Converted to numeric: {len(numeric_conversions)} columns")
                        self.logger.debug(f"       -   Examples: {', '.join(numeric_conversions[:5])}")
                        if len(numeric_conversions) > 5:
                            self.logger.debug(f"       -   ... and {len(numeric_conversions) - 5} more")

                    if string_kept:
                        self.logger.info(f"       - Kept as string: {len(string_kept)} columns with mixed/text values")
                        self.logger.debug(f"       -   Examples: {', '.join(string_kept[:5])}")
                        if len(string_kept) > 5:
                            self.logger.debug(f"       -   ... and {len(string_kept) - 5} more")
                else:
                    self.logger.debug("       - No assessment columns found to optimize")

            except ImportError:
                self.logger.warning("       - Polars not installed. Skipping type optimization")
                self.logger.info("       - Install polars for better performance: pip install polars")
        else:
            self.logger.debug("  5.2: No assessment type optimization needed")

        self.logger.info("Phase 6: Completion")

        if self.wide_df is not None:
            # Convert encounter_block to int32 if it exists (memory optimization)
            if 'encounter_block' in self.wide_df.columns:
                self.wide_df['encounter_block'] = self.wide_df['encounter_block'].astype('Int32')

            self.logger.info(f"  6.1: Wide dataset stored in self.wide_df")
            self.logger.info(f"  6.2: Dataset shape: {self.wide_df.shape[0]} rows x {self.wide_df.shape[1]} columns")
        else:
            self.logger.warning("  6.1: No wide dataset was created")

        self.logger.info("=" * 50)
        self.logger.info("✅ WIDE DATASET CREATION COMPLETED")
        self.logger.info("=" * 50)

    def convert_wide_to_hourly(
        self,
        aggregation_config: Dict[str, List[str]],
        wide_df: Optional[pd.DataFrame] = None,
        id_name: str = 'hospitalization_id',
        hourly_window: int = 1,
        fill_gaps: bool = False,
        memory_limit: str = '4GB',
        temp_directory: Optional[str] = None,
        batch_size: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Convert wide dataset to temporal aggregation using DuckDB with event-based windowing.

        Parameters
        ----------
        aggregation_config : Dict[str, List[str]]
            Dict mapping aggregation methods to columns
            Example: {
                'mean': ['heart_rate', 'sbp'],
                'max': ['spo2'],
                'min': ['map'],
                'median': ['glucose'],
                'first': ['gcs_total'],
                'last': ['assessment_value'],
                'boolean': ['norepinephrine'],
                'one_hot_encode': ['device_category']
            }
        wide_df : pd.DataFrame, optional
            Wide dataset DataFrame. If None, uses the stored wide_df from create_wide_dataset()
        id_name : str, default='hospitalization_id'
            Column name to use for grouping aggregation. Options:
            - 'hospitalization_id': Group by individual hospitalizations (default)
            - 'encounter_block': Group by encounter blocks (after encounter stitching)
            - Any other ID column present in the wide dataset
        hourly_window : int, default=1
            Aggregation window size in hours (1-72). Windows start from each
            group's first event, not calendar boundaries.
        fill_gaps : bool, default=False
            Create rows for windows with no data (filled with NaN).
            False = sparse output (current behavior), True = dense output.
        memory_limit : str, default='4GB'
            DuckDB memory limit (e.g., '4GB', '8GB')
        temp_directory : str, optional
            Directory for DuckDB temp files
        batch_size : int, optional
            Process in batches if specified

        Returns
        -------
        pd.DataFrame
            Aggregated DataFrame with columns:
            - window_number: Sequential window index (0-indexed)
            - window_start_dttm: Window start timestamp
            - window_end_dttm: Window end timestamp
            - All aggregated columns per config

        Examples
        --------
        Standard hourly with sparse output (only windows with data)::

            co.create_wide_dataset(...)
            hourly_df = co.convert_wide_to_hourly(
                aggregation_config=config,
                hourly_window=1,
                fill_gaps=False
            )

        6-hour windows with gap filling (all windows 0 to max)::

            hourly_df = co.convert_wide_to_hourly(
                aggregation_config=config,
                hourly_window=6,
                fill_gaps=True
            )
            # If data exists at windows 0, 1, 5:
            # - fill_gaps=False creates 3 rows (0, 1, 5)
            # - fill_gaps=True creates 6 rows (0, 1, 2, 3, 4, 5) with NaN in 2-4

        Using encounter blocks after stitching::

            co.run_stitch_encounters()
            co.create_wide_dataset(...)
            hourly_df = co.convert_wide_to_hourly(
                aggregation_config=config,
                id_name='encounter_block',
                hourly_window=12
            )

        Using explicit wide_df parameter::

            hourly_df = co.convert_wide_to_hourly(
                wide_df=my_df,
                aggregation_config=config,
                hourly_window=24
            )
        """
        from clifpy.utils.wide_dataset import convert_wide_to_hourly

        # Use provided wide_df or fall back to stored one
        if wide_df is None:
            if self.wide_df is None:
                raise ValueError(
                    "No wide dataset found. Please either:\n"
                    "1. Run create_wide_dataset() first, OR\n"
                    "2. Provide a wide_df parameter"
                )
            wide_df = self.wide_df

        return convert_wide_to_hourly(
            wide_df=wide_df,
            aggregation_config=aggregation_config,
            id_name=id_name,
            hourly_window=hourly_window,
            fill_gaps=fill_gaps,
            memory_limit=memory_limit,
            temp_directory=temp_directory,
            batch_size=batch_size
        )
    
    def get_sys_resource_info(self, print_summary: bool = True) -> Dict[str, Any]:
        """
        Get system resource information including CPU, memory, and practical thread limits.
        
        Parameters
        ----------
        print_summary : bool, default=True
            Whether to print a formatted summary
            
        Returns
        -------
        Dict[str, Any]
            Dictionary containing system resource information:
            
            - cpu_count_physical: Number of physical CPU cores
            - cpu_count_logical: Number of logical CPU cores
            - cpu_usage_percent: Current CPU usage percentage
            - memory_total_gb: Total RAM in GB
            - memory_available_gb: Available RAM in GB
            - memory_used_gb: Used RAM in GB
            - memory_usage_percent: Memory usage percentage
            - process_threads: Number of threads used by current process
            - max_recommended_threads: Recommended max threads for optimal performance
        """
        # Get current process
        current_process = psutil.Process()
        
        # CPU information
        cpu_count_physical = psutil.cpu_count(logical=False)
        cpu_count_logical = psutil.cpu_count(logical=True)
        cpu_usage_percent = psutil.cpu_percent(interval=1)
        
        # Memory information
        memory = psutil.virtual_memory()
        memory_total_gb = memory.total / (1024**3)
        memory_available_gb = memory.available / (1024**3)
        memory_used_gb = memory.used / (1024**3)
        memory_usage_percent = memory.percent
        
        # Thread information
        process_threads = current_process.num_threads()
        max_recommended_threads = cpu_count_physical  # Conservative recommendation
        
        resource_info = {
            'cpu_count_physical': cpu_count_physical,
            'cpu_count_logical': cpu_count_logical,
            'cpu_usage_percent': cpu_usage_percent,
            'memory_total_gb': memory_total_gb,
            'memory_available_gb': memory_available_gb,
            'memory_used_gb': memory_used_gb,
            'memory_usage_percent': memory_usage_percent,
            'process_threads': process_threads,
            'max_recommended_threads': max_recommended_threads
        }
        
        if print_summary:
            self.logger.info("=" * 50)
            self.logger.info("SYSTEM RESOURCES")
            self.logger.info("=" * 50)
            self.logger.info(f"CPU Cores (Physical): {cpu_count_physical}")
            self.logger.info(f"CPU Cores (Logical):  {cpu_count_logical}")
            self.logger.info(f"CPU Usage:            {cpu_usage_percent:.1f}%")
            self.logger.info("-" * 50)
            self.logger.info(f"Total RAM:            {memory_total_gb:.1f} GB")
            self.logger.info(f"Available RAM:        {memory_available_gb:.1f} GB")
            self.logger.info(f"Used RAM:             {memory_used_gb:.1f} GB")
            self.logger.info(f"Memory Usage:         {memory_usage_percent:.1f}%")
            self.logger.info("-" * 50)
            self.logger.info(f"Process Threads:      {process_threads}")
            self.logger.info(f"Max Recommended:      {max_recommended_threads} threads")
            self.logger.info("-" * 50)
            self.logger.info(f"RECOMMENDATION: Use {max(1, cpu_count_physical-2)}-{cpu_count_physical} threads for optimal performance")
            self.logger.info(f"(Based on {cpu_count_physical} physical CPU cores)")
            self.logger.info("=" * 50)
        
        return resource_info

    def convert_dose_units_for_continuous_meds(
        self,
        preferred_units: Dict[str, str],
        vitals_df: pd.DataFrame = None,
        hospitalization_ids: Optional[List[str]] = None,
        show_intermediate: bool = False,
        override: bool = False,
        save_to_table: bool = True
    ) -> Optional[Tuple[pd.DataFrame, pd.DataFrame]]:
        """
        Convert dose units for continuous medication data.

        Parameters
        ----------
        preferred_units : Dict[str, str]
            Dict of preferred units for each medication category
        vitals_df : pd.DataFrame, optional
            Vitals DataFrame for extracting patient weights
        hospitalization_ids : List[str], optional
            List of specific hospitalization IDs to filter and process. When provided,
            only medication and vitals data for these hospitalizations will be loaded
            and processed, improving performance for targeted analyses.
        show_intermediate : bool, default=False
            If True, includes intermediate calculation columns in output
        override : bool, default=False
            If True, continues processing with warnings for unacceptable units
        save_to_table : bool, default=True
            If True, saves the converted DataFrame to the table's df_converted
            property and stores conversion_counts as a table property. If False,
            returns the converted data without updating the table.

        Returns
        -------
        Tuple[pd.DataFrame, pd.DataFrame] or None
            (converted_df, counts_df) when save_to_table=False, None otherwise
        """
        from .utils.unit_converter import convert_dose_units_by_med_category

        # Log function entry with parameters
        self.logger.info(f"Starting dose unit conversion for continuous medications with parameters: "
                        f"preferred_units={preferred_units}, hospitalization_ids={'provided' if hospitalization_ids else 'None'}, "
                        f"show_intermediate={show_intermediate}, override={override}, save_to_table={save_to_table}")

        # Load medication table with optional hospitalization_ids filter
        if self.medication_admin_continuous is None:
            self.logger.info("Loading medication_admin_continuous table...")
            if hospitalization_ids is not None:
                self.logger.info(f"Filtering for {len(hospitalization_ids)} hospitalization(s)")
                self.load_table('medication_admin_continuous', filters={'hospitalization_id': hospitalization_ids})
            else:
                self.load_table('medication_admin_continuous')
            self.logger.debug("medication_admin_continuous table loaded successfully")

        # Determine hospitalization_ids for vitals loading if not provided
        if hospitalization_ids is None and self.medication_admin_continuous is not None:
            hospitalization_ids = self.medication_admin_continuous.df['hospitalization_id'].unique().tolist()
            self.logger.debug(f"Extracted {len(hospitalization_ids)} unique hospitalization_id(s) from medication data")

        # Load vitals df with filters for weight_kg only
        if vitals_df is None:
            self.logger.debug("No vitals_df provided, loading filtered vitals table")
            if (self.vitals is None) or (self.vitals.df is None):
                self.logger.info(f"Loading vitals table for {len(hospitalization_ids)} hospitalization(s), vital_category='weight_kg'")
                self.load_table('vitals', filters={'hospitalization_id': hospitalization_ids, 'vital_category': ['weight_kg']})
            vitals_df = self.vitals.df
            self.logger.debug(f"Using vitals data with shape: {vitals_df.shape}")
        else:
            self.logger.debug(f"Using provided vitals_df with shape: {vitals_df.shape}")

        # Call the conversion function with all parameters
        self.logger.info("Starting dose unit conversion")
        self.logger.debug(f"Input DataFrame shape: {self.medication_admin_continuous.df.shape}")

        converted_df, counts_df = convert_dose_units_by_med_category(
            self.medication_admin_continuous.df,
            vitals_df=vitals_df,
            preferred_units=preferred_units,
            show_intermediate=show_intermediate,
            override=override
        )

        self.logger.info("Dose unit conversion completed")
        self.logger.debug(f"Output DataFrame shape: {converted_df.shape}")
        self.logger.debug(f"Conversion counts summary: {len(counts_df)} conversions tracked")

        # If overwrite_raw_df is True, update the table's df and store conversion_counts
        if save_to_table:
            self.logger.info("Updating medication_admin_continuous table with converted data")
            self.medication_admin_continuous.df_converted = converted_df
            self.medication_admin_continuous.conversion_counts = counts_df
            self.logger.debug("Conversion counts stored as table property")
        else:
            self.logger.info("Returning converted data without updating table")
            return converted_df, counts_df
        
    def convert_dose_units_for_intermittent_meds(
        self,
        preferred_units: Dict[str, str],
        vitals_df: pd.DataFrame = None,
        hospitalization_ids: Optional[List[str]] = None,
        show_intermediate: bool = False,
        override: bool = False,
        save_to_table: bool = True
    ) -> Optional[Tuple[pd.DataFrame, pd.DataFrame]]:
        """
        Convert dose units for intermittent medication data.

        Parameters
        ----------
        preferred_units : Dict[str, str]
            Dict of preferred units for each medication category
        vitals_df : pd.DataFrame, optional
            Vitals DataFrame for extracting patient weights
        hospitalization_ids : List[str], optional
            List of specific hospitalization IDs to filter and process. When provided,
            only medication and vitals data for these hospitalizations will be loaded
            and processed, improving performance for targeted analyses.
        show_intermediate : bool, default=False
            If True, includes intermediate calculation columns in output
        override : bool, default=False
            If True, continues processing with warnings for unacceptable units
        save_to_table : bool, default=True
            If True, saves the converted DataFrame to the table's df_converted
            property and stores conversion_counts as a table property. If False,
            returns the converted data without updating the table.

        Returns
        -------
        Tuple[pd.DataFrame, pd.DataFrame] or None
            (converted_df, counts_df) when save_to_table=False, None otherwise
        """
        from .utils.unit_converter import convert_dose_units_by_med_category

        # Log function entry with parameters
        self.logger.info(f"Starting dose unit conversion for intermittent medications with parameters: "
                        f"preferred_units={preferred_units}, hospitalization_ids={'provided' if hospitalization_ids else 'None'}, "
                        f"show_intermediate={show_intermediate}, override={override}, save_to_table={save_to_table}")

        # Load medication table with optional hospitalization_ids filter
        if self.medication_admin_intermittent is None:
            self.logger.info("Loading medication_admin_intermittent table...")
            if hospitalization_ids is not None:
                self.logger.info(f"Filtering for {len(hospitalization_ids)} hospitalization(s)")
                self.load_table('medication_admin_intermittent', filters={'hospitalization_id': hospitalization_ids})
            else:
                self.load_table('medication_admin_intermittent')
            self.logger.debug("medication_admin_intermittent table loaded successfully")

        # Determine hospitalization_ids for vitals loading if not provided
        if hospitalization_ids is None:
            hospitalization_ids = self.medication_admin_intermittent.df['hospitalization_id'].unique().tolist()
            self.logger.debug(f"Extracted {len(hospitalization_ids)} unique hospitalization_id(s) from medication data")

        # Load vitals df with filters for weight_kg only
        if vitals_df is None:
            self.logger.debug("No vitals_df provided, loading filtered vitals table")
            if (self.vitals is None) or (self.vitals.df is None):
                self.logger.info(f"Loading vitals table for {len(hospitalization_ids)} hospitalization(s), vital_category='weight_kg'")
                self.load_table('vitals', filters={'hospitalization_id': hospitalization_ids, 'vital_category': ['weight_kg']})
            vitals_df = self.vitals.df
            self.logger.debug(f"Using vitals data with shape: {vitals_df.shape}")
        else:
            self.logger.debug(f"Using provided vitals_df with shape: {vitals_df.shape}")

        # Call the conversion function with all parameters
        self.logger.info("Starting dose unit conversion")
        self.logger.debug(f"Input DataFrame shape: {self.medication_admin_intermittent.df.shape}")

        converted_df, counts_df = convert_dose_units_by_med_category(
            self.medication_admin_intermittent.df,
            vitals_df=vitals_df,
            preferred_units=preferred_units,
            show_intermediate=show_intermediate,
            override=override
        )

        self.logger.info("Dose unit conversion completed")
        self.logger.debug(f"Output DataFrame shape: {converted_df.shape}")
        self.logger.debug(f"Conversion counts summary: {len(counts_df)} conversions tracked")

        # If save_to_table is True, update the table's df_converted and store conversion_counts
        if save_to_table:
            self.logger.info("Updating medication_admin_intermittent table with converted data")
            self.medication_admin_intermittent.df_converted = converted_df
            self.medication_admin_intermittent.conversion_counts = counts_df
            self.logger.debug("Conversion counts stored as table property")
        else:
            self.logger.info("Returning converted data without updating table")
            return converted_df, counts_df

    def compute_sofa_scores(
        self,
        wide_df: Optional[pd.DataFrame] = None,
        cohort_df: Optional[pd.DataFrame] = None,
        extremal_type: str = 'worst',
        id_name: str = 'encounter_block',
        fill_na_scores_with_zero: bool = True,
        remove_outliers: bool = True,
        create_new_wide_df: bool = True
    ) -> pd.DataFrame:
        """
        Compute SOFA (Sequential Organ Failure Assessment) scores.

        Parameters:
            wide_df: Optional wide dataset. If not provided, uses self.wide_df or creates one
            cohort_df: Optional DataFrame with columns [id_name, 'start_time', 'end_time']
                      to further filter observations by time windows
            extremal_type: 'worst' (default) or 'latest' (future feature)
            id_name: Column name for grouping (default: 'encounter_block')
                    - 'encounter_block': Groups related hospitalizations (requires encounter stitching)
                    - 'hospitalization_id': Individual hospitalizations
            fill_na_scores_with_zero: If True, missing component scores default to 0
            remove_outliers: If True, overwrite the df of the table object associated with the orchestrator with outliers nullified
            create_new_wide_df: If True, create a new wide dataset for SOFA computation and save it at .wide_df_sofa. 
                If False, use the existing .wide_df.

        Returns:
            DataFrame with SOFA component scores and total score for each ID.
            Results are stored in self.sofa_df.

        Notes:
            - Medication units should be pre-converted (e.g., 'norepinephrine_mcg_kg_min')
            - When id_name='encounter_block' and encounter mapping doesn't exist,
              it will be created automatically via run_stitch_encounters()
            - Missing data defaults to score of 0 (normal organ function)

        Examples:
            Basic usage:
            >>> co = ClifOrchestrator(config_path='config/config.yaml')
            >>> sofa_scores = co.compute_sofa_scores()

            Per hospitalization instead of encounter:
            >>> sofa_scores = co.compute_sofa_scores(id_name='hospitalization_id')

            With time filtering:
            >>> cohort_df = pd.DataFrame({
            ...     'encounter_block': ['E001', 'E002'],
            ...     'start_time': pd.to_datetime(['2024-01-01', '2024-01-02']),
            ...     'end_time': pd.to_datetime(['2024-01-03', '2024-01-04'])
            ... })
            >>> sofa_scores = co.compute_sofa_scores(cohort_df=cohort_df)
        """
        from .utils.sofa import compute_sofa, REQUIRED_SOFA_CATEGORIES_BY_TABLE

        self.logger.info(f"Computing SOFA scores with extremal_type='{extremal_type}', id_name='{id_name}'")
        
        if (cohort_df is not None) and (id_name not in cohort_df.columns):
            raise ValueError(f"id_name '{id_name}' not found in cohort_df columns")
        
        # Determine which wide_df to use
        if wide_df is not None:
            self.logger.debug("Using provided wide_df")
            df = wide_df
        elif create_new_wide_df:
            self.logger.info("Ignoring any existing .wide_df and creating a new wide dataset for SOFA computation")
            df = self.create_wide_dataset(
                tables_to_load=list(REQUIRED_SOFA_CATEGORIES_BY_TABLE.keys()),
                category_filters=REQUIRED_SOFA_CATEGORIES_BY_TABLE,
                cohort_df=cohort_df,
                return_dataframe=True
            )
            df = self.wide_df if df is None else df
            self.wide_df_sofa = df
        elif hasattr(self, 'wide_df') and self.wide_df is not None:
            self.logger.debug("Using existing self.wide_df")
            df = self.wide_df
        else:
            self.logger.info("No wide dataset available, creating one...")
            # Create wide dataset with required categories for SOFA
            
            self.create_wide_dataset(
                tables_to_load=list(REQUIRED_SOFA_CATEGORIES_BY_TABLE.keys()),
                category_filters=REQUIRED_SOFA_CATEGORIES_BY_TABLE,
                cohort_df=cohort_df
            )
            df = self.wide_df
            self.logger.debug(f"Created wide dataset with shape: {df.shape}")

        if id_name not in df.columns:
            if self.encounter_mapping is None:
                self.logger.info("Encounter mapping not found, running stitch_encounters()")
                try:
                    self.run_stitch_encounters()
                except Exception as e:
                    self.logger.error(f"Error during encounter stitching: {e}")
                    raise ValueError("Encounter stitching failed. Please run stitch_encounters() manually.")
            df = df.merge(self.encounter_mapping, on='hospitalization_id', how='left')
            self.wide_df = df
            self.logger.debug(f"Mapped {id_name} to wide_df via encounter_mapping, with shape: {df.shape}")
            
        # Compute SOFA scores
        self.logger.debug("Calling compute_sofa function")
        sofa_scores = compute_sofa(
            wide_df=df,
            cohort_df=cohort_df,
            extremal_type=extremal_type,
            id_name=id_name,
            fill_na_scores_with_zero=fill_na_scores_with_zero,
            remove_outliers=remove_outliers
        )

        # Store results in orchestrator
        self.sofa_df = sofa_scores
        self.logger.info(f"SOFA computation completed. Results stored in self.sofa_df with shape: {sofa_scores.shape}")

        return sofa_scores
