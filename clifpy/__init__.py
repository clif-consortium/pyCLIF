from . import data
# Re-export table classes at package root
from .tables import (
    Patient,
    Adt,
    Hospitalization,
    HospitalDiagnosis,
    Labs,
    RespiratorySupport,
    Vitals,
    MedicationAdminContinuous,
    MedicationAdminIntermittent,
    PatientAssessments,
    Position,
    MicrobiologyCulture,
    CrrtTherapy,
    PatientProcedures,
    MicrobiologySusceptibility,
    EcmoMcs,
    MicrobiologyNonculture,
    CodeStatus,
    # CLIF 3.0 tables (mcs replaces ecmo_mcs)
    Mcs,
    RenalReplacementTherapy,
    Input,
    Output,
    InvasiveHemodynamics,
    ConsultOrders,
    MiscIcuOrders,
    MedicationOrders,
    PatientDiagnosis,
    PlaceBasedIndex,
    Provider,
    TherapyDetails,
    Transfusion,
    ClinicalTrial,
    ClinicalNotesFacts,
    ValidatedDiagnosis,
    ModelRegistry,
    Scores,
    Radiology,
    Line,
    Drain,
    Airway,
    PatientAttributes,
    EdEncounter,
)
# Re-export ClifOrchestrator at package root
from .clif_orchestrator import ClifOrchestrator

# Re-export commonly used utility functions at package root
from .utils.stitching_encounters import stitch_encounters
from .utils.waterfall import process_resp_support_waterfall
from .utils.wide_dataset import create_wide_dataset, convert_wide_to_hourly
from .utils.comorbidity import calculate_cci
from .utils.outlier_handler import apply_outlier_handling, get_outlier_summary
from .utils.config import load_config
from .utils.io import load_data
from .utils.logging_config import setup_logging, get_logger
from .utils.crosswalk import (
    crosswalk_table_2_1_to_3_0,
    crosswalk_file_2_1_to_3_0,
    normalize_category_value,
    BETA_TABLES,
)

# SOFA-2 scoring
from .utils.sofa2 import calculate_sofa2, calculate_sofa2_daily, SOFA2Config
from .utils._duckdb_config import DuckDBResourceConfig
# Re-export Polars-based utilities at package root
from .utils.sofa_polars import compute_sofa_polars
from .utils.datetime_polars import (
    standardize_datetime_columns as standardize_datetime_columns_polars,
    ensure_datetime_precision_match as ensure_datetime_precision_match_polars,
    convert_datetime_columns_to_site_tz as convert_datetime_columns_to_site_tz_polars,
)
from .utils.io_polars import load_data_polars, load_clif_table_polars

# Version info
try:
    from importlib.metadata import version
    __version__ = version("clifpy")
except Exception:
    __version__ = "unknown"

# Public API
__all__ = [
    # Data module
    "data",
    # Table classes
    "Patient",
    "Adt",
    "Hospitalization",
    "HospitalDiagnosis",
    "Labs",
    "RespiratorySupport",
    "Vitals",
    "MedicationAdminContinuous",
    "MedicationAdminIntermittent",
    "PatientAssessments",
    "Position",
    "MicrobiologyCulture",
    "CrrtTherapy",
    "PatientProcedures",
    "MicrobiologySusceptibility",
    "EcmoMcs",
    "MicrobiologyNonculture",
    "CodeStatus",
    # CLIF 3.0 tables (mcs replaces ecmo_mcs)
    "Mcs",
    "RenalReplacementTherapy",
    "Input",
    "Output",
    "InvasiveHemodynamics",
    "ConsultOrders",
    "MiscIcuOrders",
    "MedicationOrders",
    "PatientDiagnosis",
    "PlaceBasedIndex",
    "Provider",
    "TherapyDetails",
    "Transfusion",
    "ClinicalTrial",
    "ClinicalNotesFacts",
    "ValidatedDiagnosis",
    "ModelRegistry",
    "Scores",
    "Radiology",
    "Line",
    "Drain",
    "Airway",
    "PatientAttributes",
    "EdEncounter",
    # Orchestrator
    "ClifOrchestrator",
    # Utility functions
    "stitch_encounters",
    "process_resp_support_waterfall",
    "create_wide_dataset",
    "convert_wide_to_hourly",
    "calculate_cci",
    "apply_outlier_handling",
    "get_outlier_summary",
    "load_config",
    "load_data",
    "setup_logging",
    "get_logger",
    # SOFA-2 scoring
    "calculate_sofa2",
    "calculate_sofa2_daily",
    "SOFA2Config",
    # DuckDB resource config
    "DuckDBResourceConfig",
    "crosswalk_table_2_1_to_3_0",
    "crosswalk_file_2_1_to_3_0",
    "normalize_category_value",
    "BETA_TABLES",
    # Polars-based utilities
    "compute_sofa_polars",
    "standardize_datetime_columns_polars",
    "ensure_datetime_precision_match_polars",
    "convert_datetime_columns_to_site_tz_polars",
    "load_data_polars",
    "load_clif_table_polars",
]