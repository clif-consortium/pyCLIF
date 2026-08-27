"""
Data Quality Assessment (DQA) module for CLIFpy tables.

This module provides comprehensive DQA functions implementing:

CONFORMANCE CHECKS:
- A.1. Table presence verification
- A.2. Required columns presence check
- B.1. Data type validation
- B.2. Datetime format validation
- B.4. Categorical values validation against mCIDE
- B.5. Category-to-group mapping validation

COMPLETENESS CHECKS:
- A.1. Missingness analysis for required columns
- A.2. Conditional required fields validation
- B. mCIDE value coverage checks
- C.1. Relational integrity checks

DESIGN PRINCIPLES:
- Checks run on Polars (memory-efficient, lazy evaluation). Polars is a hard
  dependency of clifpy -- ``clifpy.utils.io`` imports it unconditionally.
- pandas input is still accepted and converted on the way in, so callers holding a
  ``pd.DataFrame`` (e.g. ``BaseTable.df``) need not change.
- Uses garbage collection and cache clearing for memory management
"""
import os
import yaml
from typing import List, Dict, Any, Optional, Union
import pandas as pd
import polars as pl
import logging
from pathlib import Path
import gc

from ..schemas import DEFAULT_CLIF_VERSION, load_schema

# Logger for this module
_logger = logging.getLogger(__name__)

# Default plausibility thresholds: {check_name: {error_threshold, warning_threshold}}
# - warning_threshold: percent above which a warning is raised
# - error_threshold: percent above which an error is raised
_DEFAULT_PLAUSIBILITY_THRESHOLDS: Dict[str, Dict[str, float]] = {
    "chronological_order": {"error_threshold": 10.0, "warning_threshold": 0.0},
    "numeric_range_plausibility": {"error_threshold": 10.0, "warning_threshold": 0.0},
    "medication_dose_unit_consistency": {"error_threshold": 10.0, "warning_threshold": 0.0},
    "cross_table_temporal": {"error_threshold": 10.0, "warning_threshold": 0.0},
    "duplicate_composite_keys": {"error_threshold": 10.0, "warning_threshold": 0.0},
}


def _load_schema(
    table_name: str,
    schema_dir: Optional[str] = None,
    clif_version: str = DEFAULT_CLIF_VERSION,
) -> Optional[Dict[str, Any]]:
    """Load YAML schema for a table at the given CLIF version.

    If ``schema_dir`` is provided, it is treated as an explicit directory of
    flat ``{table}_schema.yaml`` files (legacy / override behavior). Otherwise
    the version-aware schema registry is used.
    """
    if schema_dir is not None:
        schema_file = os.path.join(schema_dir, f'{table_name}_schema.yaml')
        _logger.debug("Loading schema for '%s' from %s", table_name, schema_file)
        if not os.path.exists(schema_file):
            _logger.warning("Schema file not found: %s", schema_file)
            return None
        with open(schema_file, 'r') as f:
            return yaml.safe_load(f)

    return load_schema(table_name, clif_version)


# Sidecar column prefix used to preserve the pre-normalization (original-case)
# value of every string column during validation. Checks that cite row-level
# string values in error payloads should read from these columns so that
# users see what they actually submitted, not the lowercased form.
_ORIG_PREFIX = "__orig_"


def _normalize_columns_polars(lf: 'pl.LazyFrame') -> 'pl.LazyFrame':
    """Return a polars LazyFrame with lowercased column names and sidecar
    ``__orig_<col>`` columns holding original values for every string column.
    The main string column is lowercased+stripped. Safe for lazy evaluation.
    """
    #STEP-1: lower casing columns
    schema = lf.collect_schema()

    rename_map = {c: c.lower() for c in schema.names() if c != c.lower()}
    if rename_map:
        lf = lf.rename(rename_map)
        schema = pl.Schema({rename_map.get(n, n): dt for n, dt in schema.items()})

    #STEP-2: lowercase and strip chars for string cols
    str_cols = [
        name for name, dtype in schema.items()
        if dtype == pl.String and not name.startswith(_ORIG_PREFIX)
    ]
    if not str_cols:
        return lf

    return lf.with_columns(
        pl.col(str_cols).name.prefix(_ORIG_PREFIX),
        pl.col(str_cols).str.strip_chars().str.to_lowercase(),
    )



def _table_frame(obj) -> Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame']:
    """Return a table object's frame, preferring the polars one.

    ``BaseTable`` stores polars in ``.data`` and exposes ``.df`` as a pandas
    *conversion* that is materialized on first access and then cached on the
    object. Reading ``.df`` here would therefore cost a full pandas copy, a full
    conversion back to polars in ``_normalize_for_validation``, and a permanent
    pandas twin held on the table for the rest of the session -- roughly 2x the
    frame, on the code path documented as the memory-efficient one.

    ``.df`` remains the fallback so objects that only duck-type that attribute
    (and any caller passing a bare pandas frame) keep working unchanged.
    """
    data = getattr(obj, 'data', None)
    if isinstance(data, (pl.DataFrame, pl.LazyFrame)):
        return data
    return getattr(obj, 'df', None)


def _normalize_for_validation(
    df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame']
) -> Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame']:
    """Case-normalize a DataFrame for DQA validation.

    Produces a working copy where column names are lowercased, string values
    are lowercased + stripped, and every string column has a sidecar
    ``__orig_<col>`` preserving the original value for error reporting.

    Safe to call multiple times — if the frame already has sidecars, it is
    returned unchanged.

    Scope: validation only. Callers outside validator.py should NOT use this —
    they keep case-sensitive semantics.
    """
    # pandas is accepted at the door and converted once; everything downstream of
    # here is polars, so there is a single normalization path rather than two that
    # have to be kept in agreement.
    if isinstance(df, pd.DataFrame):
        df = pl.from_pandas(df)
    if isinstance(df, (pl.DataFrame, pl.LazyFrame)):
        lf = df.lazy() if isinstance(df, pl.DataFrame) else df
        if _has_sidecars_polars(lf):
            return lf
        return _normalize_columns_polars(lf)
    # Unknown type — return unchanged; downstream will error with a clearer message.
    return df


def _has_sidecars_polars(lf: 'pl.LazyFrame') -> bool:
    """True iff the polars LazyFrame already carries ``__orig_*`` sidecar columns."""
    return any(c.startswith(_ORIG_PREFIX) for c in lf.collect_schema().names())


def _strip_sidecars(col_names: List[str]) -> List[str]:
    """Drop ``__orig_*`` sidecar columns from a column-name list. Used by
    presence/dtype checks so sidecars don't show up in actual-column counts."""
    return [c for c in col_names if not c.startswith(_ORIG_PREFIX)]


def _count_numeric_range_leaves(table_name: str) -> int:
    """Count atomic (col, [cat], [unit]) range leaves in outlier_config.yaml.

    Mirrors the structure check_numeric_range_plausibility iterates:
    - simple ranges (col → min/max) count 1 leaf per column
    - 1-level category-dependent (col → cat → min/max) count 1 per category
    - 2-level (col → cat → unit → min/max) count 1 per (cat, unit) pair
    """
    table_config = _load_outlier_config().get('tables', {}).get(table_name, {})
    if not table_config:
        return 0

    leaves = 0
    for _col_name, col_ranges in table_config.items():
        if not isinstance(col_ranges, dict):
            continue
        if 'min' in col_ranges and 'max' in col_ranges:
            leaves += 1
            continue
        # Category-dependent: peek at the first value to distinguish 1-level vs 2-level
        for _cat_val, inner in col_ranges.items():
            if not isinstance(inner, dict):
                continue
            if 'min' in inner:
                # 1-level: inner is {min, max}
                leaves += 1
            else:
                # 2-level: inner is {unit: {min, max}}
                for _unit_val, ranges in inner.items():
                    if isinstance(ranges, dict) and 'min' in ranges:
                        leaves += 1
    return leaves


def get_schema_check_counts(
    table_name: str,
    schema_dir: Optional[str] = None,
    clif_version: str = DEFAULT_CLIF_VERSION,
) -> Dict[str, int]:
    """Compute expected atomic DQA check counts from schema + config alone.

    Returns the expected atomic check count per DQA category
    (conformance, completeness, plausibility) for a table, determined
    purely from ``{table}_schema.yaml``, ``validation_rules.yaml``, and
    ``outlier_config.yaml`` without needing any data. This allows
    present and absent tables to report comparable ``N/N`` denominators
    and lets combined reports show ``0/N`` instead of ``N/A`` for
    tables a site did not submit.

    Counts mirror the atomic accounting populated by the DQA check
    functions themselves — e.g. K.3 counts one per permissible value
    per category column, matching what ``check_mcide_value_coverage``
    reports via ``result.atomic_total``.

    Parameters
    ----------
    table_name : str
        Name of the CLIF table (e.g. ``'labs'``, ``'vitals'``).
    schema_dir : str, optional
        Override path to the schemas directory.
    clif_version : str, optional
        CLIF schema version to compute the counts against (e.g. ``"2.1"``,
        ``"3.0"``). Must match the version the data was validated at — the
        counts are denominators, so a mismatch silently skews any score
        computed from them, and a table that only exists in one version
        returns all zeros when asked for the other. Defaults to the package
        default.

    Returns
    -------
    Dict[str, int]
        Keys ``conformance``, ``completeness``, ``plausibility`` each
        mapping to the expected atomic check count for that category.
    """
    schema = _load_schema(table_name, schema_dir, clif_version)
    if schema is None:
        return {"conformance": 0, "completeness": 0, "plausibility": 0}

    columns = schema.get('columns', [])
    category_columns = set(schema.get('category_columns') or [])
    schema_col_names = {c['name'] for c in columns}

    # --- Conformance ---
    conf = 0
    # C.1 table_presence: 1 message
    conf += 1
    # C.2 required_columns: 1 per required column
    conf += len(schema.get('required_columns', []))
    # C.3 column_dtypes: 1 per column with data_type
    conf += sum(1 for c in columns if c.get('data_type'))
    # C.4 datetime_format: 1 per DATETIME or DATE column
    conf += sum(1 for c in columns if c.get('data_type') in ('DATETIME', 'DATE'))
    # C.5 categorical_values: 1 per category column with permissible_values
    conf += sum(
        1 for c in columns
        if c['name'] in category_columns and c.get('permissible_values')
    )
    # C.6 category_group_mapping: 1 per mapping entry (category → group pair).
    # When no mappings exist the check emits a "not applicable" info that
    # is filtered out by enrich_issue(), so it contributes 0 to the score.
    mapping_keys = [k for k in schema if k.endswith('_category_to_group_mapping')]
    for k in mapping_keys:
        mapping = schema.get(k, {})
        if mapping:
            conf += len(mapping)
    # C.7 lab_reference_units (labs only): 1 per lab category with defined units.
    # When absent the "not applicable" info is filtered by enrich_issue().
    if table_name == 'labs':
        lab_units = schema.get('lab_reference_units', {})
        if lab_units:
            conf += len(lab_units)
    # C.8 medication_dose_units (medication admin tables only): 1 per
    # med_category → dose unit mapping entry, plus 1 for the continuous-only
    # volume_infusion_rate_unit sub-check when the schema declares an expected
    # unit. Mirrors check_medication_dose_units, which sets
    # atomic_total = len(mapping) + (1 if the volume sub-check ran).
    # run_conformance_checks runs this check for these two tables, so it has to
    # be counted here or the denominator understates by the whole mapping.
    if table_name in ('medication_admin_continuous', 'medication_admin_intermittent'):
        dose_unit_mapping = schema.get('med_category_to_dose_unit_mapping') or {}
        if dose_unit_mapping:
            conf += len(dose_unit_mapping)
            if (table_name == 'medication_admin_continuous'
                    and schema.get('expected_volume_infusion_rate_unit')):
                conf += 1

    # --- Completeness ---
    comp = 0
    # K.1 missingness: 1 per required column
    comp += len(schema.get('required_columns', []))
    # K.2 conditional_requirements: 1 per rule in validation_rules.yaml
    comp += len(_get_default_conditions(table_name))
    # K.3 mcide_value_coverage: 1 per permissible value across category columns
    for c in columns:
        if c['name'] in category_columns:
            comp += len(c.get('permissible_values') or [])
    # K.4 relational_integrity: 1 per FK column in this table's schema whose
    # reference table is different from this one (self-refs are skipped by
    # the check)
    fk_rules = _load_validation_rules().get('relational_integrity', {})
    for fk_col, rule in fk_rules.items():
        if fk_col in schema_col_names and rule.get('references_table') != table_name:
            comp += 1
    # K.5 cross_table_conditional_completeness: 1 per rule whose target_table
    # is this table. run_cross_table_completeness_checks attaches each result
    # to its target via results.setdefault(target_table, ...)[rule_key] = ...,
    # so K.5 contributes to the target table's completeness score, not the
    # source's. K.5 does not populate atomic_total, so it falls back to
    # message-count in the scorer (one message per invocation).
    ct_cond_rules = _load_validation_rules().get('cross_table_conditional_requirements', []) or []
    for rule in ct_cond_rules:
        if rule.get('target_table') == table_name:
            comp += 1

    # --- Plausibility ---
    plaus = 0
    rules_yaml = _load_validation_rules()
    # P.1 chronological_order: 1 per rule
    plaus += len(rules_yaml.get('chronological_order', {}).get(table_name, []))
    # P.2 numeric_range_plausibility: 1 per leaf (col, [cat], [unit]) tuple
    plaus += _count_numeric_range_leaves(table_name)
    # P.3 field_plausibility: 1 per rule
    plaus += len(rules_yaml.get('field_plausibility_rules', {}).get(table_name, []))
    # P.4 medication_dose_unit_consistency: 1 per medication admin table.
    # Check does not populate atomic_total; each invocation emits exactly one
    # message, which counts as 1 via the report-generator fallback path.
    if table_name in ('medication_admin_continuous', 'medication_admin_intermittent'):
        plaus += 1
    # P.5 overlapping_periods: 1 if defined for table
    if rules_yaml.get('overlapping_periods', {}).get(table_name):
        plaus += 1
    # P.6 category_temporal_consistency: 1 per category column, only when a
    # time column the check would detect appears in this schema
    datetime_cols = [c['name'] for c in columns
                     if c.get('data_type') in ('DATETIME', 'DATE')]
    if _detect_time_column(datetime_cols, table_name) is not None:
        plaus += len(category_columns)
    # P.7 duplicate_composite_keys: 1 if composite_keys defined for table
    ck = rules_yaml.get('composite_keys', {}).get(table_name)
    if ck and ck.get('keys'):
        plaus += 1
    # P.8 cross_table_temporal: 1 per cross-table time column present in this
    # table's schema, for non-hospitalization tables that carry hospitalization_id.
    # Check emits one message per time column; no atomic_total populated.
    if table_name != 'hospitalization' and 'hospitalization_id' in schema_col_names:
        for time_col in _CROSS_TABLE_TIME_COLUMNS.get(table_name, []):
            if time_col in schema_col_names:
                plaus += 1

    return {"conformance": conf, "completeness": comp, "plausibility": plaus}


def build_absent_table_dqa_result(
    table_name: str,
    schema_dir: Optional[str] = None,
    clif_version: str = DEFAULT_CLIF_VERSION,
) -> Dict[str, Any]:
    """Build a :func:`run_full_dqa`-shaped result for a table the site did not submit.

    This is the single source of truth for how clifpy (and downstream
    consumers like CLIF-TableOne) represent an absent table in DQA
    output. Callers that would otherwise pass ``None`` to the combined
    report generators can persist this dict as ``{table}_dqa.json`` and
    render it in per-table views — the combined report helpers also use
    this function so the "0/N" denominators and "Table not present in
    dataset" message come from one place.

    Parameters
    ----------
    table_name : str
        Name of the CLIF table (e.g. ``'microbiology_susceptibility'``).
    schema_dir : str, optional
        Override path to the schemas directory.
    clif_version : str, optional
        CLIF schema version the denominators are computed against. Pass the
        same version the present tables were validated at, otherwise the
        ``0/N`` this produces is measured against a different ``N``.

    Returns
    -------
    Dict[str, Any]
        A dict with the same top-level keys as :func:`run_full_dqa`
        (``table_name``, ``clif_version``, ``backend``, ``conformance``,
        ``completeness``, ``relational``, ``plausibility``) plus:

        - ``absent`` — always ``True``
        - ``total_rows`` — always ``0``
        - ``expected_check_counts`` — per-category denominators from
          :func:`get_schema_check_counts`, so consumers can render
          ``0/N`` without re-computing.

        The ``conformance`` dict contains a single ``table_presence``
        result marked ``passed=False`` with a ``"Table not present in
        dataset"`` error; other categories are empty dicts.
    """
    expected = get_schema_check_counts(table_name, schema_dir, clif_version)
    expected_conf = int(expected.get("conformance", 1) or 1)

    # Represent an absent table as a single ERROR whose atomic footprint
    # covers the full expected conformance count — every conformance atom
    # that would have run is "failed" by the table's absence. This keeps
    # per-site scoring comparable (a site that submitted the table scores
    # N/N, a site that didn't scores 0/N with N errors).
    table_presence = {
        "check_type": "table_presence",
        "table_name": table_name,
        "passed": False,
        "errors": [{
            "message": (
                f"Table not present in dataset — {expected_conf} conformance "
                "atoms could not be evaluated"
            ),
            "details": {
                "atomic_count": expected_conf,
                "reason": "table_absent",
            },
        }],
        "warnings": [],
        "info": [],
        "metrics": {"row_count": 0, "column_count": 0},
        "atomic_total": expected_conf,
        "atomic_passed": 0,
    }

    return {
        "table_name": table_name,
        "clif_version": clif_version,
        "backend": "absent",
        "absent": True,
        "conformance": {"table_presence": table_presence},
        "completeness": {},
        "relational": {},
        "plausibility": {},
        "total_rows": 0,
        "expected_check_counts": expected,
    }


class DQAConformanceResult:
    """Container for DQA conformance check results."""

    def __init__(self, check_type: str, table_name: str):
        self.check_type = check_type
        self.table_name = table_name
        # CLIF schema version this check was run against. Stamped by the
        # validation entry points (run_full_dqa / validate_dataframe) from the
        # schema's own ``version`` key. None until stamped.
        self.clif_version: Optional[str] = None
        self.passed = True
        self.errors: List[Dict[str, Any]] = []
        self.warnings: List[Dict[str, Any]] = []
        self.info: List[Dict[str, Any]] = []
        self.metrics: Dict[str, Any] = {}
        # Atomic-granularity scoring. When a check examines N atomic units
        # (e.g. permissible values, rule×column pairs) but rolls up to fewer
        # messages, it sets these so downstream scores reflect real work
        # done rather than message count. Populated by the check from its
        # own iteration; never hardcoded. None means "fall back to message
        # count" in collect_dqa_issues.
        self.atomic_total: Optional[int] = None
        self.atomic_passed: Optional[int] = None

    def add_error(self, message: str, details: Optional[Dict] = None):
        self.passed = False
        self.errors.append({"message": message, "details": details or {}})

    def add_warning(self, message: str, details: Optional[Dict] = None):
        self.warnings.append({"message": message, "details": details or {}})

    def add_info(self, message: str, details: Optional[Dict] = None):
        self.info.append({"message": message, "details": details or {}})

    def to_dict(self) -> Dict[str, Any]:
        return {
            "check_type": self.check_type,
            "table_name": self.table_name,
            "clif_version": self.clif_version,
            "passed": self.passed,
            "errors": self.errors,
            "warnings": self.warnings,
            "info": self.info,
            "metrics": self.metrics,
            "atomic_total": self.atomic_total,
            "atomic_passed": self.atomic_passed,
        }


class DQACompletenessResult:
    """Container for DQA completeness check results."""

    def __init__(self, check_type: str, table_name: str):
        self.check_type = check_type
        self.table_name = table_name
        # CLIF schema version this check was run against (stamped by the
        # validation entry points from the schema's ``version`` key).
        self.clif_version: Optional[str] = None
        self.passed = True
        self.errors: List[Dict[str, Any]] = []
        self.warnings: List[Dict[str, Any]] = []
        self.info: List[Dict[str, Any]] = []
        self.metrics: Dict[str, Any] = {}
        self.atomic_total: Optional[int] = None
        self.atomic_passed: Optional[int] = None

    def add_error(self, message: str, details: Optional[Dict] = None):
        self.passed = False
        self.errors.append({"message": message, "details": details or {}})

    def add_warning(self, message: str, details: Optional[Dict] = None):
        self.warnings.append({"message": message, "details": details or {}})

    def add_info(self, message: str, details: Optional[Dict] = None):
        self.info.append({"message": message, "details": details or {}})

    def to_dict(self) -> Dict[str, Any]:
        return {
            "check_type": self.check_type,
            "table_name": self.table_name,
            "clif_version": self.clif_version,
            "passed": self.passed,
            "errors": self.errors,
            "warnings": self.warnings,
            "info": self.info,
            "metrics": self.metrics,
            "atomic_total": self.atomic_total,
            "atomic_passed": self.atomic_passed,
        }


class DQAPlausibilityResult:
    """Container for DQA plausibility check results."""

    def __init__(self, check_type: str, table_name: str):
        self.check_type = check_type
        self.table_name = table_name
        # CLIF schema version this check was run against (stamped by the
        # validation entry points from the schema's ``version`` key).
        self.clif_version: Optional[str] = None
        self.passed = True
        self.errors: List[Dict[str, Any]] = []
        self.warnings: List[Dict[str, Any]] = []
        self.info: List[Dict[str, Any]] = []
        self.metrics: Dict[str, Any] = {}
        self.atomic_total: Optional[int] = None
        self.atomic_passed: Optional[int] = None

    def add_error(self, message: str, details: Optional[Dict] = None):
        self.passed = False
        self.errors.append({"message": message, "details": details or {}})

    def add_warning(self, message: str, details: Optional[Dict] = None):
        self.warnings.append({"message": message, "details": details or {}})

    def add_info(self, message: str, details: Optional[Dict] = None):
        self.info.append({"message": message, "details": details or {}})

    def to_dict(self) -> Dict[str, Any]:
        return {
            "check_type": self.check_type,
            "table_name": self.table_name,
            "clif_version": self.clif_version,
            "passed": self.passed,
            "errors": self.errors,
            "warnings": self.warnings,
            "info": self.info,
            "metrics": self.metrics,
            "atomic_total": self.atomic_total,
            "atomic_passed": self.atomic_passed,
        }


# ---------------------------------------------------------------------------
# CONFORMANCE CHECKS - A. Structure Checks
# ---------------------------------------------------------------------------

# A.1. Whether table is present
def check_table_exists(
    table_path: Union[str, Path],
    table_name: str,
    filetype: str = 'parquet'
) -> DQAConformanceResult:
    """
    Check if a table file exists at the specified path.

    Parameters
    ----------
    table_path : str or Path
        Directory containing the table files
    table_name : str
        Name of the table to check
    filetype : str
        File extension (parquet, csv, etc.)

    Returns
    -------
    DQAConformanceResult
        Result object with check status
    """
    result = DQAConformanceResult("table_exists", table_name)

    table_path = Path(table_path)
    expected_file = table_path / f"{table_name}.{filetype}"

    if expected_file.exists():
        result.add_info(f"Table file found: {expected_file}")
        result.metrics["file_path"] = str(expected_file)
        result.metrics["file_size_mb"] = expected_file.stat().st_size / (1024 * 1024)
    else:
        result.add_error(
            f"Table file not found: {expected_file}",
            {"expected_path": str(expected_file)}
        )

    result.atomic_total = 1
    result.atomic_passed = 1 if not result.errors else 0
    return result


# A.1b. Table presence check (DataFrame-level)
def check_table_presence_polars(
    df: Union['pl.DataFrame', 'pl.LazyFrame'],
    table_name: str
) -> DQAConformanceResult:
    """
    Check that a loaded DataFrame has rows and columns using Polars.

    Parameters
    ----------
    df : pl.DataFrame or pl.LazyFrame
        The data to validate
    table_name : str
        Name of the table
    """
    result = DQAConformanceResult("table_presence", table_name)

    try:
        lf = df if isinstance(df, pl.LazyFrame) else df.lazy()
        column_names = _strip_sidecars(lf.collect_schema().names())
        column_count = len(column_names)
        row_count = lf.select(pl.len()).collect().item()

        result.metrics["row_count"] = row_count
        result.metrics["column_count"] = column_count

        if column_count == 0:
            result.add_error(
                f"Table '{table_name}' has no columns",
                {"column_count": column_count},
            )
        if row_count == 0:
            result.add_error(
                f"Table '{table_name}' has 0 rows",
                {"row_count": row_count},
            )
        if result.passed:
            result.add_info(
                f"Table '{table_name}' present with {row_count} rows and {column_count} columns"
            )
    except Exception as e:
        _logger.error("Check 'table_presence' failed for table '%s': %s", table_name, e)
        result.add_error(f"Error checking table presence: {str(e)}")

    result.atomic_total = 1
    result.atomic_passed = 1 if not result.errors else 0
    return result


def check_table_presence(
    df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    table_name: str
) -> DQAConformanceResult:
    """
    Check that a loaded DataFrame has rows and columns.

    Parameters
    ----------
    df : pd.DataFrame, pl.DataFrame, or pl.LazyFrame
        Data to validate (already loaded)
    table_name : str
        Name of the table
    """
    _logger.debug("check_table_presence: starting for table '%s'", table_name)
    result = check_table_presence_polars(df, table_name)
    _logger.debug("check_table_presence: table '%s' — rows=%s, cols=%s",
                  table_name, result.metrics.get("row_count"), result.metrics.get("column_count"))
    return result


# A.2. Whether all required columns are present
def check_required_columns_polars(
    df: Union['pl.DataFrame', 'pl.LazyFrame'],
    schema: Dict[str, Any],
    table_name: str
) -> DQAConformanceResult:
    """
    Check if all required columns are present using Polars.

    Memory-efficient implementation using lazy evaluation.
    """
    result = DQAConformanceResult("required_columns", table_name)

    try:
        lf = df if isinstance(df, pl.LazyFrame) else df.lazy()
        actual_columns = set(lf.collect_schema().names())
        required_list = schema.get('required_columns', [])

        missing = set(required_list) - actual_columns

        result.metrics["total_required"] = len(required_list)
        result.metrics["total_present"] = len(required_list) - len(missing)
        result.metrics["total_missing"] = len(missing)

        for col_name in required_list:
            if col_name in missing:
                result.add_error(
                    f"Column '{col_name}': missing from data",
                    {"column": col_name}
                )
            else:
                result.add_info(
                    f"Column '{col_name}': present",
                    {"column": col_name}
                )

        result.atomic_total = len(required_list)
        result.atomic_passed = len(required_list) - len(missing)
    except Exception as e:
        _logger.error("Check 'required_columns' failed for table '%s': %s", table_name, e)
        result.add_error(f"Error checking columns: {str(e)}")
        if result.atomic_total is None:
            result.atomic_total = 0
            result.atomic_passed = 0

    return result


def check_required_columns(
    df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    schema: Dict[str, Any],
    table_name: str
) -> DQAConformanceResult:
    """
    Check if all required columns are present.

    Parameters
    ----------
    df : pd.DataFrame, pl.DataFrame, or pl.LazyFrame
        Data to validate (already loaded)
    schema : dict
        Table schema containing required_columns
    table_name : str
        Name of the table
    """
    _logger.debug("check_required_columns: starting for table '%s'", table_name)
    result = check_required_columns_polars(df, schema, table_name)
    if result.metrics.get("total_missing", 0) > 0:
        _logger.info("check_required_columns: table '%s' missing %d of %d required columns",
                     table_name, result.metrics["total_missing"], result.metrics.get("total_required", 0))
    _logger.debug("check_required_columns: table '%s' — required=%s, present=%s, missing=%s",
                  table_name, result.metrics.get("total_required"),
                  result.metrics.get("total_present"), result.metrics.get("total_missing"))
    return result


# ---------------------------------------------------------------------------
# CONFORMANCE CHECKS - B. Value Checks
# ---------------------------------------------------------------------------

# B.1. Data type validation
def check_column_dtypes_polars(
    df: Union['pl.DataFrame', 'pl.LazyFrame'],
    schema: Dict[str, Any],
    table_name: str
) -> DQAConformanceResult:
    """Check if columns have correct data types using Polars."""
    result = DQAConformanceResult("column_dtypes", table_name)

    # mCIDE type to Polars type mapping
    type_mapping = {
        'VARCHAR': [pl.Utf8, pl.String, pl.Categorical],
        'DATETIME': [pl.Datetime],
        'DATE': [pl.Date],
        'INTEGER': [pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64],
        'INT': [pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64],
        'FLOAT': [pl.Float32, pl.Float64],
        'DOUBLE': [pl.Float32, pl.Float64],
    }

    try:
        lf = df if isinstance(df, pl.LazyFrame) else df.lazy()
        schema_dict = lf.collect_schema()

        dtype_errors = []
        dtype_warnings = []

        for col_spec in schema.get('columns', []):
            col_name = col_spec['name']
            expected_type = col_spec.get('data_type')

            if not expected_type or col_name not in schema_dict.names():
                continue

            actual_dtype = schema_dict[col_name]
            expected_pl_types = type_mapping.get(expected_type, [])

            type_matches = any(
                isinstance(actual_dtype, t) or actual_dtype == t
                for t in expected_pl_types
            )

            if not type_matches:
                dtype_str = str(actual_dtype)
                # Check common patterns
                if expected_type in ('DATETIME',) and 'Datetime' in dtype_str:
                    continue
                if expected_type in ('VARCHAR',) and ('Utf8' in dtype_str or 'String' in dtype_str):
                    continue

                # Skip all-null columns — type inference is unreliable with no data
                null_check = lf.select(pl.col(col_name).is_null().all()).collect()
                if null_check.item():
                    continue

                castable = _check_castable_polars(lf, col_name, expected_type)

                if castable:
                    dtype_warnings.append({
                        "column": col_name,
                        "expected": expected_type,
                        "actual": str(actual_dtype),
                        "castable": True
                    })
                else:
                    dtype_errors.append({
                        "column": col_name,
                        "expected": expected_type,
                        "actual": str(actual_dtype),
                        "castable": False
                    })

        result.metrics["columns_checked"] = len(schema.get('columns', []))
        result.metrics["dtype_errors"] = len(dtype_errors)
        result.metrics["dtype_warnings"] = len(dtype_warnings)

        issue_cols = {err['column'] for err in dtype_errors} | {warn['column'] for warn in dtype_warnings}

        for err in dtype_errors:
            result.add_error(
                f"Column '{err['column']}' has type {err['actual']}, cannot cast to {err['expected']}",
                err
            )

        for warn in dtype_warnings:
            result.add_warning(
                f"Column '{warn['column']}' has type {warn['actual']}, can be cast to {warn['expected']}",
                warn
            )

        for col_spec in schema.get('columns', []):
            col_name = col_spec['name']
            expected_type = col_spec.get('data_type')
            if not expected_type:
                continue
            if col_name not in schema_dict.names():
                result.add_info(
                    f"Column '{col_name}': not present in data (dtype check skipped)",
                    {"column": col_name, "expected": expected_type}
                )
            elif col_name not in issue_cols:
                result.add_info(
                    f"Column '{col_name}': dtype matches schema ({expected_type})",
                    {"column": col_name, "expected": expected_type}
                )

        result.atomic_total = len(schema.get('columns', []))
        result.atomic_passed = result.atomic_total - len(dtype_errors)
    except Exception as e:
        _logger.error("Check 'column_dtypes' failed for table '%s': %s", table_name, e)
        result.add_error(f"Error checking dtypes: {str(e)}")
        if result.atomic_total is None:
            result.atomic_total = 0
            result.atomic_passed = 0

    return result


def _check_castable_polars(lf: 'pl.LazyFrame', col_name: str, target_type: str) -> bool:
    """Check if a column can be cast to the target type using Polars."""
    try:
        if target_type in ('INTEGER', 'INT'):
            sample = lf.select(pl.col(col_name).drop_nulls().head(100)).collect()
            if len(sample) > 0:
                sample.select(pl.col(col_name).cast(pl.Int64))
            return True
        elif target_type in ('FLOAT', 'DOUBLE'):
            sample = lf.select(pl.col(col_name).drop_nulls().head(100)).collect()
            if len(sample) > 0:
                sample.select(pl.col(col_name).cast(pl.Float64))
            return True
        elif target_type == 'DATETIME':
            sample = lf.select(pl.col(col_name).drop_nulls().head(100)).collect()
            if len(sample) > 0:
                sample.select(pl.col(col_name).str.to_datetime())
            return True
        elif target_type == 'VARCHAR':
            return True
        return False
    except Exception:
        return False


def check_column_dtypes(
    df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    schema: Dict[str, Any],
    table_name: str
) -> DQAConformanceResult:
    """
    Check if columns have correct data types.

    Parameters
    ----------
    df : pd.DataFrame, pl.DataFrame, or pl.LazyFrame
        Data to validate (already loaded)
    schema : dict
        Table schema
    table_name : str
        Name of the table
    """
    _logger.debug("check_column_dtypes: starting for table '%s'", table_name)
    result = check_column_dtypes_polars(df, schema, table_name)
    _logger.debug("check_column_dtypes: table '%s' — checked=%s, errors=%s, warnings=%s",
                  table_name, result.metrics.get("columns_checked"),
                  result.metrics.get("dtype_errors"), result.metrics.get("dtype_warnings"))
    return result


# B.2. Datetime format validation
def check_datetime_format_polars(
    df: Union['pl.DataFrame', 'pl.LazyFrame'],
    schema: Dict[str, Any],
    table_name: str,
    expected_tz: str = 'UTC'
) -> DQAConformanceResult:
    """Validate datetime columns are in correct format using Polars."""
    result = DQAConformanceResult("datetime_format", table_name)

    try:
        lf = df if isinstance(df, pl.LazyFrame) else df.lazy()
        schema_dict = lf.collect_schema()

        all_datetime_columns = [
            col['name'] for col in schema.get('columns', [])
            if col.get('data_type') in ('DATETIME', 'DATE')
        ]
        data_col_names = set(schema_dict.names())

        result.metrics["datetime_columns_checked"] = len(all_datetime_columns)
        columns_with_messages = set()

        for col in all_datetime_columns:
            if col not in data_col_names:
                result.add_info(
                    f"Column '{col}': not present in data (datetime check skipped)",
                    {"column": col}
                )
                columns_with_messages.add(col)
                continue

            col_dtype = schema_dict[col]
            dtype_str = str(col_dtype)

            if 'Datetime' not in dtype_str and 'Date' not in dtype_str:
                result.add_warning(
                    f"Column '{col}' should be DATETIME but is {col_dtype}",
                    {"column": col, "actual_type": str(col_dtype)}
                )
                columns_with_messages.add(col)
                continue

            if expected_tz and 'Datetime' in dtype_str:
                if expected_tz not in dtype_str and 'time_zone' not in dtype_str:
                    result.add_info(
                        f"Column '{col}' may be timezone-naive, expected {expected_tz}",
                        {"column": col, "expected_tz": expected_tz}
                    )
                    columns_with_messages.add(col)

        for col in all_datetime_columns:
            if col not in columns_with_messages:
                result.add_info(
                    f"Column '{col}': datetime format valid",
                    {"column": col}
                )

        result.atomic_total = len(all_datetime_columns)
        result.atomic_passed = len(all_datetime_columns) - len(result.errors)
    except Exception as e:
        _logger.error("Check 'datetime_format' failed for table '%s': %s", table_name, e)
        result.add_error(f"Error checking datetime format: {str(e)}")
        if result.atomic_total is None:
            result.atomic_total = 0
            result.atomic_passed = 0

    return result


def check_datetime_format(
    df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    schema: Dict[str, Any],
    table_name: str,
    expected_tz: str = 'UTC'
) -> DQAConformanceResult:
    """Validate datetime columns are in correct format."""
    _logger.debug("check_datetime_format: starting for table '%s'", table_name)
    result = check_datetime_format_polars(df, schema, table_name, expected_tz)
    _logger.debug("check_datetime_format: table '%s' — columns_checked=%s",
                  table_name, result.metrics.get("datetime_columns_checked"))
    return result


# B.3. Lab reference units validation
def _resolve_accepted_units(schema: Dict[str, Any], ref_unit_entry: Any) -> List[str]:
    """Return the accepted reference-unit spellings (lowercased, stripped) for
    one ``lab_reference_units`` entry, expanded against ``allowed_unit_variants``.

    Supports two schema shapes:

    * New canonical-key format — ``lab_reference_units`` maps each category to
      a single canonical unit string, and ``allowed_unit_variants`` maps that
      canonical key to every accepted spelling::

          lab_reference_units:
            albumin: g/dl
          allowed_unit_variants:
            g/dl: [g/dl, g per dl, gram/dl, ...]

    * Legacy list format — ``lab_reference_units`` maps each category to a
      pre-expanded list of accepted spellings.

    The return is deduplicated and always includes the canonical key itself.
    Returns an empty list for unrecognized (None / non-str, non-list) inputs.
    """
    if isinstance(ref_unit_entry, (list, tuple)):
        return sorted({str(u).lower().strip() for u in ref_unit_entry if u is not None})
    if not isinstance(ref_unit_entry, str):
        return []
    canonical = ref_unit_entry.lower().strip()
    variants_map = schema.get('allowed_unit_variants') or {}
    variants_map_norm = {str(k).lower().strip(): v for k, v in variants_map.items()}
    variants = variants_map_norm.get(canonical)
    if variants:
        return sorted({str(u).lower().strip() for u in variants if u is not None} | {canonical})
    return [canonical]


def _log_lab_reference_units_schema_summary(
    schema: Dict[str, Any],
    table_name: str,
) -> None:
    """Emit one INFO line describing how the lab_reference_units check will
    resolve accepted spellings for this schema — i.e. whether it has an
    ``allowed_unit_variants`` map, and how many categories will benefit.
    """
    lab_units = schema.get('lab_reference_units') or {}
    variants_map = schema.get('allowed_unit_variants') or {}
    if not lab_units:
        return

    canonical_entries = sum(1 for v in lab_units.values() if isinstance(v, str))
    list_entries = sum(1 for v in lab_units.values() if isinstance(v, (list, tuple)))

    if not variants_map:
        _logger.info(
            "check_lab_reference_units[%s]: allowed_unit_variants NOT present — "
            "accepting only the canonical spelling for each of %d categories "
            "(%d canonical-string, %d legacy-list)",
            table_name, len(lab_units), canonical_entries, list_entries,
        )
        return

    total_variants = sum(
        len(v) for v in variants_map.values() if isinstance(v, (list, tuple))
    )
    _logger.info(
        "check_lab_reference_units[%s]: allowed_unit_variants present — "
        "%d canonical keys defining %d total accepted spellings; schema has "
        "%d categories (%d canonical-string, %d legacy-list)",
        table_name, len(variants_map), total_variants,
        len(lab_units), canonical_entries, list_entries,
    )


def _evaluate_lab_category_units(
    schema: Dict[str, Any],
    lab_cat_orig_key: str,
    expected_units_entry: Any,
    actual_pairs: list,
) -> tuple:
    """Evaluate one lab category's observed reference units against the
    accepted-spelling set (canonical + variants).

    Parameters
    ----------
    schema
        Full schema dict (needed to look up ``allowed_unit_variants``).
    lab_cat_orig_key
        Original-case category key, for user-facing messages.
    expected_units_entry
        The schema's ``lab_reference_units`` value for this category — either
        a canonical key string or a legacy list.
    actual_pairs
        List of ``(ref_unit, ref_unit_orig, lab_cat_orig, count)`` tuples
        from the data, with ``ref_unit`` already lowercased+stripped.

    Returns
    -------
    (details, is_valid, bad_list)
        ``details`` is an enriched dict suitable for add_info/add_warning
        that records canonical unit, how many accepted variants were checked
        against, a small sample of them, and per-class match counts.
        ``is_valid`` is True iff every observed unit matched.
        ``bad_list`` is the sorted invalid-unit list, or empty.
    """
    accepted = _resolve_accepted_units(schema, expected_units_entry)
    if isinstance(expected_units_entry, str):
        canonical = expected_units_entry.lower().strip()
    elif isinstance(expected_units_entry, (list, tuple)) and expected_units_entry:
        canonical = str(expected_units_entry[0]).lower().strip()
    else:
        canonical = accepted[0] if accepted else ''

    variants_map = schema.get('allowed_unit_variants') or {}
    variant_lookup_used = bool(variants_map) and isinstance(expected_units_entry, str)
    no_units = '(no units)' in accepted

    matched_canonical = 0
    matched_via_variant = 0
    bad = []
    for ref_unit, ref_unit_orig, _, count in actual_pairs:
        if no_units and not ref_unit:
            matched_canonical += count
            continue
        if ref_unit == canonical:
            matched_canonical += count
        elif ref_unit in accepted:
            matched_via_variant += count
        else:
            bad.append({
                "reference_unit": ref_unit_orig,
                "expected_units": accepted,
                "count": count,
            })

    bad.sort(key=lambda x: x['count'], reverse=True)
    invalid_records = sum(x['count'] for x in bad)
    sample = accepted[: min(5, len(accepted))]

    details = {
        "column": lab_cat_orig_key,
        "canonical_unit": canonical,
        "variant_lookup_used": variant_lookup_used,
        "accepted_variant_count": len(accepted),
        "accepted_variants_sample": sample,
        "matched_canonical_records": matched_canonical,
        "matched_via_variant_records": matched_via_variant,
        "invalid_records": invalid_records,
    }
    if bad:
        details["top_invalid_units"] = bad[:10]

    _logger.debug(
        "Lab '%s': canonical=%r, %d accepted spellings (variant_map_used=%s, "
        "sample=%s) — data: canonical=%d, via_variant=%d, invalid=%d",
        lab_cat_orig_key, canonical, len(accepted), variant_lookup_used,
        sample, matched_canonical, matched_via_variant, invalid_records,
    )

    return details, not bool(bad), bad


def check_lab_reference_units_polars(
    df: Union['pl.DataFrame', 'pl.LazyFrame'],
    schema: Dict[str, Any],
    table_name: str = 'labs'
) -> DQAConformanceResult:
    """Check if lab reference units match schema definitions using Polars."""
    result = DQAConformanceResult("lab_reference_units", table_name)

    lab_units = schema.get('lab_reference_units', {})
    if not lab_units:
        result.add_info("No lab reference units defined in schema")
        result.atomic_total = 0
        result.atomic_passed = 0
        return result

    _log_lab_reference_units_schema_summary(schema, table_name)

    try:
        lf = df if isinstance(df, pl.LazyFrame) else df.lazy()
        # Self-normalize if caller invoked the check directly (e.g. from tests).
        if not _has_sidecars_polars(lf):
            lf = _normalize_columns_polars(lf)
        col_names = lf.collect_schema().names()

        if 'lab_category' not in col_names or 'reference_unit' not in col_names:
            result.add_error("Missing required columns: lab_category and/or reference_unit")
            result.atomic_total = 1
            result.atomic_passed = 0
            return result

        orig_cat_col = f"{_ORIG_PREFIX}lab_category"
        orig_unit_col = f"{_ORIG_PREFIX}reference_unit"
        has_orig_cat = orig_cat_col in col_names
        has_orig_unit = orig_unit_col in col_names

        agg_exprs = [pl.len().alias('count')]
        if has_orig_cat:
            agg_exprs.append(pl.col(orig_cat_col).first().alias('__orig_cat'))
        if has_orig_unit:
            agg_exprs.append(pl.col(orig_unit_col).first().alias('__orig_unit'))

        unit_counts = (
            lf
            .group_by(['lab_category', 'reference_unit'])
            .agg(agg_exprs)
            .collect(engine="streaming")
        )

        # Build lookup: lab_category (lowercased) -> list of (ref_unit, ref_unit_orig, cat_orig, count)
        actual_units: Dict[str, list] = {}
        total_count = 0
        for row in unit_counts.iter_rows(named=True):
            lab_cat = row['lab_category']
            ref_unit = row['reference_unit']
            count = row['count']
            lab_cat_orig = row.get('__orig_cat', lab_cat) if has_orig_cat else lab_cat
            ref_unit_orig = row.get('__orig_unit', ref_unit) if has_orig_unit else ref_unit
            total_count += count
            actual_units.setdefault(lab_cat, []).append((ref_unit, ref_unit_orig, lab_cat_orig, count))

        result.metrics["total_records"] = total_count
        invalid_count = 0

        # Normalize schema keys for lookup robustness.
        lab_units_normalized = {str(k).lower().strip(): (k, v) for k, v in lab_units.items()}

        # Emit one message per lab_reference_units entry
        for lab_cat_norm, (lab_cat_orig_key, expected_units) in lab_units_normalized.items():
            pairs = actual_units.get(lab_cat_norm)
            if pairs is None:
                result.add_info(
                    f"Lab category '{lab_cat_orig_key}': not present in data",
                    {"column": lab_cat_orig_key}
                )
                continue

            details, is_valid, _ = _evaluate_lab_category_units(
                schema, lab_cat_orig_key, expected_units, pairs,
            )
            if is_valid:
                result.add_info(
                    f"Lab category '{lab_cat_orig_key}': reference units match schema",
                    details,
                )
            else:
                invalid_count += 1
                result.add_warning(
                    f"Lab category '{lab_cat_orig_key}': non-standard units found",
                    details,
                )

        result.metrics["invalid_unit_categories"] = invalid_count
        gc.collect()

        result.atomic_total = len(lab_units_normalized)
        result.atomic_passed = len(lab_units_normalized) - len(result.errors)
    except Exception as e:
        _logger.error("Check 'lab_reference_units' failed for table '%s': %s", table_name, e)
        result.add_error(f"Error checking lab reference units: {str(e)}")
        if result.atomic_total is None:
            result.atomic_total = 1
            result.atomic_passed = 0

    return result


def check_lab_reference_units(
    df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    schema: Dict[str, Any],
    table_name: str = 'labs'
) -> DQAConformanceResult:
    """Check if lab reference units match schema definitions."""
    _logger.debug("check_lab_reference_units: starting for table '%s'", table_name)
    result = check_lab_reference_units_polars(df, schema, table_name)
    _logger.debug("check_lab_reference_units: table '%s' — valid=%s, invalid_categories=%s",
                  table_name, result.metrics.get("valid_units"),
                  result.metrics.get("invalid_unit_categories"))
    return result


# B.3b. Medication dose units validation (3.0 mCIDE per-category expected units)
def _med_unit_matches(observed: str, expected_units: List[str], continuous: bool) -> bool:
    """Return True if a normalized observed dose unit matches any expected unit.

    ``observed`` and ``expected_units`` must already be lowercased+stripped.
    For continuous tables the expected value is the rate NUMERATOR base unit
    (observed ``mcg/kg/min`` matches expected ``mcg``) unless the expected
    value itself contains ``/`` (e.g. ``ml/hr``), which must match the full
    observed unit. Intermittent tables always require a full-unit match.
    """
    for exp in expected_units:
        if not continuous or '/' in exp:
            if observed == exp:
                return True
        elif observed.split('/', 1)[0].strip() == exp:
            return True
    return False


def _coerce_expected_units(entry: Any) -> List[str]:
    """Normalize one dose-unit mapping value (scalar or list) to a lowercased,
    stripped list of accepted units."""
    if isinstance(entry, (list, tuple)):
        return [str(u).lower().strip() for u in entry if u is not None]
    if entry is None:
        return []
    return [str(entry).lower().strip()]


def _evaluate_med_category_units(
    med_cat_orig_key: str,
    expected_units: List[str],
    actual_pairs: list,
    continuous: bool,
) -> tuple:
    """Evaluate one med category's observed dose units against the expected
    unit(s) from the schema mapping.

    Parameters
    ----------
    med_cat_orig_key
        Original-case category key, for user-facing messages.
    expected_units
        Accepted units for this category, already lowercased+stripped.
    actual_pairs
        List of ``(dose_unit, dose_unit_orig, med_cat_orig, count)`` tuples
        from the data, with ``dose_unit`` already lowercased+stripped.
    continuous
        True for medication_admin_continuous (numerator matching).

    Returns
    -------
    (details, is_valid)
        ``details`` is a dict suitable for add_info/add_warning with match
        counts and (on mismatch) the top mismatched units. ``is_valid`` is
        True iff every observed unit matched.
    """
    matched = 0
    bad = []
    for dose_unit, dose_unit_orig, _, count in actual_pairs:
        if _med_unit_matches(dose_unit, expected_units, continuous):
            matched += count
        else:
            bad.append({
                "med_dose_unit": dose_unit_orig,
                "expected_units": expected_units,
                "count": count,
            })

    bad.sort(key=lambda x: x['count'], reverse=True)
    mismatched = sum(x['count'] for x in bad)

    details = {
        "column": med_cat_orig_key,
        "med_category": med_cat_orig_key,
        "expected_units": expected_units,
        "match_mode": "numerator" if continuous else "exact",
        "matched_records": matched,
        "mismatched_records": mismatched,
    }
    if bad:
        details["top_mismatched_units"] = bad[:10]

    return details, not bool(bad)


def _report_med_dose_unit_results(
    result: DQAConformanceResult,
    mapping_normalized: Dict[str, tuple],
    actual_units: Dict[str, list],
    continuous: bool,
) -> None:
    """Shared per-category reporting loop for both backends: one info per
    matching / absent mapping entry, one warning per mismatching entry."""
    mismatch_categories = 0
    mismatched_records = 0
    for med_cat_norm, (med_cat_orig_key, expected_entry) in mapping_normalized.items():
        expected_units = _coerce_expected_units(expected_entry)
        pairs = actual_units.get(med_cat_norm)
        if pairs is None:
            result.add_info(
                f"Med category '{med_cat_orig_key}': not present in data",
                {"column": med_cat_orig_key, "med_category": med_cat_orig_key}
            )
            continue

        details, is_valid = _evaluate_med_category_units(
            med_cat_orig_key, expected_units, pairs, continuous,
        )
        if is_valid:
            result.add_info(
                f"Med category '{med_cat_orig_key}': dose units match schema",
                details,
            )
        else:
            mismatch_categories += 1
            mismatched_records += details["mismatched_records"]
            result.add_warning(
                f"Med category '{med_cat_orig_key}': unexpected dose units found",
                details,
            )

    unknown = sorted(set(actual_units) - set(mapping_normalized))
    if unknown:
        result.metrics["unknown_categories"] = [
            {"med_category": cat,
             "count": sum(p[3] for p in actual_units[cat])}
            for cat in unknown
        ]
        result.add_info(
            f"{len(unknown)} med categories in data have no expected dose unit "
            f"in the schema mapping (skipped)",
            {"med_categories": unknown[:20]}
        )

    checked = sum(1 for k in mapping_normalized if k in actual_units)
    total_records = result.metrics.get("total_records", 0)
    result.metrics["categories_checked"] = checked
    result.metrics["categories_with_mismatch"] = mismatch_categories
    result.metrics["mismatched_records"] = mismatched_records
    result.metrics["mismatch_percent"] = round(
        mismatched_records / total_records * 100, 2) if total_records else 0


def _report_volume_rate_units(
    result: DQAConformanceResult,
    expected: str,
    unit_counts: list,
) -> None:
    """Report the volume_infusion_rate_unit sub-check (continuous only).

    ``unit_counts`` is a list of ``(unit_norm, unit_orig, count)`` for
    non-null, non-empty observed values.
    """
    expected_norm = str(expected).lower().strip()
    matched = sum(c for u, _, c in unit_counts if u == expected_norm)
    bad = sorted(
        ({"volume_infusion_rate_unit": orig, "count": c}
         for u, orig, c in unit_counts if u != expected_norm),
        key=lambda x: x['count'], reverse=True,
    )
    mismatched = sum(x['count'] for x in bad)
    details = {
        "column": "volume_infusion_rate_unit",
        "expected_unit": expected_norm,
        "matched_records": matched,
        "mismatched_records": mismatched,
    }
    if bad:
        details["top_mismatched_units"] = bad[:10]
        result.add_warning(
            "Column 'volume_infusion_rate_unit': unexpected units found",
            details,
        )
    else:
        result.add_info(
            f"Column 'volume_infusion_rate_unit': all values match "
            f"'{expected_norm}'",
            details,
        )
    result.metrics["volume_infusion_rate_unit"] = details


def check_medication_dose_units_polars(
    df: Union['pl.DataFrame', 'pl.LazyFrame'],
    schema: Dict[str, Any],
    table_name: str,
) -> DQAConformanceResult:
    """Check med_dose_unit values against the schema's per-category expected
    units (``med_category_to_dose_unit_mapping``) using Polars."""
    result = DQAConformanceResult("medication_dose_units", table_name)

    mapping = schema.get('med_category_to_dose_unit_mapping') or {}
    if not mapping:
        result.add_info("No med_category dose unit mapping defined in schema")
        result.atomic_total = 0
        result.atomic_passed = 0
        return result
    if table_name not in ('medication_admin_continuous', 'medication_admin_intermittent'):
        result.add_info("Medication dose units check not applicable to this table")
        result.atomic_total = 0
        result.atomic_passed = 0
        return result
    continuous = table_name == 'medication_admin_continuous'

    try:
        lf = df if isinstance(df, pl.LazyFrame) else df.lazy()
        # Self-normalize if caller invoked the check directly (e.g. from tests).
        if not _has_sidecars_polars(lf):
            lf = _normalize_columns_polars(lf)
        col_names = lf.collect_schema().names()

        if 'med_category' not in col_names or 'med_dose_unit' not in col_names:
            result.add_error("Missing required columns: med_category and/or med_dose_unit")
            result.atomic_total = 1
            result.atomic_passed = 0
            return result

        orig_cat_col = f"{_ORIG_PREFIX}med_category"
        orig_unit_col = f"{_ORIG_PREFIX}med_dose_unit"
        has_orig_cat = orig_cat_col in col_names
        has_orig_unit = orig_unit_col in col_names

        agg_exprs = [pl.len().alias('count')]
        if has_orig_cat:
            agg_exprs.append(pl.col(orig_cat_col).first().alias('__orig_cat'))
        if has_orig_unit:
            agg_exprs.append(pl.col(orig_unit_col).first().alias('__orig_unit'))

        # Single pass: null / empty-string units are split out of the grouped
        # counts in Python rather than via extra filtered scans.
        unit_counts = (
            lf
            .group_by(['med_category', 'med_dose_unit'])
            .agg(agg_exprs)
            .collect(engine="streaming")
        )

        # Build lookup: med_category (normalized) -> [(unit, unit_orig, cat_orig, count)]
        actual_units: Dict[str, list] = {}
        total_count = 0
        empty_count = 0
        for row in unit_counts.iter_rows(named=True):
            med_cat = row['med_category']
            dose_unit = row['med_dose_unit']
            count = row['count']
            if dose_unit is None:
                continue
            if str(dose_unit).strip() == '':
                empty_count += count
                continue
            if med_cat is None:
                continue
            med_cat_orig = row.get('__orig_cat', med_cat) if has_orig_cat else med_cat
            unit_orig = row.get('__orig_unit', dose_unit) if has_orig_unit else dose_unit
            total_count += count
            actual_units.setdefault(med_cat, []).append(
                (dose_unit, unit_orig, med_cat_orig, count))

        # Empty-string dose units are an ETL deviation: advise, then exclude.
        _report_empty_strings(result, table_name, 'med_dose_unit',
                              int(empty_count), {})

        result.metrics["total_records"] = total_count

        mapping_normalized = {str(k).lower().strip(): (k, v) for k, v in mapping.items()}
        _report_med_dose_unit_results(result, mapping_normalized, actual_units, continuous)

        # Continuous only: optional volume_infusion_rate_unit sub-check.
        expected_vol = schema.get('expected_volume_infusion_rate_unit')
        vol_checked = False
        if continuous and expected_vol and 'volume_infusion_rate_unit' in col_names:
            vol_orig_col = f"{_ORIG_PREFIX}volume_infusion_rate_unit"
            has_vol_orig = vol_orig_col in col_names
            vol_aggs = [pl.len().alias('count')]
            if has_vol_orig:
                vol_aggs.append(pl.col(vol_orig_col).first().alias('__orig_vol'))
            vol_counts = (
                lf.group_by('volume_infusion_rate_unit')
                .agg(vol_aggs)
                .collect(engine="streaming")
            )
            vol_pairs = [
                (row['volume_infusion_rate_unit'],
                 row.get('__orig_vol', row['volume_infusion_rate_unit']) if has_vol_orig
                 else row['volume_infusion_rate_unit'],
                 row['count'])
                for row in vol_counts.iter_rows(named=True)
                if row['volume_infusion_rate_unit'] is not None
                and str(row['volume_infusion_rate_unit']).strip() != ''
            ]
            if vol_pairs:
                _report_volume_rate_units(result, expected_vol, vol_pairs)
                vol_checked = True
            else:
                result.add_info("Column 'volume_infusion_rate_unit': no non-null values to check")
        elif continuous and expected_vol:
            result.add_info("Column 'volume_infusion_rate_unit' not found in table")

        gc.collect()

        result.atomic_total = len(mapping_normalized) + (1 if vol_checked else 0)
        result.atomic_passed = result.atomic_total - len(result.errors)
    except Exception as e:
        _logger.error("Check 'medication_dose_units' failed for table '%s': %s", table_name, e)
        result.add_error(f"Error checking medication dose units: {str(e)}")
        if result.atomic_total is None:
            result.atomic_total = 1
            result.atomic_passed = 0

    return result


def check_medication_dose_units(
    df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    schema: Dict[str, Any],
    table_name: str,
) -> DQAConformanceResult:
    """Check med_dose_unit values against per-category expected units."""
    _logger.debug("check_medication_dose_units: starting for table '%s'", table_name)
    result = check_medication_dose_units_polars(df, schema, table_name)
    _logger.debug(
        "check_medication_dose_units: table '%s' — categories_with_mismatch=%s",
        table_name, result.metrics.get("categories_with_mismatch"))
    return result


# B.4. Categorical values validation
def check_categorical_values_polars(
    df: Union['pl.DataFrame', 'pl.LazyFrame'],
    schema: Dict[str, Any],
    table_name: str
) -> DQAConformanceResult:
    """Check if categorical values match mCIDE permissible values using Polars."""
    result = DQAConformanceResult("categorical_values", table_name)

    try:
        lf = df if isinstance(df, pl.LazyFrame) else df.lazy()
        # Self-normalize if caller invoked the check directly (e.g. from tests).
        if not _has_sidecars_polars(lf):
            lf = _normalize_columns_polars(lf)
        col_names = lf.collect_schema().names()

        category_columns = schema.get('category_columns') or []
        invalid_values_by_col = {}
        columns_checked = []
        columns_missing = set()

        for col_spec in schema.get('columns', []):
            col_name = col_spec['name']
            permissible = col_spec.get('permissible_values', [])

            if not permissible:
                continue

            if col_name not in category_columns:
                continue

            columns_checked.append(col_name)

            if col_name not in col_names:
                columns_missing.add(col_name)
                continue

            orig_col = f"{_ORIG_PREFIX}{col_name}"
            has_orig = orig_col in col_names

            if has_orig:
                unique_vals = (
                    lf
                    .select([pl.col(col_name), pl.col(orig_col)])
                    .drop_nulls(subset=[col_name])
                    .group_by(col_name)
                    .agg([pl.len().alias('count'),
                          pl.col(orig_col).first().alias('__orig_val')])
                    .collect(engine="streaming")
                )
            else:
                unique_vals = (
                    lf
                    .select(pl.col(col_name))
                    .drop_nulls()
                    .group_by(col_name)
                    .agg(pl.len().alias('count'))
                    .collect(engine="streaming")
                )

            permissible_lower = {str(v).lower().strip() for v in permissible}

            invalid_for_col = []
            for row in unique_vals.iter_rows(named=True):
                val = row[col_name]
                count = row['count']
                # Cite the original (pre-normalization) value to the user so
                # error messages show what they actually submitted.
                display_val = row.get('__orig_val', val) if has_orig else val

                val_str = str(val).lower().strip() if val is not None else ''
                if val_str not in permissible_lower and val not in permissible:
                    invalid_for_col.append({
                        "value": display_val,
                        "count": count
                    })

            if invalid_for_col:
                invalid_for_col.sort(key=lambda x: x['count'], reverse=True)
                invalid_values_by_col[col_name] = {
                    "invalid_values": invalid_for_col[:20],
                    "total_invalid_unique": len(invalid_for_col),
                    "permissible_values": permissible
                }

        result.metrics["category_columns_checked"] = len(columns_checked)
        result.metrics["columns_with_invalid_values"] = len(invalid_values_by_col)

        for col_name in columns_checked:
            if col_name in columns_missing:
                result.add_info(
                    f"Column '{col_name}': not present in data (categorical check skipped)",
                    {"column": col_name}
                )
            elif col_name in invalid_values_by_col:
                details = invalid_values_by_col[col_name]
                result.add_warning(
                    f"{details['total_invalid_unique']} invalid categorical values",
                    {
                        "column": col_name,
                        "top_invalid": details['invalid_values'][:10],
                        "permissible_values": details['permissible_values']
                    }
                )
            else:
                result.add_info(
                    f"Column '{col_name}': all values match mCIDE permissible values",
                    {"column": col_name}
                )

        gc.collect()

        result.atomic_total = len(columns_checked)
        result.atomic_passed = len(columns_checked) - len(result.errors)
    except Exception as e:
        _logger.error("Check 'categorical_values' failed for table '%s': %s", table_name, e)
        result.add_error(f"Error checking categorical values: {str(e)}")
        if result.atomic_total is None:
            result.atomic_total = 0
            result.atomic_passed = 0

    return result


def check_categorical_values(
    df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    schema: Dict[str, Any],
    table_name: str
) -> DQAConformanceResult:
    """Check if categorical values match mCIDE permissible values."""
    _logger.debug("check_categorical_values: starting for table '%s'", table_name)
    result = check_categorical_values_polars(df, schema, table_name)
    _logger.debug("check_categorical_values: table '%s' — columns_checked=%s, columns_with_invalid=%s",
                  table_name, result.metrics.get("category_columns_checked"),
                  result.metrics.get("columns_with_invalid_values"))
    return result


# B.5. Category-to-group mapping validation

def check_category_group_mapping_polars(
    df: Union['pl.DataFrame', 'pl.LazyFrame'],
    schema: Dict[str, Any],
    table_name: str
) -> DQAConformanceResult:
    """Check if category-to-group mappings match schema definitions using Polars."""
    result = DQAConformanceResult("category_group_mapping", table_name)

    # Discover all *_category_to_group_mapping keys in the schema
    mapping_keys = [k for k in schema if k.endswith('_category_to_group_mapping')]
    if not mapping_keys:
        result.add_info("No category-to-group mappings defined in schema")
        result.atomic_total = 0
        result.atomic_passed = 0
        return result

    try:
        lf = df if isinstance(df, pl.LazyFrame) else df.lazy()
        # Self-normalize if caller invoked the check directly (e.g. from tests).
        if not _has_sidecars_polars(lf):
            lf = _normalize_columns_polars(lf)
        col_names = lf.collect_schema().names()

        # Atomic counting: one atomic check per (category → group) pair.
        # Accumulated from the loop so the count reflects real iteration.
        total_pairs = 0
        mismatch_total = 0

        for mapping_key in mapping_keys:
            mapping = schema[mapping_key]
            if not mapping:
                continue

            total_pairs += len(mapping)

            # Derive column names: e.g. "med_category_to_group_mapping" -> category_col="med_category", group_col="med_group"
            category_col = mapping_key.replace('_to_group_mapping', '')
            group_col = category_col.replace('_category', '_group')

            if category_col not in col_names or group_col not in col_names:
                # Emit one info per mapping entry so the count stays deterministic
                for cat_val in mapping:
                    result.add_info(
                        f"Category '{cat_val}': columns not in data (mapping check skipped)",
                        {"column": cat_val, "category_column": category_col, "group_column": group_col}
                    )
                continue

            # Also pull original-case sidecars when available so error payloads
            # show what the site actually submitted.
            orig_group_col = f"{_ORIG_PREFIX}{group_col}"
            has_orig_group = orig_group_col in col_names
            agg_exprs = [pl.len().alias('count')]
            if has_orig_group:
                agg_exprs.append(pl.col(orig_group_col).first().alias('__orig_group_val'))

            # Group by (category_col, group_col) where both non-null
            pair_counts = (
                lf
                .filter(pl.col(category_col).is_not_null() & pl.col(group_col).is_not_null())
                .group_by([category_col, group_col])
                .agg(agg_exprs)
                .collect(engine="streaming")
            )

            # Build lookup: category_val (lowercased) -> list of (grp_val, grp_val_orig, count)
            actual_groups: Dict[str, list] = {}
            total_count = 0
            for row in pair_counts.iter_rows(named=True):
                cat_val = row[category_col]
                grp_val = row[group_col]
                grp_val_orig = row.get('__orig_group_val', grp_val) if has_orig_group else grp_val
                count = row['count']
                total_count += count
                actual_groups.setdefault(cat_val, []).append((grp_val, grp_val_orig, count))

            result.metrics[f"{mapping_key}_total_records"] = total_count
            mismatch_count = 0

            # Normalize schema keys too (defense-in-depth) so the lookup is robust
            # if the schema YAML happens to have mixed case. Values may be a
            # single group name or a list of permissible groups (e.g.
            # epoprostenol maps to both "pulmonary vasodilators (IV)" and
            # "pulmonary vasodilators (inhaled)"); normalize to a list here
            # and keep the original form for error reporting.
            mapping_normalized = {
                str(k).lower().strip(): (k, v if isinstance(v, list) else [v])
                for k, v in mapping.items()
            }

            # Emit one message per mapping entry
            for cat_val_norm, (cat_val_orig, expected_groups) in mapping_normalized.items():
                pairs = actual_groups.get(cat_val_norm)
                if pairs is None:
                    result.add_info(
                        f"Category '{cat_val_orig}': not present in data",
                        {"column": cat_val_orig, "category_column": category_col, "group_column": group_col}
                    )
                    continue

                allowed_lowers = {str(g).lower().strip() for g in expected_groups}
                expected_display = (
                    " or ".join(f"'{g}'" for g in expected_groups)
                    if len(expected_groups) > 1
                    else f"'{expected_groups[0]}'"
                )

                bad = []
                for grp_val, grp_val_orig, count in pairs:
                    if grp_val not in allowed_lowers:
                        bad.append({
                            "category": cat_val_orig,
                            "actual_group": grp_val_orig,
                            "expected_group": (
                                list(expected_groups) if len(expected_groups) > 1
                                else expected_groups[0]
                            ),
                            "count": count,
                        })

                if bad:
                    mismatch_count += 1
                    result.add_warning(
                        f"Category '{cat_val_orig}': group mismatch (expected {expected_display})",
                        {"column": cat_val_orig, "category_column": category_col,
                         "group_column": group_col, "mismatched_pairs": bad}
                    )
                else:
                    result.add_info(
                        f"Category '{cat_val_orig}': group mapping correct",
                        {"column": cat_val_orig, "category_column": category_col, "group_column": group_col}
                    )

            result.metrics[f"{mapping_key}_mismatch_count"] = mismatch_count
            mismatch_total += mismatch_count

        result.atomic_total = total_pairs
        result.atomic_passed = total_pairs - mismatch_total
        gc.collect()

    except Exception as e:
        _logger.error("Check 'category_group_mapping' failed for table '%s': %s", table_name, e)
        result.add_error(f"Error checking category-group mapping: {str(e)}")
        if result.atomic_total is None:
            result.atomic_total = 1
            result.atomic_passed = 0

    return result


def check_category_group_mapping(
    df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    schema: Dict[str, Any],
    table_name: str
) -> DQAConformanceResult:
    """Check if category-to-group mappings match schema definitions."""
    _logger.debug("check_category_group_mapping: starting for table '%s'", table_name)
    result = check_category_group_mapping_polars(df, schema, table_name)
    _logger.debug("check_category_group_mapping: table '%s' — completed", table_name)
    return result


# ---------------------------------------------------------------------------
# COMPLETENESS CHECKS
# ---------------------------------------------------------------------------

# A.1. Missingness in required columns
def check_missingness_polars(
    df: Union['pl.DataFrame', 'pl.LazyFrame'],
    schema: Dict[str, Any],
    table_name: str,
    error_threshold: float = 50.0,
    warning_threshold: float = 10.0
) -> DQACompletenessResult:
    """Check missingness in required columns using Polars."""
    result = DQACompletenessResult("missingness", table_name)

    try:
        lf = df if isinstance(df, pl.LazyFrame) else df.lazy()
        col_names = lf.collect_schema().names()

        required_columns = schema.get('required_columns', [])
        required_not_in_df = [c for c in required_columns if c not in col_names]
        required_in_df = [c for c in required_columns if c in col_names]

        # Skip columns covered by conditional requirements (checked in K.2)
        conditions = _get_default_conditions(table_name)
        conditional_cols = {col for cond in conditions for col in cond.get('then_required', [])}
        required_in_df = [c for c in required_in_df if c not in conditional_cols]

        # Skip columns marked nullable in schema (e.g. death_dttm)
        nullable_cols = {col['name'] for col in schema.get('columns', []) if col.get('nullable')}
        required_in_df = [c for c in required_in_df if c not in nullable_cols]

        # Columns with allow_missing: severity capped at warning (never error)
        allow_missing_cols = {col['name'] for col in schema.get('columns', []) if col.get('allow_missing')}

        total_rows = lf.select(pl.len()).collect()[0, 0]

        if total_rows == 0:
            result.add_error("DataFrame is empty")
            # Empty table: every required column is fully missing. Populate
            # atomic counts before returning so downstream scoring
            # (report_generator.collect_dqa_issues) doesn't choke on None.
            result.atomic_total = max(len(required_columns), 1)
            result.atomic_passed = 0
            return result

        null_exprs = [
            pl.col(c).is_null().sum().alias(f"{c}_null")
            for c in required_in_df
        ]

        null_counts = lf.select(null_exprs).collect(engine="streaming")

        missingness_stats = []
        high_missingness = []

        for col in required_in_df:
            null_count = null_counts[0, f"{col}_null"]
            pct_missing = (null_count / total_rows) * 100

            missingness_stats.append({
                "column": col,
                "null_count": int(null_count),
                "total_rows": int(total_rows),
                "percent_missing": round(pct_missing, 2)
            })

            # For allow_missing columns, cap severity at warning
            if col in allow_missing_cols:
                if pct_missing >= warning_threshold:
                    high_missingness.append({
                        "column": col,
                        "percent_missing": round(pct_missing, 2),
                        "severity": "warning"
                    })
            elif pct_missing >= error_threshold:
                high_missingness.append({
                    "column": col,
                    "percent_missing": round(pct_missing, 2),
                    "severity": "error"
                })
            elif pct_missing >= warning_threshold:
                high_missingness.append({
                    "column": col,
                    "percent_missing": round(pct_missing, 2),
                    "severity": "warning"
                })

        missingness_stats.sort(key=lambda x: x['percent_missing'], reverse=True)

        result.metrics["total_rows"] = int(total_rows)
        result.metrics["required_columns_checked"] = len(required_in_df)
        result.metrics["missingness_stats"] = missingness_stats

        high_miss_cols = {item['column'] for item in high_missingness}

        for item in high_missingness:
            if item["severity"] == "error":
                result.add_error(
                    f"Column '{item['column']}' has {item['percent_missing']}% missing values",
                    item
                )
            else:
                result.add_warning(
                    f"Column '{item['column']}' has {item['percent_missing']}% missing values",
                    item
                )

        for stat in missingness_stats:
            if stat['column'] not in high_miss_cols:
                result.add_info(
                    f"Column '{stat['column']}': {stat['percent_missing']}% missing (below thresholds)",
                    {"column": stat['column'],
                     "percent_missing": stat['percent_missing'],
                     "error_threshold": error_threshold,
                     "warning_threshold": warning_threshold},
                )

        for col in required_not_in_df:
            if col not in conditional_cols:
                result.add_info(
                    f"Column '{col}': not present in data (missingness check skipped)",
                    {"column": col},
                )

        # Atomic counting: 1 per required column in the schema.
        # Lenient scoring — only errors (columns that crossed error_threshold
        # or are absent from data outside K.2 coverage) reduce atomic_passed.
        # Warning-level missingness is flagged as a warning row but doesn't
        # fail the atom.
        result.atomic_total = len(required_columns)
        failed_errors = sum(
            1 for item in high_missingness if item.get('severity') == 'error'
        ) + sum(
            1 for c in required_not_in_df if c not in conditional_cols
        )
        result.atomic_passed = result.atomic_total - failed_errors

        gc.collect()

    except Exception as e:
        _logger.error("Check 'missingness' failed for table '%s': %s", table_name, e)
        result.add_error(f"Error checking missingness: {str(e)}")
        if result.atomic_total is None:
            result.atomic_total = 1
            result.atomic_passed = 0

    return result


def check_missingness(
    df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    schema: Dict[str, Any],
    table_name: str,
    error_threshold: float = 50.0,
    warning_threshold: float = 10.0
) -> DQACompletenessResult:
    """
    Check missingness in required columns.

    Parameters
    ----------
    df : pd.DataFrame, pl.DataFrame, or pl.LazyFrame
        Data to validate (already loaded)
    schema : dict
        Table schema containing required_columns
    table_name : str
        Name of the table
    error_threshold : float
        Percent missing above which an error is raised
    warning_threshold : float
        Percent missing above which a warning is raised

    Returns
    -------
    DQACompletenessResult
        Result containing missingness statistics
    """
    _logger.debug("check_missingness: starting for table '%s' (error_threshold=%.1f%%, warning_threshold=%.1f%%)",
                  table_name, error_threshold, warning_threshold)
    result = check_missingness_polars(df, schema, table_name, error_threshold, warning_threshold)
    if result.errors:
        for err in result.errors:
            _logger.info("check_missingness: table '%s' — %s", table_name, err["message"])
    _logger.debug("check_missingness: table '%s' — columns_checked=%s",
                  table_name, result.metrics.get("required_columns_checked"))
    return result


# A.2. Conditional required fields
def _load_validation_rules() -> Dict[str, Any]:
    """Load the centralised validation rules YAML."""
    rules_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), 'schemas', 'validation_rules.yaml'
    )
    if not os.path.exists(rules_path):
        _logger.warning("Validation rules file not found: %s", rules_path)
        return {}
    with open(rules_path, 'r') as f:
        return yaml.safe_load(f) or {}


def _get_default_conditions(table_name: str) -> List[Dict[str, Any]]:
    """Load default conditional requirements for a table from validation_rules.yaml."""
    rules = _load_validation_rules()
    return rules.get('conditional_requirements', {}).get(table_name, [])


def check_conditional_requirements_polars(
    df: Union['pl.DataFrame', 'pl.LazyFrame'],
    table_name: str,
    conditions: Optional[List[Dict[str, Any]]] = None
) -> DQACompletenessResult:
    """Check conditional required fields using Polars."""
    result = DQACompletenessResult("conditional_requirements", table_name)

    if conditions is None:
        conditions = _get_default_conditions(table_name)

    if not conditions:
        result.add_info("No conditional requirements defined for this table")
        result.atomic_total = 0
        result.atomic_passed = 0
        return result

    try:
        lf = df if isinstance(df, pl.LazyFrame) else df.lazy()
        col_names = lf.collect_schema().names()

        # Atomic counting: 1 per (rule × then_required column) — one row is
        # emitted for each combo (warning when missing, info when satisfied),
        # so the atomic unit matches what the user sees. Lenient: warnings
        # don't fail an atom; only errors do.
        atomic_total = 0
        atomic_failed_by_errors = 0

        for cond in conditions:
            when_col = cond['when_column']
            when_values = cond['when_value']
            then_required = cond['then_required']
            description = cond.get('description', '')

            # Count all then_required cols present in the schema toward the
            # atomic total, regardless of whether when_col is present — if
            # the condition can't be evaluated we still counted the slots.
            atomic_total += sum(1 for c in then_required if c in col_names)

            if when_col not in col_names:
                continue

            if not isinstance(when_values, list):
                when_values = [when_values]

            filtered = lf.filter(
                pl.col(when_col).cast(pl.Utf8).str.to_lowercase().str.strip_chars().is_in(
                    [str(v).lower().strip() for v in when_values]
                )
            )

            # Optional compound condition: and_column / and_value
            and_col = cond.get('and_column')
            and_values = cond.get('and_value')
            if and_col and and_values is not None:
                if and_col not in col_names:
                    continue
                if not isinstance(and_values, list):
                    and_values = [and_values]
                filtered = filtered.filter(
                    pl.col(and_col).cast(pl.Utf8).str.to_lowercase().str.strip_chars().is_in(
                        [str(v).lower().strip() for v in and_values]
                    )
                )

            condition_label = f"{when_col} IN {when_values}"
            if and_col and and_values is not None:
                condition_label += f" AND {and_col} IN {and_values}"

            for req_col in then_required:
                if req_col not in col_names:
                    continue

                stats = filtered.select([
                    pl.len().alias('total'),
                    pl.col(req_col).is_null().sum().alias('null_count')
                ]).collect(engine="streaming")

                total = stats[0, 'total']
                null_count = stats[0, 'null_count']

                if total > 0 and null_count > 0:
                    pct_missing = (null_count / total) * 100
                    result.add_warning(
                        f"Conditional requirement violated: {description}",
                        {
                            "condition": condition_label,
                            "required_column": req_col,
                            "rows_meeting_condition": int(total),
                            "rows_with_missing": int(null_count),
                            "percent_missing": round(pct_missing, 2)
                        }
                    )
                elif total > 0:
                    result.add_info(
                        f"Conditional requirement satisfied: {description} — {int(total):,}/{int(total):,} rows present (100%)",
                        {"column": req_col, "condition": condition_label,
                         "rows_meeting_condition": int(total),
                         "rows_present": int(total),
                         "percent_present": 100.0}
                    )

        result.atomic_total = atomic_total
        # Lenient: only errors fail atoms. K.2 currently never emits errors
        # (only warnings / infos), so all atoms pass.
        result.atomic_passed = atomic_total - atomic_failed_by_errors
        gc.collect()

    except Exception as e:
        _logger.error("Check 'conditional_requirements' failed for table '%s': %s", table_name, e)
        result.add_error(f"Error checking conditional requirements: {str(e)}")
        if result.atomic_total is None:
            result.atomic_total = 1
            result.atomic_passed = 0

    return result


def check_conditional_requirements(
    df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    table_name: str,
    conditions: Optional[List[Dict[str, Any]]] = None
) -> DQACompletenessResult:
    """Check conditional required fields."""
    n_conditions = len(conditions) if conditions else 0
    _logger.debug("check_conditional_requirements: starting for table '%s' (%d explicit conditions)",
                  table_name, n_conditions)
    result = check_conditional_requirements_polars(df, table_name, conditions)
    if result.warnings:
        _logger.info("check_conditional_requirements: table '%s' — %d violations found",
                     table_name, len(result.warnings))
    _logger.debug("check_conditional_requirements: table '%s' complete", table_name)
    return result

# B. mCIDE value coverage
def check_mcide_value_coverage_polars(
    df: Union['pl.DataFrame', 'pl.LazyFrame'],
    schema: Dict[str, Any],
    table_name: str
) -> DQACompletenessResult:
    """Check if all mCIDE standardized values are present in the data using Polars."""
    result = DQACompletenessResult("mcide_value_coverage", table_name)

    try:
        lf = df if isinstance(df, pl.LazyFrame) else df.lazy()
        col_names = lf.collect_schema().names()

        category_columns = schema.get('category_columns') or []
        coverage_by_col = {}
        columns_missing = set()
        # Atomic counting: every (category_column × permissible_value) pair
        # the check looks at counts as one atomic check. Accumulated from the
        # loop below so the count always reflects real iteration.
        expected_total = 0

        for col_spec in schema.get('columns', []):
            col_name = col_spec['name']
            permissible = col_spec.get('permissible_values', [])

            if not permissible:
                continue

            if col_name not in category_columns:
                continue

            expected_total += len(permissible)

            if col_name not in col_names:
                columns_missing.add(col_name)
                continue

            unique_vals = (
                lf
                .select(pl.col(col_name).drop_nulls().unique())
                .collect(engine="streaming")
                .to_series()
                .to_list()
            )

            unique_vals_lower = {str(v).lower().strip() for v in unique_vals if v is not None}

            missing_vals = [
                v for v in permissible
                if str(v).lower().strip() not in unique_vals_lower
            ]

            coverage_pct = ((len(permissible) - len(missing_vals)) / len(permissible)) * 100

            coverage_by_col[col_name] = {
                "expected_values": len(permissible),
                "found_values": len(permissible) - len(missing_vals),
                "missing_values": missing_vals,
                "coverage_percent": round(coverage_pct, 2)
            }

        result.metrics["category_columns_checked"] = len(coverage_by_col) + len(columns_missing)
        result.metrics["coverage_by_column"] = coverage_by_col
        result.atomic_total = expected_total
        result.atomic_passed = sum(c['found_values'] for c in coverage_by_col.values())

        for col_name, details in coverage_by_col.items():
            # Emit an ERROR listing missing values (if any) AND always emit
            # an INFO reflecting the per-column silent-pass atom count so
            # each row's Checks cell matches its own narrative. Before this
            # split, the single INFO row for a fully-clean column would get
            # its atomic_count inflated by the reconciler with the silent-
            # passes from other columns too ("all 3 present, checks=565").
            if details['missing_values']:
                result.add_error(
                    f"Missing {len(details['missing_values'])} mCIDE values: {', '.join(str(v) for v in details['missing_values'])}",
                    {
                        "column": col_name,
                        "missing_values": details['missing_values'],
                        "coverage_percent": details['coverage_percent']
                    }
                )
            if details['found_values'] > 0:
                result.add_info(
                    f"Column '{col_name}': {details['found_values']}/{details['expected_values']} mCIDE values present",
                    {
                        "column": col_name,
                        "found_values": details['found_values'],
                        "expected_values": details['expected_values'],
                        "coverage_percent": details['coverage_percent'],
                        "atomic_count": details['found_values'],
                    }
                )

        for col_name in columns_missing:
            result.add_info(
                f"Column '{col_name}': not present in data (coverage check skipped)",
                {"column": col_name}
            )

        gc.collect()

    except Exception as e:
        _logger.error("Check 'mcide_value_coverage' failed for table '%s': %s", table_name, e)
        result.add_error(f"Error checking mCIDE value coverage: {str(e)}")
        if result.atomic_total is None:
            result.atomic_total = 1
            result.atomic_passed = 0

    return result


def check_mcide_value_coverage(
    df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    schema: Dict[str, Any],
    table_name: str
) -> DQACompletenessResult:
    """Check if all mCIDE standardized values are present in the data."""
    _logger.debug("check_mcide_value_coverage: starting for table '%s'", table_name)
    result = check_mcide_value_coverage_polars(df, schema, table_name)
    _logger.debug("check_mcide_value_coverage: table '%s' — columns_checked=%s",
                  table_name, result.metrics.get("category_columns_checked"))
    return result

# C. Relational integrity checks
def check_relational_integrity_polars(
    source_df: Union['pl.DataFrame', 'pl.LazyFrame'],
    reference_df: Union['pl.DataFrame', 'pl.LazyFrame'],
    source_table: str,
    reference_table: str,
    key_column: str
) -> DQACompletenessResult:
    """Check relational integrity between tables using Polars."""
    result = DQACompletenessResult("relational_integrity", f"{source_table}->{reference_table}")

    try:
        source_lf = source_df if isinstance(source_df, pl.LazyFrame) else source_df.lazy()
        ref_lf = reference_df if isinstance(reference_df, pl.LazyFrame) else reference_df.lazy()

        # Stay in Arrow: the check needs three counts, not the IDs themselves.
        # Materializing both id columns into Python sets to take a difference
        # costs ~5x the Arrow representation (a 500k-id set is ~60 MB of
        # PyObject pointers); an anti-join computes the same number lazily.
        source_ids = source_lf.select(pl.col(key_column).drop_nulls().unique())
        ref_ids = ref_lf.select(pl.col(key_column).drop_nulls().unique())

        total_source = source_ids.select(pl.len()).collect(engine="streaming").item()
        total_ref = ref_ids.select(pl.len()).collect(engine="streaming").item()
        total_orphan = (
            source_ids
            .join(ref_ids, on=key_column, how="anti")
            .select(pl.len())
            .collect(engine="streaming")
            .item()
        )

        coverage_pct = ((total_source - total_orphan) / total_source * 100) if total_source > 0 else 100

        result.metrics["source_unique_ids"] = total_source
        result.metrics["reference_unique_ids"] = total_ref
        result.metrics["orphan_ids"] = total_orphan
        result.metrics["coverage_percent"] = round(coverage_pct, 2)

        if total_orphan:
            result.add_warning(
                f"{total_orphan}/{total_source} {key_column} values in {source_table} "
                f"not found in {reference_table} ({round(coverage_pct, 1)}% coverage)",
                {
                    "orphan_count": total_orphan,
                    "coverage_percent": round(coverage_pct, 2)
                }
            )
        else:
            result.add_info(f"All {key_column} values in {source_table} exist in {reference_table}")

        gc.collect()

        result.atomic_total = 1
        result.atomic_passed = 1 if not result.errors else 0
    except Exception as e:
        _logger.error("Check 'relational_integrity' failed for '%s' -> '%s': %s", source_table, reference_table, e)
        result.add_error(f"Error checking relational integrity: {str(e)}")
        if result.atomic_total is None:
            result.atomic_total = 1
            result.atomic_passed = 0

    return result


def check_relational_integrity(
    target_df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    reference_df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    target_table: str,
    reference_table: str,
    key_column: str
) -> DQACompletenessResult:
    """Check bidirectional relational integrity between tables.

    Runs the backend-specific check in both directions:
    - **Forward** (reference → target): What percentage of reference IDs
      appear in the target table?  (e.g., "what % of hospitalizations
      have labs?")
    - **Reverse** (target → reference): What percentage of target IDs
      exist in the reference table?  (e.g., "what % of lab hosp_ids are
      valid?")

    Parameters
    ----------
    target_df : DataFrame
        The target table (e.g., labs).
    reference_df : DataFrame
        The reference table (e.g., hospitalization).
    target_table : str
        Name of the target table.
    reference_table : str
        Name of the reference table.
    key_column : str
        The shared key column (e.g., ``hospitalization_id``).

    Returns
    -------
    DQACompletenessResult
        Consolidated result with forward/reverse coverage metrics.
    """
    _logger.debug(
        "check_relational_integrity: '%s' <-> '%s' on key '%s'",
        target_table, reference_table, key_column,
    )
    result = DQACompletenessResult(
        "relational_integrity",
        f"{target_table}<->{reference_table}",
    )

    _backend_fn = check_relational_integrity_polars

    try:
        # Forward: reference → target  (source=reference, ref=target)
        fwd = _backend_fn(
            reference_df, target_df, reference_table, target_table, key_column
        )
        # Reverse: target → reference  (source=target, ref=reference)
        rev = _backend_fn(
            target_df, reference_df, target_table, reference_table, key_column
        )

        result.metrics["forward_coverage_percent"] = fwd.metrics.get("coverage_percent", 0)
        result.metrics["forward_orphan_ids"] = fwd.metrics.get("orphan_ids", 0)
        result.metrics["forward_reference_unique_ids"] = fwd.metrics.get("source_unique_ids", 0)
        result.metrics["reverse_coverage_percent"] = rev.metrics.get("coverage_percent", 0)
        result.metrics["reverse_orphan_ids"] = rev.metrics.get("orphan_ids", 0)
        result.metrics["reverse_target_unique_ids"] = rev.metrics.get("source_unique_ids", 0)

        # `fwd` here = reverse-cardinality direction ("does every reference
        # row have a child in the source?"). On inpatient data this commonly
        # fails legitimately — surface as warning, atomic_count=0.
        for w in fwd.warnings:
            fwd_details = dict(w.get("details", {}) or {})
            fwd_details.setdefault('atomic_count', 0)
            result.add_warning(w['message'], fwd_details)
        for e in fwd.errors:
            # A code-level error on the reverse-cardinality direction still
            # signals a real problem; propagate as-is.
            result.add_error(e['message'], e.get("details", {}))

        # `rev` = FK integrity direction ("does every source FK value resolve
        # in the reference?"). Apply thresholds: >10% orphans → ERROR,
        # >1% → WARNING, else silent pass.
        rev_coverage = result.metrics["reverse_coverage_percent"]
        rev_orphan_pct = 100 - rev_coverage
        for w in rev.warnings:
            if rev_orphan_pct > 10.0:
                result.add_error(w['message'], w.get("details", {}))
            elif rev_orphan_pct > 1.0:
                result.add_warning(w['message'], w.get("details", {}))
            # else: silent pass (≤1% orphans is acceptable drift)
        for e in rev.errors:
            result.add_error(e['message'], e.get("details", {}))

        # Info when both directions are clean (no errors AND no warnings)
        if fwd.passed and rev.passed and not fwd.warnings and not rev.warnings:
            result.add_info(
                f"Full bidirectional coverage for {key_column} between "
                f"{target_table} and {reference_table}"
            )

        _logger.info(
            "check_relational_integrity: '%s' <-> '%s' — "
            "fwd_coverage=%.1f%%, rev_coverage=%.1f%%",
            target_table, reference_table,
            result.metrics["forward_coverage_percent"],
            result.metrics["reverse_coverage_percent"],
        )

        # Atomic granularity: 1 per FK. Only the FK integrity direction
        # (`rev` in this dispatcher) contributes to the scored atom.
        result.atomic_total = 1
        result.atomic_passed = 1 if not result.errors else 0
    except Exception as e:
        _logger.error(
            "Check 'relational_integrity' failed for '%s' <-> '%s': %s",
            target_table, reference_table, e,
        )
        result.add_error(f"Error checking relational integrity: {str(e)}")
        if result.atomic_total is None:
            result.atomic_total = 1
            result.atomic_passed = 0

    return result


# ---------------------------------------------------------------------------
# PLAUSIBILITY CHECKS
# ---------------------------------------------------------------------------

# Helpers for plausibility checks

def _load_outlier_config() -> Dict[str, Any]:
    """Load the outlier_config.yaml for numeric range checks."""
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), 'schemas', 'outlier_config.yaml'
    )
    if not os.path.exists(config_path):
        _logger.warning("Outlier config file not found: %s", config_path)
        return {}
    with open(config_path, 'r') as f:
        return yaml.safe_load(f) or {}


def _get_chronological_order_rules(table_name: str) -> List[Dict[str, str]]:
    """Load chronological order rules for a table from validation_rules.yaml."""
    rules = _load_validation_rules()
    return rules.get('chronological_order', {}).get(table_name, [])


def _get_field_plausibility_rules(table_name: str) -> List[Dict[str, Any]]:
    """Load field plausibility rules for a table from validation_rules.yaml."""
    rules = _load_validation_rules()
    return rules.get('field_plausibility_rules', {}).get(table_name, [])


def _get_composite_keys(table_name: str, schema: Optional[Dict[str, Any]] = None) -> List[str]:
    """Get composite keys for a table from schema or validation_rules.yaml."""
    if schema and 'composite_keys' in schema:
        return schema['composite_keys']
    rules = _load_validation_rules()
    entry = rules.get('composite_keys', {}).get(table_name, {})
    return entry.get('keys', [])


# Category column mapping for numeric range checks
_CATEGORY_COLUMN_MAP = {
    'labs': {'lab_value_numeric': 'lab_category'},
    'vitals': {'vital_value': 'vital_category'},
    'patient_assessments': {'numerical_value': 'assessment_category'},
    'medication_admin_continuous': {'med_dose': ('med_category', 'med_dose_unit')},
    'medication_admin_intermittent': {'med_dose': ('med_category', 'med_dose_unit')},
    'ecmo_mcs': {'sweep': 'device_category', 'flow': 'device_category', 'fdO2': 'device_category'},
}

# Reverse map: (table_name, category_col) → numeric_col (or None to skip avg)
_CATEGORY_TO_NUMERIC_MAP = {}
for _tbl, _col_map in _CATEGORY_COLUMN_MAP.items():
    for _num_col, _cat_col_info in _col_map.items():
        if isinstance(_cat_col_info, tuple):
            # Tuple: (category_col, unit_col) — averaging is safe when grouped by unit
            _CATEGORY_TO_NUMERIC_MAP[(_tbl, _cat_col_info[0])] = _num_col
        else:
            _key = (_tbl, _cat_col_info)
            # Multiple numeric cols → same category (e.g. ecmo_mcs): skip avg
            if _key in _CATEGORY_TO_NUMERIC_MAP:
                _CATEGORY_TO_NUMERIC_MAP[_key] = None
            else:
                _CATEGORY_TO_NUMERIC_MAP[_key] = _num_col

# Maps (table_name, category_col) → unit column name present in the data.
# Tables listed here will GROUP BY this column during monthly aggregation.
_CATEGORY_UNIT_COL_MAP = {
    ('labs', 'lab_category'): 'reference_unit',
    ('medication_admin_continuous', 'med_category'): 'med_dose_unit',
    ('medication_admin_intermittent', 'med_category'): 'med_dose_unit',
}


def _get_schema_units(schema: Dict[str, Any], cat_col: str) -> Optional[Dict[str, str]]:
    """Extract a {category_value: unit} map from schema *_units keys.

    Works for schemas like vitals (vital_units: {temp_c: Celsius, ...}).
    For list values (e.g. labs lab_reference_units), takes the first element.
    Returns None if no matching _units key is found.
    """
    for key, mapping in schema.items():
        if key.endswith('_units') and isinstance(mapping, dict):
            result: Dict[str, str] = {}
            for cat_val, unit in mapping.items():
                if isinstance(unit, list):
                    result[cat_val] = str(unit[0]) if unit else ''
                else:
                    result[cat_val] = str(unit)
            if result:
                return result
    return None


# Datetime columns per table for cross-table temporal checks
_CROSS_TABLE_TIME_COLUMNS = {
    'adt': ['in_dttm', 'out_dttm'],
    'labs': ['lab_order_dttm', 'lab_collect_dttm', 'lab_result_dttm'],
    'vitals': ['recorded_dttm'],
    'respiratory_support': ['recorded_dttm'],
    'medication_admin_continuous': ['admin_dttm'],
    'medication_admin_intermittent': ['admin_dttm'],
    'patient_assessments': ['recorded_dttm'],
    'position': ['recorded_dttm'],
    'microbiology_culture': ['order_dttm', 'collect_dttm', 'result_dttm'],
    'microbiology_nonculture': ['order_dttm', 'collect_dttm', 'result_dttm'],
    'crrt_therapy': ['recorded_dttm'],
    'ecmo_mcs': ['recorded_dttm'],
}

# Time-denominator patterns for medication dose unit checks
_TIME_DENOMINATOR_PATTERNS = ['/sec', '/min', '/hr', '/hour', '/day']


# ---------------------------------------------------------------------------
# A.1 Chronological order
# ---------------------------------------------------------------------------

def check_chronological_order_polars(
    df: Union['pl.DataFrame', 'pl.LazyFrame'],
    table_name: str,
    chronological_rules: Optional[List[Dict[str, str]]] = None,
    warning_threshold: float = 0.0,
    error_threshold: float = 10.0,
) -> DQAPlausibilityResult:
    """Check that datetime pairs follow expected chronological order using Polars."""
    result = DQAPlausibilityResult("chronological_order", table_name)

    if chronological_rules is None:
        chronological_rules = _get_chronological_order_rules(table_name)

    if not chronological_rules:
        result.add_info("No chronological order rules defined for this table")
        result.atomic_total = 0
        result.atomic_passed = 0
        return result

    try:
        lf = df if isinstance(df, pl.LazyFrame) else df.lazy()
        col_names = lf.collect_schema().names()
        violations_by_pair = {}

        for rule in chronological_rules:
            earlier = rule['earlier']
            later = rule['later']
            strict = rule.get('strict', False)
            description = rule.get('description', f"{earlier} {'<' if strict else '<='} {later}")

            if earlier not in col_names or later not in col_names:
                continue

            applicable = lf.filter(
                pl.col(earlier).is_not_null() & pl.col(later).is_not_null()
            )

            if strict:
                violation_expr = (pl.col(earlier) >= pl.col(later)).sum().alias('violations')
            else:
                violation_expr = (pl.col(earlier) > pl.col(later)).sum().alias('violations')

            stats = applicable.select([
                pl.len().alias('total'),
                violation_expr
            ]).collect(engine="streaming")

            total = stats[0, 'total']
            violation_count = stats[0, 'violations']
            pct = (violation_count / total * 100) if total > 0 else 0

            violations_by_pair[f"{earlier}->{later}"] = {
                "total_applicable": int(total),
                "violations": int(violation_count),
                "violation_percent": round(pct, 2),
                "description": description,
            }

            if pct > error_threshold:
                result.add_error(
                    f"Chronological order violation: {description} — {violation_count}/{total} rows ({pct:.1f}%)",
                    {"column": f"{earlier}, {later}",
                     "pair": f"{earlier}->{later}", "violations": int(violation_count),
                     "total": int(total), "percent": round(pct, 2)}
                )
            elif pct > warning_threshold:
                result.add_warning(
                    f"Chronological order violation: {description} — {violation_count}/{total} rows ({pct:.1f}%)",
                    {"column": f"{earlier}, {later}",
                     "pair": f"{earlier}->{later}", "violations": int(violation_count),
                     "total": int(total), "percent": round(pct, 2)}
                )
            else:
                if total > 0:
                    result.add_info(
                        f"Chronological order satisfied: {description} — {int(total):,}/{int(total):,} rows valid (100%)",
                        {"column": f"{earlier}, {later}",
                         "rows_checked": int(total),
                         "rows_valid": int(total),
                         "percent_valid": 100.0}
                    )

        result.metrics["pairs_checked"] = len(violations_by_pair)
        result.metrics["violations_by_pair"] = violations_by_pair

        gc.collect()

        result.atomic_total = len(chronological_rules)
        result.atomic_passed = len(chronological_rules) - len(result.errors)
    except Exception as e:
        _logger.error("Check 'chronological_order' failed for table '%s': %s", table_name, e)
        result.add_error(f"Error checking chronological order: {str(e)}")
        if result.atomic_total is None:
            result.atomic_total = len(chronological_rules) if chronological_rules else 1
            result.atomic_passed = 0

    return result


def check_chronological_order(
    df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    table_name: str,
    chronological_rules: Optional[List[Dict[str, str]]] = None,
    warning_threshold: float = 0.0,
    error_threshold: float = 10.0,
) -> DQAPlausibilityResult:
    """Check that datetime pairs follow expected chronological order."""
    _logger.debug("check_chronological_order: starting for table '%s'", table_name)
    result = check_chronological_order_polars(df, table_name, chronological_rules,
                                            warning_threshold=warning_threshold,
                                            error_threshold=error_threshold)
    _logger.debug("check_chronological_order: table '%s' — pairs_checked=%s",
                  table_name, result.metrics.get("pairs_checked"))
    return result


# ---------------------------------------------------------------------------
# A.2 Numeric range plausibility
# ---------------------------------------------------------------------------

def check_numeric_range_plausibility_polars(
    df: Union['pl.DataFrame', 'pl.LazyFrame'],
    table_name: str,
    outlier_config: Optional[Dict[str, Any]] = None,
    warning_threshold: float = 0.0,
    error_threshold: float = 10.0,
) -> DQAPlausibilityResult:
    """Check numeric values are within plausible ranges using Polars."""
    result = DQAPlausibilityResult("numeric_range_plausibility", table_name)

    if outlier_config is None:
        outlier_config = _load_outlier_config()

    table_config = outlier_config.get('tables', {}).get(table_name, {})
    if not table_config:
        result.add_info("No numeric range configuration for this table")
        result.atomic_total = 0
        result.atomic_passed = 0
        return result

    try:
        lf = df if isinstance(df, pl.LazyFrame) else df.lazy()
        col_names = lf.collect_schema().names()
        oor_summary = {}
        cat_map = _CATEGORY_COLUMN_MAP.get(table_name, {})
        # Atomic counting: one atomic check per leaf (col, [cat], [unit])
        # range tuple the check actually evaluates. Silent passes (no warning
        # or error emitted) count as passed.
        atomic_total = 0
        atomic_passed = 0

        # Pre-cast: ensure all configured columns are numeric (string → Float64)
        schema = lf.collect_schema()
        cast_exprs = []
        for _col in table_config:
            if _col in schema.names():
                _dtype = schema[_col]
                if _dtype in (pl.Utf8, pl.String):
                    cast_exprs.append(pl.col(_col).cast(pl.Float64, strict=False))
        if cast_exprs:
            lf = lf.with_columns(cast_exprs)

        for col_name, col_ranges in table_config.items():
            if col_name not in col_names:
                continue

            if isinstance(col_ranges, dict) and 'min' in col_ranges and 'max' in col_ranges:
                # Simple range: one atomic leaf
                atomic_total += 1
                rmin, rmax = col_ranges['min'], col_ranges['max']
                stats = lf.select([
                    pl.col(col_name).drop_nulls().len().alias('total'),
                    ((pl.col(col_name) < rmin) | (pl.col(col_name) > rmax)).sum().alias('oor'),
                    (pl.col(col_name) < rmin).sum().alias('below'),
                    (pl.col(col_name) > rmax).sum().alias('above'),
                ]).collect(engine="streaming")

                # Polars sum() over an all-null column returns None — coerce to 0.
                total = stats[0, 'total'] or 0
                oor = stats[0, 'oor'] or 0
                below = stats[0, 'below'] or 0
                above = stats[0, 'above'] or 0
                pct = (oor / total * 100) if total > 0 else 0

                oor_summary[col_name] = {
                    "total_non_null": int(total), "out_of_range": int(oor),
                    "out_of_range_percent": round(pct, 2),
                    "below_min": int(below), "above_max": int(above),
                    "min": rmin, "max": rmax,
                }

                # Lenient: only errors fail the atom. Warnings (above
                # warning_threshold but below error_threshold) still emit
                # a warning row but count as a pass. Silent passes get
                # their own INFO row so the synth reconcile doesn't inherit
                # 16 columns' worth of warnings into a 1-atom INFO.
                if pct > error_threshold:
                    result.add_error(
                        f"Column '{col_name}': {oor}/{total} values ({pct:.1f}%) outside range [{rmin}, {rmax}]",
                        {"column": col_name, "percent": round(pct, 2)}
                    )
                else:
                    if pct > warning_threshold:
                        result.add_warning(
                            f"Column '{col_name}': {oor}/{total} values ({pct:.1f}%) outside range [{rmin}, {rmax}]",
                            {"column": col_name, "percent": round(pct, 2)}
                        )
                    else:
                        result.add_info(
                            f"Column '{col_name}': all {int(total):,} values within range [{rmin}, {rmax}]",
                            {"column": col_name,
                             "total": int(total),
                             "atomic_count": 1}
                        )
                    atomic_passed += 1

            elif isinstance(col_ranges, dict):
                # Category-dependent ranges
                cat_col_info = cat_map.get(col_name)
                if cat_col_info is None:
                    continue

                if isinstance(cat_col_info, tuple):
                    # 2-level: (category_col, unit_col) — batched single scan
                    cat_col, unit_col = cat_col_info
                    if cat_col not in col_names or unit_col not in col_names:
                        continue

                    combo_keys = []  # (cat_val, unit_val, rmin, rmax)
                    range_exprs = []
                    lf_cat = lf.with_columns([
                        pl.col(cat_col).cast(pl.Utf8).str.to_lowercase().str.strip_chars().alias('_cat_norm'),
                        pl.col(unit_col).cast(pl.Utf8).str.to_lowercase().str.strip_chars().alias('_unit_norm'),
                    ])

                    for cat_val, unit_ranges in col_ranges.items():
                        if not isinstance(unit_ranges, dict):
                            continue
                        for unit_val, ranges in unit_ranges.items():
                            if not isinstance(ranges, dict) or 'min' not in ranges:
                                continue
                            rmin, rmax = ranges['min'], ranges['max']
                            idx = len(combo_keys)
                            combo_keys.append((cat_val, unit_val, rmin, rmax))
                            cat_norm = str(cat_val).lower().strip()
                            unit_norm = str(unit_val).lower().strip()
                            mask = (
                                (pl.col('_cat_norm') == cat_norm) &
                                (pl.col('_unit_norm') == unit_norm) &
                                pl.col(col_name).is_not_null()
                            )
                            oor = (pl.col(col_name) < rmin) | (pl.col(col_name) > rmax)
                            range_exprs.append(mask.sum().alias(f'_t{idx}'))
                            range_exprs.append((mask & oor).sum().alias(f'_o{idx}'))

                    if not range_exprs:
                        continue

                    atomic_total += len(combo_keys)
                    stats_row = lf_cat.select(range_exprs).collect(engine="streaming")
                    total_oor = 0
                    total_count = 0
                    for idx, (cat_val, unit_val, rmin, rmax) in enumerate(combo_keys):
                        cat_total = int(stats_row[0, f'_t{idx}'] or 0)
                        cat_oor = int(stats_row[0, f'_o{idx}'] or 0)
                        cat_pct = (cat_oor / cat_total * 100) if cat_total > 0 else 0
                        total_count += cat_total
                        total_oor += cat_oor

                        # Lenient: only errors fail the atom.
                        if cat_oor > 0 and cat_pct > error_threshold:
                            result.add_error(
                                f"Column '{col_name}' ({cat_val}, {unit_val}): {cat_oor}/{cat_total} values ({cat_pct:.1f}%) outside range [{rmin}, {rmax}]",
                                {"column": col_name, "category": cat_val, "unit": unit_val, "percent": round(cat_pct, 2)}
                            )
                        else:
                            if cat_oor > 0 and cat_pct > warning_threshold:
                                result.add_warning(
                                    f"Column '{col_name}' ({cat_val}, {unit_val}): {cat_oor}/{cat_total} values ({cat_pct:.1f}%) outside range [{rmin}, {rmax}]",
                                    {"column": col_name, "category": cat_val, "unit": unit_val, "percent": round(cat_pct, 2)}
                                )
                            atomic_passed += 1

                    pct = (total_oor / total_count * 100) if total_count > 0 else 0
                    oor_summary[col_name] = {
                        "total_non_null": int(total_count), "out_of_range": int(total_oor),
                        "out_of_range_percent": round(pct, 2),
                    }
                else:
                    # 1-level category-dependent — batched single scan
                    cat_col = cat_col_info
                    if cat_col not in col_names:
                        continue

                    combo_keys = []  # (cat_val, rmin, rmax)
                    range_exprs = []
                    lf_cat = lf.with_columns(
                        pl.col(cat_col).cast(pl.Utf8).str.to_lowercase().str.strip_chars().alias('_cat_norm')
                    )

                    for cat_val, ranges in col_ranges.items():
                        if not isinstance(ranges, dict) or 'min' not in ranges:
                            continue
                        rmin, rmax = ranges['min'], ranges['max']
                        idx = len(combo_keys)
                        combo_keys.append((cat_val, rmin, rmax))
                        cat_norm = str(cat_val).lower().strip()
                        mask = (pl.col('_cat_norm') == cat_norm) & pl.col(col_name).is_not_null()
                        oor = (pl.col(col_name) < rmin) | (pl.col(col_name) > rmax)
                        range_exprs.append(mask.sum().alias(f'_t{idx}'))
                        range_exprs.append((mask & oor).sum().alias(f'_o{idx}'))

                    if not range_exprs:
                        continue

                    atomic_total += len(combo_keys)
                    stats_row = lf_cat.select(range_exprs).collect(engine="streaming")
                    total_oor = 0
                    total_count = 0
                    for idx, (cat_val, rmin, rmax) in enumerate(combo_keys):
                        cat_total = int(stats_row[0, f'_t{idx}'] or 0)
                        cat_oor = int(stats_row[0, f'_o{idx}'] or 0)
                        cat_pct = (cat_oor / cat_total * 100) if cat_total > 0 else 0
                        total_count += cat_total
                        total_oor += cat_oor

                        # Lenient: only errors fail the atom.
                        if cat_oor > 0 and cat_pct > error_threshold:
                            result.add_error(
                                f"Column '{col_name}' ({cat_val}): {cat_oor}/{cat_total} values ({cat_pct:.1f}%) outside range [{rmin}, {rmax}]",
                                {"column": col_name, "category": cat_val, "percent": round(cat_pct, 2)}
                            )
                        else:
                            if cat_oor > 0 and cat_pct > warning_threshold:
                                result.add_warning(
                                    f"Column '{col_name}' ({cat_val}): {cat_oor}/{cat_total} values ({cat_pct:.1f}%) outside range [{rmin}, {rmax}]",
                                    {"column": col_name, "category": cat_val, "percent": round(cat_pct, 2)}
                                )
                            atomic_passed += 1

                    pct = (total_oor / total_count * 100) if total_count > 0 else 0
                    oor_summary[col_name] = {
                        "total_non_null": int(total_count), "out_of_range": int(total_oor),
                        "out_of_range_percent": round(pct, 2),
                    }

        result.metrics["columns_checked"] = len(oor_summary)
        result.metrics["out_of_range_summary"] = oor_summary
        result.atomic_total = atomic_total
        result.atomic_passed = atomic_passed

        if not result.warnings and not result.errors:
            if oor_summary:
                cols_str = ', '.join(sorted(oor_summary.keys()))
                result.add_info(
                    f"All numeric values within plausible ranges ({cols_str})",
                    {"columns_checked": sorted(oor_summary.keys())},
                )
            else:
                result.add_info("No numeric columns with range configuration to check")

        gc.collect()

    except Exception as e:
        _logger.error("Check 'numeric_range_plausibility' failed for table '%s': %s", table_name, e)
        result.add_error(f"Error checking numeric range plausibility: {str(e)}")
        if result.atomic_total is None:
            result.atomic_total = 1
            result.atomic_passed = 0

    return result


def check_numeric_range_plausibility(
    df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    table_name: str,
    outlier_config: Optional[Dict[str, Any]] = None,
    warning_threshold: float = 0.0,
    error_threshold: float = 10.0,
) -> DQAPlausibilityResult:
    """Check numeric values are within plausible ranges."""
    _logger.debug("check_numeric_range_plausibility: starting for table '%s'", table_name)
    result = check_numeric_range_plausibility_polars(df, table_name, outlier_config,
                                                     warning_threshold=warning_threshold,
                                                     error_threshold=error_threshold)
    _logger.debug("check_numeric_range_plausibility: table '%s' — columns_checked=%s",
                  table_name, result.metrics.get("columns_checked"))
    return result


# ---------------------------------------------------------------------------
# A.3 Field-level plausibility rules
# ---------------------------------------------------------------------------

def _report_empty_strings(
    result: Union['DQAPlausibilityResult', 'DQAConformanceResult'],
    table_name: str,
    column: str,
    count: int,
    seen: Dict[str, int],
) -> None:
    """Emit one per-column advisory when '' is used instead of null.

    Field plausibility treats empty/whitespace-only strings as absent, so
    they never count as rule violations — but '' instead of null is an ETL
    format deviation worth its own warning.
    """
    if count <= 0 or column in seen:
        return
    seen[column] = count
    result.add_warning(
        f"Column '{column}' has {count:,} rows with empty strings ('') where "
        f"null is expected. Empty strings were treated as missing for this "
        f"check. Please convert '' to null in your ETL.",
        {"column": column, "empty_string_rows": count,
         "recommendation": "Store missing values as null, not ''"}
    )


def check_field_plausibility_polars(
    df: Union['pl.DataFrame', 'pl.LazyFrame'],
    table_name: str,
    rules: Optional[List[Dict[str, Any]]] = None,
) -> DQAPlausibilityResult:
    """Check field-level plausibility constraints using Polars."""
    result = DQAPlausibilityResult("field_plausibility", table_name)

    if rules is None:
        rules = _get_field_plausibility_rules(table_name)

    if not rules:
        result.add_info("No field plausibility rules defined for this table")
        result.atomic_total = 0
        result.atomic_passed = 0
        return result

    try:
        lf = df if isinstance(df, pl.LazyFrame) else df.lazy()
        col_names = lf.collect_schema().names()
        violations_by_rule = {}
        empty_string_columns = {}

        for rule in rules:
            when_col = rule['when_column']
            description = rule.get('description', '')

            if when_col not in col_names:
                continue

            # New-variant: when_not_null + then_column + then_not_value
            if rule.get('when_not_null'):
                then_col = rule['then_column']
                forbidden = rule['then_not_value']
                if not isinstance(forbidden, list):
                    forbidden = [forbidden]

                if then_col not in col_names:
                    continue

                # Empty strings carry no value: treat them like nulls in the filter
                when_absent = (
                    pl.col(when_col).is_null()
                    | (pl.col(when_col).cast(pl.Utf8).str.strip_chars() == "")
                )
                filtered = lf.filter(~when_absent)
                stats = filtered.select([
                    pl.len().alias('total'),
                    pl.col(then_col).cast(pl.Utf8).str.to_lowercase().str.strip_chars().is_in(
                        [str(v).lower().strip() for v in forbidden]
                    ).sum().alias('violations')
                ]).collect(engine="streaming")

                total = stats[0, 'total']
                violations = stats[0, 'violations']
                pct = (violations / total * 100) if total > 0 else 0

                if violations > 0:
                    violations_by_rule[description] = {
                        "total_applicable": int(total),
                        "violations": int(violations),
                        "violation_percent": round(pct, 2),
                    }
                    result.add_warning(
                        f"Field plausibility violation: {description} — {violations}/{total} rows ({pct:.1f}%)",
                        {"rule": description, "violations": int(violations),
                         "total": int(total), "percent": round(pct, 2)}
                    )
                else:
                    if total > 0:
                        result.add_info(
                            f"Field plausibility satisfied: {description} — {int(total):,}/{int(total):,} rows valid (100%)",
                            {"column": then_col,
                             "rows_checked": int(total),
                             "rows_valid": int(total),
                             "percent_valid": 100.0}
                        )
                continue

            # Old-variant: when_not_value + then_null_or_absent
            when_not_values = rule['when_not_value']
            then_null_cols = rule['then_null_or_absent']

            if not isinstance(when_not_values, list):
                when_not_values = [when_not_values]

            # Filter rows where when_col is NOT in the specified values
            filtered = lf.filter(
                ~pl.col(when_col).cast(pl.Utf8).str.to_lowercase().str.strip_chars().is_in(
                    [str(v).lower().strip() for v in when_not_values]
                )
            )

            for check_col in then_null_cols:
                if check_col not in col_names:
                    continue

                # "" carries no value: only non-null, non-empty values violate
                # the rule. Empty strings are surfaced separately below.
                stripped = pl.col(check_col).cast(pl.Utf8).str.strip_chars()
                present = pl.col(check_col).is_not_null() & (stripped != "")
                stats = filtered.select([
                    pl.len().alias('total'),
                    present.sum().alias('non_null')
                ]).collect(engine="streaming")

                empty_count = int(
                    lf.select(
                        (pl.col(check_col).is_not_null() & (stripped == ""))
                        .sum().alias('n')
                    ).collect(engine="streaming")[0, 'n']
                )
                _report_empty_strings(result, table_name, check_col, empty_count,
                                      empty_string_columns)

                total = stats[0, 'total']
                non_null = stats[0, 'non_null']
                pct = (non_null / total * 100) if total > 0 else 0

                if non_null > 0:
                    violations_by_rule[description] = {
                        "total_applicable": int(total),
                        "violations": int(non_null),
                        "violation_percent": round(pct, 2),
                    }
                    result.add_warning(
                        f"Field plausibility violation: {description} — {non_null}/{total} rows ({pct:.1f}%)",
                        {"rule": description, "violations": int(non_null),
                         "total": int(total), "percent": round(pct, 2)}
                    )
                else:
                    if total > 0:
                        result.add_info(
                            f"Field plausibility satisfied: {description} — {int(total):,}/{int(total):,} rows valid (100%)",
                            {"column": check_col,
                             "rows_checked": int(total),
                             "rows_valid": int(total),
                             "percent_valid": 100.0}
                        )

        result.metrics["rules_checked"] = len(rules)
        result.metrics["violations_by_rule"] = violations_by_rule
        result.metrics["empty_string_columns"] = empty_string_columns

        gc.collect()

        result.atomic_total = len(rules)
        result.atomic_passed = len(rules) - len(result.errors)
    except Exception as e:
        _logger.error("Check 'field_plausibility' failed for table '%s': %s", table_name, e)
        result.add_error(f"Error checking field plausibility: {str(e)}")
        if result.atomic_total is None:
            result.atomic_total = len(rules) if rules else 1
            result.atomic_passed = 0

    return result


def check_field_plausibility(
    df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    table_name: str,
    rules: Optional[List[Dict[str, Any]]] = None,
) -> DQAPlausibilityResult:
    """Check field-level plausibility constraints."""
    _logger.debug("check_field_plausibility: starting for table '%s'", table_name)
    result = check_field_plausibility_polars(df, table_name, rules)
    _logger.debug("check_field_plausibility: table '%s' — rules_checked=%s",
                  table_name, result.metrics.get("rules_checked"))
    return result


# ---------------------------------------------------------------------------
# A.4 Medication dose unit consistency
# ---------------------------------------------------------------------------

def check_medication_dose_unit_consistency_polars(
    df: Union['pl.DataFrame', 'pl.LazyFrame'],
    table_name: str,
    warning_threshold: float = 0.0,
    error_threshold: float = 10.0,
) -> DQAPlausibilityResult:
    """Check medication dose unit consistency using Polars."""
    result = DQAPlausibilityResult("medication_dose_unit_consistency", table_name)

    if table_name not in ('medication_admin_continuous', 'medication_admin_intermittent'):
        result.add_info("Medication dose unit check not applicable to this table")
        result.atomic_total = 0
        result.atomic_passed = 0
        return result

    try:
        lf = df if isinstance(df, pl.LazyFrame) else df.lazy()
        col_names = lf.collect_schema().names()

        if 'med_dose_unit' not in col_names:
            result.add_info("Column 'med_dose_unit' not found in table")
            result.atomic_total = 0
            result.atomic_passed = 0
            return result

        rules = _load_validation_rules().get('medication_dose_unit_rules', {}).get(table_name, {})
        expect = rules.get('expect', 'per_time' if 'continuous' in table_name else 'discrete')
        exempt_units = [str(u).lower().strip() for u in rules.get('exempt_units', [])]

        # Build expression to detect time denominators
        has_time_denom = pl.lit(False)
        for pat in _TIME_DENOMINATOR_PATTERNS:
            has_time_denom = has_time_denom | pl.col('med_dose_unit').cast(pl.Utf8).str.contains(pat.replace('/', '\\/'))

        # Units exempt from the per-time requirement (e.g. nitric_oxide in ppm)
        is_exempt = pl.lit(False)
        if exempt_units:
            is_exempt = (
                pl.col('med_dose_unit').cast(pl.Utf8).str.strip_chars()
                .str.to_lowercase().is_in(exempt_units)
            )

        non_null = lf.filter(pl.col('med_dose_unit').is_not_null())
        total = non_null.select(pl.len()).collect(engine="streaming").item()

        if total == 0:
            result.add_info("No non-null med_dose_unit values to check")
            result.atomic_total = 0
            result.atomic_passed = 0
            return result

        if expect == 'per_time':
            # Continuous: expect time denominators
            violations = non_null.filter(~has_time_denom & ~is_exempt).select(pl.len()).collect(engine="streaming").item()
        else:
            # Intermittent: expect NO time denominators
            violations = non_null.filter(has_time_denom).select(pl.len()).collect(engine="streaming").item()

        pct = (violations / total * 100) if total > 0 else 0

        result.metrics["total_rows"] = int(total)
        result.metrics["unit_pattern_violations"] = int(violations)
        result.metrics["violation_percent"] = round(pct, 2)

        if violations > 0:
            # Get sample violations
            if expect == 'per_time':
                sample = non_null.filter(~has_time_denom & ~is_exempt)
            else:
                sample = non_null.filter(has_time_denom)

            sample_cols = ['med_dose_unit']
            if 'med_category' in col_names:
                sample_cols.insert(0, 'med_category')

            sample_data = (
                sample
                .group_by(sample_cols)
                .agg(pl.len().alias('count'))
                .sort('count', descending=True)
                .head(10)
                .collect(engine="streaming")
            )
            sample_list = sample_data.to_dicts()
            result.metrics["sample_violations"] = sample_list

            if expect == 'per_time':
                warn_msg = f"Medication dose unit inconsistency: {violations}/{total} rows ({pct:.1f}%) use non-rate-based units unexpected for continuous administration"
            else:
                warn_msg = f"Medication dose unit inconsistency: {violations}/{total} rows ({pct:.1f}%) use rate-based units unexpected for intermittent administration"
            if pct > error_threshold:
                result.add_error(warn_msg, {"violations": int(violations), "total": int(total), "percent": round(pct, 2)})
            elif pct > warning_threshold:
                result.add_warning(warn_msg, {"violations": int(violations), "total": int(total), "percent": round(pct, 2)})
        else:
            if expect == 'per_time':
                result.add_info("All med_dose_unit values use rate-based units (e.g. mcg/kg/min) appropriate for continuous administration")
            else:
                result.add_info("All med_dose_unit values use dose-based units (e.g. mg, mL) appropriate for intermittent administration")

        gc.collect()

        result.atomic_total = 1
        result.atomic_passed = 1 if not result.errors else 0
    except Exception as e:
        _logger.error("Check 'medication_dose_unit_consistency' failed for table '%s': %s", table_name, e)
        result.add_error(f"Error checking medication dose unit consistency: {str(e)}")
        if result.atomic_total is None:
            result.atomic_total = 1
            result.atomic_passed = 0

    return result


def check_medication_dose_unit_consistency(
    df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    table_name: str,
    warning_threshold: float = 0.0,
    error_threshold: float = 10.0,
) -> DQAPlausibilityResult:
    """Check medication dose unit consistency."""
    _logger.debug("check_medication_dose_unit_consistency: starting for table '%s'", table_name)
    result = check_medication_dose_unit_consistency_polars(df, table_name,
                                                           warning_threshold=warning_threshold,
                                                           error_threshold=error_threshold)
    _logger.debug("check_medication_dose_unit_consistency: table '%s' — violations=%s",
                  table_name, result.metrics.get("unit_pattern_violations"))
    return result


# ---------------------------------------------------------------------------
# B.1 Cross-table temporal plausibility
# ---------------------------------------------------------------------------

def check_cross_table_temporal_plausibility_polars(
    target_df: Union['pl.DataFrame', 'pl.LazyFrame'],
    hospitalization_df: Union['pl.DataFrame', 'pl.LazyFrame'],
    target_table: str,
    time_columns: List[str],
    warning_threshold: float = 0.0,
    error_threshold: float = 10.0,
) -> DQAPlausibilityResult:
    """Check that datetime values fall within hospitalization bounds using Polars."""
    result = DQAPlausibilityResult("cross_table_temporal", target_table)

    try:
        target_lf = target_df if isinstance(target_df, pl.LazyFrame) else target_df.lazy()
        hosp_lf = hospitalization_df if isinstance(hospitalization_df, pl.LazyFrame) else hospitalization_df.lazy()

        target_cols = target_lf.collect_schema().names()
        hosp_cols = hosp_lf.collect_schema().names()

        if 'hospitalization_id' not in target_cols or 'hospitalization_id' not in hosp_cols:
            result.add_info("Missing hospitalization_id column; skipping cross-table check")
            result.atomic_total = 0
            result.atomic_passed = 0
            return result

        if 'admission_dttm' not in hosp_cols or 'discharge_dttm' not in hosp_cols:
            result.add_info("Missing admission/discharge columns in hospitalization table")
            result.atomic_total = 0
            result.atomic_passed = 0
            return result

        hosp_bounds = hosp_lf.select([
            'hospitalization_id', 'admission_dttm', 'discharge_dttm'
        ])

        joined = target_lf.join(hosp_bounds, on='hospitalization_id', how='inner')
        violations_by_col = {}

        for time_col in time_columns:
            if time_col not in target_cols:
                continue

            applicable = joined.filter(
                pl.col(time_col).is_not_null() &
                pl.col('admission_dttm').is_not_null() &
                pl.col('discharge_dttm').is_not_null()
            )

            stats = applicable.select([
                pl.len().alias('total'),
                (pl.col(time_col) < pl.col('admission_dttm')).sum().alias('before_admission'),
                (pl.col(time_col) > pl.col('discharge_dttm')).sum().alias('after_discharge'),
            ]).collect(engine="streaming")

            total = stats[0, 'total']
            before = stats[0, 'before_admission']
            after = stats[0, 'after_discharge']
            violation_total = before + after
            pct = (violation_total / total * 100) if total > 0 else 0

            violations_by_col[time_col] = {
                "total_joined": int(total),
                "before_admission": int(before),
                "after_discharge": int(after),
                "violation_count": int(violation_total),
                "violation_percent": round(pct, 2),
            }

            if pct > error_threshold:
                result.add_error(
                    f"Column '{time_col}': {violation_total}/{total} records ({pct:.1f}%) outside admission-to-discharge window ({before} before admission, {after} after discharge)",
                    {"column": time_col, "before_admission": int(before),
                     "after_discharge": int(after), "percent": round(pct, 2)}
                )
            elif pct > warning_threshold:
                result.add_warning(
                    f"Column '{time_col}': {violation_total}/{total} records ({pct:.1f}%) outside admission-to-discharge window ({before} before admission, {after} after discharge)",
                    {"column": time_col, "before_admission": int(before),
                     "after_discharge": int(after), "percent": round(pct, 2)}
                )
            else:
                result.add_info(
                    f"Column '{time_col}': all records within admission-to-discharge window",
                    {"column": time_col, "total_joined": int(total)}
                )

        result.metrics["time_columns_checked"] = list(violations_by_col.keys())
        result.metrics["violations_by_column"] = violations_by_col

        gc.collect()

        result.atomic_total = max(1, len(violations_by_col))
        result.atomic_passed = result.atomic_total - len(result.errors)
    except Exception as e:
        _logger.error("Check 'cross_table_temporal' failed for table '%s': %s", target_table, e)
        result.add_error(f"Error checking cross-table temporal plausibility: {str(e)}")
        if result.atomic_total is None:
            result.atomic_total = 1
            result.atomic_passed = 0

    return result


def check_cross_table_temporal_plausibility(
    target_df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    hospitalization_df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    target_table: str,
    time_columns: List[str],
    warning_threshold: float = 0.0,
    error_threshold: float = 10.0,
) -> DQAPlausibilityResult:
    """Check that datetime values fall within hospitalization bounds."""
    _logger.debug("check_cross_table_temporal_plausibility: starting for table '%s'", target_table)
    result = check_cross_table_temporal_plausibility_polars(
        target_df, hospitalization_df, target_table, time_columns,
        warning_threshold=warning_threshold, error_threshold=error_threshold)
    _logger.debug("check_cross_table_temporal_plausibility: table '%s' complete", target_table)
    return result


# ---------------------------------------------------------------------------
# C.1 Overlapping time periods
# ---------------------------------------------------------------------------

def check_overlapping_periods_polars(
    df: Union['pl.DataFrame', 'pl.LazyFrame'],
    table_name: str,
    entity_col: str = 'hospitalization_id',
    start_col: str = 'in_dttm',
    end_col: str = 'out_dttm',
) -> DQAPlausibilityResult:
    """Check for overlapping time periods within entities using Polars."""
    result = DQAPlausibilityResult("overlapping_periods", table_name)

    try:
        lf = df if isinstance(df, pl.LazyFrame) else df.lazy()
        col_names = lf.collect_schema().names()

        if entity_col not in col_names or start_col not in col_names or end_col not in col_names:
            result.add_info(f"Required columns ({entity_col}, {start_col}, {end_col}) not all present")
            result.atomic_total = 0
            result.atomic_passed = 0
            return result

        # Filter to rows where both start and end are non-null, sort, then compare with previous
        sorted_lf = lf.filter(
            pl.col(start_col).is_not_null() & pl.col(end_col).is_not_null()
        ).sort([entity_col, start_col])

        with_prev = sorted_lf.with_columns(
            pl.col(end_col).shift(1).over(entity_col).alias('_prev_end')
        )

        overlaps_df = with_prev.filter(
            pl.col('_prev_end').is_not_null() & (pl.col(start_col) < pl.col('_prev_end'))
        )

        total_records = sorted_lf.select(pl.len()).collect(engine="streaming").item()
        overlap_count = overlaps_df.select(pl.len()).collect(engine="streaming").item()
        entities_checked = sorted_lf.select(
            pl.col(entity_col).n_unique()
        ).collect(engine="streaming").item()
        pct = (overlap_count / total_records * 100) if total_records > 0 else 0

        result.metrics["total_records"] = int(total_records)
        result.metrics["entities_checked"] = int(entities_checked)
        result.metrics["overlapping_records"] = int(overlap_count)
        result.metrics["overlap_percent"] = round(pct, 2)

        if overlap_count > 0:
            result.add_warning(
                f"{overlap_count} overlapping time periods detected ({pct:.1f}% of records)",
                {"column": f"{start_col}, {end_col}",
                 "overlapping_records": int(overlap_count), "percent": round(pct, 2)}
            )
        else:
            result.add_info(
                f"No overlapping time periods detected for {entity_col} on {start_col}/{end_col} — {int(total_records):,} records checked",
                {"column": f"{start_col}, {end_col}",
                 "entity_col": entity_col,
                 "records_checked": int(total_records),
                 "entities_checked": int(entities_checked)}
            )

        gc.collect()

        result.atomic_total = 1
        result.atomic_passed = 1 if not result.errors else 0
    except Exception as e:
        _logger.error("Check 'overlapping_periods' failed for table '%s': %s", table_name, e)
        result.add_error(f"Error checking overlapping periods: {str(e)}")
        if result.atomic_total is None:
            result.atomic_total = 1
            result.atomic_passed = 0

    return result


def check_overlapping_periods(
    df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    table_name: str,
    entity_col: str = 'hospitalization_id',
    start_col: str = 'in_dttm',
    end_col: str = 'out_dttm',
) -> DQAPlausibilityResult:
    """Check for overlapping time periods within entities."""
    _logger.debug("check_overlapping_periods: starting for table '%s'", table_name)
    result = check_overlapping_periods_polars(df, table_name, entity_col, start_col, end_col)
    _logger.debug("check_overlapping_periods: table '%s' — overlaps=%s",
                  table_name, result.metrics.get("overlapping_records"))
    return result


# ---------------------------------------------------------------------------
# C.2 Category temporal consistency
# ---------------------------------------------------------------------------

def _detect_time_column(col_names: List[str], table_name: str) -> Optional[str]:
    """Auto-detect the primary datetime column for a table."""
    candidates = ['recorded_dttm', 'admin_dttm', 'admission_dttm',
                   'lab_result_dttm', 'in_dttm', 'procedure_billed_dttm',
                   'result_dttm', 'start_dttm']
    for c in candidates:
        if c in col_names:
            return c
    return None


def check_category_temporal_consistency_polars(
    df: Union['pl.DataFrame', 'pl.LazyFrame'],
    schema: Dict[str, Any],
    table_name: str,
    time_column: Optional[str] = None,
    hosp_years: Optional[set] = None,
) -> DQAPlausibilityResult:
    """Check category distribution consistency over time using Polars."""
    result = DQAPlausibilityResult("category_temporal_consistency", table_name)

    try:
        lf = df if isinstance(df, pl.LazyFrame) else df.lazy()
        col_names = lf.collect_schema().names()

        if time_column is None:
            time_column = _detect_time_column(col_names, table_name)

        if time_column is None or time_column not in col_names:
            result.add_info("No suitable datetime column found for temporal consistency check")
            result.atomic_total = 0
            result.atomic_passed = 0
            return result

        # Find category columns from schema
        category_columns = schema.get('category_columns') or []
        cat_cols_present = [c for c in category_columns if c in col_names]

        if not cat_cols_present:
            result.add_info("No category columns found for temporal consistency check")
            result.atomic_total = 0
            result.atomic_passed = 0
            return result

        # Need hospitalization_id for unique ID counting
        id_col = 'hospitalization_id' if 'hospitalization_id' in col_names else (
            'patient_id' if 'patient_id' in col_names else None
        )

        yearly_distributions = {}
        missing_in_years = {}
        monthly_trends = {}

        for cat_col in cat_cols_present:
            # Get yearly distribution
            if id_col:
                yearly = (
                    lf.filter(pl.col(time_column).is_not_null() & pl.col(cat_col).is_not_null())
                    .with_columns(pl.col(time_column).dt.year().alias('_year'))
                    .group_by(['_year', cat_col])
                    .agg(pl.col(id_col).n_unique().alias('unique_ids'))
                    .sort(['_year', cat_col])
                    .collect(engine="streaming")
                )
            else:
                yearly = (
                    lf.filter(pl.col(time_column).is_not_null() & pl.col(cat_col).is_not_null())
                    .with_columns(pl.col(time_column).dt.year().alias('_year'))
                    .group_by(['_year', cat_col])
                    .agg(pl.len().alias('unique_ids'))
                    .sort(['_year', cat_col])
                    .collect(engine="streaming")
                )

            if len(yearly) == 0:
                continue

            # Build year -> {value: count} dict
            dist = {}
            all_years = set()
            all_values = set()
            for row in yearly.iter_rows(named=True):
                year = int(row['_year'])
                val = row[cat_col]
                count = row['unique_ids']
                all_years.add(year)
                all_values.add(val)
                dist.setdefault(year, {})[val] = int(count)

            yearly_distributions[cat_col] = dist

            # Check for values absent in some years (requires >= 2 years)
            if len(all_years) >= 2:
                absent = {}
                for val in all_values:
                    absent_years = [y for y in sorted(all_years) if val not in dist.get(y, {})]
                    if absent_years and len(absent_years) < len(all_years):
                        absent[str(val)] = absent_years
                if absent:
                    missing_in_years[cat_col] = absent

            # Monthly trends for CSV export
            num_col = _CATEGORY_TO_NUMERIC_MAP.get((table_name, cat_col))
            unit_col = _CATEGORY_UNIT_COL_MAP.get((table_name, cat_col))
            avg_expr = ([pl.col(num_col).mean().alias('avg')]
                        if num_col and num_col in col_names else [])

            group_cols = ['month_year', cat_col]
            if unit_col and unit_col in col_names:
                group_cols.append(unit_col)

            # n = unique hospitalization_ids (matches yearly distribution above and the
            # PDF YearlySparkBar sparklines). avg remains a row-weighted mean, so n and
            # avg use different denominators — n * avg does NOT approximate the sum of
            # values. For tables grouped by a unit column, a hospitalization recording
            # the same category in multiple units counts once per unit row.
            n_expr = (pl.col(id_col).n_unique() if id_col else pl.len()).alias('n')
            monthly = (
                lf.filter(pl.col(time_column).is_not_null() & pl.col(cat_col).is_not_null())
                .with_columns(pl.col(time_column).dt.strftime('%Y-%m').alias('month_year'))
                .group_by(group_cols)
                .agg([n_expr] + avg_expr)
                .sort(group_cols)
                .collect(engine="streaming")
            )

            # For tables with schema-based units (no unit col in data), add unit column
            if num_col and num_col in col_names and not (unit_col and unit_col in col_names):
                schema_unit_map = _get_schema_units(schema, cat_col)
                if schema_unit_map:
                    monthly = monthly.with_columns(
                        pl.col(cat_col).replace_strict(
                            schema_unit_map, default=None
                        ).alias('unit')
                    )

            monthly_trends[cat_col] = monthly.to_dicts()

        result.metrics["category_columns_checked"] = len(cat_cols_present)
        result.metrics["yearly_distributions"] = yearly_distributions
        result.metrics["missing_in_years"] = missing_in_years
        result.metrics["monthly_trends"] = monthly_trends

        # Emit one row per (cat_col × distinct value) so each value gets its
        # own sparkline. WARNING when the value is absent in some years,
        # INFO when it's present in all years. For noisy enum columns
        # (organism_category) we emit INFO for absences too, to avoid
        # flooding the warning list.
        atomic_total = 0
        for cat_col in cat_cols_present:
            dist = yearly_distributions.get(cat_col, {})
            all_col_years = sorted(dist.keys())
            total_years = len(all_col_years)
            all_vals = {v for yr in dist.values() for v in yr}
            atomic_total += len(all_vals)

            if total_years == 0 or not all_vals:
                result.add_info(f"No temporal data for {cat_col}",
                                {"column": cat_col})
                continue

            year_range = f"{all_col_years[0]}-{all_col_years[-1]}"
            absent_for_col = missing_in_years.get(cat_col, {})

            for val in sorted(all_vals, key=str):
                yearly_counts = {y: dist.get(y, {}).get(val, 0)
                                 for y in all_col_years}
                n_present = sum(1 for c in yearly_counts.values() if c > 0)

                if val in absent_for_col:
                    n_absent = total_years - n_present
                    msg = (f"{cat_col}: {val} absent in "
                           f"{n_absent}/{total_years} years ({year_range})")
                    emit = (result.add_info
                            if cat_col == "organism_category"
                            else result.add_warning)
                    emit(msg, {
                        "column": cat_col,
                        "value": val,
                        "absent_years": absent_for_col[val],
                        "total_years": total_years,
                        "yearly_counts": yearly_counts,
                    })
                else:
                    msg = (f"{cat_col}: {val} present in "
                           f"{n_present}/{total_years} years ({year_range})")
                    result.add_info(msg, {
                        "column": cat_col,
                        "value": str(val),
                        "yearly_counts": yearly_counts,
                    })

        # Atomic counting: 1 per (cat_col × value present in data). Lenient
        # scoring — only errors reduce atomic_passed; warnings about values
        # absent in some years are shown in the issues table but do not fail
        # the atom.
        result.atomic_total = atomic_total
        result.atomic_passed = atomic_total  # P.6 emits no errors in normal flow

        gc.collect()

    except Exception as e:
        _logger.error("Check 'category_temporal_consistency' failed for table '%s': %s", table_name, e)
        result.add_error(f"Error checking category temporal consistency: {str(e)}")
        if result.atomic_total is None:
            result.atomic_total = 1
            result.atomic_passed = 0

    return result


def check_category_temporal_consistency(
    df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    schema: Dict[str, Any],
    table_name: str,
    time_column: Optional[str] = None,
    hosp_years: Optional[set] = None,
) -> DQAPlausibilityResult:
    """Check category distribution consistency over time."""
    _logger.debug("check_category_temporal_consistency: starting for table '%s'", table_name)
    result = check_category_temporal_consistency_polars(df, schema, table_name, time_column, hosp_years)
    _logger.debug("check_category_temporal_consistency: table '%s' — columns_checked=%s",
                  table_name, result.metrics.get("category_columns_checked"))
    return result


# ---------------------------------------------------------------------------
# D.1 Duplicate composite keys
# ---------------------------------------------------------------------------

def check_duplicate_composite_keys_polars(
    df: Union['pl.DataFrame', 'pl.LazyFrame'],
    table_name: str,
    composite_keys: Optional[List[str]] = None,
    schema: Optional[Dict[str, Any]] = None,
    warning_threshold: float = 0.0,
    error_threshold: float = 10.0,
) -> DQAPlausibilityResult:
    """Check for duplicate composite keys using Polars."""
    result = DQAPlausibilityResult("duplicate_composite_keys", table_name)

    if composite_keys is None:
        composite_keys = _get_composite_keys(table_name, schema)

    if not composite_keys:
        result.add_info("No composite keys defined for this table")
        result.atomic_total = 0
        result.atomic_passed = 0
        return result

    try:
        lf = df if isinstance(df, pl.LazyFrame) else df.lazy()
        col_names = lf.collect_schema().names()

        # Verify all key columns exist
        missing_keys = [k for k in composite_keys if k not in col_names]
        if missing_keys:
            result.add_info(f"Composite key columns missing: {missing_keys}")
            result.atomic_total = 0
            result.atomic_passed = 0
            return result

        total = lf.select(pl.len()).collect(engine="streaming").item()
        unique = (
            lf.group_by(composite_keys)
            .agg(pl.len().alias('_n'))
            .select(pl.len())
            .collect(engine="streaming")
            .item()
        )
        duplicates = total - unique
        pct = (duplicates / total * 100) if total > 0 else 0

        result.metrics["composite_keys"] = composite_keys
        result.metrics["total_records"] = int(total)
        result.metrics["unique_records"] = int(unique)
        result.metrics["duplicate_records"] = int(duplicates)
        result.metrics["duplicate_percent"] = round(pct, 2)

        keys_str = ', '.join(composite_keys)
        if pct > error_threshold:
            result.add_error(
                f"{duplicates} duplicate records ({pct:.1f}%) on composite key ({keys_str}) in {table_name}",
                {"duplicate_records": int(duplicates), "percent": round(pct, 2),
                 "keys": composite_keys}
            )
        elif pct > warning_threshold:
            result.add_warning(
                f"{duplicates} duplicate records ({pct:.1f}%) on composite key ({keys_str}) in {table_name}",
                {"duplicate_records": int(duplicates), "percent": round(pct, 2),
                 "keys": composite_keys}
            )
        else:
            result.add_info("No duplicate composite keys found")

        gc.collect()

        result.atomic_total = 1
        result.atomic_passed = 1 if not result.errors else 0
    except Exception as e:
        _logger.error("Check 'duplicate_composite_keys' failed for table '%s': %s", table_name, e)
        result.add_error(f"Error checking duplicate composite keys: {str(e)}")
        if result.atomic_total is None:
            result.atomic_total = 1
            result.atomic_passed = 0

    return result


def check_duplicate_composite_keys(
    df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    table_name: str,
    composite_keys: Optional[List[str]] = None,
    schema: Optional[Dict[str, Any]] = None,
    warning_threshold: float = 0.0,
    error_threshold: float = 10.0,
) -> DQAPlausibilityResult:
    """Check for duplicate composite keys."""
    _logger.debug("check_duplicate_composite_keys: starting for table '%s'", table_name)
    result = check_duplicate_composite_keys_polars(df, table_name, composite_keys, schema,
                                                   warning_threshold=warning_threshold,
                                                   error_threshold=error_threshold)
    _logger.debug("check_duplicate_composite_keys: table '%s' — duplicates=%s",
                  table_name, result.metrics.get("duplicate_records"))
    return result
# ---------------------------------------------------------------------------
# COMPREHENSIVE DQA RUNNER
# ---------------------------------------------------------------------------

def run_conformance_checks(
    df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    schema: Dict[str, Any],
    table_name: str
) -> Dict[str, DQAConformanceResult]:
    """
    Run all conformance checks on a table.

    Parameters
    ----------
    df : DataFrame
        The data to validate
    schema : dict
        Schema for the table
    table_name : str
        Name of the table

    Returns
    -------
    Dict[str, DQAConformanceResult]
        Dictionary of check results keyed by check type
    """
    _logger.info("Running conformance checks for table '%s'", table_name)

    # Convert pandas DataFrame to Polars if the active backend is Polars
    if isinstance(df, pd.DataFrame):
        _logger.debug("Converting pandas DataFrame to Polars for table '%s'", table_name)
        df = pl.from_pandas(df)

    # Case-normalize for validation: lowercase column names + string values,
    # with __orig_<col> sidecars preserving original case for error messages.
    df = _normalize_for_validation(df)

    results = {}

    results['table_presence'] = check_table_presence(df, table_name)
    gc.collect()

    results['required_columns'] = check_required_columns(df, schema, table_name)
    gc.collect()

    results['column_dtypes'] = check_column_dtypes(df, schema, table_name)
    gc.collect()

    results['datetime_format'] = check_datetime_format(df, schema, table_name)
    gc.collect()

    if table_name == 'labs':
        results['lab_reference_units'] = check_lab_reference_units(df, schema, table_name)
        gc.collect()

    if table_name in ('medication_admin_continuous', 'medication_admin_intermittent'):
        results['medication_dose_units'] = check_medication_dose_units(df, schema, table_name)
        gc.collect()

    results['categorical_values'] = check_categorical_values(df, schema, table_name)
    gc.collect()

    results['category_group_mapping'] = check_category_group_mapping(df, schema, table_name)
    gc.collect()

    passed = sum(1 for r in results.values() if r.passed)
    failed = len(results) - passed
    _logger.info("Conformance checks complete for '%s': %d passed, %d failed", table_name, passed, failed)

    return results


def run_completeness_checks(
    df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    schema: Dict[str, Any],
    table_name: str,
    error_threshold: float = 50.0,
    warning_threshold: float = 10.0
) -> Dict[str, DQACompletenessResult]:
    """
    Run all completeness checks on a table.

    Parameters
    ----------
    df : DataFrame
        The data to validate
    schema : dict
        Schema for the table
    table_name : str
        Name of the table
    error_threshold : float
        Percent missing above which an error is raised
    warning_threshold : float
        Percent missing above which a warning is raised

    Returns
    -------
    Dict[str, DQACompletenessResult]
        Dictionary of check results keyed by check type
    """
    _logger.info("Running completeness checks for table '%s'", table_name)

    # Convert pandas DataFrame to Polars if the active backend is Polars
    if isinstance(df, pd.DataFrame):
        _logger.debug("Converting pandas DataFrame to Polars for table '%s'", table_name)
        df = pl.from_pandas(df)

    # Case-normalize for validation (see run_conformance_checks).
    df = _normalize_for_validation(df)

    results = {}

    results['missingness'] = check_missingness(
        df, schema, table_name, error_threshold, warning_threshold
    )
    gc.collect()

    results['conditional_requirements'] = check_conditional_requirements(df, table_name)
    gc.collect()

    results['mcide_value_coverage'] = check_mcide_value_coverage(df, schema, table_name)
    gc.collect()

    passed = sum(1 for r in results.values() if r.passed)
    failed = len(results) - passed
    _logger.info("Completeness checks complete for '%s': %d passed, %d failed", table_name, passed, failed)

    return results


def run_relational_integrity_checks(
    tables: list,
) -> Dict[str, Dict[str, DQACompletenessResult]]:
    """Auto-detect and run relational integrity checks for loaded tables.

    Reads FK rules from ``validation_rules.yaml`` and runs
    :func:`check_relational_integrity` for every applicable
    (table, fk_column) pair.

    Parameters
    ----------
    tables : list
        Objects with ``.table_name`` (str) and a frame attribute — ``.data``
        (polars) is preferred, ``.df`` is the fallback. Typically
        :class:`BaseTable` instances.

    Returns
    -------
    Dict[str, Dict[str, DQACompletenessResult]]
        ``{table_name: {fk_column: DQACompletenessResult}}``.
    """
    _logger.info("run_relational_integrity_checks: starting with %d tables", len(tables))

    # Build lookup: table_name -> (DataFrame, schema_col_names)
    # Convert pandas DataFrames to Polars when the Polars backend is active,
    # matching the pattern used by run_conformance_checks / run_completeness_checks.
    lookup = {}
    for obj in tables:
        df = _table_frame(obj)
        if isinstance(df, pd.DataFrame):
            _logger.debug("Converting pandas DataFrame to Polars for table '%s'", obj.table_name)
            df = pl.from_pandas(df)
        # Case-normalize for validation (see run_conformance_checks).
        df = _normalize_for_validation(df)
        # Extract schema-defined column names (only check FK columns that belong
        # to the table's schema, not extra columns that happen to be in the data).
        schema = getattr(obj, 'schema', None) or {}
        schema_cols = {c['name'] for c in schema.get('columns', [])} if schema.get('columns') else None
        lookup[obj.table_name] = (df, schema_cols)

    # Load FK rules
    fk_rules = _load_validation_rules().get('relational_integrity', {})
    _logger.debug("run_relational_integrity_checks: loaded %d FK rules", len(fk_rules))

    results: Dict[str, Dict[str, DQACompletenessResult]] = {}

    for table_name, (df, schema_cols) in lookup.items():
        # Use schema columns when available; fall back to DataFrame columns
        if schema_cols is not None:
            col_names = schema_cols
        elif isinstance(df, (pl.DataFrame, pl.LazyFrame)):
            lf = df if isinstance(df, pl.LazyFrame) else df.lazy()
            col_names = set(lf.collect_schema().names())
        else:
            col_names = set(df.columns.tolist())

        for fk_column, rule in fk_rules.items():
            if fk_column not in col_names:
                continue

            ref_table_name = rule['references_table']

            # Skip self-references
            if ref_table_name == table_name:
                _logger.debug(
                    "run_relational_integrity_checks: skipping self-ref "
                    "%s.%s -> %s", table_name, fk_column, ref_table_name,
                )
                continue

            # Skip if the reference table isn't loaded
            if ref_table_name not in lookup:
                _logger.debug(
                    "run_relational_integrity_checks: skipping %s.%s — "
                    "reference table '%s' not loaded",
                    table_name, fk_column, ref_table_name,
                )
                continue

            _logger.info(
                "run_relational_integrity_checks: checking %s.%s -> %s",
                table_name, fk_column, ref_table_name,
            )
            result = check_relational_integrity(
                target_df=df,
                reference_df=lookup[ref_table_name][0],
                target_table=table_name,
                reference_table=ref_table_name,
                key_column=fk_column,
            )
            results.setdefault(table_name, {})[fk_column] = result

    checked = sum(len(v) for v in results.values())
    _logger.info(
        "run_relational_integrity_checks: completed %d checks across %d tables",
        checked, len(results),
    )
    return results


# ---------------------------------------------------------------------------
# CACHE-BASED CROSS-TABLE FUNCTIONS (memory-optimised pipeline)
# ---------------------------------------------------------------------------


def extract_cross_table_cache(table_obj) -> Dict[str, Any]:
    """Extract a lightweight cache from a single table object.

    Used by the optimised pipeline in CLIF-TableOne's runner to avoid
    keeping full DataFrames in memory for cross-table checks.

    Parameters
    ----------
    table_obj : BaseTable
        Object with ``.table_name``, ``.schema``, and a frame attribute —
        ``.data`` (polars) is preferred, ``.df`` is the fallback.

    Returns
    -------
    dict
        Keys: ``table_name``, ``fk_ids``, ``schema_cols``,
        ``temporal_df``, ``hosp_bounds_df``, ``hosp_years``.

        ``fk_ids`` and the ``conditional_*_ids`` maps hold single-column
        :class:`pl.DataFrame` objects of distinct ids, not Python ``set``
        objects. The ``*_from_cache`` consumers compare them with anti-joins;
        callers doing their own set algebra should join, or call
        ``.to_series().to_list()`` to get the values back.
    """
    tname = getattr(table_obj, 'table_name', '').replace('clif_', '')
    df = _table_frame(table_obj)
    schema = getattr(table_obj, 'schema', None) or {}

    # Case-normalize for validation (column names + string values; sidecars preserved).
    # This makes downstream FK membership checks case-insensitive too.
    df = _normalize_for_validation(df)

    # Schema-defined column names
    schema_cols = (
        {c['name'] for c in schema.get('columns', [])}
        if schema.get('columns') else None
    )

    # Determine actual columns in the DataFrame (exclude __orig_ sidecars)
    lf = df if isinstance(df, pl.LazyFrame) else df.lazy()
    actual_cols = set(_strip_sidecars(lf.collect_schema().names()))
    # Use schema cols when available; fall back to actual cols
    col_names = schema_cols if schema_cols is not None else actual_cols

    # --- FK ID sets ---
    # Held as single-column polars frames rather than Python sets. These caches
    # are built for every table and kept alive simultaneously, so they dominate
    # the pipeline's memory; a set of 500k ids costs ~60 MB of PyObject pointers
    # against ~12 MB in Arrow. The `*_from_cache` consumers below use anti-joins,
    # which need no Python-level set at all.
    fk_rules = _load_validation_rules().get('relational_integrity', {})
    fk_ids: Dict[str, 'pl.DataFrame'] = {}
    for fk_column in fk_rules:
        if fk_column not in col_names or fk_column not in actual_cols:
            continue
        lf = df if isinstance(df, pl.LazyFrame) else df.lazy()
        fk_ids[fk_column] = (
            lf.select(pl.col(fk_column).drop_nulls().unique())
            .collect(engine="streaming")
        )
    # --- Temporal subset for cross-table plausibility ---
    time_cols = _CROSS_TABLE_TIME_COLUMNS.get(tname, [])
    temporal_df = None
    if time_cols and 'hospitalization_id' in actual_cols:
        needed = ['hospitalization_id'] + [c for c in time_cols if c in actual_cols]
        lf = df if isinstance(df, pl.LazyFrame) else df.lazy()
        temporal_df = lf.select(needed).collect()
    # --- Hospitalization-specific caches ---
    hosp_bounds_df = None
    hosp_years = None
    if tname == 'hospitalization':
        if isinstance(df, (pl.DataFrame, pl.LazyFrame)):
            lf = df if isinstance(df, pl.LazyFrame) else df.lazy()
            hosp_cols_available = set(lf.collect_schema().names())
            bound_cols = [c for c in ['hospitalization_id', 'admission_dttm', 'discharge_dttm']
                          if c in hosp_cols_available]
            if 'hospitalization_id' in bound_cols:
                hosp_bounds_df = lf.select(bound_cols).collect()
            if 'admission_dttm' in hosp_cols_available:
                hosp_years = set(
                    lf.select(pl.col('admission_dttm').dt.year().alias('yr'))
                    .filter(pl.col('yr').is_not_null())
                    .unique()
                    .collect()
                    .get_column('yr')
                    .to_list()
                )

    # --- Cross-table conditional requirements cache ---
    ct_cond_rules = _load_validation_rules().get('cross_table_conditional_requirements', [])
    conditional_source_ids: Dict[str, 'pl.DataFrame'] = {}
    conditional_target_ids: Dict[str, 'pl.DataFrame'] = {}

    for rule in ct_cond_rules:
        rule_key = f"{rule['source_column']}_{rule['target_column']}"
        join_col = rule['join_column']

        if tname == rule['source_table'] and join_col in actual_cols and rule['source_column'] in actual_cols:
            # Extract join_column IDs where source_column matches source_value (case-insensitive).
            # `df` has already passed through `_normalize_for_validation` above, so string
            # columns are lowercased + stripped; skip the redundant per-row transformation
            # when the column is already string-typed (the common case).
            match_values = [str(v).strip().lower() for v in rule['source_value']]
            lf = df if isinstance(df, pl.LazyFrame) else df.lazy()
            src_dtype = lf.collect_schema()[rule['source_column']]
            if src_dtype == pl.Utf8 or src_dtype == pl.String:
                filter_expr = pl.col(rule['source_column']).is_in(match_values)
            else:
                filter_expr = pl.col(rule['source_column']).cast(pl.Utf8).str.strip_chars().str.to_lowercase().is_in(match_values)
            conditional_source_ids[rule_key] = (
                lf.filter(filter_expr)
                .select(pl.col(join_col).drop_nulls().unique())
                .collect(engine="streaming")
            )
        if tname == rule['target_table'] and join_col in actual_cols and rule['target_column'] in actual_cols:
            # Extract join_column IDs where target_column is not null (and not empty string)
            lf = df if isinstance(df, pl.LazyFrame) else df.lazy()
            tcol = pl.col(rule['target_column'])
            # Handle both datetime and string columns: not null AND not empty string
            not_null_filter = tcol.is_not_null()
            dtype = lf.collect_schema()[rule['target_column']]
            if dtype == pl.Utf8 or dtype == pl.String:
                not_null_filter = not_null_filter & (tcol.str.strip_chars().str.len_chars() > 0)
            conditional_target_ids[rule_key] = (
                lf.filter(not_null_filter)
                .select(pl.col(join_col).drop_nulls().unique())
                .collect(engine="streaming")
            )
    cache = {
        'table_name': tname,
        'fk_ids': fk_ids,
        'schema_cols': schema_cols,
        'temporal_df': temporal_df,
        'hosp_bounds_df': hosp_bounds_df,
        'hosp_years': hosp_years,
        'conditional_source_ids': conditional_source_ids,
        'conditional_target_ids': conditional_target_ids,
    }
    _logger.info(
        "extract_cross_table_cache: table='%s', fk_keys=%s, temporal=%s, hosp_bounds=%s, "
        "cond_source_keys=%s, cond_target_keys=%s",
        tname, list(fk_ids.keys()), temporal_df is not None, hosp_bounds_df is not None,
        list(conditional_source_ids.keys()), list(conditional_target_ids.keys()),
    )
    return cache


def run_relational_integrity_checks_from_cache(
    caches: Dict[str, Dict[str, Any]],
) -> Dict[str, Dict[str, DQACompletenessResult]]:
    """Run relational integrity checks using pre-extracted caches.

    Equivalent to :func:`run_relational_integrity_checks` but operates on
    pre-extracted distinct-id frames instead of scanning full DataFrames.

    Parameters
    ----------
    caches : dict
        ``{table_name: cache_dict}`` as returned by
        :func:`extract_cross_table_cache`.

    Returns
    -------
    Dict[str, Dict[str, DQACompletenessResult]]
        Same structure as :func:`run_relational_integrity_checks`.
    """
    _logger.info(
        "run_relational_integrity_checks_from_cache: starting with %d cached tables",
        len(caches),
    )

    fk_rules = _load_validation_rules().get('relational_integrity', {})
    results: Dict[str, Dict[str, DQACompletenessResult]] = {}

    for table_name, cache in caches.items():
        col_names = cache['schema_cols'] if cache['schema_cols'] is not None else set(cache['fk_ids'].keys())

        for fk_column, rule in fk_rules.items():
            if fk_column not in col_names:
                continue

            ref_table_name = rule['references_table']

            if ref_table_name == table_name:
                continue

            if ref_table_name not in caches:
                _logger.debug(
                    "run_relational_integrity_checks_from_cache: skipping %s.%s — "
                    "reference table '%s' not cached",
                    table_name, fk_column, ref_table_name,
                )
                continue

            # Get the distinct-id frames
            source_ids = cache['fk_ids'].get(fk_column)
            if source_ids is None:
                continue

            ref_cache = caches[ref_table_name]
            ref_ids = ref_cache['fk_ids'].get(fk_column)
            if ref_ids is None:
                # Reference table doesn't have this FK column cached — skip
                continue

            _logger.info(
                "run_relational_integrity_checks_from_cache: checking %s.%s -> %s",
                table_name, fk_column, ref_table_name,
            )

            # Build the bidirectional result (same structure as check_relational_integrity)
            result = DQACompletenessResult(
                "relational_integrity",
                f"{table_name}<->{ref_table_name}",
            )

            try:
                # Both directions are orphan *counts*, so an anti-join answers
                # them without materializing either id column as a Python set.
                fwd_total = ref_ids.height
                rev_total = source_ids.height

                fwd_orphan_count = ref_ids.join(source_ids, on=fk_column, how="anti").height
                rev_orphan_count = source_ids.join(ref_ids, on=fk_column, how="anti").height

                fwd_coverage = ((fwd_total - fwd_orphan_count) / fwd_total * 100) if fwd_total > 0 else 100
                rev_coverage = ((rev_total - rev_orphan_count) / rev_total * 100) if rev_total > 0 else 100

                result.metrics["forward_coverage_percent"] = round(fwd_coverage, 2)
                result.metrics["forward_orphan_ids"] = fwd_orphan_count
                result.metrics["forward_reference_unique_ids"] = fwd_total
                result.metrics["reverse_coverage_percent"] = round(rev_coverage, 2)
                result.metrics["reverse_orphan_ids"] = rev_orphan_count
                result.metrics["reverse_target_unique_ids"] = rev_total

                # `fwd` here is the reverse-cardinality question
                # ("does every reference row have a child in the source?"),
                # which on normal inpatient data routinely fails (many
                # hospitalizations don't have, e.g., CRRT / procedures /
                # ECMO). Always surface it as a warning but mark atomic_count=0
                # so it doesn't contribute to the FK's one-atom denominator.
                if fwd_orphan_count:
                    result.add_warning(
                        f"{fwd_orphan_count}/{fwd_total} {fk_column} values in {ref_table_name} "
                        f"not found in {table_name} ({round(fwd_coverage, 1)}% coverage)",
                        {"orphan_count": fwd_orphan_count,
                         "coverage_percent": round(fwd_coverage, 2),
                         "atomic_count": 0}
                    )

                # `rev` is the real FK integrity check ("does every source
                # FK value resolve in the reference?"). Apply thresholds:
                # >10% orphans → ERROR, >1% → WARNING, else silent pass.
                if rev_orphan_count:
                    orphan_pct = 100 - rev_coverage
                    emit = (result.add_error if orphan_pct > 10.0
                            else result.add_warning if orphan_pct > 1.0
                            else None)
                    if emit is not None:
                        emit(
                            f"{rev_orphan_count}/{rev_total} {fk_column} values in {table_name} "
                            f"not found in {ref_table_name} ({round(rev_coverage, 1)}% coverage)",
                            {"orphan_count": rev_orphan_count,
                             "coverage_percent": round(rev_coverage, 2),
                             "orphan_percent": round(orphan_pct, 2)}
                        )

                if not fwd_orphan_count and not rev_orphan_count:
                    result.add_info(
                        f"Full bidirectional coverage for {fk_column} between "
                        f"{table_name} and {ref_table_name}"
                    )

                _logger.info(
                    "run_relational_integrity_checks_from_cache: '%s' <-> '%s' — "
                    "fwd_coverage=%.1f%%, rev_coverage=%.1f%%",
                    table_name, ref_table_name, fwd_coverage, rev_coverage,
                )

                # Atomic granularity: 1 per FK. Forward direction (source → reference)
                # is the real integrity check; the reverse is informational only.
                result.atomic_total = 1
                result.atomic_passed = 1 if not result.errors else 0
            except Exception as e:
                _logger.error(
                    "Cached relational check failed for '%s' <-> '%s': %s",
                    table_name, ref_table_name, e,
                )
                result.add_error(f"Error checking relational integrity: {str(e)}")
                if result.atomic_total is None:
                    result.atomic_total = 1
                    result.atomic_passed = 0

            results.setdefault(table_name, {})[fk_column] = result

    checked = sum(len(v) for v in results.values())
    _logger.info(
        "run_relational_integrity_checks_from_cache: completed %d checks across %d tables",
        checked, len(results),
    )
    return results


def run_cross_table_completeness_checks_from_cache(
    caches: Dict[str, Dict[str, Any]],
) -> Dict[str, Dict[str, DQACompletenessResult]]:
    """Run cross-table conditional completeness checks (K.5) from caches.

    For each YAML rule in ``cross_table_conditional_requirements``, computes
    the set of join-column IDs that satisfy the source condition but are
    missing the required target column value.

    Parameters
    ----------
    caches : dict
        ``{table_name: cache_dict}`` as returned by
        :func:`extract_cross_table_cache`.

    Returns
    -------
    Dict[str, Dict[str, DQACompletenessResult]]
        Results keyed by **target** table name, then by a descriptive
        check key.
    """
    _logger.info(
        "run_cross_table_completeness_checks_from_cache: starting with %d cached tables",
        len(caches),
    )

    ct_cond_rules = _load_validation_rules().get('cross_table_conditional_requirements', [])
    if not ct_cond_rules:
        return {}

    results: Dict[str, Dict[str, DQACompletenessResult]] = {}

    for rule in ct_cond_rules:
        rule_key = f"{rule['source_column']}_{rule['target_column']}"
        source_table = rule['source_table']
        target_table = rule['target_table']

        # Both tables must be cached
        if source_table not in caches or target_table not in caches:
            _logger.debug(
                "K.5: skipping rule '%s' — source '%s' or target '%s' not cached",
                rule_key, source_table, target_table,
            )
            continue

        source_cache = caches[source_table]
        target_cache = caches[target_table]

        source_ids = source_cache.get('conditional_source_ids', {}).get(rule_key)
        target_ids = target_cache.get('conditional_target_ids', {}).get(rule_key)

        result = DQACompletenessResult(
            "cross_table_conditional_completeness",
            f"{source_table}->{target_table}",
        )

        if source_ids is None or target_ids is None:
            result.add_info(
                "No cross-table conditional requirements applicable"
            )
            result.atomic_total = 0
            result.atomic_passed = 0
            results.setdefault(target_table, {})[rule_key] = result
            continue

        total = source_ids.height
        if total == 0:
            result.add_info(
                f"No {rule['source_column']} = {rule['source_value']} found in "
                f"{source_table}; cross-table conditional check not triggered"
            )
            result.atomic_total = 0
            result.atomic_passed = 0
            results.setdefault(target_table, {})[rule_key] = result
            continue

        join_col = rule['join_column']
        missing_ids = source_ids.join(target_ids, on=join_col, how="anti")
        missing_count = missing_ids.height
        pct = round(missing_count / total * 100, 1) if total > 0 else 0

        result.metrics["total_matching_source"] = total
        result.metrics["missing_in_target"] = missing_count
        result.metrics["coverage_percent"] = round(100 - pct, 2)

        if missing_count:
            # sort().head(10) reproduces the previous sorted(list(<set>))[:10] sample
            sample = missing_ids[join_col].sort().head(10).to_list()
            result.add_warning(
                f"{missing_count}/{total} patients discharged as {rule['source_value']} "
                f"in {source_table} are missing {rule['target_column']} in {target_table} "
                f"({pct}% missing)",
                {
                    "column": rule['target_column'],
                    "missing_count": missing_count,
                    "total_matching": total,
                    "percent_missing": pct,
                    "sample_ids": sample,
                    "source_condition": f"{rule['source_column']} in {rule['source_value']}",
                },
            )
        else:
            result.add_info(
                f"All {total} patients discharged as {rule['source_value']} in "
                f"{source_table} have {rule['target_column']} in {target_table}"
            )

        result.atomic_total = 1
        result.atomic_passed = 1 if not result.errors else 0
        results.setdefault(target_table, {})[rule_key] = result
        _logger.info(
            "K.5: %s->%s rule '%s': %d/%d missing (%.1f%%)",
            source_table, target_table, rule_key, missing_count, total, pct,
        )

    checked = sum(len(v) for v in results.values())
    _logger.info(
        "run_cross_table_completeness_checks_from_cache: completed %d checks across %d tables",
        checked, len(results),
    )
    return results


def run_cross_table_completeness_checks(
    tables: list,
) -> Dict[str, Dict[str, DQACompletenessResult]]:
    """Run cross-table conditional completeness checks (K.5) on full DataFrames.

    Parameters
    ----------
    tables : list
        Objects with ``.table_name`` and a frame attribute — ``.data``
        (polars) is preferred, ``.df`` is the fallback.

    Returns
    -------
    Dict[str, Dict[str, DQACompletenessResult]]
        Results keyed by **target** table name.
    """
    _logger.info("run_cross_table_completeness_checks: starting with %d tables", len(tables))

    ct_cond_rules = _load_validation_rules().get('cross_table_conditional_requirements', [])
    if not ct_cond_rules:
        return {}

    lookup = {}
    for obj in tables:
        tname = getattr(obj, 'table_name', '').replace('clif_', '')
        tdf = _table_frame(obj)
        if isinstance(tdf, pd.DataFrame):
            tdf = pl.from_pandas(tdf)
        # Case-normalize for validation (see run_conformance_checks).
        tdf = _normalize_for_validation(tdf)
        lookup[tname] = tdf

    results: Dict[str, Dict[str, DQACompletenessResult]] = {}

    for rule in ct_cond_rules:
        rule_key = f"{rule['source_column']}_{rule['target_column']}"
        source_table = rule['source_table']
        target_table = rule['target_table']
        join_col = rule['join_column']

        if source_table not in lookup or target_table not in lookup:
            continue

        src_df = lookup[source_table]
        tgt_df = lookup[target_table]
        match_values = [str(v).strip().lower() for v in rule['source_value']]

        # Get source IDs matching condition. `src_df` has already been passed
        # through `_normalize_for_validation` at the top of this function, so
        # string columns are lowercased + stripped — skip the redundant per-row
        # transformation when the column is already string-typed (common case).
        lf = src_df if isinstance(src_df, pl.LazyFrame) else src_df.lazy()
        schema = lf.collect_schema()
        if rule['source_column'] not in schema.names() or join_col not in schema.names():
            continue
        src_dtype = schema[rule['source_column']]
        if src_dtype == pl.Utf8 or src_dtype == pl.String:
            filter_expr = pl.col(rule['source_column']).is_in(match_values)
        else:
            filter_expr = pl.col(rule['source_column']).cast(pl.Utf8).str.strip_chars().str.to_lowercase().is_in(match_values)
        # Kept lazy: only the counts and a 10-id sample are needed below, so the
        # id columns never have to become Python sets.
        source_ids = (
            lf.filter(filter_expr)
            .select(pl.col(join_col).drop_nulls().unique())
        )
        # Get target IDs with non-null target column (also exclude empty strings)
        lf = tgt_df if isinstance(tgt_df, pl.LazyFrame) else tgt_df.lazy()
        schema_names = lf.collect_schema().names()
        if rule['target_column'] not in schema_names or join_col not in schema_names:
            continue
        tcol = pl.col(rule['target_column'])
        not_null_filter = tcol.is_not_null()
        dtype = lf.collect_schema()[rule['target_column']]
        if dtype == pl.Utf8 or dtype == pl.String:
            not_null_filter = not_null_filter & (tcol.str.strip_chars().str.len_chars() > 0)
        target_ids = (
            lf.filter(not_null_filter)
            .select(pl.col(join_col).drop_nulls().unique())
        )
        result = DQACompletenessResult(
            "cross_table_conditional_completeness",
            f"{source_table}->{target_table}",
        )

        total = source_ids.select(pl.len()).collect(engine="streaming").item()

        if total == 0:
            result.add_info(
                f"No {rule['source_column']} = {rule['source_value']} found in "
                f"{source_table}; cross-table conditional check not triggered"
            )
            result.atomic_total = 0
            result.atomic_passed = 0
            results.setdefault(target_table, {})[rule_key] = result
            continue

        missing_ids = source_ids.join(target_ids, on=join_col, how="anti")
        missing_count = missing_ids.select(pl.len()).collect(engine="streaming").item()
        pct = round(missing_count / total * 100, 1) if total > 0 else 0

        result.metrics["total_matching_source"] = total
        result.metrics["missing_in_target"] = missing_count
        result.metrics["coverage_percent"] = round(100 - pct, 2)

        if missing_count:
            # sort().head(10) reproduces the previous sorted(list(<set>))[:10] sample
            sample = (
                missing_ids
                .select(pl.col(join_col).sort().head(10))
                .collect(engine="streaming")[join_col]
                .to_list()
            )
            result.add_warning(
                f"{missing_count}/{total} patients discharged as {rule['source_value']} "
                f"in {source_table} are missing {rule['target_column']} in {target_table} "
                f"({pct}% missing)",
                {
                    "column": rule['target_column'],
                    "missing_count": missing_count,
                    "total_matching": total,
                    "percent_missing": pct,
                    "sample_ids": sample,
                    "source_condition": f"{rule['source_column']} in {rule['source_value']}",
                },
            )
        else:
            result.add_info(
                f"All {total} patients discharged as {rule['source_value']} in "
                f"{source_table} have {rule['target_column']} in {target_table}"
            )

        result.atomic_total = 1
        result.atomic_passed = 1 if not result.errors else 0
        results.setdefault(target_table, {})[rule_key] = result

    checked = sum(len(v) for v in results.values())
    _logger.info(
        "run_cross_table_completeness_checks: completed %d checks across %d tables",
        checked, len(results),
    )
    return results


def run_cross_table_plausibility_checks_from_cache(
    caches: Dict[str, Dict[str, Any]],
    plausibility_thresholds: Optional[Dict[str, Dict[str, float]]] = None,
) -> Dict[str, Dict[str, DQAPlausibilityResult]]:
    """Run cross-table plausibility checks using pre-extracted caches.

    Equivalent to :func:`run_cross_table_plausibility_checks` but uses
    cached temporal subset DataFrames and hospitalization bounds instead
    of full DataFrames.

    Parameters
    ----------
    caches : dict
        ``{table_name: cache_dict}`` as returned by
        :func:`extract_cross_table_cache`.
    plausibility_thresholds : dict, optional
        Override default plausibility thresholds per check.

    Returns
    -------
    Dict[str, Dict[str, DQAPlausibilityResult]]
        Same structure as :func:`run_cross_table_plausibility_checks`.
    """
    _logger.info(
        "run_cross_table_plausibility_checks_from_cache: starting with %d cached tables",
        len(caches),
    )

    # Merge caller overrides with defaults
    thresholds = {k: dict(v) for k, v in _DEFAULT_PLAUSIBILITY_THRESHOLDS.items()}
    if plausibility_thresholds:
        for check_name, overrides in plausibility_thresholds.items():
            if check_name in thresholds:
                thresholds[check_name].update(overrides)
            else:
                thresholds[check_name] = overrides

    # Need hospitalization bounds
    hosp_cache = caches.get('hospitalization')
    if hosp_cache is None or hosp_cache.get('hosp_bounds_df') is None:
        _logger.info("Hospitalization cache not available; skipping cross-table plausibility")
        return {}

    hosp_bounds_df = hosp_cache['hosp_bounds_df']
    # Ensure hosp_bounds_df matches active backend
    if isinstance(hosp_bounds_df, pd.DataFrame):
        hosp_bounds_df = pl.from_pandas(hosp_bounds_df)

    results: Dict[str, Dict[str, DQAPlausibilityResult]] = {}

    for tbl_name, cache in caches.items():
        if tbl_name == 'hospitalization':
            continue

        temporal_df = cache.get('temporal_df')
        if temporal_df is None:
            continue

        # Ensure temporal_df matches active backend
        if isinstance(temporal_df, pd.DataFrame):
            temporal_df = pl.from_pandas(temporal_df)

        time_cols = _CROSS_TABLE_TIME_COLUMNS.get(tbl_name, [])
        if not time_cols:
            continue

        # Determine available time columns in the cached temporal subset
        actual_cols = (temporal_df.collect_schema().names()
                      if isinstance(temporal_df, pl.LazyFrame)
                      else temporal_df.columns)
        available_time_cols = [c for c in time_cols if c in actual_cols]
        if not available_time_cols or 'hospitalization_id' not in actual_cols:
            continue

        result = check_cross_table_temporal_plausibility(
            temporal_df, hosp_bounds_df, tbl_name, available_time_cols,
            **thresholds['cross_table_temporal']
        )
        results.setdefault(tbl_name, {})["cross_table_temporal"] = result

    checked = sum(len(v) for v in results.values())
    _logger.info(
        "run_cross_table_plausibility_checks_from_cache: completed %d checks across %d tables",
        checked, len(results),
    )
    return results


def run_plausibility_checks(
    df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    schema: Dict[str, Any],
    table_name: str,
    hosp_years: Optional[set] = None,
    plausibility_thresholds: Optional[Dict[str, Dict[str, float]]] = None,
) -> Dict[str, DQAPlausibilityResult]:
    """
    Run all single-table plausibility checks on a table.

    Parameters
    ----------
    df : DataFrame
        The data to validate
    schema : dict
        Schema for the table
    table_name : str
        Name of the table
    plausibility_thresholds : dict, optional
        Override default plausibility thresholds per check.

    Returns
    -------
    Dict[str, DQAPlausibilityResult]
        Dictionary of check results keyed by check type
    """
    _logger.info("Running plausibility checks for table '%s'", table_name)

    # Merge caller overrides with defaults
    thresholds = {k: dict(v) for k, v in _DEFAULT_PLAUSIBILITY_THRESHOLDS.items()}
    if plausibility_thresholds:
        for check_name, overrides in plausibility_thresholds.items():
            if check_name in thresholds:
                thresholds[check_name].update(overrides)
            else:
                thresholds[check_name] = overrides

    if isinstance(df, pd.DataFrame):
        _logger.debug("Converting pandas DataFrame to Polars for table '%s'", table_name)
        df = pl.from_pandas(df)

    # Case-normalize for validation (see run_conformance_checks).
    df = _normalize_for_validation(df)

    results = {}

    # A.1 Chronological order
    results['chronological_order'] = check_chronological_order(
        df, table_name, **thresholds['chronological_order'])
    gc.collect()

    # A.2 Numeric range plausibility
    results['numeric_range_plausibility'] = check_numeric_range_plausibility(
        df, table_name, **thresholds['numeric_range_plausibility'])
    gc.collect()

    # A.3 Field-level plausibility
    results['field_plausibility'] = check_field_plausibility(df, table_name)
    gc.collect()

    # A.4 Medication dose unit consistency (only for med tables)
    if table_name in ('medication_admin_continuous', 'medication_admin_intermittent'):
        results['medication_dose_unit_consistency'] = check_medication_dose_unit_consistency(
            df, table_name, **thresholds['medication_dose_unit_consistency'])
        gc.collect()

    # C.1 Overlapping periods
    overlap_rules = _load_validation_rules().get('overlapping_periods', {}).get(table_name)
    if overlap_rules:
        results['overlapping_periods'] = check_overlapping_periods(
            df, table_name,
            entity_col=overlap_rules.get('entity_column', 'hospitalization_id'),
            start_col=overlap_rules.get('start_column', 'in_dttm'),
            end_col=overlap_rules.get('end_column', 'out_dttm'),
        )
        gc.collect()

    # C.2 Category temporal consistency
    results['category_temporal_consistency'] = check_category_temporal_consistency(df, schema, table_name, hosp_years=hosp_years)
    gc.collect()

    # D.1 Duplicate composite keys
    results['duplicate_composite_keys'] = check_duplicate_composite_keys(
        df, table_name, schema=schema, **thresholds['duplicate_composite_keys'])
    gc.collect()

    passed = sum(1 for r in results.values() if r.passed)
    failed = len(results) - passed
    _logger.info("Plausibility checks complete for '%s': %d passed, %d failed", table_name, passed, failed)
    return results


def run_cross_table_plausibility_checks(
    tables: list,
    plausibility_thresholds: Optional[Dict[str, Dict[str, float]]] = None,
) -> Dict[str, Dict[str, DQAPlausibilityResult]]:
    """Run cross-table plausibility checks (B.1).

    Parameters
    ----------
    tables : list
        Objects with ``.table_name`` and a frame attribute — ``.data``
        (polars) is preferred, ``.df`` is the fallback.
    plausibility_thresholds : dict, optional
        Override default plausibility thresholds per check.

    Returns
    -------
    Dict[str, Dict[str, DQAPlausibilityResult]]
        ``{table_name: {"cross_table_temporal": DQAPlausibilityResult}}``.
    """
    _logger.info("run_cross_table_plausibility_checks: starting with %d tables", len(tables))

    # Merge caller overrides with defaults
    thresholds = {k: dict(v) for k, v in _DEFAULT_PLAUSIBILITY_THRESHOLDS.items()}
    if plausibility_thresholds:
        for check_name, overrides in plausibility_thresholds.items():
            if check_name in thresholds:
                thresholds[check_name].update(overrides)
            else:
                thresholds[check_name] = overrides

    lookup = {}
    for obj in tables:
        tdf = _table_frame(obj)
        if isinstance(tdf, pd.DataFrame):
            tdf = pl.from_pandas(tdf)
        # Case-normalize for validation (see run_conformance_checks).
        tdf = _normalize_for_validation(tdf)
        lookup[obj.table_name] = tdf

    if 'hospitalization' not in lookup:
        _logger.info("Hospitalization table not loaded; skipping cross-table plausibility")
        return {}

    hosp_df = lookup['hospitalization']
    results: Dict[str, Dict[str, DQAPlausibilityResult]] = {}

    for tbl_name, tdf in lookup.items():
        if tbl_name == 'hospitalization':
            continue
        time_cols = _CROSS_TABLE_TIME_COLUMNS.get(tbl_name, [])
        if not time_cols:
            continue

        lf = tdf if isinstance(tdf, pl.LazyFrame) else tdf.lazy()
        actual_cols = lf.collect_schema().names()
        available_time_cols = [c for c in time_cols if c in actual_cols]
        if not available_time_cols or 'hospitalization_id' not in actual_cols:
            continue

        result = check_cross_table_temporal_plausibility(
            tdf, hosp_df, tbl_name, available_time_cols,
            **thresholds['cross_table_temporal']
        )
        results.setdefault(tbl_name, {})["cross_table_temporal"] = result

    checked = sum(len(v) for v in results.values())
    _logger.info(
        "run_cross_table_plausibility_checks: completed %d checks across %d tables",
        checked, len(results),
    )
    return results


def _resolve_clif_version(
    schema: Optional[Dict[str, Any]],
    clif_version: Optional[str],
) -> Optional[str]:
    """Resolve the CLIF version a validation actually ran against.

    The schema's own ``version`` key is the source of truth (it reflects the
    schema file that was loaded); the ``clif_version`` argument is the
    fallback used when the schema carries no version (e.g. a hand-built
    schema dict).
    """
    if schema:
        version = schema.get('version')
        if version:
            return str(version)
    return clif_version


def _stamp_clif_version(results: Dict[str, Any], clif_version: Optional[str]) -> Dict[str, Any]:
    """Tag each DQA result object with the CLIF version it was run against."""
    if clif_version:
        for result in results.values():
            result.clif_version = clif_version
    return results


def run_full_dqa(
    df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    schema: Optional[Dict[str, Any]] = None,
    table_name: str = "",
    tables: Optional[list] = None,
    error_threshold: float = 50.0,
    warning_threshold: float = 10.0,
    hosp_years: Optional[set] = None,
    plausibility_thresholds: Optional[Dict[str, Dict[str, float]]] = None,
    clif_version: str = DEFAULT_CLIF_VERSION,
) -> Dict[str, Any]:
    """Run the complete DQA suite on a single table.

    Orchestrates conformance checks, completeness checks, plausibility
    checks, and — when *tables* is provided — auto-detected relational
    integrity and cross-table plausibility checks.

    Parameters
    ----------
    df : DataFrame
        The data to validate.
    schema : dict, optional
        Schema for the table.  When *None* (the default), the schema is
        loaded automatically from the built-in schemas using *table_name*.
    table_name : str
        Name of the table.
    tables : list, optional
        Objects with ``.table_name`` and a frame attribute (``.data``
        preferred, ``.df`` fallback) (e.g.
        :class:`BaseTable` instances).  When provided, relational
        integrity and cross-table plausibility checks are run.
    error_threshold : float
        Percent missing above which an error is raised (default 50).
    warning_threshold : float
        Percent missing above which a warning is raised (default 10).
    hosp_years : set, optional
        Pre-extracted hospitalization years for P.6 temporal consistency.
        When provided, skips scanning the hospitalization table to
        extract years.
    plausibility_thresholds : dict, optional
        Override default plausibility thresholds per check.
    clif_version : str, optional
        CLIF schema version to auto-load when *schema* is None
        (e.g. "2.1", "3.0"). Ignored when *schema* is passed explicitly.
        Defaults to the package default (3.0).

    Returns
    -------
    Dict[str, Any]
        Keys: ``table_name``, ``backend``, ``conformance``,
        ``completeness``, ``relational``, ``plausibility``.
    """
    if not table_name:
        raise ValueError("table_name is required")
    if schema is None:
        schema = _load_schema(table_name, clif_version=clif_version)
        if schema is None:
            raise FileNotFoundError(
                f"No built-in schema found for table '{table_name}' "
                f"(CLIF {clif_version}). Pass a schema dict explicitly."
            )
    _logger.info("Starting full DQA for table: %s", table_name)

    resolved_version = _resolve_clif_version(schema, clif_version)

    results: Dict[str, Any] = {
        'table_name': table_name,
        'clif_version': resolved_version,
        'backend': 'polars',
        'conformance': {},
        'completeness': {},
        'relational': {},
        'plausibility': {},
    }

    results['conformance'] = {
        k: v.to_dict()
        for k, v in _stamp_clif_version(
            run_conformance_checks(df, schema, table_name), resolved_version
        ).items()
    }

    results['completeness'] = {
        k: v.to_dict()
        for k, v in _stamp_clif_version(
            run_completeness_checks(
                df, schema, table_name, error_threshold, warning_threshold
            ),
            resolved_version,
        ).items()
    }

    if tables is not None:
        rel_results = run_relational_integrity_checks(tables)
        if table_name in rel_results:
            results['relational'] = {
                k: v.to_dict()
                for k, v in _stamp_clif_version(
                    rel_results[table_name], resolved_version
                ).items()
            }

    # Extract hospitalization years for P.6 temporal consistency context
    # (skip scan if hosp_years was provided by caller)
    if hosp_years is None and tables is not None:
        for obj in tables:
            tname = getattr(obj, 'table_name', '').replace('clif_', '')
            if tname == 'hospitalization':
                # Prefer the stored polars frame; a caller holding pandas is
                # coerced once here rather than via a parallel pandas branch.
                hdf = _table_frame(obj)
                if isinstance(hdf, pd.DataFrame):
                    hdf = pl.from_pandas(hdf)
                if isinstance(hdf, (pl.DataFrame, pl.LazyFrame)):
                    hlf = hdf if isinstance(hdf, pl.LazyFrame) else hdf.lazy()
                    if 'admission_dttm' in hlf.collect_schema().names():
                        hosp_years = set(
                            hlf.select(pl.col('admission_dttm').dt.year().alias('yr'))
                            .filter(pl.col('yr').is_not_null())
                            .unique()
                            .collect()
                            .get_column('yr')
                            .to_list()
                        )
                break

    # Plausibility checks (single-table)
    results['plausibility'] = {
        k: v.to_dict()
        for k, v in _stamp_clif_version(
            run_plausibility_checks(
                df, schema, table_name, hosp_years=hosp_years,
                plausibility_thresholds=plausibility_thresholds,
            ),
            resolved_version,
        ).items()
    }

    # Cross-table plausibility checks (when tables provided)
    if tables is not None:
        cross_plaus = run_cross_table_plausibility_checks(
            tables, plausibility_thresholds=plausibility_thresholds)
        if table_name in cross_plaus:
            for k, v in _stamp_clif_version(cross_plaus[table_name], resolved_version).items():
                results['plausibility'][k] = v.to_dict()

    gc.collect()
    _logger.info("Completed full DQA for table: %s", table_name)
    return results


# ---------------------------------------------------------------------------
# CLIF-TABLEONE COMPATIBILITY LAYER
# ---------------------------------------------------------------------------
# These functions provide backward compatibility with CLIF-TableOne's
# validation interface expectations.

def validate_dataframe(
    df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    schema: Dict[str, Any],
    table_name: Optional[str] = None,
    plausibility_thresholds: Optional[Dict[str, Dict[str, float]]] = None,
    clif_version: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Validate a dataframe against schema and return list of errors.

    This function provides compatibility with CLIF-TableOne's expected
    validation interface.

    Parameters
    ----------
    df : pd.DataFrame, pl.DataFrame, or pl.LazyFrame
        Data to validate
    schema : dict
        Table schema containing columns, required_columns, etc.
    table_name : str, optional
        Name of the table (inferred from schema if not provided)
    plausibility_thresholds : dict, optional
        Override default plausibility thresholds per check.
    clif_version : str, optional
        CLIF schema version this validation runs against. Falls back to the
        schema's own ``version`` key when not provided. Stamped onto every
        returned error dict as ``clif_version``.

    Returns
    -------
    List[Dict[str, Any]]
        List of error dictionaries with keys:
        - type: str - Error type/check name
        - description: str - Human-readable error description
        - details: dict - Additional error details
        - category: str - 'schema' or 'data_quality'
        - clif_version: str - CLIF schema version validated against
    """
    table_name = table_name or schema.get('table_name', 'unknown')
    resolved_version = _resolve_clif_version(schema, clif_version)
    _logger.info("validate_dataframe: starting validation for table '%s'", table_name)
    errors = []

    # Schema-level checks (affect 'incomplete' status)
    schema_checks = ['required_columns', 'column_dtypes']

    # Run conformance checks
    conformance_results = run_conformance_checks(df, schema, table_name)
    for check_name, result in conformance_results.items():
        category = 'schema' if check_name in schema_checks else 'data_quality'

        for err in result.errors:
            errors.append({
                'type': _format_check_type(check_name),
                'description': err['message'],
                'details': err.get('details', {}),
                'category': category
            })

        # Include warnings as data quality issues
        for warn in result.warnings:
            errors.append({
                'type': _format_check_type(check_name),
                'description': warn['message'],
                'details': warn.get('details', {}),
                'category': 'data_quality',
                'severity': 'warning'
            })

    # Run completeness checks
    completeness_results = run_completeness_checks(df, schema, table_name)
    for check_name, result in completeness_results.items():
        for err in result.errors:
            errors.append({
                'type': _format_check_type(check_name),
                'description': err['message'],
                'details': err.get('details', {}),
                'category': 'data_quality'
            })

        for warn in result.warnings:
            errors.append({
                'type': _format_check_type(check_name),
                'description': warn['message'],
                'details': warn.get('details', {}),
                'category': 'data_quality',
                'severity': 'warning'
            })

    # Run plausibility checks
    plausibility_results = run_plausibility_checks(
        df, schema, table_name,
        plausibility_thresholds=plausibility_thresholds,
    )
    for check_name, result in plausibility_results.items():
        for err in result.errors:
            errors.append({
                'type': _format_check_type(check_name),
                'description': err['message'],
                'details': err.get('details', {}),
                'category': 'data_quality'
            })

        for warn in result.warnings:
            errors.append({
                'type': _format_check_type(check_name),
                'description': warn['message'],
                'details': warn.get('details', {}),
                'category': 'data_quality',
                'severity': 'warning'
            })

    gc.collect()
    for err in errors:
        err['clif_version'] = resolved_version
    error_count = sum(1 for e in errors if e.get('severity', 'error') == 'error')
    warning_count = sum(1 for e in errors if e.get('severity') == 'warning')
    _logger.info("validate_dataframe: table '%s' (CLIF %s) complete — %d errors, %d warnings",
                 table_name, resolved_version, error_count, warning_count)
    return errors


def _format_check_type(check_name: str) -> str:
    """Convert internal check names to human-readable format."""
    type_mapping = {
        'required_columns': 'Missing Required Columns',
        'column_dtypes': 'Data Type Mismatch',
        'datetime_format': 'Datetime Format Issue',
        'lab_reference_units': 'Lab Unit Mismatch',
        'categorical_values': 'Invalid Categorical Values',
        'missingness': 'High Missingness',
        'conditional_requirements': 'Conditional Requirement Violation',
        'mcide_value_coverage': 'mCIDE Coverage Gap',
        'relational_integrity': 'Relational Integrity',
        'chronological_order': 'Chronological Order Violation',
        'numeric_range_plausibility': 'Numeric Range Implausibility',
        'field_plausibility': 'Field Plausibility Violation',
        'medication_dose_unit_consistency': 'Medication Dose Unit Inconsistency',
        'cross_table_temporal': 'Cross-Table Temporal Implausibility',
        'overlapping_periods': 'Overlapping Time Periods',
        'category_temporal_consistency': 'Category Distribution Shift',
        'duplicate_composite_keys': 'Duplicate Composite Keys',
    }
    return type_mapping.get(check_name, check_name.replace('_', ' ').title())


def format_clifpy_error(
    error: Dict[str, Any],
    row_count: int,
    table_name: str
) -> Dict[str, Any]:
    """
    Format a validation error for display.

    Parameters
    ----------
    error : dict
        Error dictionary from validate_dataframe()
    row_count : int
        Total row count of the table
    table_name : str
        Name of the table

    Returns
    -------
    dict
        Formatted error with type, description, category, and details
    """
    formatted = {
        'type': error.get('type', 'Unknown Error'),
        'description': error.get('description', str(error)),
        'category': error.get('category', 'other'),
        'details': error.get('details', {}),
        'table_name': table_name,
        'row_count': row_count
    }

    # Add severity if present
    if 'severity' in error:
        formatted['severity'] = error['severity']

    return formatted


def determine_validation_status(
    errors: List[Dict[str, Any]],
    required_columns: Optional[List[str]] = None,
    table_name: Optional[str] = None
) -> str:
    """
    Determine validation status based on errors.

    Status Logic:
    - INCOMPLETE (red): Missing required columns OR non-castable datatype errors
      OR 100% null in required columns
    - PARTIAL (yellow): Required columns present but has data quality issues
      (missing categorical values, high missingness, etc.)
    - COMPLETE (green): All required columns present, no critical issues

    Parameters
    ----------
    errors : list
        List of formatted error dictionaries
    required_columns : list, optional
        List of required column names
    table_name : str, optional
        Name of the table (for table-specific logic)

    Returns
    -------
    str
        'complete', 'partial', or 'incomplete'
    """
    if not errors:
        _logger.debug("determine_validation_status: no errors — status='complete'")
        return 'complete'

    # Check for schema-level errors that make data incomplete
    for error in errors:
        error_type = error.get('type', '').lower()
        category = error.get('category', '')
        details = error.get('details', {})
        severity = error.get('severity', 'error')

        # Missing required columns -> incomplete
        if 'missing required columns' in error_type or 'missing_columns' in details:
            _logger.debug("determine_validation_status: missing required columns — status='incomplete'")
            return 'incomplete'

        # Non-castable data type errors -> incomplete
        if 'data type' in error_type and category == 'schema':
            if details.get('castable') is False:
                _logger.debug("determine_validation_status: non-castable dtype — status='incomplete'")
                return 'incomplete'

        # 100% missingness in required columns -> incomplete
        if 'missingness' in error_type and severity != 'warning':
            pct_missing = details.get('percent_missing', 0)
            column = details.get('column', '')
            if pct_missing >= 100 and required_columns and column in required_columns:
                _logger.debug("determine_validation_status: 100%% null in '%s' — status='incomplete'", column)
                return 'incomplete'

    # Has errors but not critical -> partial
    has_errors = any(
        e.get('severity', 'error') == 'error'
        for e in errors
    )

    if has_errors:
        _logger.debug("determine_validation_status: has non-critical errors — status='partial'")
        return 'partial'

    # Only warnings -> complete
    _logger.debug("determine_validation_status: warnings only — status='complete'")
    return 'complete'


def classify_errors_by_status_impact(
    errors: Dict[str, List[Dict[str, Any]]],
    required_columns: List[str],
    table_name: str,
    config_timezone: Optional[str] = None
) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
    """
    Classify errors into status-affecting and informational categories.

    Used by PDF/report generators to separate critical errors from
    informational messages.

    Parameters
    ----------
    errors : dict
        Dictionary with keys 'schema_errors', 'data_quality_issues', 'other_errors'
    required_columns : list
        List of required column names
    table_name : str
        Name of the table
    config_timezone : str, optional
        Configured timezone (to filter timezone-related errors)

    Returns
    -------
    dict
        Dictionary with 'status_affecting' and 'informational', each containing
        'schema_errors', 'data_quality_issues', and 'other_errors' lists
    """
    status_affecting = {
        'schema_errors': [],
        'data_quality_issues': [],
        'other_errors': []
    }
    informational = {
        'schema_errors': [],
        'data_quality_issues': [],
        'other_errors': []
    }

    # Columns that are optional and shouldn't affect status
    optional_columns = {
        'patient': ['race', 'ethnicity', 'language'],
        'hospitalization': ['discharge_category'],
    }
    table_optional = optional_columns.get(table_name, [])

    # Process each error category
    for category_key in ['schema_errors', 'data_quality_issues', 'other_errors']:
        for error in errors.get(category_key, []):
            error_type = error.get('type', '').lower()
            details = error.get('details', {})
            description = error.get('description', '').lower()

            is_informational = False

            # Filter out timezone errors for configured timezone
            if config_timezone and 'timezone' in error_type:
                if config_timezone.lower() in description:
                    is_informational = True

            # Filter out optional column issues
            column = details.get('column', '')
            if column in table_optional:
                is_informational = True

            # mCIDE coverage gaps are informational
            if 'mcide' in error_type or 'coverage' in error_type:
                is_informational = True

            # Plausibility warnings are informational; plausibility errors affect status
            plausibility_keywords = ['chronological order', 'numeric range', 'field plausibility',
                                     'dose unit', 'overlapping', 'distribution shift',
                                     'duplicate composite', 'cross-table temporal']
            is_plausibility = any(p in error_type for p in plausibility_keywords)
            if is_plausibility and error.get('severity') == 'warning':
                is_informational = True

            # Warnings are generally informational
            if error.get('severity') == 'warning':
                is_informational = True

            # Classify into appropriate bucket
            if is_informational:
                informational[category_key].append(error)
            else:
                status_affecting[category_key].append(error)

    return {
        'status_affecting': status_affecting,
        'informational': informational
    }


def get_validation_summary(validation_results: Dict[str, Any]) -> str:
    """
    Generate a text summary of validation results.

    Parameters
    ----------
    validation_results : dict
        Validation results from validate() method

    Returns
    -------
    str
        Human-readable summary string
    """
    status = validation_results.get('status', 'unknown')
    errors = validation_results.get('errors', {})

    schema_count = len(errors.get('schema_errors', []))
    dq_count = len(errors.get('data_quality_issues', []))
    other_count = len(errors.get('other_errors', []))
    total = schema_count + dq_count + other_count

    status_emoji = {
        'complete': '✓',
        'partial': '⚠',
        'incomplete': '✗'
    }

    summary_parts = [
        f"Status: {status_emoji.get(status, '?')} {status.upper()}"
    ]

    if total > 0:
        summary_parts.append(f"Issues: {total} total")
        if schema_count:
            summary_parts.append(f"  - Schema: {schema_count}")
        if dq_count:
            summary_parts.append(f"  - Data Quality: {dq_count}")
        if other_count:
            summary_parts.append(f"  - Other: {other_count}")
    else:
        summary_parts.append("No issues found")

    return '\n'.join(summary_parts)
