"""Stage 2: canonical base units -> the caller's preferred units.

This is where patient weight is consumed (only for the XOR cases) and where
`_convert_status` records why a row could not be converted.
"""

import pandas as pd
import duckdb

from clifpy.utils.logging_config import get_logger

from ._grammar import (
    KG_PER_LB, ALL_ACCEPTABLE_UNITS, AMOUNT_FACTOR_PATTERNS,
    TIME_FACTOR_PATTERNS, MASS_REGEX, VOLUME_REGEX, UNIT_REGEX,
    SUBCLASS_REGEX, RATE_UNITS_STR, AMOUNT_UNITS_STR,
    HR_REGEX, L_REGEX, MU_REGEX, MG_REGEX, NG_REGEX, G_REGEX,
    _weight_qual_clause,
)
from ._sql import _concat_builders_by_patterns, _pattern_to_factor_builder_for_preferred

logger = get_logger('utils.unit_converter')


def _convert_base_units_to_preferred_units(
    med_df: pd.DataFrame | duckdb.DuckDBPyRelation,
    override: bool = False,
    show_intermediate: bool = False
) -> duckdb.DuckDBPyRelation:
    """Convert base standardized units to user-preferred units.

    Performs the second stage of unit conversion, transforming from standardized
    base units (mcg/min, ml/min, u/min) to medication-specific preferred units
    while maintaining unit class consistency.

    Parameters
    ----------
    med_df : pd.DataFrame
        DataFrame with required columns from first-stage conversion:

        - _base_dose: Dose values in standardized units
        - _base_unit: Standardized unit strings (may be NULL)
        - _preferred_unit: Target unit strings for each medication
        - weight_kg: Patient weights (optional, used for weight-based conversions)

    override : bool, default False
        If True, prints warnings but continues when encountering:

        - Unacceptable preferred units not in ALL_ACCEPTABLE_UNITS
        - Cross-class conversions (e.g., rate to amount)
        - Cross-subclass conversions (e.g., mass to volume)

        If False, raises ValueError for these conditions.

    show_intermediate : bool, default False
        If True, expose intermediate columns (_amount_multiplier_preferred,
        _time_multiplier_preferred, _weight_multiplier_preferred) for QA purposes.
        If False (default), inline multiplier expressions to avoid materializing
        intermediate columns.

    Returns
    -------
    pd.DataFrame
        Original DataFrame with additional columns:

        - _unit_class: Classification of base unit ('rate', 'amount', 'unrecognized')
        - _unit_subclass: Subclassification ('mass', 'volume', 'unit', 'unrecognized')
        - _unit_class_preferred: Classification of preferred unit
        - _unit_subclass_preferred: Subclassification of preferred unit
        - _convert_status: Success or failure reason message
        - med_dose_converted: Final converted dose value
        - med_dose_unit_converted: Final unit string after conversion

        If show_intermediate=True, also includes:

        - _amount_multiplier_preferred: Conversion factor for amount units
        - _time_multiplier_preferred: Conversion factor for time units
        - _weight_multiplier_preferred: Conversion factor for weight-based units

    Raises
    ------
    ValueError
        If required columns are missing from med_df or if preferred units are not
        in ALL_ACCEPTABLE_UNITS (when override=False).

    Notes
    -----
    Conversion rules enforced:

    - Conversions only allowed within same unit class (rate→rate, amount→amount)
    - Cannot convert between incompatible subclasses (e.g., mass→volume)
    - When conversion fails, falls back to base units and dose values
    - Missing units (NULL) are handled with 'original unit is missing' status

    The function uses DuckDB SQL for efficient processing and applies regex
    pattern matching to classify units and calculate conversion factors.

    See Also
    --------
    _convert_clean_dose_units_to_base_units : First-stage conversion
    convert_dose_units_by_med_category : Public API for complete conversion pipeline
    """
    # ---- required column check ----
    required_columns = {'_base_dose', '_preferred_unit'}
    missing_columns = required_columns - set(med_df.columns)
    if missing_columns:
        raise ValueError(f"The following column(s) are required but not found: {missing_columns}")

    # ---- preferred-unit acceptability validation via ANTI JOIN (no .to_df()) ----
    # Per docs/duckdb_perf_guide.md §7e and §1: ANTI JOIN keeps everything lazy
    # and only materializes the violations (typically empty) via .fetchall().
    #
    # NOTE: when the orchestrator supplies `_preferred_is_explicit`, only rows
    # the caller actually requested are validated. Rows where the column is
    # false carry a unit COALESCE'd from `_base_unit` -- clifpy's own derived
    # value, not user input -- and an unconvertible one there is reported per
    # row via `_convert_status` rather than aborting the call (clifpy#153).
    # Called directly without that column, every row is validated as before.
    explicit_filter = (
        "AND _preferred_is_explicit"
        if '_preferred_is_explicit' in med_df.columns
        else ""
    )
    acceptable_units_relation = pd.DataFrame({'unit': sorted(ALL_ACCEPTABLE_UNITS)})
    bad_units_rows = duckdb.sql(f"""
        SELECT DISTINCT _preferred_unit
        FROM med_df
        ANTI JOIN acceptable_units_relation ON _preferred_unit = unit
        WHERE _preferred_unit IS NOT NULL
        {explicit_filter}
    """).fetchall()
    if bad_units_rows:
        bad_set = {row[0] for row in bad_units_rows}
        error_msg = (
            f"Cannot accommodate the conversion to the following preferred units: "
            f"{bad_set}. Consult the function documentation for a list of acceptable units."
        )
        if override:
            logger.warning(error_msg)
        else:
            raise ValueError(error_msg)

    # ---- multiplier clauses ----
    # Amount and time use the inverse-pattern builder (factor: canonical -> preferred).
    # Weight is now handled separately by the 9-case transition factor below,
    # NOT by the inverse-pattern builder, since the new base unit preserves
    # the weight qualifier.
    amount_clause = _concat_builders_by_patterns(
        builder=_pattern_to_factor_builder_for_preferred,
        patterns=[L_REGEX, MU_REGEX, MG_REGEX, NG_REGEX, G_REGEX],
        else_case='1'
    )
    time_clause = _concat_builders_by_patterns(
        builder=_pattern_to_factor_builder_for_preferred,
        patterns=[HR_REGEX],
        else_case='1'
    )

    # Weight-transition factor based on (base_wt, pref_wt). Only kg<->none and
    # lb<->none cases reference `weight_kg`. kg<->lb is the constant `KG_PER_LB`.
    weight_factor_clause = f"""
        CASE
            WHEN _base_wt = _pref_wt THEN 1
            WHEN _base_wt = '/kg' AND _pref_wt = '/lb' THEN 1.0/{KG_PER_LB}
            WHEN _base_wt = '/lb' AND _pref_wt = '/kg' THEN {KG_PER_LB}
            WHEN _base_wt = ''    AND _pref_wt = '/kg' THEN 1.0/weight_kg
            WHEN _base_wt = ''    AND _pref_wt = '/lb' THEN 1.0/(weight_kg * {KG_PER_LB})
            WHEN _base_wt = '/kg' AND _pref_wt = ''    THEN weight_kg
            WHEN _base_wt = '/lb' AND _pref_wt = ''    THEN weight_kg * {KG_PER_LB}
            ELSE 1
            END
    """

    # Schema-aware: only emit columns that aren't already there
    cols = set(med_df.columns)
    has_unit_class = '_unit_class' in cols
    has_clean_unit = '_clean_unit' in cols
    has_med_dose = 'med_dose' in cols
    has_weight_kg = 'weight_kg' in cols

    # Fallback values when conversion cannot proceed
    dose_fallback = "med_dose" if has_med_dose else "_base_dose"
    unit_fallback = "_clean_unit" if has_clean_unit else "_base_unit"
    # Identity short-circuit: only meaningful when both _clean_unit and med_dose
    # are available. Returns med_dose bit-exact (no multiplication).
    identity_dose_branch = (
        f"WHEN _convert_status = 'success' AND _clean_unit = _preferred_unit THEN {dose_fallback}\n            "
        if has_clean_unit and has_med_dose else ""
    )

    # If weight_kg isn't in the schema, treat it as NULL throughout.
    weight_kg_expr = "weight_kg" if has_weight_kg else "CAST(NULL AS DOUBLE)"

    classify_extra = (
        f""", _unit_class: CASE
                WHEN _base_unit IN ('{RATE_UNITS_STR}') THEN 'rate'
                WHEN _base_unit IN ('{AMOUNT_UNITS_STR}') THEN 'amount'
                ELSE 'unrecognized' END"""
        if not has_unit_class else ""
    )

    # CTE chain: classified -> statused -> final select. Each step adds named
    # columns the next can reference, eliminating the verbose nested-CASE
    # duplication of the previous implementation.
    q = f"""
    WITH classified AS (
        SELECT l.*
            {classify_extra}
            , _unit_subclass: CASE
                WHEN regexp_matches(_base_unit, '{MASS_REGEX}') THEN 'mass'
                WHEN regexp_matches(_base_unit, '{VOLUME_REGEX}') THEN 'volume'
                WHEN regexp_matches(_base_unit, '{UNIT_REGEX}') THEN 'unit'
                ELSE 'unrecognized' END
            , _unit_class_preferred: CASE
                WHEN _preferred_unit IN ('{RATE_UNITS_STR}') THEN 'rate'
                WHEN _preferred_unit IN ('{AMOUNT_UNITS_STR}') THEN 'amount'
                ELSE 'unrecognized' END
            , _unit_subclass_preferred: CASE
                WHEN regexp_matches(_preferred_unit, '{MASS_REGEX}') THEN 'mass'
                WHEN regexp_matches(_preferred_unit, '{VOLUME_REGEX}') THEN 'volume'
                WHEN regexp_matches(_preferred_unit, '{UNIT_REGEX}') THEN 'unit'
                ELSE 'unrecognized' END
            , _base_wt: {_weight_qual_clause('_base_unit')}
            , _pref_wt: {_weight_qual_clause('_preferred_unit')}
        FROM med_df l
    )
    , statused AS (
        SELECT *
            -- _needs_wt = 1 iff exactly one side has a weight qualifier (XOR)
            , _needs_wt: CASE
                WHEN (_base_wt != '' AND _pref_wt = '')
                  OR (_base_wt = '' AND _pref_wt != '') THEN 1
                ELSE 0 END
            , _convert_status: CASE
                WHEN _base_unit IS NULL
                    THEN 'original unit is missing'
                WHEN _unit_class = 'unrecognized' OR _unit_subclass = 'unrecognized'
                    THEN 'original unit ' || _base_unit || ' is not recognized'
                WHEN _unit_class_preferred = 'unrecognized' OR _unit_subclass_preferred = 'unrecognized'
                    THEN 'user-preferred unit ' || _preferred_unit || ' is not recognized'
                WHEN _unit_class != _unit_class_preferred
                    THEN 'cannot convert ' || _unit_class || ' to ' || _unit_class_preferred
                WHEN _unit_subclass != _unit_subclass_preferred
                    THEN 'cannot convert ' || _unit_subclass || ' to ' || _unit_subclass_preferred
                -- two-message split: weight required but missing
                WHEN _base_wt != '' AND _pref_wt = '' AND {weight_kg_expr} IS NULL
                    THEN 'cannot convert weighted to unweighted: weight_kg is missing'
                WHEN _base_wt = '' AND _pref_wt != '' AND {weight_kg_expr} IS NULL
                    THEN 'cannot convert unweighted to weighted: weight_kg is missing'
                ELSE 'success'
                END
        FROM classified
    )"""

    if show_intermediate:
        q += f"""
    SELECT *
        , _amount_multiplier_preferred: {amount_clause}
        , _time_multiplier_preferred: {time_clause}
        , _weight_multiplier_preferred: {weight_factor_clause}
        , med_dose_converted: CASE
            {identity_dose_branch}WHEN _convert_status = 'success' THEN _base_dose * _amount_multiplier_preferred * _time_multiplier_preferred * _weight_multiplier_preferred
            ELSE {dose_fallback}
            END
        , med_dose_unit_converted: CASE
            WHEN _convert_status = 'success' THEN _preferred_unit
            ELSE {unit_fallback}
            END
    FROM statused
    """
    else:
        q += f"""
    SELECT *
        , med_dose_converted: CASE
            {identity_dose_branch}WHEN _convert_status = 'success' THEN _base_dose * ({amount_clause}) * ({time_clause}) * ({weight_factor_clause})
            ELSE {dose_fallback}
            END
        , med_dose_unit_converted: CASE
            WHEN _convert_status = 'success' THEN _preferred_unit
            ELSE {unit_fallback}
            END
    FROM statused
    """
    return duckdb.sql(q)


