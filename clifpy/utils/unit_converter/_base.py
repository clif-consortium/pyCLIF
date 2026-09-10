"""Stage 1: raw dose units -> canonical base units.

Normalises the amount axis (mass->mcg, volume->ml, unit->u) and the time axis
(->/min), and collapses `/lb` into `/kg` by a constant factor. Stage 1 never
references patient weight.
"""

import pandas as pd
import duckdb
from typing import Tuple, Collection

from clifpy.utils.logging_config import get_logger

from ._grammar import (
    KG_PER_LB, AMOUNT_FACTOR_PATTERNS, TIME_FACTOR_PATTERNS,
    SUBCLASS_REGEX, MASS_REGEX, VOLUME_REGEX, UNIT_REGEX,
    LB_REGEX, WEIGHT_REGEX, RATE_UNITS_STR, AMOUNT_UNITS_STR,
    DEFAULT_COUNTABLE_UNITS, _convert_set_to_str_for_sql,
)
from ._sql import (
    _concat_builders_by_patterns,
    _pattern_to_factor_builder_for_base,
    _base_unit_case_ladder,
)
from ._clean import _clean_dose_unit_formats_duckdb, _clean_dose_unit_names_duckdb
from ._counts import _create_unit_conversion_counts_table

logger = get_logger('utils.unit_converter')


def _convert_clean_units_to_base_units(
    med_df: pd.DataFrame | duckdb.DuckDBPyRelation,
    show_intermediate: bool = False,
    countable_units: Collection[str] | None = None
) -> duckdb.DuckDBPyRelation:
    """Convert clean dose units to base units (weight-preserving).

    Stage-1 of the conversion pipeline. Normalizes the **amount** axis
    (mass→mcg, volume→ml, unit→u) and the **time** axis (/hr→/min) but
    preserves the weight qualifier (`/kg`, `/lb`, or none) verbatim from
    `_clean_unit`. Stage 1 never references `weight_kg` — patient weight is
    consumed only in stage 2 (preferred-unit conversion), and only when source
    and target differ in *presence* of a weight qualifier.

    Parameters
    ----------
    med_df : pd.DataFrame or duckdb.DuckDBPyRelation
        Medication data with required columns:

        - `_clean_unit`: cleaned unit strings (after format + name cleaning)
        - `med_dose`: original dose values

        `weight_kg` is **not** required by this stage; it is ignored if present.

    show_intermediate : bool, default False
        If True, expose intermediate `_amount_multiplier` and `_time_multiplier`
        columns for QA. If False, inline the multiplier expressions.

    Returns
    -------
    duckdb.DuckDBPyRelation
        Input columns plus:

        - `_unit_class`: 'rate', 'amount', or 'unrecognized'
        - `_base_dose`: dose in base units (no weight scaling applied)
        - `_base_unit`: base unit, preserving weight qualifier
            * mass rate `mcg/kg/hr` → `mcg/kg/min`
            * volume rate `l/hr` → `ml/min`
            * unit rate `mu/min` → `u/min`
            * mass amount `mg` → `mcg`
            * unrecognized → original `_clean_unit`

    Notes
    -----
    Stage 1 is bit-exact: every multiplier is a constant (no `weight_kg`),
    so float drift is bounded by the amount/time factors only. Identity
    conversions (e.g. `mcg/kg/min` → `mcg/kg/min`) preserve the input dose
    exactly via factor 1×1.
    """

    # Countable dosage forms pass through untouched, exactly like unrecognized
    # units; only the reported class and status differ.
    countable_units_str = _convert_set_to_str_for_sql(
        set(DEFAULT_COUNTABLE_UNITS if countable_units is None else countable_units)
    )
    # Classes whose dose must not be scaled.
    passthrough = "('unrecognized', 'countable')"

    amount_clause = _concat_builders_by_patterns(
        builder=_pattern_to_factor_builder_for_base,
        patterns=AMOUNT_FACTOR_PATTERNS,
        else_case='1'
        )

    time_clause = _concat_builders_by_patterns(
        builder=_pattern_to_factor_builder_for_base,
        patterns=TIME_FACTOR_PATTERNS,
        else_case='1'
        )

    # Stage 1 weight handling: collapse `/lb` into the canonical `/kg` axis
    # using a CONSTANT factor (no patient weight). The base unit's weight
    # qualifier is in {'/kg', ''} only — never `/lb`. This keeps the base set
    # as small as possible, in line with the goal of `_base_unit` being a
    # canonical pivot.
    #
    # - `weight_const_expr`: 1 for `/kg` or unweighted, KG_PER_LB (2.20462)
    #   for `/lb`. NOT a function of patient weight.
    # - `base_weight_qual_expr`: '/kg' if source had `/kg` OR `/lb`, else ''.
    #   This is INTENTIONALLY different from `_weight_qual_clause()`, which
    #   preserves the actual `/kg` vs `/lb` axis for stage 2's transition
    #   factor. Here in stage 1 we collapse both into `/kg` because the
    #   canonical `_base_unit` never carries `/lb`. The two expressions are
    #   not interchangeable.
    weight_const_expr = (
        f"CASE WHEN regexp_matches(_clean_unit, '{LB_REGEX}') THEN {KG_PER_LB} "
        f"ELSE 1 END"
    )
    base_weight_qual_expr = (
        f"CASE WHEN regexp_matches(_clean_unit, '{WEIGHT_REGEX}') THEN '/kg' "
        f"ELSE '' END"
    )
    # One branch pair per subclass, derived from SUBCLASS_SPEC. Standalone
    # subclasses (ppm, cells) get a single amount branch with no qualifier.
    base_unit_ladder = _base_unit_case_ladder(base_weight_qual_expr)

    if show_intermediate:
        q = f"""
        SELECT *
            , _unit_class: CASE
                WHEN _clean_unit IN ('{RATE_UNITS_STR}') THEN 'rate'
                WHEN _clean_unit IN ('{AMOUNT_UNITS_STR}') THEN 'amount'
                WHEN _clean_unit IN ('{countable_units_str}') THEN 'countable'
                ELSE 'unrecognized' END
            , _amount_multiplier: CASE
                WHEN _unit_class IN {passthrough} THEN 1 ELSE ({amount_clause}) END
            , _time_multiplier: CASE
                WHEN _unit_class IN {passthrough} THEN 1 ELSE ({time_clause}) END
            , _weight_multiplier: CASE
                WHEN _unit_class IN {passthrough} THEN 1 ELSE ({weight_const_expr}) END
            -- amount + time + (constant) lb→kg scaling. No patient weight.
            , _base_dose: CASE
                WHEN _unit_class IN {passthrough} THEN med_dose
                ELSE med_dose * _amount_multiplier * _time_multiplier * _weight_multiplier
                END
            -- base unit collapses /lb into /kg; unweighted stays unweighted
            , _base_unit: CASE
                WHEN _unit_class IN {passthrough} THEN _clean_unit
                {base_unit_ladder}
                END
        FROM med_df
        """
    else:
        amount_expr = f"CASE WHEN _unit_class IN {passthrough} THEN 1 ELSE ({amount_clause}) END"
        time_expr = f"CASE WHEN _unit_class IN {passthrough} THEN 1 ELSE ({time_clause}) END"
        weight_expr = f"CASE WHEN _unit_class IN {passthrough} THEN 1 ELSE ({weight_const_expr}) END"

        q = f"""
        WITH classified AS (
            SELECT *
                , _unit_class: CASE
                    WHEN _clean_unit IN ('{RATE_UNITS_STR}') THEN 'rate'
                    WHEN _clean_unit IN ('{AMOUNT_UNITS_STR}') THEN 'amount'
                    WHEN _clean_unit IN ('{countable_units_str}') THEN 'countable'
                    ELSE 'unrecognized' END
            FROM med_df
        )
        SELECT *
            , _base_dose: CASE
                WHEN _unit_class IN {passthrough} THEN med_dose
                ELSE med_dose * ({amount_expr}) * ({time_expr}) * ({weight_expr})
                END
            , _base_unit: CASE
                WHEN _unit_class IN {passthrough} THEN _clean_unit
                {base_unit_ladder}
                END
        FROM classified
        """
    return duckdb.sql(q)


def standardize_dose_to_base_units(
    med_df: pd.DataFrame,
    vitals_df: pd.DataFrame = None,
    show_intermediate: bool = False,
    id_name: str = 'hospitalization_id',
    countable_units: Collection[str] | None = None,
) -> Tuple[duckdb.DuckDBPyRelation, duckdb.DuckDBPyRelation]:
    """Standardize medication dose units to a base set of standard units.

    Main public API function that performs complete dose unit standardization
    pipeline: format cleaning, name cleaning, and unit conversion.
    Returns both base data and a summary table of conversions.

    Parameters
    ----------
    med_df : pd.DataFrame
        Medication DataFrame with required columns:

        - med_dose_unit: Original dose unit strings
        - med_dose: Dose values
        - weight_kg: Patient weights (optional, can be added from vitals_df)

        Additional columns are preserved in output.

    vitals_df : pd.DataFrame, optional
        Vitals DataFrame for extracting patient weights if not in med_df.
        Required columns if weight_kg missing from med_df:

        - hospitalization_id: Patient identifier
        - recorded_dttm: Timestamp of vital recording
        - vital_category: Must include 'weight_kg' values
        - vital_value: Weight values

    show_intermediate : bool, default False
        If True, expose intermediate columns (_amount_multiplier, _time_multiplier,
        _weight_multiplier) for QA purposes. If False (default), inline multiplier
        expressions to avoid materializing intermediate columns.

    Returns
    -------
    Tuple[pd.DataFrame, pd.DataFrame]
        A tuple containing:

        - [0] base medication DataFrame with additional columns:

            * _clean_unit: Cleaned unit string
            * _unit_class: 'rate', 'amount', or 'unrecognized'
            * _base_dose: base dose value
            * _base_unit: base unit

            If show_intermediate=True, also includes:

            * _amount_multiplier, _time_multiplier, _weight_multiplier: Conversion factors

        - [1] Summary counts DataFrame showing conversion patterns and frequencies

    Raises
    ------
    ValueError
        If required columns are missing from med_df.

    Examples
    --------
    >>> import pandas as pd
    >>> med_df = pd.DataFrame({
    ...     'med_dose': [6, 100, 500],
    ...     'med_dose_unit': ['MCG/KG/HR', 'mL / hr', 'mg'],
    ...     'weight_kg': [70, 80, 75]
    ... })
    >>> base_df, counts_df = standardize_dose_to_base_units(med_df)
    >>> '_base_unit' in base_df.columns
    True
    >>> 'count' in counts_df.columns
    True

    Notes
    -----
    Standard (base) units for conversion:

    - Rate units (with optional weight qualifier): mcg/min, mcg/kg/min,
      ml/min, ml/kg/min, u/min, u/kg/min — all per minute, `/lb` collapsed
      into `/kg` via the constant `KG_PER_LB`.
    - Amount units (with optional weight qualifier): mcg, mcg/kg, ml, ml/kg,
      u, u/kg — `/lb` likewise collapsed into `/kg`.

    The function automatically handles:

    - Weight-based dosing (/kg, /lb) using the constant `KG_PER_LB` to collapse
      `/lb` into `/kg` in stage 1 (no patient weight needed). `weight_kg` is
      consumed only in stage 2 (preferred-unit conversion) when source and
      target differ in *presence* of a weight qualifier.
    - Time conversions (per hour to per minute)
    - Volume conversions (L to mL)
    - Mass conversions (mg, ng, g to mcg)
    - Unit conversions (milli-units to units)

    Unrecognized units are flagged but preserved in the output.
    """
    logger.info("Standardizing dose units to base...")

    # NOTE: under the weight-aware redesign, base conversion no longer needs
    # `weight_kg`. The `vitals_df` parameter is retained for API compatibility
    # but is unused here. Patient weight is consumed only in stage 2
    # (preferred-unit conversion), and only for rows where source and target
    # differ in weight-qualifier presence. See `convert_dose_units_by_med_category`.
    _ = vitals_df  # explicitly mark as unused

    # check if the required columns are present (weight_kg no longer required)
    required_columns = {'med_dose_unit', 'med_dose'}
    missing_columns = required_columns - set(med_df.columns)
    if missing_columns:
        raise ValueError(f"The following column(s) are required but not found: {missing_columns}")

    # Clean dose units using DuckDB to avoid pandas materialization
    logger.debug("Cleaning unit formats...")
    med_df_cleaned = _clean_dose_unit_formats_duckdb(med_df)
    logger.debug("Cleaning unit names...")
    med_df_cleaned = _clean_dose_unit_names_duckdb(med_df_cleaned)
    logger.debug("Converting to base units...")
    med_df_base = _convert_clean_units_to_base_units(
        med_df_cleaned,
        show_intermediate=show_intermediate,
        countable_units=countable_units,
    )
    convert_counts_df = _create_unit_conversion_counts_table(
        med_df_base,
        group_by=['med_dose_unit', '_clean_unit', '_base_unit', '_unit_class']
        )

    logger.info("Standardization complete")
    return med_df_base, convert_counts_df
    
