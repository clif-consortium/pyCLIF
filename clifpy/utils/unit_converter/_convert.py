"""Orchestration: the public `convert_dose_units_by_med_category` entry point.

Wires stage 1, the lazy weight join and stage 2 together, manages the DuckDB
temp-table lifecycle, and preserves timezone metadata across the pipeline.
"""

import pandas as pd
import duckdb
from pathlib import Path
from typing import Tuple, List, Dict, Union, Literal, Collection, overload
from duckdb import DuckDBPyRelation

from clifpy.utils.logging_config import get_logger
from clifpy.utils._duckdb_helpers import (
    _register_temp_table,
    _cleanup_temp_tables,
)

from ._grammar import ALL_ACCEPTABLE_UNITS, _weight_qual_clause
from ._clean import _clean_dose_unit_formats_duckdb, _clean_dose_unit_names_duckdb
from ._columns import (
    build_rename_map,
    rename_to_internal,
    rename_from_internal,
    validate_column_name,
)
from ._base import standardize_dose_to_base_units
from ._weight import find_most_recent_weight
from ._preferred import _convert_base_units_to_preferred_units
from ._counts import _create_unit_conversion_counts_table
from ._targets import load_dose_unit_targets

logger = get_logger('utils.unit_converter')


@overload
def convert_dose_units_by_med_category(
    med_df: pd.DataFrame | DuckDBPyRelation,
    vitals_df: pd.DataFrame | DuckDBPyRelation = ...,
    preferred_units: dict = ...,
    show_intermediate: bool = ...,
    override: bool = ...,
    return_rel: Literal[False] = ...,
    id_name: str = ...,
    fallback_on_earliest: bool = ...,
) -> Tuple[pd.DataFrame, pd.DataFrame]: ...


@overload
def convert_dose_units_by_med_category(
    med_df: pd.DataFrame | DuckDBPyRelation,
    vitals_df: pd.DataFrame | DuckDBPyRelation = ...,
    preferred_units: dict = ...,
    show_intermediate: bool = ...,
    override: bool = ...,
    return_rel: Literal[True] = ...,
    id_name: str = ...,
    fallback_on_earliest: bool = ...,
) -> Tuple[DuckDBPyRelation, DuckDBPyRelation]: ...


def _capture_tz_columns(df: pd.DataFrame) -> dict:
    """Map ``{column_name: tzinfo}`` for every tz-aware datetime column in ``df``.

    Used to preserve input timestamp timezones across the DuckDB round-trip in
    :func:`convert_dose_units_by_med_category`: DuckDB renders TIMESTAMPTZ to pandas
    in the *connection's* TimeZone, which can differ from the input's zone. Returns
    an empty dict for naive input (no zones to preserve). See docs/tz_dx.md (§9).
    """
    return {
        col: df[col].dtype.tz
        for col in df.columns
        if isinstance(df[col].dtype, pd.DatetimeTZDtype)
    }


def _relabel_tz_columns(df: pd.DataFrame, tz_map: dict) -> pd.DataFrame:
    """Restore each captured column's original tz label (instant-preserving).

    ``tz_convert`` only relabels the zone, not the underlying UTC instant, so this
    corrects a mislabel introduced by rendering TIMESTAMPTZ under a different
    default-connection TimeZone. No-op when ``tz_map`` is empty (naive input).
    """
    for col, tz in tz_map.items():
        if col in df.columns and isinstance(df[col].dtype, pd.DatetimeTZDtype):
            df[col] = df[col].dt.tz_convert(tz)
    return df


def _pin_default_tz_for_deferred_render(tz_map: dict) -> None:
    """Pin the default connection's TimeZone so a later caller ``.df()`` on a
    returned relation renders TIMESTAMPTZ in the intended zone.

    Persistent by design (a deferred render cannot be intercepted). Best-effort:
    swallows errors so returning the relation never fails on an exotic zone. Only
    relevant for ``return_rel=True`` with tz-aware input. See docs/tz_dx.md (§9).
    """
    if not tz_map:
        return
    tz = tz_map.get('admin_dttm') or next(iter(tz_map.values()))
    try:
        duckdb.execute(f"SET TimeZone = '{tz}'")
    except Exception:
        logger.warning(f"Could not pin default-connection TimeZone to '{tz}' for deferred render")


def convert_dose_units_by_med_category(
    med_df: pd.DataFrame | DuckDBPyRelation,
    vitals_df: pd.DataFrame | DuckDBPyRelation = None,
    preferred_units: dict = None,
    show_intermediate: bool = False,
    override: bool = False,
    return_rel: bool = False,
    id_name: str = 'hospitalization_id',
    fallback_on_earliest: bool = False,
    countable_units: Collection[str] | None = None,
    category_col: str = 'med_category',
    dose_col: str = 'med_dose',
    unit_col: str = 'med_dose_unit',
    time_col: str = 'admin_dttm',
    converted_dose_col: str = 'med_dose_converted',
    converted_unit_col: str = 'med_dose_unit_converted',
) -> Union[Tuple[pd.DataFrame, pd.DataFrame], Tuple[DuckDBPyRelation, DuckDBPyRelation]]:
    """Convert medication dose units to preferred units, weight-aware and DuckDB-native.

    Two-stage pipeline:

    1. **Standardize to base units** (weight-preserving). Stage 1 normalizes amount
       and time but keeps the weight qualifier (`/kg`, `/lb`, or none) verbatim.
    2. **Convert to preferred units**. Stage 2 applies amount/time factors and a
       9-case weight-transition factor. Patient weight (`weight_kg`) is consumed
       *only* when source and target differ in weight-qualifier presence.

    Performance follows `docs/duckdb_perf_guide.md`: input is materialized once
    into a DuckDB temp table (boundary 2a), validations use ANTI JOIN + fetchall
    (no `.to_df()`), and the lazy weight join uses `UNION ALL BY NAME` over an
    ASOF subset.

    Parameters
    ----------
    med_df : pd.DataFrame or DuckDBPyRelation
        Medication data with required columns:

        - `med_dose`: original dose values (numeric)
        - `med_dose_unit`: original dose unit strings
        - `med_category`: medication category (e.g., 'propofol')
        - `weight_kg`: optional patient weight. If absent and any conversion
          requires patient weight, vitals_df is consulted (lazy join).
        - `admin_dttm`, `{id_name}`: required when our internal weight lookup
          fires (i.e., `weight_kg` not in `med_df` and at least one row needs weight).

    vitals_df : pd.DataFrame or DuckDBPyRelation, optional
        Vitals data for weight lookup. Required only if `weight_kg` is missing
        AND at least one row needs a weighted ↔ unweighted transition.
    preferred_units : dict, optional
        `{med_category: target_unit_string}`. Categories without an entry use
        their base unit as the target.
    show_intermediate : bool, default False
        If True, retain QA columns (`_amount_multiplier_preferred`, etc.).
    override : bool, default False
        If True, log warnings instead of raising on validation failures.
    return_rel : bool, default False
        If True, return lazy DuckDBPyRelations. Caller is then responsible for
        any cleanup of temp tables registered during the call.
    id_name : str, default 'hospitalization_id'
        ID column name for the weight ASOF join.
    fallback_on_earliest : bool, default False
        Forwarded to `find_most_recent_weight` when our internal lookup fires.
        When True, rows whose ASOF returns NULL fall back to the earliest
        charted weight for the same hospitalization (handles documentation lag).

    Returns
    -------
    Tuple[pd.DataFrame, pd.DataFrame] or Tuple[DuckDBPyRelation, DuckDBPyRelation]
        Tuple of `(converted, counts)`. Format controlled by `return_rel`.

        Converted columns include `med_dose_converted`, `med_dose_unit_converted`,
        `_convert_status`. With `show_intermediate=True`, also `_needs_wt`,
        `_weight_source`, `_amount_multiplier_preferred`, etc.

    Raises
    ------
    ValueError
        Required columns missing, or validation failures (when `override=False`).

    Notes
    -----
    User-prefilled `weight_kg` is honored exactly: if the column is present in
    `med_df`, our lookup is skipped entirely (NULLs are preserved as-is).
    """
    n_categories = len(preferred_units) if preferred_units else 0
    logger.info(f"Converting dose units for {n_categories} med categories...")

    # ------------------------------------------------------------------
    # Boundary 2a (per docs/duckdb_perf_guide.md): materialize pandas input
    # once into a DuckDB temp table. This input is referenced multiple times
    # downstream (validation, base conversion, preferred-unit join, weight
    # join, conversion). Without this, every reference re-scans the pandas
    # DataFrame with no statistics.
    # ------------------------------------------------------------------
    materialized_input = False
    input_tz_map = {}
    if isinstance(med_df, pd.DataFrame):
        # Capture tz-aware input zones BEFORE the DuckDB round-trip so we can
        # restore them on output — DuckDB renders TIMESTAMPTZ in the default
        # connection's zone, not necessarily the input's. See docs/tz_dx.md (§9).
        input_tz_map = _capture_tz_columns(med_df)
        duckdb.execute("CREATE OR REPLACE TEMP TABLE _med_unit_input AS SELECT * FROM med_df")
        _register_temp_table("_med_unit_input")
        med_df = duckdb.table("_med_unit_input")
        materialized_input = True

    # Project the caller's columns onto the names the pipeline's SQL is written
    # against. Everything downstream then works on fixed literal names, so no
    # caller-supplied identifier ever reaches an internal f-string. Validation
    # and quoting happen inside these two helpers. See _columns.py.
    rename_map = build_rename_map(category_col, dose_col, unit_col, time_col)
    validate_column_name(converted_dose_col, 'converted_dose_col')
    validate_column_name(converted_unit_col, 'converted_unit_col')
    med_df = rename_to_internal(med_df, rename_map)

    try:
        # --------------------------------------------------------------
        # Validate requested med_categories via ANTI JOIN (no .to_df()).
        # --------------------------------------------------------------
        # Lookup table carrying BOTH the caller's spelling and its normalised
        # form. Built once, off the (small) preferred_units dict rather than
        # the med table.
        #
        # WARNING: `_preferred_unit_clean` is not cosmetic. Stage 2 matches the
        # preferred unit against the factor regexes, and the caller's raw
        # string does not necessarily match them. mCIDE spells pitocin's target
        # `milli-units/min`, which fails `MU_REGEX = ^(mu)` because it starts
        # "mi" -- the multiplier would fall through to 1 and the dose come out
        # 1000x wrong. `units/kg/hr` only survives by accident, since `^(u)`
        # matches the leading "u" of "units". `_preferred_unit` keeps the
        # caller's spelling for output; `_preferred_unit_clean` drives every
        # regex match and the acceptability check below.
        preferred_units_df = pd.DataFrame(
            preferred_units.items() if preferred_units else [],
            columns=['med_category', '_preferred_unit'],
        )
        if len(preferred_units_df):
            _pref_rel = _clean_dose_unit_formats_duckdb(
                preferred_units_df, col='_preferred_unit',
                out_col='_preferred_unit_clean',
            )
            _pref_rel = _clean_dose_unit_names_duckdb(
                _pref_rel, col='_preferred_unit_clean',
            )
            preferred_units_df = _pref_rel.to_df()
        else:
            preferred_units_df['_preferred_unit_clean'] = pd.Series(dtype='object')

        if preferred_units:
            requested_categories_df = pd.DataFrame(
                {'med_category': sorted(preferred_units.keys())}
            )
            extra_rows = duckdb.sql("""
                SELECT med_category
                FROM requested_categories_df
                ANTI JOIN (SELECT DISTINCT med_category FROM med_df) existing
                  ON requested_categories_df.med_category = existing.med_category
            """).fetchall()
            if extra_rows:
                extras = sorted(row[0] for row in extra_rows)
                # Standardizing against a schema of a few hundred categories
                # will always list many the data does not contain, so summarise
                # rather than dumping every name into the log.
                shown = ', '.join(extras[:8])
                if len(extras) > 8:
                    shown += f", ... (+{len(extras) - 8} more)"
                error_msg = (
                    f"{len(extras)} med_category value(s) are given a preferred unit "
                    f"but not found in the input med_df: {shown}"
                )
                if override:
                    logger.warning(error_msg)
                else:
                    raise ValueError(error_msg)

            # ----------------------------------------------------------
            # Validate the units the CALLER actually asked for.
            #
            # NOTE: this deliberately runs before the fallback join below.
            # Categories with no entry in `preferred_units` fall back to their
            # own `_base_unit`, and if stage 1 could not map a drug's original
            # unit that unmappable string carries through as its
            # `_preferred_unit`. Validating post-fallback made one
            # unconvertible drug abort a call that never mentioned it -- see
            # clifpy#153, where asking for vasopressor units failed with
            # `{'meq/min'}` because sodium bicarbonate happened to be in the
            # same table. Fallback units are clifpy's own derived values, not
            # user input, and their unconvertibility is already reported per
            # row through `_convert_status`.
            # NOTE: iterate with zip, not itertuples -- pandas renames
            # leading-underscore columns to positional _1/_2 there.
            bad_by_category = {
                category: raw
                for category, raw, clean in zip(
                    preferred_units_df['med_category'],
                    preferred_units_df['_preferred_unit'],
                    preferred_units_df['_preferred_unit_clean'],
                )
                if clean not in ALL_ACCEPTABLE_UNITS
            }
            if bad_by_category:
                error_msg = (
                    f"Cannot accommodate the conversion to the following preferred "
                    f"units: {bad_by_category}. Consult the function documentation "
                    f"for a list of acceptable units."
                )
                if override:
                    logger.warning(error_msg)
                else:
                    raise ValueError(error_msg)

        # --------------------------------------------------------------
        # Stage 1: standardize to base units (no weight needed).
        # --------------------------------------------------------------
        try:
            med_df_base, _ = standardize_dose_to_base_units(
                med_df, vitals_df, show_intermediate=show_intermediate,
                id_name=id_name, countable_units=countable_units,
            )
        except ValueError as e:
            raise ValueError(f"Error standardizing dose units to base units: {e}")

        # --------------------------------------------------------------
        # Join preferred units onto base table.
        # --------------------------------------------------------------
        try:
            med_df_preferred = duckdb.sql("""
                SELECT l.*
                    -- categories without an explicit preferred unit fall back to base
                    , _preferred_unit: COALESCE(r._preferred_unit, l._base_unit)
                    -- Marks whether the caller actually requested this unit.
                    -- Stage 2 validates only explicit rows; fallback units are
                    -- derived by clifpy and are reported per row via
                    -- `_convert_status` instead (clifpy#153).
                    , _preferred_is_explicit: r._preferred_unit IS NOT NULL
                    -- `_base_unit` is already canonical, so the fallback needs
                    -- no further normalisation.
                    , _preferred_unit_clean: COALESCE(
                        r._preferred_unit_clean, l._base_unit)
                FROM med_df_base l
                LEFT JOIN preferred_units_df r USING (med_category)
            """)
        except Exception as e:
            raise ValueError(f"Error joining preferred units: {e}")

        # --------------------------------------------------------------
        # Lazy weight join.
        # Skip entirely if the user pre-filled `weight_kg` (their strategy wins).
        # Otherwise, only ASOF-join the rows that need weight; UNION ALL BY NAME
        # the rest with NULL weight values. Empty-needs case skips the join.
        # --------------------------------------------------------------
        if 'weight_kg' not in med_df_preferred.columns:
            base_wt_expr = _weight_qual_clause('_base_unit')
            pref_wt_expr = _weight_qual_clause('_preferred_unit')
            needs_wt_filter = (
                f"(({base_wt_expr}) != '' AND ({pref_wt_expr}) = '') "
                f"OR (({base_wt_expr}) = '' AND ({pref_wt_expr}) != '')"
            )

            # fetchone()-based gate: cheap single-tuple materialization; no DataFrame.
            any_needs_wt = duckdb.sql(f"""
                SELECT 1 FROM med_df_preferred
                WHERE {needs_wt_filter}
                LIMIT 1
            """).fetchone() is not None

            if any_needs_wt:
                if vitals_df is None:
                    error_msg = (
                        "weight_kg is missing from med_df and at least one conversion "
                        "requires patient weight (weighted <-> unweighted transition), "
                        "but vitals_df=None. Either pre-fill med_df['weight_kg'] or "
                        "provide vitals_df."
                    )
                    if override:
                        logger.warning(error_msg)
                        # fall through with weight_kg=NULL; the conversion will
                        # mark these rows as failed via _convert_status.
                        med_df_preferred = duckdb.sql("""
                            SELECT *
                                , CAST(NULL AS DOUBLE) AS weight_kg
                                , CAST(NULL AS TIMESTAMP) AS _weight_recorded_dttm
                                , CAST(NULL AS VARCHAR) AS _weight_source
                            FROM med_df_preferred
                        """)
                    else:
                        raise ValueError(error_msg)
                else:
                    # Split, ASOF only the needs-wt subset, UNION ALL BY NAME the rest.
                    needs_wt_subset = duckdb.sql(f"""
                        SELECT * FROM med_df_preferred WHERE {needs_wt_filter}
                    """)
                    no_wt_subset = duckdb.sql(f"""
                        SELECT *
                            , CAST(NULL AS DOUBLE) AS weight_kg
                            , CAST(NULL AS TIMESTAMP) AS _weight_recorded_dttm
                            , CAST(NULL AS VARCHAR) AS _weight_source
                        FROM med_df_preferred
                        WHERE NOT ({needs_wt_filter})
                    """)
                    needs_wt_joined = find_most_recent_weight(
                        needs_wt_subset,
                        vitals_df,
                        id_name=id_name,
                        fallback_on_earliest=fallback_on_earliest,
                    )
                    med_df_preferred = duckdb.sql("""
                        SELECT * FROM needs_wt_joined
                        UNION ALL BY NAME
                        SELECT * FROM no_wt_subset
                    """)
            else:
                # No row needs weight. Add NULL placeholders for schema consistency.
                logger.debug("No rows require patient weight; skipping vitals join.")
                med_df_preferred = duckdb.sql("""
                    SELECT *
                        , CAST(NULL AS DOUBLE) AS weight_kg
                        , CAST(NULL AS TIMESTAMP) AS _weight_recorded_dttm
                        , CAST(NULL AS VARCHAR) AS _weight_source
                    FROM med_df_preferred
                """)
        # else: user pre-filled weight_kg; trust it as-is.

        # --------------------------------------------------------------
        # Stage 2: convert to preferred units.
        # --------------------------------------------------------------
        try:
            logger.debug("Converting to preferred units...")
            med_df_converted = _convert_base_units_to_preferred_units(
                med_df_preferred, override=override, show_intermediate=show_intermediate
            )
        except ValueError as e:
            raise ValueError(f"Error converting dose units to preferred units: {e}")

        # --------------------------------------------------------------
        # Counts table.
        # --------------------------------------------------------------
        try:
            convert_counts_df = _create_unit_conversion_counts_table(
                med_df_converted,
                group_by=[
                    'med_category',
                    'med_dose_unit', '_clean_unit', '_base_unit', '_unit_class',
                    '_preferred_unit', 'med_dose_unit_converted', '_convert_status',
                ],
            )
        except ValueError as e:
            raise ValueError(f"Error creating unit conversion counts table: {e}")

        logger.info("Dose unit conversion complete")

        # Restore the caller's column names and apply the requested output
        # names. The counts table keeps the internal names: it is a QA summary
        # keyed on clifpy's own vocabulary, not a projection of the input.
        med_df_converted = rename_from_internal(
            med_df_converted, rename_map, converted_dose_col, converted_unit_col
        )

        # --------------------------------------------------------------
        # Output column hygiene + final return.
        # --------------------------------------------------------------
        if show_intermediate:
            if return_rel:
                # Caller owns cleanup; do not drop temp tables yet.
                _pin_default_tz_for_deferred_render(input_tz_map)
                materialized_input = False
                return med_df_converted, convert_counts_df
            return (
                _relabel_tz_columns(med_df_converted.to_df(), input_tz_map),
                convert_counts_df.to_df(),
            )

        # Default (show_intermediate=False): drop QA columns the user didn't ask for.
        possible_cols_to_exclude = {
            '_weight_recorded_dttm',
            '_weight_source',
            '_needs_wt',
            '_base_dose', '_base_unit',
            '_base_wt', '_pref_wt',
            '_preferred_unit',
            '_preferred_unit_clean',
            '_preferred_is_explicit',
            '_unit_class_preferred',
            '_unit_subclass', '_unit_subclass_preferred',
            '_amount_multiplier', '_time_multiplier', '_weight_multiplier',
            '_amount_multiplier_preferred', '_time_multiplier_preferred',
            '_weight_multiplier_preferred',
        }
        existing_cols = set(med_df_converted.columns)
        cols_to_exclude = tuple(possible_cols_to_exclude & existing_cols)

        if cols_to_exclude:
            result_rel = duckdb.sql(f"""
                SELECT * EXCLUDE {cols_to_exclude}
                FROM med_df_converted
            """)
        else:
            result_rel = med_df_converted

        if return_rel:
            # Caller owns cleanup; do not drop temp tables yet.
            _pin_default_tz_for_deferred_render(input_tz_map)
            materialized_input = False
            return result_rel, convert_counts_df
        return (
            _relabel_tz_columns(result_rel.to_df(), input_tz_map),
            convert_counts_df.to_df(),
        )

    finally:
        # Cleanup temp tables only when we own the lifecycle (return_rel=False
        # path materialized to DataFrames above; return_rel=True branches above
        # set `materialized_input = False` to skip cleanup since the caller
        # still references the relation lazily).
        if materialized_input:
            _cleanup_temp_tables()


def standardize_med_dose_units(
    med_df: pd.DataFrame | DuckDBPyRelation,
    target_schema: Union[str, Path, Dict[str, str]],
    *,
    vitals_df: pd.DataFrame | DuckDBPyRelation = None,
    reader=None,
    category_col: str = 'med_category',
    unit_col: str = 'med_dose_unit',
    schema_category_col: str = 'med_category',
    schema_unit_col: str = 'med_dose_unit',
    override: bool = True,
    **kwargs,
) -> Union[Tuple[pd.DataFrame, pd.DataFrame], Tuple[DuckDBPyRelation, DuckDBPyRelation]]:
    """Standardize a medication table against an external mCIDE schema.

    Convenience wrapper over :func:`convert_dose_units_by_med_category` that
    takes the per-category target units from a schema file instead of an
    inline dict.

    Parameters
    ----------
    med_df : pd.DataFrame or DuckDBPyRelation
        Medication administration table.
    target_schema : str, Path, or dict
        Schema file path/URL, or an already-parsed
        ``{med_category: target_unit}`` mapping.
    vitals_df : pd.DataFrame or DuckDBPyRelation, optional
        Vitals table, needed only when a conversion crosses the weight axis
        (weighted to unweighted or the reverse).
    reader : callable, optional
        Override the schema reader; see
        :func:`~clifpy.utils.unit_converter.load_dose_unit_targets`.
    category_col, unit_col : str
        Column names in `med_df`. Forwarded to the converter.
    schema_category_col, schema_unit_col : str
        Column names in the *schema file*. Kept separate from `category_col` /
        `unit_col` because the schema and the data are different artefacts and
        need not agree; pass `schema_unit_col='volume_infusion_rate_units'` to
        read the volume targets from the same file.
    override : bool, default True
        Note this default differs from
        :func:`convert_dose_units_by_med_category`, which defaults to False.
        Standardizing a whole table against a schema covering a few hundred
        categories will always meet categories the schema does not list, and
        that must warn rather than abort. clifpy#153 is what makes this safe
        rather than a blunt instrument: only units the caller actually supplied
        are validated, so unlisted categories simply keep their base units and
        are reported per row in `_convert_status`.
    **kwargs
        Passed through to :func:`convert_dose_units_by_med_category`
        (`id_name`, `return_rel`, `show_intermediate`, `countable_units`,
        `converted_dose_col`, ...).

    Returns
    -------
    tuple
        `(converted, counts)`, as
        :func:`convert_dose_units_by_med_category` returns.

    Examples
    --------
    >>> from clifpy.utils.unit_converter import standardize_med_dose_units
    >>> URL = (                                        # doctest: +SKIP
    ...     'https://raw.githubusercontent.com/'
    ...     'Common-Longitudinal-ICU-data-Format/CLIF/3.0/mCIDE/'
    ...     'medication_admin_continuous/'
    ...     'clif_medication_admin_continuous_med_categories.csv')
    >>> out, counts = standardize_med_dose_units(      # doctest: +SKIP
    ...     mac_df, URL, vitals_df=vitals_df)

    See Also
    --------
    convert_dose_units_by_med_category : The underlying conversion.
    clifpy.utils.unit_converter.load_dose_unit_targets : Schema parsing.
    """
    if isinstance(target_schema, dict):
        targets = dict(target_schema)
    else:
        targets = load_dose_unit_targets(
            target_schema,
            reader=reader,
            category_col=schema_category_col,
            unit_col=schema_unit_col,
        )
    if not targets:
        raise ValueError(
            "target schema produced no usable targets; check the schema "
            "columns and that the file is not empty"
        )

    logger.info(
        "Standardizing med dose units against %d schema target(s)", len(targets)
    )
    return convert_dose_units_by_med_category(
        med_df,
        vitals_df=vitals_df,
        preferred_units=targets,
        override=override,
        category_col=category_col,
        unit_col=unit_col,
        **kwargs,
    )
