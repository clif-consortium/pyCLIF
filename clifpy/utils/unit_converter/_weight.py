"""Patient-weight lookup for weight-qualified dose conversions.

Only the XOR cases need it: converting a weighted unit to an unweighted one, or
the reverse. `/lb` <-> `/kg` is a constant factor and never touches this module.
"""

import pandas as pd
import duckdb

from clifpy.utils.logging_config import get_logger

logger = get_logger('utils.unit_converter')


def find_most_recent_weight(
    med_df: pd.DataFrame | duckdb.DuckDBPyRelation,
    vitals_df: pd.DataFrame | duckdb.DuckDBPyRelation,
    id_name: str = 'hospitalization_id',
    fallback_on_earliest: bool = False,
    ) -> duckdb.DuckDBPyRelation:
    """Find the most recent weight for each medication administration via ASOF join.

    Single-purpose utility: for each row in `med_df`, attach the most recent
    `weight_kg` recorded **at or before** `admin_dttm` for the same hospitalization.

    Parameters
    ----------
    med_df : pd.DataFrame or duckdb.DuckDBPyRelation
        Medication-admin rows with at least `{id_name}` and `admin_dttm` columns.
    vitals_df : pd.DataFrame or duckdb.DuckDBPyRelation
        Vitals rows with `{id_name}`, `recorded_dttm`, `vital_category`, `vital_value`.
    id_name : str, default 'hospitalization_id'
        ID column to join on.
    fallback_on_earliest : bool, default False
        When True, rows whose ASOF returns NULL (med admin precedes the first
        charted weight — common when documentation lags admission) fall back to
        the **earliest** charted weight for that hospitalization. Surfaced in
        the new `_weight_source` column.

    Returns
    -------
    duckdb.DuckDBPyRelation
        Input columns plus:

        - `weight_kg`: matched weight value (NULL if no prior or fallback weight)
        - `_weight_recorded_dttm`: timestamp of matched weight
        - `_weight_source`: 'asof' | 'earliest_fallback' | NULL — provenance for QA

    Notes
    -----
    Caller is responsible for filtering `med_df` to rows that actually need
    weight before calling. This function does not know about `_needs_wt`.
    """
    logger.info("Finding most recent weights...")

    if fallback_on_earliest:
        # ASOF (most recent prior) + LEFT JOIN earliest-per-hosp; COALESCE both.
        # Per docs/duckdb_perf_guide.md §7e: SEMI/ANTI joins prefered over
        # IN-subqueries. Here we use LEFT JOIN since we need the earliest row's
        # value, not just existence.
        q = f"""
        WITH weights AS (
            SELECT {id_name}, recorded_dttm, vital_value
            FROM vitals_df
            WHERE vital_category = 'weight_kg' AND vital_value IS NOT NULL
        )
        , earliest_weights AS (
            SELECT {id_name}
                , MIN(recorded_dttm) AS first_recorded_dttm
                , ARG_MIN(vital_value, recorded_dttm) AS first_weight
            FROM weights
            GROUP BY {id_name}
        )
        SELECT m.*
            , COALESCE(v.vital_value, ew.first_weight) AS weight_kg
            , COALESCE(v.recorded_dttm, ew.first_recorded_dttm) AS _weight_recorded_dttm
            , CASE
                WHEN v.vital_value IS NOT NULL THEN 'asof'
                WHEN ew.first_weight IS NOT NULL THEN 'earliest_fallback'
                ELSE NULL END AS _weight_source
        FROM med_df m
        ASOF LEFT JOIN weights v
            ON m.{id_name} = v.{id_name}
            AND v.recorded_dttm <= m.admin_dttm
        LEFT JOIN earliest_weights ew
            ON m.{id_name} = ew.{id_name}
        ORDER BY m.{id_name}, m.admin_dttm, m.med_category
        """
    else:
        q = f"""
        WITH weights AS (
            SELECT {id_name}, recorded_dttm, vital_value
            FROM vitals_df
            WHERE vital_category = 'weight_kg' AND vital_value IS NOT NULL
        )
        SELECT m.*
            , v.vital_value AS weight_kg
            , v.recorded_dttm AS _weight_recorded_dttm
            , CASE WHEN v.vital_value IS NOT NULL THEN 'asof' ELSE NULL END AS _weight_source
        FROM med_df m
        ASOF LEFT JOIN weights v
            ON m.{id_name} = v.{id_name}
            AND v.recorded_dttm <= m.admin_dttm
        ORDER BY m.{id_name}, m.admin_dttm, m.med_category
        """
    result = duckdb.sql(q)
    logger.info("Weight lookup complete")
    return result

