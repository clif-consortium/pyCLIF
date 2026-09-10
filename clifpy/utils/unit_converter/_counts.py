"""The per-(category, unit) conversion summary table.

Imported by both stage 1 and the orchestrator, so it lives on its own to keep
the package's import graph acyclic.
"""

import pandas as pd
import duckdb
from typing import List

from clifpy.utils.logging_config import get_logger

logger = get_logger('utils.unit_converter')


def _create_unit_conversion_counts_table(
    med_df: pd.DataFrame | duckdb.DuckDBPyRelation,
    group_by: List[str]
    ) -> duckdb.DuckDBPyRelation:
    """Create summary table of unit conversion counts.

    Generates a grouped summary showing the frequency of each unit conversion
    pattern, useful for data quality assessment and identifying common or
    problematic unit patterns.

    Parameters
    ----------
    med_df : pd.DataFrame
        DataFrame with required columns from conversion process:

        - med_dose_unit: Original unit string
        - _clean_unit: Cleaned unit string
        - _base_unit: base standard unit
        - _unit_class: Classification (rate/amount/unrecognized)
    group_by : List[str]
        List of columns to group by.

    Returns
    -------
    pd.DataFrame
        Summary DataFrame with columns:

        - med_dose_unit: Original unit
        - _clean_unit: After cleaning
        - _base_unit: After conversion
        - _unit_class: Classification
        - count: Number of occurrences

    Raises
    ------
    ValueError
        If required columns are missing from input DataFrame.

    Examples
    --------
    >>> import pandas as pd
    >>> # df_base = standardize_dose_to_base_units(med_df)[0]
    >>> # counts = _create_unit_conversion_counts_table(df_base, ['med_dose_unit'])
    >>> # 'count' in counts.columns
    True

    Notes
    -----
    This table is particularly useful for:

    - Identifying unrecognized units that need handling
    - Understanding the distribution of unit types in your data
    - Quality control and validation of conversions
    """
    # check presense of all the group by columns
    # required_columns = {'med_dose_unit', 'med_dose_unit_normalized', 'med_dose_unit_limited', 'unit_class'}
    missing_columns = set(group_by) - set(med_df.columns)
    if missing_columns:
        raise ValueError(f"The following column(s) are required but not found: {missing_columns}")
    
    # build the string that enumerates the group by columns 
    # e.g. 'med_dose_unit, med_dose_unit_normalized, unit_class'
    cols_enum_str = f"{', '.join(group_by)}"
    order_by_clause = f"med_category, count DESC" if 'med_category' in group_by else "count DESC"
    
    q = f"""
    SELECT {cols_enum_str}   
        , COUNT(*) as count
    FROM med_df
    GROUP BY {cols_enum_str}
    ORDER BY {order_by_clause}
    """
    return duckdb.sql(q)

