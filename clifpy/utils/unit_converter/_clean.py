"""Normalisation of raw dose-unit strings into clifpy's internal tokens.

Two passes, in order: `_clean_dose_unit_formats*` handles casing/whitespace and
empty-to-NULL, then `_clean_dose_unit_names*` rewrites spelling variants
(`milliliter/hour` -> `ml/hr`) via `UNIT_NAMING_VARIANTS`.
"""

import pandas as pd
import duckdb

from ._grammar import UNIT_NAMING_VARIANTS


def _clean_dose_unit_formats(s: pd.Series) -> pd.Series:
    """Clean dose unit formatting by removing spaces and converting to lowercase.

    This is the first step in the cleaning pipeline. It standardizes
    the basic formatting of dose units before applying name cleaning.

    Parameters
    ----------
    s : pd.Series
        Series containing dose unit strings to clean.

    Returns
    -------
    pd.Series
        Series with cleaned formatting (no spaces, lowercase).

    Examples
    --------
    >>> import pandas as pd
    >>> s = pd.Series(['mL / hr', 'MCG/KG/MIN', ' Mg/Hr '])
    >>> result = _clean_dose_unit_formats(s)
    >>> list(result)
    ['ml/hr', 'mcg/kg/min', 'mg/hr']

    Notes
    -----
    This function is typically used as the first step in the cleaning
    pipeline, followed by _clean_dose_unit_names().

    .. deprecated::
        Use _clean_dose_unit_formats_duckdb for better performance.
    """
    return s.str.replace(r'\s+', '', regex=True).str.lower().replace('', None, regex=False)

def _clean_dose_unit_formats_duckdb(
    relation: pd.DataFrame | duckdb.DuckDBPyRelation,
    col: str = 'med_dose_unit'
) -> duckdb.DuckDBPyRelation:
    """Clean dose unit formatting using DuckDB to avoid pandas materialization.

    Removes whitespace, converts to lowercase, and replaces empty strings with NULL.

    Parameters
    ----------
    relation : pd.DataFrame | duckdb.DuckDBPyRelation
        Input data containing the column to clean.
    col : str, default 'med_dose_unit'
        Name of the column containing dose unit strings.

    Returns
    -------
    duckdb.DuckDBPyRelation
        Relation with new '_clean_unit' column added.

    Examples
    --------
    >>> import pandas as pd
    >>> df = pd.DataFrame({'med_dose_unit': ['mL / hr', 'MCG/KG/MIN', ' Mg/Hr ']})
    >>> result = _clean_dose_unit_formats_duckdb(df).to_df()
    >>> list(result['_clean_unit'])
    ['ml/hr', 'mcg/kg/min', 'mg/hr']
    """
    return duckdb.sql(f"""
        SELECT *,
            NULLIF(lower(regexp_replace({col}, '\\s+', '', 'g')), '') as _clean_unit
        FROM relation
    """)
    
def _clean_dose_unit_names(s: pd.Series) -> pd.Series:
    """Clean dose unit name variants to standard abbreviations.

    Applies regex patterns to convert various unit name variants to their
    standard abbreviated forms (e.g., 'milliliter' -> 'ml', 'hour' -> 'hr').

    Parameters
    ----------
    s : pd.Series
        Series containing dose unit strings with name variants.
        Should already be format-cleaned (lowercase, no spaces).

    Returns
    -------
    pd.Series
        Series with clean unit names.

    Examples
    --------
    >>> import pandas as pd
    >>> s = pd.Series(['milliliter/hour', 'units/minute', 'µg/kg/h'])
    >>> result = _clean_dose_unit_names(s)
    >>> list(result)
    ['ml/hr', 'u/min', 'mcg/kg/hr']

    Notes
    -----
    Handles conversions including:

    - Time: hour/h -> hr, minute/m -> min
    - Volume: liter/liters/litre/litres -> l
    - Units: units/unit -> u, milli-units -> mu
    - Mass: µg/ug -> mcg, gram -> g

    This function should be applied after _clean_dose_unit_formats().

    .. deprecated::
        Use _clean_dose_unit_names_duckdb for better performance.
    """
    for repl, pattern in UNIT_NAMING_VARIANTS.items():
        s = s.str.replace(pattern, repl, regex=True)
    return s

def _clean_dose_unit_names_duckdb(
    relation: duckdb.DuckDBPyRelation,
    col: str = '_clean_unit'
) -> duckdb.DuckDBPyRelation:
    """Clean dose unit name variants using DuckDB to avoid pandas materialization.

    Applies regex patterns to convert various unit name variants to their
    standard abbreviated forms.

    Parameters
    ----------
    relation : duckdb.DuckDBPyRelation
        Input relation containing the column to clean.
    col : str, default '_clean_unit'
        Name of the column containing dose unit strings.

    Returns
    -------
    duckdb.DuckDBPyRelation
        Relation with the column replaced by cleaned values.

    Examples
    --------
    >>> import pandas as pd
    >>> import duckdb
    >>> df = pd.DataFrame({'_clean_unit': ['milliliter/hour', 'units/minute', 'µg/kg/h']})
    >>> rel = duckdb.sql("SELECT * FROM df")
    >>> result = _clean_dose_unit_names_duckdb(rel).to_df()
    >>> list(result['_clean_unit'])
    ['ml/hr', 'u/min', 'mcg/kg/hr']
    """
    # Build nested regexp_replace calls for all patterns
    expr = col
    for repl, pattern in UNIT_NAMING_VARIANTS.items():
        expr = f"regexp_replace({expr}, '{pattern}', '{repl}', 'g')"

    return duckdb.sql(f"""
        SELECT * EXCLUDE ({col}), {expr} as {col}
        FROM relation
    """)

