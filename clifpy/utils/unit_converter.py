"""Unit converter for standardizing medication dose units.

This module provides utilities for converting medication dose units between
different formats and standardizing them to a common base set. It handles
weight-based dosing, time unit conversions, and various unit name variants.

In general, convert both rate and amount indiscriminately and report them
as well as unrecognized units.
"""

from types import NoneType
import pandas as pd
import duckdb
from typing import Any, Set, Tuple, List, Union, Literal, overload
from duckdb import DuckDBPyRelation

from clifpy.utils.logging_config import get_logger
from clifpy.utils._duckdb_helpers import (
    _register_temp_table,
    _cleanup_temp_tables,
)

logger = get_logger('utils.unit_converter')

# NOTE: 1 kg = 2.20462 lb. Centralised so SQL builders, Python fallbacks,
# and test fixtures can all reference the same literal (avoids float drift).
KG_PER_LB = 2.20462

# Temp-table lifecycle: boundary 2a per docs/duckdb_perf_guide.md. When a
# pandas input is referenced by multiple downstream SQL queries (cleaning,
# validation, base conversion, preferred-unit join, weight join), promote it
# once into a DuckDB temp table via _register_temp_table; the orchestrator's
# `finally` block calls _cleanup_temp_tables() to drop it. Both helpers live
# in clifpy/utils/_duckdb_helpers.py so future modules can share the registry.


UNIT_NAMING_VARIANTS = {
    # time
    '/hr': '/h(r|our)?$',
    '/min': '/m(in|inute)?$',
    # unit -- NOTE: plaural always go first to avoid having result like "us" or "gs"
    'u': 'u(nits|nit)?',
    # milli
    'm': 'milli-?',
    # volume
    "l": 'l(iters|itres|itre|iter)?'    ,
    # mass
    'mcg': '^(u|µ|μ)g',
    'g': '^g(rams|ram)?',
    # dose
    # 'dose': '^doses?',
}

AMOUNT_ENDER = "($|/*)"

# ===========================================================================
# Unit grammar
# ===========================================================================
# A dose unit is parsed on three independent axes:
#
#     amount  x  weight (/kg, /lb, none)  x  time (/min, /hr, /day)
#
# `SUBCLASS_SPEC` is the single source of truth for the amount axis. Every
# regex, acceptable-unit set and SQL CASE ladder below is DERIVED from it, so
# adding a unit family is a one-line change here rather than eight
# hand-synchronised edits scattered through the SQL builders.
#
# `base` is the canonical token each family collapses to in stage 1. Families
# never convert across subclass boundaries: mEq -> mg would need ion valence
# and molar mass, which the medication tables do not carry, so the subclass
# guard in `_convert_base_units_to_preferred_units` refuses it.
SUBCLASS_SPEC = {
    'mass':   {'tokens': ('mcg', 'mg', 'ng', 'g'), 'base': 'mcg'},
    'volume': {'tokens': ('ml', 'l'),              'base': 'ml'},
    'unit':   {'tokens': ('u', 'mu'),              'base': 'u'},
}

# Subclasses kept OUT of the weight x time cartesian product: a gas fraction
# or a cell count has no meaningful `/kg` or `/min` form. They convert only to
# themselves.
STANDALONE_SUBCLASSES: frozenset = frozenset()

# Multiplicative factor from each token to its subclass `base`. Values are SQL
# expression strings, not numbers, because they are inlined into generated SQL.
# Identity tokens (the bases themselves) are omitted and fall through to 1.
TOKEN_TO_BASE_FACTOR = {
    # volume -> ml
    'l': '1000',
    # unit -> u
    'mu': '1/1000',       # milli-units
    # mass -> mcg
    'mg': '1000',
    'ng': '1/1000',
    'g': '1000000',
}

# Time axis. `/min` is the canonical base, so it is omitted (factor 1).
TIME_TO_BASE_FACTOR = {
    '/hr': '1/60',
}

ACCEPTABLE_WEIGHT_UNITS = ('/kg', '/lb', '')
ACCEPTABLE_TIME_UNITS = tuple(['/min'] + sorted(TIME_TO_BASE_FACTOR))


def _tokens_alternation(tokens) -> str:
    """Build a regex alternation with the longest tokens first.

    Regex alternation is leftmost-match, so `^(mg|mcg)` would match only `mg`
    against the string "mcg". Sorting by descending length guarantees that a
    longer token always wins over a shorter one that prefixes it.

    Parameters
    ----------
    tokens : Iterable[str]
        Unit tokens belonging to one subclass.

    Returns
    -------
    str
        Pipe-separated alternation, longest token first.

    Examples
    --------
    >>> _tokens_alternation(('u', 'mu'))
    'mu|u'
    >>> _tokens_alternation(('mcg', 'mg', 'ng', 'g'))
    'mcg|mg|ng|g'
    """
    return '|'.join(sorted(tokens, key=lambda t: (-len(t), t)))


def _token_regex(token: str) -> str:
    """Build the anchored amount regex for a single unit token."""
    return f"^({token}){AMOUNT_ENDER}"


# Per-subclass amount regexes, derived from SUBCLASS_SPEC.
SUBCLASS_REGEX = {
    name: f"^({_tokens_alternation(spec['tokens'])}){AMOUNT_ENDER}"
    for name, spec in SUBCLASS_SPEC.items()
}

# Back-compat aliases: published in docs/api/utilities.md and imported by tests.
MASS_REGEX = SUBCLASS_REGEX['mass']
VOLUME_REGEX = SUBCLASS_REGEX['volume']
UNIT_REGEX = SUBCLASS_REGEX['unit']

# time
HR_REGEX = f"/hr$"

# mass
MU_REGEX = _token_regex('mu')
MG_REGEX = _token_regex('mg')
NG_REGEX = _token_regex('ng')
G_REGEX = _token_regex('g')

# volume
L_REGEX = _token_regex('l')

# weight
# NOTE: trailing alternation `(/|$)` matches both rates (where the weight
# qualifier is followed by a time unit slash, e.g. `/kg/min`) and weighted
# amounts (where the qualifier ends the string, e.g. `mcg/kg`). Without the
# `$` branch, weighted amounts would be silently misclassified as unweighted.
LB_REGEX = f"/lb(/|$)"
KG_REGEX = f"/kg(/|$)"
WEIGHT_REGEX = f"/(lb|kg)(/|$)"

# Ordered pattern lists consumed by the amount/time multiplier builders.
# Longest token first, mirroring _tokens_alternation's rationale.
AMOUNT_FACTOR_PATTERNS = [
    _token_regex(t)
    for t in sorted(TOKEN_TO_BASE_FACTOR, key=lambda t: (-len(t), t))
]
TIME_FACTOR_PATTERNS = [
    f"{t}$" for t in sorted(TIME_TO_BASE_FACTOR, key=lambda t: (-len(t), t))
]

REGEX_TO_FACTOR_MAPPER = {
    # time -> /min
    **{f"{t}$": f for t, f in TIME_TO_BASE_FACTOR.items()},

    # amount -> subclass base (volume -> ml, unit -> u, mass -> mcg)
    **{_token_regex(t): f for t, f in TOKEN_TO_BASE_FACTOR.items()},

    # weight (consumed only in stage 2 / preferred conversion under the
    # weight-aware redesign — kept here for reference; stage 1 ignores them)
    KG_REGEX: 'weight_kg',
    LB_REGEX: f'weight_kg * {KG_PER_LB}',
}


def _weight_qual_clause(col: str) -> str:
    """Build a SQL CASE expression that extracts the weight qualifier of a unit column.

    Returns one of '/kg', '/lb', or '' (empty string for unweighted / unrecognized).

    Used to drive the 9-case weight-transition factor in stage 2 and the
    `_needs_wt` planning column. NULL inputs return ''.

    NOTE: this helper preserves the **actual** axis (`/kg` vs `/lb`) so stage 2
    can apply the correct transition factor. Stage 1 deliberately uses a
    different expression (`base_weight_qual_expr` in
    `_convert_clean_units_to_base_units`) that collapses `/lb` into `/kg`,
    because the canonical `_base_unit` never carries `/lb`. The two expressions
    are not interchangeable.
    """
    return (
        f"CASE "
        f"WHEN {col} IS NULL THEN '' "
        f"WHEN regexp_matches({col}, '{KG_REGEX}') THEN '/kg' "
        f"WHEN regexp_matches({col}, '{LB_REGEX}') THEN '/lb' "
        f"ELSE '' END"
    )

# Base amount tokens (no weight qualifier). The amount-axis "vocabulary"
# shared by both `_acceptable_amount_units` and `_acceptable_rate_units`.
ACCEPTABLE_BASE_AMOUNT_UNITS = {
    "ml", "l", # volume
    "mu", "u", # unit
    "mcg", "mg", "ng", 'g' # mass
    # "dose" # dose
    }

def _acceptable_amount_units() -> Set[str]:
    """Generate all acceptable amount unit combinations (with optional weight qualifier).

    Mirrors `_acceptable_rate_units` but without a time axis. Weight qualifiers
    are `/kg`, `/lb`, or none — same axis as rate units.

    Returns
    -------
    Set[str]
        Set of all valid amount unit combinations.

    Examples
    --------
    >>> amount_units = _acceptable_amount_units()
    >>> 'mcg' in amount_units
    True
    >>> 'mcg/kg' in amount_units
    True
    >>> 'mcg/lb' in amount_units
    True
    >>> 'mcg/hr' in amount_units
    False

    Notes
    -----
    Amount units are combinations of:

    - Amount units: ml, l, mu, u, mcg, mg, ng, g
    - Weight qualifiers: /kg, /lb, or none

    See Also
    --------
    _acceptable_rate_units : Same, plus a time axis.
    """
    acceptable_weight_units = {'/kg', '/lb', ''}
    return {a + b for a in ACCEPTABLE_BASE_AMOUNT_UNITS for b in acceptable_weight_units}

ACCEPTABLE_AMOUNT_UNITS = _acceptable_amount_units()

def _acceptable_rate_units() -> Set[str]:
    """Generate all acceptable rate unit combinations.

    Creates a cartesian product of amount units, weight qualifiers, and time units
    to generate all valid rate unit patterns that the converter can handle.

    Returns
    -------
    Set[str]
        Set of all valid rate unit combinations.

    Examples
    --------
    >>> rate_units = _acceptable_rate_units()
    >>> 'mcg/kg/hr' in rate_units
    True
    >>> 'ml/min' in rate_units
    True
    >>> 'tablespoon/hr' in rate_units
    False

    Notes
    -----
    Rate units are combinations of:

    - Amount units: ml, l, mu, u, mcg, mg, ng, g
    - Weight qualifiers: /kg, /lb, or none
    - Time units: /hr, /min

    See Also
    --------
    _acceptable_amount_units : Same, minus the time axis.
    """
    acceptable_weight_units = {'/kg', '/lb', ''}
    acceptable_time_units = {'/hr', '/min'}
    # find the cartesian product of the three sets
    return {a + b + c for a in ACCEPTABLE_BASE_AMOUNT_UNITS for b in acceptable_weight_units for c in acceptable_time_units}

ACCEPTABLE_RATE_UNITS = _acceptable_rate_units()

ALL_ACCEPTABLE_UNITS = ACCEPTABLE_RATE_UNITS | ACCEPTABLE_AMOUNT_UNITS

def _convert_set_to_str_for_sql(s: Set[str]) -> str:
    """Convert a set of strings to SQL IN clause format.

    Transforms a Python set into a comma-separated string suitable for use
    in SQL IN clauses within DuckDB queries.

    Parameters
    ----------
    s : Set[str]
        Set of strings to be formatted for SQL.

    Returns
    -------
    str
        Comma-separated string with items separated by "','".
        Does not include outer quotes - those are added in SQL query.

    Examples
    --------
    >>> units = {'ml/hr', 'mcg/min', 'u/hr'}
    >>> _convert_set_to_str_for_sql(units)
    "ml/hr','mcg/min','u/hr"

    Usage in SQL queries:

    >>> # f"WHERE unit IN ('{_convert_set_to_str_for_sql(units)}')"

    Notes
    -----
    This is a helper function for building DuckDB SQL queries that need to check
    if values are in a set of acceptable units.
    """
    return "','".join(s)

RATE_UNITS_STR = _convert_set_to_str_for_sql(ACCEPTABLE_RATE_UNITS)
AMOUNT_UNITS_STR = _convert_set_to_str_for_sql(ACCEPTABLE_AMOUNT_UNITS)

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

def _concat_builders_by_patterns(builder: callable, patterns: list, else_case: str = '1') -> str:
    """Concatenate multiple SQL CASE WHEN statements from patterns.

    Helper function that combines multiple regex pattern builders into a single
    SQL CASE statement for DuckDB queries. Used internally to build conversion
    factor calculations for different unit components (amount, time, weight).

    Parameters
    ----------
    builder : callable
        Function that generates CASE WHEN clauses from regex patterns.
        Should accept a pattern string and return a WHEN...THEN clause.
    patterns : list
        List of regex patterns to process with the builder function.
    else_case : str, default '1'
        Value to use in the ELSE clause when no patterns match.
        Default is '1' (no conversion factor).

    Returns
    -------
    str
        Complete SQL CASE statement with all pattern conditions.

    Examples
    --------
    >>> patterns = ['/hr$', '/min$']
    >>> builder = lambda p: f"WHEN regexp_matches(col, '{p}') THEN factor"
    >>> result = _concat_builders_by_patterns(builder, patterns)
    >>> 'CASE WHEN' in result and 'ELSE 1 END' in result
    True

    Notes
    -----
    This function is used internally by conversion functions to build
    SQL queries that apply different conversion factors based on unit patterns.
    """
    return "CASE " + " ".join([builder(pattern) for pattern in patterns]) + f" ELSE {else_case} END"

def _pattern_to_factor_builder_for_base(pattern: str) -> str:
    """Build SQL CASE WHEN statement for regex pattern matching.

    Helper function that generates SQL CASE WHEN clauses for DuckDB queries
    based on regex patterns and their corresponding conversion factors.

    Parameters
    ----------
    pattern : str
        Regex pattern to match (must exist in REGEX_TO_FACTOR_MAPPER).

    Returns
    -------
    str
        SQL CASE WHEN clause string.

    Raises
    ------
    ValueError
        If the pattern is not found in REGEX_TO_FACTOR_MAPPER.

    Examples
    --------
    >>> clause = _pattern_to_factor_builder_for_base(HR_REGEX)
    >>> 'WHEN regexp_matches' in clause and 'THEN' in clause
    True

    Notes
    -----
    This function is used internally by _convert_clean_dose_units_to_base_units
    to build the SQL query for unit conversion.
    """
    if pattern in REGEX_TO_FACTOR_MAPPER:
        return f"WHEN regexp_matches(_clean_unit, '{pattern}') THEN {REGEX_TO_FACTOR_MAPPER.get(pattern)}"
    raise ValueError(f"regex pattern {pattern} not found in REGEX_TO_FACTOR_MAPPER dict")

def _pattern_to_factor_builder_for_preferred(pattern: str) -> str:
    """Build SQL CASE WHEN statement for preferred unit conversion.

    Generates SQL clauses for converting from base units back to preferred units
    by applying the inverse of the original conversion factor. Used when converting
    from standardized base units to medication-specific preferred units.

    Parameters
    ----------
    pattern : str
        Regex pattern to match in _preferred_unit column.
        Must exist in REGEX_TO_FACTOR_MAPPER dictionary.

    Returns
    -------
    str
        SQL CASE WHEN clause with inverse conversion factor.

    Raises
    ------
    ValueError
        If the pattern is not found in REGEX_TO_FACTOR_MAPPER.

    Examples
    --------
    >>> clause = _pattern_to_factor_builder_for_preferred('/hr$')
    >>> 'WHEN regexp_matches(_preferred_unit' in clause and 'THEN 1/' in clause
    True

    Notes
    -----
    This function applies the inverse of the factor used in
    _pattern_to_factor_builder_for_base, allowing bidirectional conversion
    between unit systems. The inverse is calculated as 1/(original_factor).

    See Also
    --------
    _pattern_to_factor_builder_for_base : Builds patterns for base unit conversion
    """
    if pattern in REGEX_TO_FACTOR_MAPPER:
        return f"WHEN regexp_matches(_preferred_unit, '{pattern}') THEN 1/({REGEX_TO_FACTOR_MAPPER.get(pattern)})"
    raise ValueError(f"regex pattern {pattern} not found in REGEX_TO_FACTOR_MAPPER dict")

def _convert_clean_units_to_base_units(
    med_df: pd.DataFrame | duckdb.DuckDBPyRelation,
    show_intermediate: bool = False
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

    amount_clause = _concat_builders_by_patterns(
        builder=_pattern_to_factor_builder_for_base,
        patterns=[L_REGEX, MU_REGEX, MG_REGEX, NG_REGEX, G_REGEX],
        else_case='1'
        )

    time_clause = _concat_builders_by_patterns(
        builder=_pattern_to_factor_builder_for_base,
        patterns=[HR_REGEX],
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

    if show_intermediate:
        q = f"""
        SELECT *
            , _unit_class: CASE
                WHEN _clean_unit IN ('{RATE_UNITS_STR}') THEN 'rate'
                WHEN _clean_unit IN ('{AMOUNT_UNITS_STR}') THEN 'amount'
                ELSE 'unrecognized' END
            , _amount_multiplier: CASE
                WHEN _unit_class = 'unrecognized' THEN 1 ELSE ({amount_clause}) END
            , _time_multiplier: CASE
                WHEN _unit_class = 'unrecognized' THEN 1 ELSE ({time_clause}) END
            , _weight_multiplier: CASE
                WHEN _unit_class = 'unrecognized' THEN 1 ELSE ({weight_const_expr}) END
            -- amount + time + (constant) lb→kg scaling. No patient weight.
            , _base_dose: CASE
                WHEN _unit_class = 'unrecognized' THEN med_dose
                ELSE med_dose * _amount_multiplier * _time_multiplier * _weight_multiplier
                END
            -- base unit collapses /lb into /kg; unweighted stays unweighted
            , _base_unit: CASE
                WHEN _unit_class = 'unrecognized' THEN _clean_unit
                WHEN _unit_class = 'rate' AND regexp_matches(_clean_unit, '{MASS_REGEX}')
                    THEN 'mcg' || ({base_weight_qual_expr}) || '/min'
                WHEN _unit_class = 'rate' AND regexp_matches(_clean_unit, '{VOLUME_REGEX}')
                    THEN 'ml' || ({base_weight_qual_expr}) || '/min'
                WHEN _unit_class = 'rate' AND regexp_matches(_clean_unit, '{UNIT_REGEX}')
                    THEN 'u' || ({base_weight_qual_expr}) || '/min'
                -- amount branches mirror rate branches: append the canonical
                -- weight qualifier (`/kg` or `''`) so weighted amounts like
                -- `mcg/kg`, `mg/kg`, `mcg/lb` round-trip through stage 1.
                WHEN _unit_class = 'amount' AND regexp_matches(_clean_unit, '{MASS_REGEX}')
                    THEN 'mcg' || ({base_weight_qual_expr})
                WHEN _unit_class = 'amount' AND regexp_matches(_clean_unit, '{VOLUME_REGEX}')
                    THEN 'ml' || ({base_weight_qual_expr})
                WHEN _unit_class = 'amount' AND regexp_matches(_clean_unit, '{UNIT_REGEX}')
                    THEN 'u' || ({base_weight_qual_expr})
                END
        FROM med_df
        """
    else:
        amount_expr = f"CASE WHEN _unit_class = 'unrecognized' THEN 1 ELSE ({amount_clause}) END"
        time_expr = f"CASE WHEN _unit_class = 'unrecognized' THEN 1 ELSE ({time_clause}) END"
        weight_expr = f"CASE WHEN _unit_class = 'unrecognized' THEN 1 ELSE ({weight_const_expr}) END"

        q = f"""
        WITH classified AS (
            SELECT *
                , _unit_class: CASE
                    WHEN _clean_unit IN ('{RATE_UNITS_STR}') THEN 'rate'
                    WHEN _clean_unit IN ('{AMOUNT_UNITS_STR}') THEN 'amount'
                    ELSE 'unrecognized' END
            FROM med_df
        )
        SELECT *
            , _base_dose: CASE
                WHEN _unit_class = 'unrecognized' THEN med_dose
                ELSE med_dose * ({amount_expr}) * ({time_expr}) * ({weight_expr})
                END
            , _base_unit: CASE
                WHEN _unit_class = 'unrecognized' THEN _clean_unit
                WHEN _unit_class = 'rate' AND regexp_matches(_clean_unit, '{MASS_REGEX}')
                    THEN 'mcg' || ({base_weight_qual_expr}) || '/min'
                WHEN _unit_class = 'rate' AND regexp_matches(_clean_unit, '{VOLUME_REGEX}')
                    THEN 'ml' || ({base_weight_qual_expr}) || '/min'
                WHEN _unit_class = 'rate' AND regexp_matches(_clean_unit, '{UNIT_REGEX}')
                    THEN 'u' || ({base_weight_qual_expr}) || '/min'
                -- amount branches mirror rate branches: append the canonical
                -- weight qualifier (`/kg` or `''`) so weighted amounts like
                -- `mcg/kg`, `mg/kg`, `mcg/lb` round-trip through stage 1.
                WHEN _unit_class = 'amount' AND regexp_matches(_clean_unit, '{MASS_REGEX}')
                    THEN 'mcg' || ({base_weight_qual_expr})
                WHEN _unit_class = 'amount' AND regexp_matches(_clean_unit, '{VOLUME_REGEX}')
                    THEN 'ml' || ({base_weight_qual_expr})
                WHEN _unit_class = 'amount' AND regexp_matches(_clean_unit, '{UNIT_REGEX}')
                    THEN 'u' || ({base_weight_qual_expr})
                END
        FROM classified
        """
    return duckdb.sql(q)

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

def standardize_dose_to_base_units(
    med_df: pd.DataFrame,
    vitals_df: pd.DataFrame = None,
    show_intermediate: bool = False,
    id_name: str = 'hospitalization_id',
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
    med_df_base = _convert_clean_units_to_base_units(med_df_cleaned, show_intermediate=show_intermediate)
    convert_counts_df = _create_unit_conversion_counts_table(
        med_df_base,
        group_by=['med_dose_unit', '_clean_unit', '_base_unit', '_unit_class']
        )

    logger.info("Standardization complete")
    return med_df_base, convert_counts_df
    
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
    acceptable_units_relation = pd.DataFrame({'unit': sorted(ALL_ACCEPTABLE_UNITS)})
    bad_units_rows = duckdb.sql(f"""
        SELECT DISTINCT _preferred_unit
        FROM med_df
        ANTI JOIN acceptable_units_relation ON _preferred_unit = unit
        WHERE _preferred_unit IS NOT NULL
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

    try:
        # --------------------------------------------------------------
        # Validate requested med_categories via ANTI JOIN (no .to_df()).
        # --------------------------------------------------------------
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
                extras = {row[0] for row in extra_rows}
                error_msg = (
                    f"The following med_categories are given a preferred unit but not "
                    f"found in the input med_df: {extras}"
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
                med_df, vitals_df, show_intermediate=show_intermediate, id_name=id_name
            )
        except ValueError as e:
            raise ValueError(f"Error standardizing dose units to base units: {e}")

        # --------------------------------------------------------------
        # Join preferred units onto base table.
        # --------------------------------------------------------------
        try:
            preferred_units_df = pd.DataFrame(
                preferred_units.items() if preferred_units else [],
                columns=['med_category', '_preferred_unit']
            )
            med_df_preferred = duckdb.sql("""
                SELECT l.*
                    -- categories without an explicit preferred unit fall back to base
                    , _preferred_unit: COALESCE(r._preferred_unit, l._base_unit)
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
