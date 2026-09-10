"""Unit grammar: the vocabulary of dose units clifpy can parse and emit.

`SUBCLASS_SPEC` is the single source of truth for the amount axis; the regexes,
factor tables and acceptable-unit sets in this module are all derived from it.
Adding a unit family is a one-line change here.
"""

from typing import Set


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
