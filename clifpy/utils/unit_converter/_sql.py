"""Builders that turn regex/factor pairs into DuckDB CASE expressions.

Kept separate from the grammar so the SQL-generation strategy can change
without touching the vocabulary, and vice versa.
"""

from ._grammar import (
    REGEX_TO_FACTOR_MAPPER,
    SUBCLASS_SPEC,
    SUBCLASS_REGEX,
    STANDALONE_SUBCLASSES,
)


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
        Regex pattern to match in the _preferred_unit_clean column.
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
    >>> 'WHEN regexp_matches(_preferred_unit_clean' in clause and 'THEN 1/' in clause
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
        # NOTE: matches against `_preferred_unit_clean`, the normalised form,
        # not the caller's raw `_preferred_unit`. A raw string like
        # `milli-units/min` does not match `^(mu)` and would silently take the
        # else branch (factor 1), giving a 1000x error reported as 'success'.
        return f"WHEN regexp_matches(_preferred_unit_clean, '{pattern}') THEN 1/({REGEX_TO_FACTOR_MAPPER.get(pattern)})"
    raise ValueError(f"regex pattern {pattern} not found in REGEX_TO_FACTOR_MAPPER dict")



def _subclass_case_ladder(col: str, indent: str = ' ' * 16) -> str:
    """Build the CASE branches that map a unit column to its subclass name.

    Derived from `SUBCLASS_SPEC`, so a new unit family needs no edit here.
    Emits only the WHEN branches; the caller supplies `CASE`, any preceding
    branches, the `ELSE` and the `END`.

    Parameters
    ----------
    col : str
        Name of the unit column to classify (e.g. `_base_unit`).
    indent : str, default 16 spaces
        Leading whitespace for continuation lines, for readable generated SQL.

    Returns
    -------
    str
        Newline-joined `WHEN regexp_matches(...) THEN '<subclass>'` branches.

    Examples
    --------
    >>> ladder = _subclass_case_ladder('_base_unit')
    >>> "THEN 'mass'" in ladder and "THEN 'volume'" in ladder
    True
    """
    return f"\n{indent}".join(
        f"WHEN regexp_matches({col}, '{SUBCLASS_REGEX[name]}') THEN '{name}'"
        for name in SUBCLASS_SPEC
    )


def _base_unit_case_ladder(
    weight_qual_expr: str,
    col: str = '_clean_unit',
    indent: str = ' ' * 16,
) -> str:
    """Build the `_base_unit` CASE branches for every subclass.

    Each non-standalone subclass gets two branches -- one for rates (canonical
    base + weight qualifier + `/min`) and one for amounts (canonical base +
    weight qualifier). Standalone subclasses (`ppm`, `cells`) get a single
    amount branch with no qualifier, since they carry neither axis.

    Derived from `SUBCLASS_SPEC` and `STANDALONE_SUBCLASSES`, replacing what
    were four hand-synchronised copies of this ladder.

    Parameters
    ----------
    weight_qual_expr : str
        SQL expression yielding the canonical weight qualifier (`'/kg'` or
        `''`). Stage 1 collapses `/lb` into `/kg`, so this is NOT
        `_weight_qual_clause`.
    col : str, default '_clean_unit'
        Unit column the branches match against.
    indent : str, default 16 spaces
        Leading whitespace for continuation lines.

    Returns
    -------
    str
        Newline-joined `WHEN ... THEN ...` branches, ordered rate-then-amount
        within each subclass. The caller supplies `CASE`, the preceding
        unrecognized branch, and `END`.
    """
    parts = []
    for name, spec in SUBCLASS_SPEC.items():
        rx = SUBCLASS_REGEX[name]
        base = spec['base']
        if name in STANDALONE_SUBCLASSES:
            # No weight or time axis: the token is already the base unit.
            parts.append(
                f"WHEN _unit_class = 'amount' AND regexp_matches({col}, '{rx}')"
                f"\n{indent}    THEN '{base}'"
            )
            continue
        parts.append(
            f"WHEN _unit_class = 'rate' AND regexp_matches({col}, '{rx}')"
            f"\n{indent}    THEN '{base}' || ({weight_qual_expr}) || '/min'"
        )
        # Amount branches mirror rate branches: append the canonical weight
        # qualifier (`/kg` or `''`) so weighted amounts like `mcg/kg`, `mg/kg`
        # and `mcg/lb` round-trip through stage 1.
        parts.append(
            f"WHEN _unit_class = 'amount' AND regexp_matches({col}, '{rx}')"
            f"\n{indent}    THEN '{base}' || ({weight_qual_expr})"
        )
    return f"\n{indent}".join(parts)
