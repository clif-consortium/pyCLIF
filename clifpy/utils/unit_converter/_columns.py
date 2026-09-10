"""Boundary renaming, so the converter can run on arbitrary column names.

The pipeline is written against fixed internal names (`med_category`,
`med_dose`, `med_dose_unit`, `admin_dttm`). Rather than thread caller-supplied
names through every SQL string -- dozens of interpolation sites, every one an
injection surface -- the caller's columns are projected onto the internal names
on the way in and renamed back on the way out.

Two consequences worth stating plainly:

* Every internal SQL string keeps its literal column names, so it has no
  injection surface at all. Caller-supplied identifiers reach SQL in exactly
  two projections, both built here, both validated and quoted.
* Converting a different column pair -- `volume_infusion_rate` /
  `volume_infusion_rate_unit`, say -- needs no change to the pipeline.
"""

import re
from typing import Dict

import duckdb

from clifpy.utils.logging_config import get_logger

logger = get_logger('utils.unit_converter')

# Canonical names the pipeline's SQL is written against.
INTERNAL_CATEGORY_COL = 'med_category'
INTERNAL_DOSE_COL = 'med_dose'
INTERNAL_UNIT_COL = 'med_dose_unit'
INTERNAL_TIME_COL = 'admin_dttm'

# Prefix used to park a caller column that collides with an internal name.
_STASH_PREFIX = '_saved__'

# A SQL identifier we are willing to emit. Deliberately strict: unquoted-safe
# ASCII only. Anything else is refused rather than escaped, because a column
# name needing escaping is far more likely to be an injection attempt than a
# real column.
_SAFE_IDENTIFIER = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')


def validate_column_name(name: str, param: str) -> str:
    """Reject any column name that is not a plain SQL identifier.

    Parameters
    ----------
    name : str
        Caller-supplied column name.
    param : str
        Parameter name, for the error message.

    Returns
    -------
    str
        The name, unchanged, if it is safe.

    Raises
    ------
    ValueError
        If the name is not a plain identifier.

    Examples
    --------
    >>> validate_column_name('volume_infusion_rate', 'dose_col')
    'volume_infusion_rate'
    >>> validate_column_name('x"; DROP TABLE y --', 'dose_col')
    Traceback (most recent call last):
        ...
    ValueError: dose_col must be a plain SQL identifier ...
    """
    if not isinstance(name, str) or not _SAFE_IDENTIFIER.match(name):
        raise ValueError(
            f"{param} must be a plain SQL identifier matching "
            f"[A-Za-z_][A-Za-z0-9_]*; got {name!r}"
        )
    return name


def quote_identifier(name: str) -> str:
    """Quote a validated identifier for embedding in SQL."""
    return f'"{name}"'


def build_rename_map(
    category_col: str,
    dose_col: str,
    unit_col: str,
    time_col: str,
) -> Dict[str, str]:
    """Map caller column names to the pipeline's internal names.

    Only entries that actually differ are returned, so the default call is a
    no-op and the rename projections are skipped entirely.
    """
    pairs = [
        ('category_col', category_col, INTERNAL_CATEGORY_COL),
        ('dose_col', dose_col, INTERNAL_DOSE_COL),
        ('unit_col', unit_col, INTERNAL_UNIT_COL),
        ('time_col', time_col, INTERNAL_TIME_COL),
    ]
    mapping = {}
    for param, given, internal in pairs:
        validate_column_name(given, param)
        if given != internal:
            mapping[given] = internal
    return mapping


def rename_to_internal(
    rel: duckdb.DuckDBPyRelation,
    rename_map: Dict[str, str],
) -> duckdb.DuckDBPyRelation:
    """Project caller columns onto internal names.

    A caller column that collides with an internal name it is not itself
    mapped to is parked under `_saved__<name>` rather than dropped, and
    restored by :func:`rename_from_internal`. Converting
    `volume_infusion_rate` on a frame that also carries a real `med_dose`
    column must not lose the latter.
    """
    if not rename_map:
        return rel

    present = set(rel.columns)
    missing = [src for src in rename_map if src not in present]
    if missing:
        raise ValueError(
            f"column(s) not found in the input: {sorted(missing)}. "
            f"Available: {sorted(present)}"
        )

    targets = set(rename_map.values())
    # Columns that would be clobbered: they already carry an internal name but
    # are not the source we are mapping onto it.
    to_stash = [c for c in present if c in targets and c not in rename_map]

    projections = []
    for col in rel.columns:
        q = quote_identifier(col)
        if col in rename_map:
            projections.append(f'{q} AS {quote_identifier(rename_map[col])}')
        elif col in to_stash:
            projections.append(f'{q} AS {quote_identifier(_STASH_PREFIX + col)}')
        else:
            projections.append(q)

    if to_stash:
        logger.debug(
            "Parking column(s) %s during conversion to avoid a name collision",
            sorted(to_stash),
        )
    return duckdb.sql(f"SELECT {', '.join(projections)} FROM rel")


def rename_from_internal(
    rel: duckdb.DuckDBPyRelation,
    rename_map: Dict[str, str],
    converted_dose_col: str,
    converted_unit_col: str,
) -> duckdb.DuckDBPyRelation:
    """Restore caller column names and apply the output column names.

    Inverts :func:`rename_to_internal`, un-parks any stashed columns, and
    renames `med_dose_converted` / `med_dose_unit_converted` to the names the
    caller asked for.
    """
    inverse = {internal: given for given, internal in rename_map.items()}
    out_map = {}
    if converted_dose_col != 'med_dose_converted':
        out_map['med_dose_converted'] = converted_dose_col
    if converted_unit_col != 'med_dose_unit_converted':
        out_map['med_dose_unit_converted'] = converted_unit_col

    if not inverse and not out_map:
        return rel

    projections = []
    for col in rel.columns:
        q = quote_identifier(col)
        if col in inverse:
            projections.append(f'{q} AS {quote_identifier(inverse[col])}')
        elif col in out_map:
            projections.append(f'{q} AS {quote_identifier(out_map[col])}')
        elif col.startswith(_STASH_PREFIX):
            projections.append(
                f'{q} AS {quote_identifier(col[len(_STASH_PREFIX):])}'
            )
        else:
            projections.append(q)
    return duckdb.sql(f"SELECT {', '.join(projections)} FROM rel")
