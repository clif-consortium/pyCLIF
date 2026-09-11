"""Crosswalk a site's standardized CLIF columns from one schema version to another.

The dominant change from CLIF 2.1 to 3.0 is that the standardized
``*_category`` / ``*_group`` / ``*_type`` column **values** were lowercased and
snake_cased, with a minority of non-derivable renames (abbreviations and
semantic changes, e.g. ``High Flow NC`` -> ``hfnc``, ``DNR/DNI`` -> ``dnr_or_dni``,
``Psychiatric Hospital`` -> ``mental_health_hosp``).

This module provides a standalone converter that maps those values to their 3.0
form. Most values are handled by :func:`normalize_category_value` (a
deterministic lowercase + snake_case transform); the non-derivable ones come
from a curated resource (``clifpy/schemas/crosswalks/clif_2.1_to_3.0.yaml``).

The converter is non-mutating: it returns a converted *copy* of the input
DataFrame plus a structured report. Values it cannot confidently map (1->many
splits like ``albumin`` -> ``albumin_5``/``albumin_25``, or anything that does
not resolve to a valid 3.0 value) are left unchanged and surfaced in the report.

This is intentionally separate from ``validator._normalize_columns_pandas``,
which only case-normalizes *all* string columns for validation; the rules here
(``&`` -> ``and``, ``/`` -> ``or``) are value-token specific.
"""

import os
import re
from typing import Any, Dict, Optional, Tuple

import pandas as pd
import yaml

from ..schemas import load_schema

# The 16 CLIF "beta" tables — the default scope for 2.1 -> 3.0 migration.
BETA_TABLES = (
    "patient",
    "hospitalization",
    "adt",
    "vitals",
    "labs",
    "patient_assessments",
    "medication_admin_continuous",
    "medication_admin_intermittent",
    "respiratory_support",
    "position",
    "patient_procedures",
    "code_status",
    "crrt_therapy",
    "hospital_diagnosis",
    "microbiology_culture",
    "microbiology_susceptibility",
)

# Suffixes that mark a "standardized" column whose values are crosswalked.
_STANDARDIZED_SUFFIXES = ("_category", "_group", "_type")

_CROSSWALK_FILENAME = "clif_2.1_to_3.0.yaml"
_MULTI_UNDERSCORE = re.compile(r"_+")

# Cache the parsed crosswalk resource so repeated calls don't re-read the file.
_CROSSWALK_CACHE: Optional[Dict[str, Any]] = None


def normalize_category_value(value: Any) -> Any:
    """Deterministically lowercase + snake_case a single category value.

    Reproduces the CLIF 3.0 token convention for values that changed by case /
    punctuation only. Non-string input (``None``, ``NaN``, numbers, bools) is
    returned unchanged so numeric flag columns (e.g. ``tracheostomy`` 0/1) pass
    through untouched.

    Rules, applied in order:

    1. Non-string / null -> returned unchanged.
    2. ``str``, strip surrounding whitespace, lowercase.
    3. ``&`` -> ``_and_``, ``/`` -> ``_or_`` (so ``l&d`` -> ``l_and_d`` and
       ``DNR/DNI`` -> ``dnr_or_dni`` after collapsing underscores).
    4. Replace space, ``-``, ``,``, ``(``, ``)`` with ``_``.
    5. Collapse runs of ``_`` to a single ``_``; strip leading/trailing ``_``.

    Examples
    --------
    >>> normalize_category_value("Non-Hispanic")
    'non_hispanic'
    >>> normalize_category_value("l&d")
    'l_and_d'
    >>> normalize_category_value("pulmonary vasodilators (IV)")
    'pulmonary_vasodilators_iv'
    """
    if value is None or not isinstance(value, str):
        # Covers None, NaN (float), ints, bools — leave as-is.
        return value

    s = value.strip().lower()
    s = s.replace("&", "_and_").replace("/", "_or_")
    for ch in (" ", "-", ",", "(", ")"):
        s = s.replace(ch, "_")
    s = _MULTI_UNDERSCORE.sub("_", s).strip("_")
    return s


def _crosswalk_path() -> str:
    """Absolute path to the bundled 2.1 -> 3.0 crosswalk resource."""
    return os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "schemas",
        "crosswalks",
        _CROSSWALK_FILENAME,
    )


def load_crosswalk(crosswalk_path: Optional[str] = None) -> Dict[str, Any]:
    """Load (and cache) the curated 2.1 -> 3.0 crosswalk resource.

    Returns a dict with ``from_version``, ``to_version``, ``renames`` and
    ``unresolved`` keys. Missing ``renames``/``unresolved`` default to ``{}``.
    """
    global _CROSSWALK_CACHE
    if crosswalk_path is None:
        if _CROSSWALK_CACHE is not None:
            return _CROSSWALK_CACHE
        path = _crosswalk_path()
    else:
        path = crosswalk_path

    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    data.setdefault("renames", {})
    data.setdefault("unresolved", {})

    if crosswalk_path is None:
        _CROSSWALK_CACHE = data
    return data


def _permissible_map(schema: Optional[Dict[str, Any]]) -> Dict[str, list]:
    """Map column name -> permissible_values list for columns that define one."""
    if not schema:
        return {}
    out = {}
    for col in schema.get("columns", []) or []:
        if col.get("permissible_values"):
            out[col["name"]] = col["permissible_values"]
    return out


def _standardized_columns(table_name: str) -> set:
    """Union of ``*_category``/``*_group``/``*_type`` columns named in the 2.1
    or 3.0 schema for ``table_name``.

    Taking the union makes column discovery robust to the 2.1->3.0 flag flip
    (e.g. ``assessment_group``/``med_group`` move from category to group), since
    the column *name* is unchanged across versions.
    """
    cols = set()
    for version in ("2.1", "3.0"):
        schema = load_schema(table_name, version)
        if not schema:
            continue
        for col in schema.get("columns", []) or []:
            name = col.get("name", "")
            if name.endswith(_STANDARDIZED_SUFFIXES):
                cols.add(name)
    return cols


def crosswalk_table_2_1_to_3_0(
    df,
    table_name: str,
    crosswalk_path: Optional[str] = None,
) -> Tuple[Any, Dict[str, Any]]:
    """Convert a table's standardized column values from CLIF 2.1 to 3.0.

    Transforms values in ``*_category``/``*_group``/``*_type`` columns to their
    CLIF 3.0 form. Each value is mapped via (1) the curated rename map, else
    (2) :func:`normalize_category_value`. Values flagged as 1->many
    ``unresolved`` (e.g. ``albumin``) are left unchanged, as are values that do
    not resolve to a valid 3.0 permissible value — all such cases are reported.

    Column header names are NOT changed, and the input is never mutated.

    Accepts a **pandas** or an eager **polars** ``DataFrame`` and returns a
    converted copy of the **same type** (pandas in -> pandas out, polars in ->
    polars out). For polars ``LazyFrame`` or larger-than-memory data, use
    :func:`crosswalk_file_2_1_to_3_0` instead.

    Parameters
    ----------
    df : pandas.DataFrame or polars.DataFrame
        A site's table data in CLIF 2.1 format.
    table_name : str
        CLIF table name (e.g. ``"respiratory_support"``).
    crosswalk_path : str, optional
        Override path to the crosswalk resource (defaults to the bundled file).

    Returns
    -------
    (converted_df, report) : tuple
        ``converted_df`` is a copy of ``df`` (same type as the input) with values
        converted. ``report`` has keys: ``table``, ``from_version``,
        ``to_version``, ``columns`` (per-column ``n_values_converted`` /
        ``ambiguous`` / ``unresolved``), and ``is_complete``.
    """
    crosswalk = load_crosswalk(crosswalk_path)
    permissible_30 = _permissible_map(load_schema(table_name, "3.0"))

    if _is_polars_frame(df):
        return _crosswalk_polars(df, table_name, crosswalk, permissible_30)

    # pandas path (default)
    target_cols = _target_cols(table_name, df.columns)
    out = df.copy()
    report = _new_report(table_name)
    for col in target_cols:
        counts = out[col].value_counts(dropna=True).to_dict()
        value_map, col_report = _plan_column(table_name, col, counts, crosswalk, permissible_30)
        # Apply the unique-value map (NaN/None untouched: not in value_map).
        out[col] = out[col].map(lambda v: value_map.get(v, v))
        report["columns"][col] = col_report
        if col_report["ambiguous"] or col_report["unresolved"]:
            report["is_complete"] = False
    return out, report


def _is_polars_frame(df) -> bool:
    """True for a polars DataFrame/LazyFrame (without importing polars to check)."""
    return type(df).__module__.split(".", 1)[0] == "polars"


def _crosswalk_polars(df, table_name, crosswalk, permissible_30):
    """Polars implementation of the crosswalk; returns a polars DataFrame."""
    import polars as pl

    if isinstance(df, pl.LazyFrame):
        raise TypeError(
            "crosswalk_table_2_1_to_3_0 accepts an eager polars DataFrame, not a LazyFrame. "
            "Collect it first (df.collect()), or use crosswalk_file_2_1_to_3_0 for large data."
        )

    target_cols = _target_cols(table_name, df.columns)
    out = df.clone()
    report = _new_report(table_name)
    for col in target_cols:
        # value_counts() -> 2-col frame [<value>, count]; read positionally for
        # version-robustness, skipping nulls.
        counts = {}
        for row in out[col].value_counts().iter_rows():
            value, count = row[0], row[1]
            if value is not None:
                counts[value] = int(count)
        value_map, col_report = _plan_column(table_name, col, counts, crosswalk, permissible_30)
        changed = {old: new for old, new in value_map.items() if old != new}
        if changed:
            # replace() leaves unmapped values (incl. nulls) untouched.
            out = out.with_columns(pl.col(col).replace(changed).alias(col))
        report["columns"][col] = col_report
        if col_report["ambiguous"] or col_report["unresolved"]:
            report["is_complete"] = False
    return out, report


# --------------------------------------------------------------------------- #
# Shared planning helpers (used by the in-memory and file-based converters)
# --------------------------------------------------------------------------- #

def _new_report(table_name: str) -> Dict[str, Any]:
    return {
        "table": table_name,
        "from_version": "2.1",
        "to_version": "3.0",
        "columns": {},
        "is_complete": True,
    }


def _target_cols(table_name: str, available_columns) -> list:
    """Standardized columns (by name) that are present in ``available_columns``."""
    std = _standardized_columns(table_name)
    return [c for c in available_columns if c in std]


def _plan_column(
    table_name: str,
    col: str,
    counts: Dict[Any, int],
    crosswalk: Dict[str, Any],
    permissible_30: Dict[str, list],
) -> Tuple[Dict[Any, Any], Dict[str, Any]]:
    """Given per-value counts for one column, build the value map + column report.

    This is the single source of truth for crosswalk semantics; every backend
    (in-memory, chunked-pandas, DuckDB) feeds it a ``{value: count}`` dict.
    """
    col_renames = crosswalk.get("renames", {}).get(table_name, {}).get(col, {})
    col_unresolved = crosswalk.get("unresolved", {}).get(table_name, {}).get(col, {})
    allowed_30 = set(permissible_30.get(col, []))

    value_map: Dict[Any, Any] = {}
    ambiguous, unresolved = [], []
    n_converted = 0

    for raw_value, count in counts.items():
        count = int(count)
        if raw_value in col_unresolved:
            entry = col_unresolved[raw_value] or {}
            value_map[raw_value] = raw_value  # leave unchanged
            ambiguous.append({
                "original": raw_value,
                "candidates": entry.get("candidates", []),
                "reason": entry.get("reason", ""),
                "count": count,
            })
            continue

        produced = col_renames[raw_value] if raw_value in col_renames else normalize_category_value(raw_value)
        value_map[raw_value] = produced
        if produced != raw_value:
            n_converted += count
        if allowed_30 and produced not in allowed_30:
            unresolved.append({"original": raw_value, "produced": produced, "count": count})

    col_report = {"n_values_converted": n_converted, "ambiguous": ambiguous, "unresolved": unresolved}
    return value_map, col_report


# --------------------------------------------------------------------------- #
# File-to-file converter for large / out-of-core data
# --------------------------------------------------------------------------- #

def crosswalk_file_2_1_to_3_0(
    input_path: str,
    output_path: str,
    table_name: str,
    *,
    backend: str = "duckdb",
    chunk_size: int = 1_000_000,
    crosswalk_path: Optional[str] = None,
) -> Dict[str, Any]:
    """Crosswalk a CLIF 2.1 table file to 3.0 **out-of-core**, writing the result.

    For tables too large to hold in memory comfortably. Reads ``input_path``,
    converts the standardized columns, and writes the converted table to
    ``output_path`` (parquet or CSV, inferred from the extensions). Returns the
    same change report shape as :func:`crosswalk_table_2_1_to_3_0` (aggregated
    over the whole file).

    Parameters
    ----------
    input_path, output_path : str
        Source and destination files. ``.parquet``/``.pq`` or ``.csv``.
    table_name : str
        CLIF table name.
    backend : {"duckdb", "pandas"}, default "duckdb"
        - ``"duckdb"`` streams the whole transform in SQL (parquet/CSV ->
          parquet/CSV), handling larger-than-RAM inputs. Robust on platforms
          where Polars is finicky; uses clifpy's existing DuckDB dependency.
        - ``"pandas"`` reads the file in ``chunk_size`` row batches, converts
          each with the in-memory path, and appends to ``output_path`` — bounded
          memory without DuckDB.
    chunk_size : int
        Row batch size for the pandas backend.
    crosswalk_path : str, optional
        Override path to the crosswalk resource.

    Returns
    -------
    dict
        Aggregated change report.
    """
    if backend == "duckdb":
        return _crosswalk_file_duckdb(input_path, output_path, table_name, crosswalk_path)
    if backend == "pandas":
        return _crosswalk_file_pandas(input_path, output_path, table_name, chunk_size, crosswalk_path)
    raise ValueError(f"Unknown backend {backend!r}; use 'duckdb' or 'pandas'.")


def _is_csv(path: str) -> bool:
    return str(path).lower().endswith(".csv")


def _sql_str(value: str) -> str:
    """Single-quote and escape a SQL string literal."""
    return "'" + str(value).replace("'", "''") + "'"


def _crosswalk_file_duckdb(input_path, output_path, table_name, crosswalk_path):
    import duckdb

    crosswalk = load_crosswalk(crosswalk_path)
    permissible_30 = _permissible_map(load_schema(table_name, "3.0"))

    con = duckdb.connect()
    try:
        reader = (f"read_csv_auto({_sql_str(input_path)})" if _is_csv(input_path)
                  else f"read_parquet({_sql_str(input_path)})")
        cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {reader}").fetchall()]
        target_cols = _target_cols(table_name, cols)

        report = _new_report(table_name)
        case_exprs = {}
        for col in target_cols:
            counts = dict(con.execute(
                f'SELECT "{col}" AS v, COUNT(*) AS n FROM {reader} '
                f'WHERE "{col}" IS NOT NULL GROUP BY "{col}"'
            ).fetchall())
            value_map, col_report = _plan_column(table_name, col, counts, crosswalk, permissible_30)
            report["columns"][col] = col_report
            if col_report["ambiguous"] or col_report["unresolved"]:
                report["is_complete"] = False
            # Only emit a CASE for columns that actually change at least one value.
            whens = [(o, n) for o, n in value_map.items() if o != n]
            if whens:
                branches = " ".join(f'WHEN "{col}" = {_sql_str(o)} THEN {_sql_str(n)}' for o, n in whens)
                case_exprs[col] = f'CASE {branches} ELSE "{col}" END AS "{col}"'

        if case_exprs:
            exclude = ", ".join(f'"{c}"' for c in case_exprs)
            select = f"* EXCLUDE ({exclude}), " + ", ".join(case_exprs.values())
        else:
            select = "*"  # nothing to change; passthrough copy

        fmt = "FORMAT CSV, HEADER" if _is_csv(output_path) else "FORMAT PARQUET"
        con.execute(f"COPY (SELECT {select} FROM {reader}) TO {_sql_str(output_path)} ({fmt})")
        return report
    finally:
        con.close()


def _crosswalk_file_pandas(input_path, output_path, table_name, chunk_size, crosswalk_path):
    import pandas as pd

    crosswalk = load_crosswalk(crosswalk_path)
    permissible_30 = _permissible_map(load_schema(table_name, "3.0"))

    # Stream input in row batches.
    if _is_csv(input_path):
        batches = pd.read_csv(input_path, chunksize=chunk_size)
    else:
        import pyarrow.parquet as pq
        pf = pq.ParquetFile(input_path)
        batches = (b.to_pandas() for b in pf.iter_batches(batch_size=chunk_size))

    # Accumulate per-(col, value) counts across chunks, write converted rows as we go.
    from collections import defaultdict
    global_counts = defaultdict(lambda: defaultdict(int))
    value_maps: Dict[str, Dict[Any, Any]] = {}
    target_cols = None
    csv_out = _is_csv(output_path)
    pq_writer = None
    first_csv = True

    import pyarrow as pa
    import pyarrow.parquet as pq_w

    try:
        for chunk in batches:
            if target_cols is None:
                target_cols = _target_cols(table_name, chunk.columns)
            for col in target_cols:
                vc = chunk[col].value_counts(dropna=True)
                vm = value_maps.setdefault(col, {})
                for v, c in vc.items():
                    global_counts[col][v] += int(c)
                    if v not in vm:
                        vm[v] = (crosswalk.get("renames", {}).get(table_name, {}).get(col, {}).get(v)
                                 or normalize_category_value(v)) if v not in (
                                 crosswalk.get("unresolved", {}).get(table_name, {}).get(col, {})) else v
                chunk[col] = chunk[col].map(lambda x: vm.get(x, x))

            if csv_out:
                chunk.to_csv(output_path, mode="w" if first_csv else "a", header=first_csv, index=False)
                first_csv = False
            else:
                table = pa.Table.from_pandas(chunk, preserve_index=False)
                if pq_writer is None:
                    pq_writer = pq_w.ParquetWriter(output_path, table.schema)
                pq_writer.write_table(table)
    finally:
        if pq_writer is not None:
            pq_writer.close()

    # Build the aggregated report from accumulated counts (exact, whole-file).
    report = _new_report(table_name)
    for col in (target_cols or []):
        _, col_report = _plan_column(table_name, col, dict(global_counts[col]), crosswalk, permissible_30)
        report["columns"][col] = col_report
        if col_report["ambiguous"] or col_report["unresolved"]:
            report["is_complete"] = False
    return report
