
"""Data loading + timezone handling for clifpy (``clif_*`` tables).

:func:`load_data` returns the format named by ``return_format``:

- ``'polars'`` (default) -- ``pl.DataFrame``, tz-aware in ``site_tz``
- ``'polars_lazy'`` -- ``pl.LazyFrame``, same relabelling applied as deferred exprs
- ``'duckdb'`` -- bare ``DuckDBPyRelation``
- ``'pandas'`` -- ``pd.DataFrame`` (**deprecated**), tz-aware in ``site_tz``

Timezone handling -- every materialized format is **tz-aware** and relabels to
``site_tz or 'UTC'`` *unconditionally*, which makes it independent of whichever
connection ran the query. ``'duckdb'`` is the exception: a relation is an unevaluated
plan with nowhere to carry a label, so it renders in the *rendering connection's* zone
at the caller's later ``.df()``/``.pl()``. Pass
``duckdb_con=new_duckdb_con(site_tz=...)`` to get a labelled relation.

polars relabels via **chrono-tz**, which freezes its DST table like pytz rather than
projecting the rule forward like zoneinfo/ICU -- so it agrees with the pandas/pytz
decoder on CLIF-MIMIC's far-future dates. See docs/tz_dx.md 11 and
docs/io_return_formats_dx.md 3.1.

Deprecated: ``return_rel=True`` (use ``return_format='duckdb'``) and ``lazy=True`` /
``LazyRelation`` / ``fetch_lazy_result`` / ``close_lazy_relation``.
"""

import pandas as pd
import polars as pl
import os
import duckdb
import pytz
import warnings
import weakref
from contextlib import contextmanager
from typing import Dict, List, Optional, Any, Union, Literal, overload
from duckdb import DuckDBPyRelation, DuckDBPyConnection
import yaml
import logging
from .config import get_config_or_params, load_config as _load_clif_config

# Initialize logger for this module
logger = logging.getLogger('clifpy.utils.io')


class LazyRelation:
    """
    Wrapper around DuckDB relation that keeps the connection alive.

    This class holds both the DuckDB relation and its connection, ensuring
    the connection isn't garbage collected while you're still using the relation.

    All DuckDB relation methods are proxied through, so you can use it
    exactly like a regular DuckDB relation.

    Examples
    --------
    rel = load_data('labs', path, 'parquet', lazy=True)

    # Chain operations (lazy - nothing executed yet)
    result = rel.filter("lab_category = 'sodium'").limit(100)

    # Execute and fetch
    df = result.fetchdf()

    # Clean up when done
    rel.close()
    """

    def __init__(self, relation: duckdb.DuckDBPyRelation, connection: duckdb.DuckDBPyConnection):
        self._relation = relation
        self._connection = connection

    def __getattr__(self, name):
        """Proxy all attribute access to the underlying relation."""
        attr = getattr(self._relation, name)
        if callable(attr):
            def wrapper(*args, **kwargs):
                result = attr(*args, **kwargs)
                # If the result is a relation, wrap it to keep connection alive
                if isinstance(result, duckdb.DuckDBPyRelation):
                    return LazyRelation(result, self._connection)
                return result
            return wrapper
        return attr

    def close(self):
        """Close the underlying connection.

        Call this explicitly when you're done. Note: the connection is shared
        between this LazyRelation and any children produced by chained calls
        (e.g. ``rel.filter(...)``), so closing here invalidates all of them.
        """
        if self._connection:
            try:
                self._connection.close()
            except Exception:
                pass
            self._connection = None

    def __repr__(self):
        return f"LazyRelation({self._relation})"

def _cast_id_cols_to_string(df: pd.DataFrame) -> pd.DataFrame:
    """Cast all columns ending with '_id' to string dtype.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.

    Returns
    -------
    pd.DataFrame
        DataFrame with ID columns cast to string.
    """
    id_cols = [c for c in df.columns if c.endswith("_id")]
    for col in id_cols:
        if df[col].dtype in ('float64', 'float32', 'Float64'):
            # Float IDs like 123456.0 → "123456" (cast through Int64 to strip .0)
            df[col] = df[col].astype("Int64").astype("string")
        else:
            df[col] = df[col].astype("string")
    return df


RETURN_FORMATS = ('polars', 'polars_lazy', 'duckdb', 'pandas')
"""Valid ``return_format`` values for :func:`load_data` / :func:`load_parquet_with_tz`."""

_DEFAULT_RETURN_FORMAT = 'polars'

# Internal sentinel for the deprecated lazy=True / LazyRelation path. Not a public
# return_format value; it never appears in RETURN_FORMATS.
_LAZY_SENTINEL = '_lazy_relation'

# Connections created by new_duckdb_con(). Their zone was chosen deliberately, so
# _prepare_connection() must not warn about a non-UTC setting on them -- that is the
# sanctioned way to obtain a site_tz-labelled relation. WeakSet so we never keep a
# connection alive past its natural lifetime.
_OWNED_CONNECTIONS: "weakref.WeakSet" = weakref.WeakSet()


def new_duckdb_con(site_tz: str = 'UTC') -> DuckDBPyConnection:
    """Create a fresh DuckDB connection pinned to ``site_tz``.

    Pass the result as ``duckdb_con=`` to :func:`load_data` when you want relations
    isolated from DuckDB's process-wide default connection -- e.g. so a later load
    cannot re-pin the zone your relations render in.

    Relations **cannot cross connections**: pass the *same* connection to every load
    whose relations you intend to join, and run those joins on it
    (``con.sql(...)``), not on the global ``duckdb.sql(...)``.

    Parameters
    ----------
    site_tz : str, optional
        Zone to pin the connection to. Default ``'UTC'``, matching the zone
        ``load_data`` pins the default connection to.

    Returns
    -------
    duckdb.DuckDBPyConnection
        A new connection with ``timezone`` and ``pandas_analyze_sample`` applied.

    Examples
    --------
    >>> con = new_duckdb_con(site_tz='US/Eastern')
    >>> rel = load_data('vitals', return_format='duckdb', duckdb_con=con)
    """
    con = duckdb.connect()
    con.execute(f"SET timezone = '{site_tz}';")
    con.execute("SET pandas_analyze_sample=0;")
    _OWNED_CONNECTIONS.add(con)
    return con


def _cast_id_cols_to_utf8(
    df: Union[pl.DataFrame, pl.LazyFrame]
) -> Union[pl.DataFrame, pl.LazyFrame]:
    """Cast every ``*_id`` column to ``Utf8``, the polars twin of :func:`_cast_id_cols_to_string`.

    Works on both eager and lazy frames; the lazy path reads ``collect_schema()`` and
    collects no data. Float IDs go through ``Int64`` so ``123456.0`` becomes
    ``"123456"`` rather than ``"123456.0"``.
    """
    is_lazy = isinstance(df, pl.LazyFrame)
    schema = df.collect_schema() if is_lazy else df.schema

    id_cols = [c for c in schema.keys() if c.endswith('_id')]
    if not id_cols:
        return df

    exprs = []
    for col in id_cols:
        if schema[col] in (pl.Float32, pl.Float64):
            exprs.append(pl.col(col).cast(pl.Int64, strict=False).cast(pl.Utf8).alias(col))
        else:
            exprs.append(pl.col(col).cast(pl.Utf8).alias(col))
    return df.with_columns(exprs)


def _convert_dttm_cols_polars(
    df: Union[pl.DataFrame, pl.LazyFrame],
    site_tz: str,
    time_unit: Optional[str] = None,
    verbose: bool = False,
) -> Union[pl.DataFrame, pl.LazyFrame]:
    """Relabel ``*_dttm`` columns to ``site_tz`` in polars -- the twin of
    :func:`convert_datetime_columns_to_site_tz`.

    polars resolves zones through **chrono-tz**, which -- like pytz and unlike
    zoneinfo/ICU -- freezes its DST transition table rather than projecting the rule
    forward. It therefore agrees with the pandas/pytz decoder on CLIF-MIMIC's
    far-future dates, which is what makes this a safe substitute. See
    docs/io_return_formats_dx.md 3.1 and docs/tz_dx.md 11.

    CRITICAL: the ``convert_time_zone`` call is emitted **unconditionally**, even when
    the column is already in ``site_tz``. It looks like a no-op and is not. A
    ``polars_lazy`` frame is a ``PYTHON SCAN`` over a live DuckDB relation, so its label
    otherwise follows whatever the connection's ``TimeZone`` says at ``.collect()``
    time. Conversion is instant-preserving, so emitting it always pins the label
    deterministically. Removing it reintroduces the float -- see
    tests/utils/io_return_formats/test_label_float.py.

    Parameters
    ----------
    df : pl.DataFrame or pl.LazyFrame
        Frame whose datetime columns should be relabelled.
    site_tz : str
        Target zone. Callers pass ``site_tz or 'UTC'`` -- never ``None``.
    time_unit : str, optional
        ``'ms' | 'us' | 'ns'``. ``None`` (default) keeps each column's source unit,
        which is ``us`` from DuckDB.
    verbose : bool, optional
        If True, log a per-run summary.

    Returns
    -------
    pl.DataFrame or pl.LazyFrame
        Same frame type, with every datetime column tz-aware in ``site_tz``.
    """
    is_lazy = isinstance(df, pl.LazyFrame)
    schema = df.collect_schema() if is_lazy else df.schema

    # Union of both prior conventions: io.py matched on the name, datetime_polars.py
    # matched on the dtype. Taking both means neither module's callers lose a column.
    dttm_cols = [
        c for c, dtype in schema.items()
        if isinstance(dtype, pl.Datetime) or 'dttm' in c.lower()
    ]
    if not dttm_cols:
        logger.debug("No datetime columns found in frame")
        return df

    exprs, converted, naive, problem = [], [], [], []
    for col in dttm_cols:
        dtype = schema[col]
        if not isinstance(dtype, pl.Datetime):
            problem.append(col)
            logger.warning(f"{col}: Expected datetime but found {dtype}")
            continue

        unit = time_unit or dtype.time_unit

        if dtype.time_zone is None:
            # Attach the zone to a naive wall-clock. ambiguous='earliest' mirrors
            # pandas' ambiguous=True. NOTE polars has no equivalent of pandas'
            # nonexistent='shift_forward', so spring-forward gap times null out
            # instead of shifting -- documented divergence, see io_return_formats_dx.md 3.1.
            exprs.append(
                pl.col(col)
                .cast(pl.Datetime(unit))
                .dt.replace_time_zone(site_tz, ambiguous='earliest', non_existent='null')
                .alias(col)
            )
            naive.append(col)
            logger.warning(
                f"{col}: Naive datetime localized to {site_tz}. Please verify this is correct."
            )
        else:
            # Always convert -- see the CRITICAL note above. Do not add an
            # "already in target zone" shortcut; that is what lets the label float.
            exprs.append(
                pl.col(col)
                .cast(pl.Datetime(unit, dtype.time_zone))
                .dt.convert_time_zone(site_tz)
                .alias(col)
            )
            converted.append(col)

    if exprs:
        df = df.with_columns(exprs)

    if verbose and (converted or naive or problem):
        parts = []
        if converted:
            parts.append(f"{len(converted)} converted to {site_tz}")
        if naive:
            parts.append(f"{len(naive)} naive dates localized")
        if problem:
            parts.append(f"{len(problem)} problematic")
        logger.info(f"Timezone processing complete (polars): {', '.join(parts)}")

    return df


def _compile_filters_sql(filters: Optional[Dict[str, Union[str, List[str]]]]) -> List[str]:
    """Compile the ``filters`` mapping into SQL WHERE clauses.

    Paired with :func:`_compile_filters_polars` -- both consume the *same* mapping so
    the two backends cannot interpret a caller's filters differently. If you change one,
    change the other, and see the differential test in
    tests/utils/io_return_formats/test_return_formats.py.
    """
    if not filters:
        return []
    clauses = []
    for col, val in filters.items():
        if isinstance(val, list):
            vals = ", ".join(["'" + str(v).replace("'", "''") + "'" for v in val])
            clauses.append(f"{col} IN ({vals})")
        else:
            value = str(val).replace("'", "''")
            clauses.append(f"{col} = '{value}'")
    return clauses


def _compile_filters_polars(filters: Optional[Dict[str, Union[str, List[str]]]]) -> List:
    """Compile the ``filters`` mapping into polars expressions.

    The polars twin of :func:`_compile_filters_sql`; see that docstring.
    """
    if not filters:
        return []
    exprs = []
    for col, val in filters.items():
        if isinstance(val, list):
            exprs.append(pl.col(col).is_in([str(v) for v in val]))
        else:
            exprs.append(pl.col(col) == str(val))
    return exprs


def _scan_polars(
    file_path: str,
    table_format_type: str,
    columns: Optional[List[str]],
    filters: Optional[Dict[str, Union[str, List[str]]]],
    sample_size: Optional[int],
    site_tz: Optional[str],
    time_unit: Optional[str],
    verbose: bool,
) -> pl.LazyFrame:
    """Build a LazyFrame with polars' own scanners, so predicates reach the file reader.

    Routing the polars formats through DuckDB works, but hands polars an opaque
    ``PYTHON SCAN``: a caller's later ``.filter()`` cannot be pushed into the parquet
    reader, so every row materializes through Arrow first. Measured on 8M rows that is
    ~11x slower than a native scan for exactly the chain a LazyFrame exists to support.
    See docs/io_return_formats_dx.md.

    ``try_parse_dates=True`` is REQUIRED for CSV -- without it ``*_dttm`` columns come
    back as ``String`` and the timezone step silently skips them. That was a real bug in
    the old io_polars.load_csv_polars.
    """
    if table_format_type == 'csv':
        lf = pl.scan_csv(file_path, try_parse_dates=True)
    else:
        lf = pl.scan_parquet(file_path)

    if columns:
        lf = lf.select(columns)

    for expr in _compile_filters_polars(filters):
        lf = lf.filter(expr)

    if sample_size:
        lf = lf.limit(sample_size)

    lf = _cast_id_cols_to_utf8(lf)
    return _convert_dttm_cols_polars(lf, site_tz or 'UTC', time_unit=time_unit, verbose=verbose)


def _resolve_site_tz_from_config(config_path: Optional[str] = None) -> Optional[str]:
    """Best-effort lookup of ``timezone`` from the CLIF config file.

    Used when the caller supplied ``table_path``/``table_format_type`` explicitly, which
    otherwise skips config resolution entirely and silently falls back to UTC. Returns
    ``None`` if no config resolves -- a missing config is a normal, supported state
    here, unlike in :func:`get_config_or_params` where it is an error.
    """
    try:
        return _load_clif_config(config_path).get('timezone')
    except Exception:
        # No config file, unreadable, or missing required fields -- all mean
        # "no site timezone available", which the caller handles by defaulting to UTC.
        return None


@contextmanager
def _pinned_pandas():
    """Suppress the ``return_format='pandas'`` deprecation for in-package callers.

    ``BaseTable`` and friends are pinned to pandas deliberately while the table layer
    migrates (see docs/io_return_formats_dx.md 7). Warning on every ``from_file()``
    would be noise a user cannot act on -- the deprecation is aimed at code that
    *chooses* pandas, not at code we have pinned on their behalf.
    """
    with warnings.catch_warnings():
        warnings.filterwarnings(
            'ignore', message=".*return_format='pandas' is deprecated.*",
            category=DeprecationWarning,
        )
        yield


def _resolve_return_format(
    return_rel: bool,
    lazy: bool,
    return_format: Optional[str],
) -> str:
    """Reconcile the deprecated ``return_rel``/``lazy`` flags with ``return_format``.

    Returns the effective format string. ``lazy=True`` resolves to the sentinel
    ``'_lazy_relation'``, which callers handle on the separate ``LazyRelation`` path.
    """
    if return_rel and lazy:
        raise ValueError(
            "return_rel and lazy are mutually exclusive. "
            "Use return_rel=True for a bare DuckDBPyRelation (default connection), "
            "or lazy=True for a LazyRelation wrapping an isolated connection."
        )

    if return_format == _LAZY_SENTINEL:
        return _LAZY_SENTINEL          # internal passthrough from load_data

    if return_format is not None:
        if return_rel or lazy:
            raise ValueError(
                "return_format cannot be combined with the deprecated return_rel/lazy "
                "flags. Use return_format='duckdb' instead of return_rel=True."
            )
        if return_format not in RETURN_FORMATS:
            raise ValueError(
                f"Unknown return_format {return_format!r}; use one of "
                f"{', '.join(repr(f) for f in RETURN_FORMATS)}."
            )
        if return_format == 'pandas':
            warnings.warn(
                "return_format='pandas' is deprecated and will be removed once the "
                "table layer migrates; use 'polars' (or .to_pandas() at the point you "
                "need a pandas frame).",
                DeprecationWarning,
                stacklevel=3,
            )
        return return_format

    if return_rel:
        warnings.warn(
            "return_rel=True is deprecated; use return_format='duckdb' instead.",
            DeprecationWarning,
            stacklevel=3,
        )
        return 'duckdb'

    if lazy:
        warnings.warn(
            "lazy=True and LazyRelation are deprecated and will be removed in a future "
            "release; use return_format='polars_lazy', or "
            "return_format='duckdb' with duckdb_con=new_duckdb_con() for an isolated "
            "DuckDB connection.",
            DeprecationWarning,
            stacklevel=3,
        )
        return _LAZY_SENTINEL

    return _DEFAULT_RETURN_FORMAT


def _validate_format_combination(
    fmt: str,
    site_tz: Optional[str],
    duckdb_con: Optional[DuckDBPyConnection],
    site_tz_was_explicit: bool,
) -> None:
    """Reject argument combinations where one argument would be silently ignored.

    Two combinations have no effect, and both used to pass quietly:

    - ``duckdb_con`` with a non-DuckDB format. The materialized formats relabel
      unconditionally, so the connection cannot influence the result -- it would only
      change which engine ran the query, and force the slower path for polars.
    - ``site_tz`` with ``return_format='duckdb'``. A relation is an unevaluated plan;
      its timezone label comes from the connection that eventually renders it, so there
      is nowhere to record ``site_tz``. Pass a connection pinned to the zone instead.
    """
    if duckdb_con is not None and fmt != 'duckdb':
        raise ValueError(
            f"duckdb_con is only meaningful with return_format='duckdb'; got {fmt!r}. "
            f"For {fmt!r} the result is materialized and relabelled to site_tz, so the "
            f"connection cannot affect it."
        )

    if fmt == 'duckdb' and site_tz_was_explicit and site_tz is not None:
        warnings.warn(
            "site_tz is ignored when return_format='duckdb': a relation carries no "
            "timezone label of its own, it renders in the zone of whichever connection "
            "materializes it. Use "
            "duckdb_con=new_duckdb_con(site_tz=...) for a relation labelled in that "
            "zone, or relabel after calling .df()/.pl().",
            UserWarning,
            stacklevel=3,
        )


def _prepare_connection(
    duckdb_con: Optional[DuckDBPyConnection],
    verbose: bool = False,
):
    """Return the executor for queries: the caller's connection, or the duckdb module.

    With ``duckdb_con=None`` the process-wide default connection is pinned to UTC, as
    it has always been. A **caller-owned connection is never mutated** -- silently
    re-pinning someone's connection is the exact bug ``duckdb_con`` exists to prevent
    -- so its zone is inspected and warned about instead.
    """
    if duckdb_con is None:
        duckdb.execute("SET timezone = 'UTC';")          # read & return in UTC
        duckdb.execute("SET pandas_analyze_sample=0;")   # avoid sampling issues
        return duckdb

    if duckdb_con in _OWNED_CONNECTIONS:
        return duckdb_con      # zone chosen deliberately via new_duckdb_con()

    try:
        current = duckdb_con.sql("SELECT current_setting('TimeZone')").fetchone()[0]
    except Exception:  # pragma: no cover - defensive; setting always exists
        current = None
    if current is not None and str(current).upper() != 'UTC':
        logger.warning(
            f"duckdb_con has TimeZone={current!r}, not 'UTC'. Its setting is left "
            f"untouched. Relations returned with return_format='duckdb' will render "
            f"in {current!r}; materialized formats are unaffected."
        )
    return duckdb_con


def _finalize(
    rel: DuckDBPyRelation,
    return_format: str,
    site_tz: Optional[str],
    time_unit: Optional[str] = None,
    verbose: bool = False,
):
    """Convert a built relation into the requested output format.

    ``'duckdb'`` returns the relation untouched: a relation is an unevaluated plan with
    nowhere to carry a tz label (the label comes from the rendering connection at
    materialization), so ``site_tz`` is deliberately not applied. Every other format
    materializes here and relabels to ``site_tz or 'UTC'`` unconditionally, which makes
    them independent of the connection's zone. See docs/io_return_formats_dx.md 5.
    """
    if return_format == 'duckdb':
        return rel

    target_tz = site_tz or 'UTC'

    if return_format == 'pandas':
        df = rel.df()                        # tz-aware in the connection's zone
        df = _cast_id_cols_to_string(df)
        # Unconditional: with site_tz=None this is a no-op on a UTC connection, but it
        # stops a caller-supplied duckdb_con pinned elsewhere from leaking its label.
        return convert_datetime_columns_to_site_tz(df, target_tz, verbose)

    frame = rel.pl(lazy=(return_format == 'polars_lazy'))
    frame = _cast_id_cols_to_utf8(frame)
    return _convert_dttm_cols_polars(frame, target_tz, time_unit=time_unit, verbose=verbose)


def close_lazy_relation(rel: Union['LazyRelation', duckdb.DuckDBPyRelation]) -> None:
    """
    Close the connection associated with a lazy relation.

    Call this when you're done with a lazy relation to free resources.

    Parameters
    ----------
    rel : LazyRelation
        A lazy relation returned from load_data(..., lazy=True)

    Examples
    --------
    rel = load_data('labs', path, 'parquet', lazy=True)
    df = rel.filter("lab_category = 'sodium'").fetchdf()
    close_lazy_relation(rel)  # Or just: rel.close()
    """
    if hasattr(rel, 'close'):
        rel.close()


def fetch_lazy_result(
    rel: Union['LazyRelation', duckdb.DuckDBPyRelation],
    cast_ids: bool = True,
    site_tz: str = None,
    close_connection: bool = True,
    verbose: bool = False
) -> pd.DataFrame:
    """
    Fetch results from a lazy relation and apply standard post-processing.

    This is a convenience function that fetches the DataFrame from a lazy
    relation and applies the same post-processing as eager load_data().

    Parameters
    ----------
    rel : LazyRelation
        A lazy relation from load_data(..., lazy=True)
    cast_ids : bool, optional
        If True (default), cast ID columns to string type.
    site_tz : str, optional
        Timezone string for datetime conversion.
    close_connection : bool, optional
        If True (default), close the connection after fetching.
    verbose : bool, optional
        If True, show detailed messages.

    Returns
    -------
    pd.DataFrame
        The fetched and processed DataFrame.

    Examples
    --------
    # Load lazily, filter, then fetch with post-processing
    rel = load_data('labs', path, 'parquet', lazy=True)
    filtered = rel.filter("lab_category = 'sodium'")
    df = fetch_lazy_result(filtered, site_tz='America/New_York')
    """
    df = rel.fetchdf()

    if cast_ids:
        df = _cast_id_cols_to_string(df)

    if site_tz:
        df = convert_datetime_columns_to_site_tz(df, site_tz, verbose)

    if close_connection:
        close_lazy_relation(rel)

    return df


def load_config(file_path: str) -> Dict[str, Any]:
    with open(file_path, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)
    return config


@overload
def load_parquet_with_tz(
    file_path: str, columns: Optional[List[str]] = ..., filters: Optional[Dict[str, Union[str, List[str]]]] = ...,
    sample_size: Optional[int] = ..., site_tz: Optional[str] = ..., verbose: bool = ...,
    return_rel: bool = ..., lazy: bool = ..., *,
    return_format: Literal['polars'] = ..., duckdb_con: Optional[DuckDBPyConnection] = ...,
    time_unit: Optional[str] = ...,
) -> pl.DataFrame: ...


@overload
def load_parquet_with_tz(
    file_path: str, columns: Optional[List[str]] = ..., filters: Optional[Dict[str, Union[str, List[str]]]] = ...,
    sample_size: Optional[int] = ..., site_tz: Optional[str] = ..., verbose: bool = ...,
    return_rel: bool = ..., lazy: bool = ..., *,
    return_format: Literal['polars_lazy'], duckdb_con: Optional[DuckDBPyConnection] = ...,
    time_unit: Optional[str] = ...,
) -> pl.LazyFrame: ...


@overload
def load_parquet_with_tz(
    file_path: str, columns: Optional[List[str]] = ..., filters: Optional[Dict[str, Union[str, List[str]]]] = ...,
    sample_size: Optional[int] = ..., site_tz: Optional[str] = ..., verbose: bool = ...,
    return_rel: bool = ..., lazy: bool = ..., *,
    return_format: Literal['duckdb'], duckdb_con: Optional[DuckDBPyConnection] = ...,
    time_unit: Optional[str] = ...,
) -> DuckDBPyRelation: ...


@overload
def load_parquet_with_tz(
    file_path: str, columns: Optional[List[str]] = ..., filters: Optional[Dict[str, Union[str, List[str]]]] = ...,
    sample_size: Optional[int] = ..., site_tz: Optional[str] = ..., verbose: bool = ...,
    return_rel: bool = ..., lazy: bool = ..., *,
    return_format: Literal['pandas'], duckdb_con: Optional[DuckDBPyConnection] = ...,
    time_unit: Optional[str] = ...,
) -> pd.DataFrame: ...


def load_parquet_with_tz(
    file_path: str,
    columns: Optional[List[str]] = None,
    filters: Optional[Dict[str, Union[str, List[str]]]] = None,
    sample_size: Optional[int] = None,
    site_tz: Optional[str] = None,
    verbose: bool = False,
    return_rel: bool = False,
    lazy: bool = False,
    *,
    return_format: Optional[str] = None,
    duckdb_con: Optional[DuckDBPyConnection] = None,
    time_unit: Optional[str] = None,
    _site_tz_explicit: Optional[bool] = None,
) -> Union[pl.DataFrame, pl.LazyFrame, DuckDBPyRelation, pd.DataFrame, 'LazyRelation']:
    """Load a parquet file, returning the format named by ``return_format``.

    Parameters
    ----------
    file_path : str
        Path to the parquet file.
    columns : list of str, optional
        Column names to load.
    filters : dict, optional
        ``{column: value}`` or ``{column: [values]}``.
    sample_size : int, optional
        Number of rows to load (LIMIT clause).
    site_tz : str, optional
        Target timezone for ``*_dttm`` columns. Applied to every format **except**
        ``'duckdb'`` -- a relation has nowhere to carry a label (see Notes).
        ``None`` means UTC.
    verbose : bool, optional
        If True, log detailed loading messages.
    return_rel : bool, optional
        **Deprecated** -- use ``return_format='duckdb'``.
    lazy : bool, optional
        **Deprecated** -- returns a ``LazyRelation``. Use
        ``return_format='polars_lazy'``, or ``duckdb_con=new_duckdb_con()``.
    return_format : {'polars', 'polars_lazy', 'duckdb', 'pandas'}, optional
        Output format. ``None`` (default) means ``'polars'``.
    duckdb_con : duckdb.DuckDBPyConnection, optional
        Only valid with ``return_format='duckdb'``; a ``ValueError`` is raised otherwise,
        because the materialized formats relabel unconditionally and so cannot be
        affected by the connection. Builds the relation on this connection instead of
        DuckDB's process-wide default. Its ``TimeZone`` is **never modified** -- pin it
        at creation with :func:`new_duckdb_con` to control the zone a relation renders
        in. Relations cannot cross connections, so pass the same connection to every
        load whose relations you intend to join.
    time_unit : {'ms', 'us', 'ns'}, optional
        Datetime precision for the polars formats. ``None`` keeps the source unit
        (``us`` from DuckDB).

    Returns
    -------
    pl.DataFrame | pl.LazyFrame | DuckDBPyRelation | pd.DataFrame | LazyRelation

    Raises
    ------
    ValueError
        If ``return_format`` is unknown, or combined with ``return_rel``/``lazy``,
        or if both deprecated flags are set.

    Notes
    -----
    ``'duckdb'`` returns raw ``TIMESTAMPTZ`` that renders in the *rendering
    connection's* zone at the caller's later ``.df()``/``.pl()``. To get a
    ``site_tz``-labelled relation, pass ``duckdb_con=new_duckdb_con(site_tz=...)``.
    See docs/io_return_formats_dx.md.
    """
    fmt = _resolve_return_format(return_rel, lazy, return_format)
    # _site_tz_explicit is threaded in by load_data, which resolves site_tz from config
    # before calling here -- without it a config-supplied zone would look explicit.
    _validate_format_combination(
        fmt, site_tz, duckdb_con,
        site_tz is not None if _site_tz_explicit is None else _site_tz_explicit,
    )

    filename = os.path.basename(file_path)
    if verbose:
        logger.info(f"Loading {filename} ({fmt})")

    # ---- LazyRelation path (deprecated): isolated per-call connection, lifetime-wrapped ----
    if fmt == _LAZY_SENTINEL:
        con = duckdb.connect()
        con.execute("SET timezone = 'UTC';")          # read & return in UTC
        con.execute("SET pandas_analyze_sample=0;")   # avoid sampling issues

        # Build the relation lazily using DuckDB's Relational API
        rel = con.read_parquet(file_path)

        # Apply column selection (lazy)
        if columns:
            rel = rel.select(*columns)

        # Apply filters (lazy)
        if filters:
            for col, val in filters.items():
                if isinstance(val, list):
                    vals = ", ".join([f"'{v}'" for v in val])
                    rel = rel.filter(f"{col} IN ({vals})")
                else:
                    rel = rel.filter(f"{col} = '{val}'")

        # Apply limit (lazy)
        if sample_size:
            rel = rel.limit(sample_size)

        return LazyRelation(rel, con)

    # ---- polars formats: native scan, so predicates reach the file reader ----
    # Skipped when the caller supplied duckdb_con: they want *that* connection
    # (attached DBs, extensions, memory limits), so honour it for every format.
    if fmt in ('polars', 'polars_lazy'):
        lf = _scan_polars(file_path, 'parquet', columns, filters, sample_size,
                          site_tz, time_unit, verbose)
        return lf if fmt == 'polars_lazy' else lf.collect()

    # ---- duckdb / pandas (and any format with an explicit duckdb_con) ----
    executor = _prepare_connection(duckdb_con, verbose)

    # The query always selects raw TIMESTAMPTZ -> tz-AWARE. _finalize() then relabels
    # to site_tz for every format except 'duckdb', which returns the bare relation
    # (it renders in the rendering connection's zone at the caller's later .df()).
    # See docs/tz_dx.md and docs/io_return_formats_dx.md.
    sel = "*" if columns is None else ", ".join(columns)

    query = f"SELECT {sel} FROM parquet_scan('{file_path}')"

    _clauses = _compile_filters_sql(filters)
    if _clauses:
        query += " WHERE " + " AND ".join(_clauses)

    if sample_size:
        query += f" LIMIT {sample_size}"

    return _finalize(executor.sql(query), fmt, site_tz, time_unit, verbose)


@overload
def load_data(
    table_name: str, table_path: Optional[str] = ..., table_format_type: Optional[str] = ...,
    sample_size: Optional[int] = ..., columns: Optional[List[str]] = ...,
    filters: Optional[Dict[str, Union[str, List[str]]]] = ..., site_tz: Optional[str] = ...,
    verbose: bool = ..., return_rel: bool = ..., lazy: bool = ..., config_path: Optional[str] = ..., *,
    return_format: Literal['polars'] = ...,
    duckdb_con: Optional[DuckDBPyConnection] = ..., time_unit: Optional[str] = ...,
) -> pl.DataFrame: ...


@overload
def load_data(
    table_name: str, table_path: Optional[str] = ..., table_format_type: Optional[str] = ...,
    sample_size: Optional[int] = ..., columns: Optional[List[str]] = ...,
    filters: Optional[Dict[str, Union[str, List[str]]]] = ..., site_tz: Optional[str] = ...,
    verbose: bool = ..., return_rel: bool = ..., lazy: bool = ..., config_path: Optional[str] = ..., *,
    return_format: Literal['polars_lazy'],
    duckdb_con: Optional[DuckDBPyConnection] = ..., time_unit: Optional[str] = ...,
) -> pl.LazyFrame: ...


@overload
def load_data(
    table_name: str, table_path: Optional[str] = ..., table_format_type: Optional[str] = ...,
    sample_size: Optional[int] = ..., columns: Optional[List[str]] = ...,
    filters: Optional[Dict[str, Union[str, List[str]]]] = ..., site_tz: Optional[str] = ...,
    verbose: bool = ..., return_rel: bool = ..., lazy: bool = ..., config_path: Optional[str] = ..., *,
    return_format: Literal['duckdb'],
    duckdb_con: Optional[DuckDBPyConnection] = ..., time_unit: Optional[str] = ...,
) -> DuckDBPyRelation: ...


@overload
def load_data(
    table_name: str, table_path: Optional[str] = ..., table_format_type: Optional[str] = ...,
    sample_size: Optional[int] = ..., columns: Optional[List[str]] = ...,
    filters: Optional[Dict[str, Union[str, List[str]]]] = ..., site_tz: Optional[str] = ...,
    verbose: bool = ..., return_rel: bool = ..., lazy: bool = ..., config_path: Optional[str] = ..., *,
    return_format: Literal['pandas'],
    duckdb_con: Optional[DuckDBPyConnection] = ..., time_unit: Optional[str] = ...,
) -> pd.DataFrame: ...


def load_data(
    table_name: str,
    table_path: Optional[str] = None,
    table_format_type: Optional[str] = None,
    sample_size: Optional[int] = None,
    columns: Optional[List[str]] = None,
    filters: Optional[Dict[str, Union[str, List[str]]]] = None,
    site_tz: Optional[str] = None,
    verbose: bool = False,
    return_rel: bool = False,
    lazy: bool = False,
    config_path: Optional[str] = None,
    *,
    return_format: Optional[str] = None,
    duckdb_con: Optional[DuckDBPyConnection] = None,
    time_unit: Optional[str] = None,
) -> Union[pl.DataFrame, pl.LazyFrame, DuckDBPyRelation, pd.DataFrame, 'LazyRelation']:
    """Load a CLIF table, returning the format named by ``return_format``.

    ``table_path`` and ``table_format_type`` fall back to the config file when omitted.

    Parameters
    ----------
    table_name : str
        Table to load, e.g. ``'vitals'``, ``'labs'``, ``'adt'``.
    table_path : str, optional
        Directory holding the data file. From config ``data_directory`` if None.
    table_format_type : str, optional
        ``'csv'`` or ``'parquet'``. From config ``filetype`` if None.
    sample_size : int, optional
        Number of rows to load.
    columns : list of str, optional
        Column names to load.
    filters : dict, optional
        ``{column: value}`` or ``{column: [values]}``.
    site_tz : str, optional
        Target timezone for ``*_dttm`` columns. From config ``timezone`` if None.
        Applied to every format **except** ``'duckdb'``; ``None`` means UTC.
    verbose : bool, optional
        If True, log detailed loading messages.
    return_rel : bool, optional
        **Deprecated** -- use ``return_format='duckdb'``.
    lazy : bool, optional
        **Deprecated** -- returns a ``LazyRelation``. Use
        ``return_format='polars_lazy'``, or ``duckdb_con=new_duckdb_con()``.
    config_path : str, optional
        Path to config file; auto-detected in the working directory if None.
    return_format : {'polars', 'polars_lazy', 'duckdb', 'pandas'}, optional
        Output format. ``None`` (default) means ``'polars'``.

        .. versionchanged:: 0.6.0
           The default changed from a pandas DataFrame to a polars DataFrame. Pass
           ``return_format='pandas'`` to keep the old behaviour.
    duckdb_con : duckdb.DuckDBPyConnection, optional
        Only valid with ``return_format='duckdb'``; a ``ValueError`` is raised otherwise.
        Builds the relation on this connection instead of DuckDB's process-wide default.
        Its ``TimeZone`` is **never modified** -- pin it at creation with
        :func:`new_duckdb_con` to control the zone a relation renders in. Relations
        cannot cross connections, so pass the same connection to every load whose
        relations you intend to join, and join them with ``con.sql(...)``.
    time_unit : {'ms', 'us', 'ns'}, optional
        Datetime precision for the polars formats. ``None`` keeps the source unit.

    Returns
    -------
    pl.DataFrame | pl.LazyFrame | DuckDBPyRelation | pd.DataFrame | LazyRelation

    Raises
    ------
    FileNotFoundError
        If the resolved file does not exist.
    ValueError
        If ``return_format`` is unknown, or combined with ``return_rel``/``lazy``,
        or if the filetype is unsupported.

    Examples
    --------
    # polars DataFrame (default)
    >>> df = load_data('vitals')

    # polars LazyFrame -- nothing executes until .collect()
    >>> lf = load_data('vitals', return_format='polars_lazy')

    # DuckDB relation, composable with other relations on the same connection
    >>> rel = load_data('vitals', return_format='duckdb')

    # A relation labelled in a specific zone
    >>> con = new_duckdb_con(site_tz='US/Eastern')
    >>> rel = load_data('vitals', return_format='duckdb', duckdb_con=con)

    # Explicit parameters, no config file
    >>> df = load_data('vitals', '/path/to/data', 'parquet', site_tz='US/Eastern')
    """
    fmt = _resolve_return_format(return_rel, lazy, return_format)
    # Captured before config resolution: only an explicitly-passed site_tz is worth
    # warning about for the duckdb format. A zone inherited from config is ambient and
    # applies to the other formats in the same script, so warning on it would be noise.
    _site_tz_explicit = site_tz is not None
    _validate_format_combination(fmt, site_tz, duckdb_con, _site_tz_explicit)

    # Load config if table_path or table_format_type not provided
    if table_path is None or table_format_type is None:
        config = get_config_or_params(
            config_path=config_path,
            data_directory=table_path,
            filetype=table_format_type,
            timezone=site_tz
        )
        table_path = config['data_directory']
        table_format_type = config['filetype']
        # Use timezone from config if site_tz not explicitly provided
        if site_tz is None:
            site_tz = config.get('timezone')
    elif site_tz is None:
        # Both path args were supplied, so the config was never consulted above -- but
        # its `timezone` should still beat the UTC fallback. Without this, pointing
        # clifpy at a directory explicitly silently ignores the site timezone.
        site_tz = _resolve_site_tz_from_config(config_path)

    file_path = os.path.join(table_path, 'clif_' + table_name + '.' + table_format_type)

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The file {file_path} does not exist in the specified directory.")

    if table_format_type == 'csv':
        # CSV now supports every return_format. Previously return_rel=True silently
        # downgraded to a DataFrame here; that downgrade is gone, since the relation
        # was always available from read_csv_auto and a silent type change is worse
        # now that the default format is polars. See docs/io_return_formats_dx.md.
        if fmt == _LAZY_SENTINEL:
            if verbose:
                logger.info('Loading CSV file (lazy)')
            con = duckdb.connect()
            con.execute("SET timezone = 'UTC';")
            con.execute("SET pandas_analyze_sample=0;")

            rel = con.read_csv(file_path)

            if columns:
                rel = rel.select(*columns)

            if filters:
                for column, values in filters.items():
                    if isinstance(values, list):
                        values_list = ', '.join(["'" + str(v).replace("'", "''") + "'" for v in values])
                        rel = rel.filter(f"{column} IN ({values_list})")
                    else:
                        value = str(values).replace("'", "''")
                        rel = rel.filter(f"{column} = '{value}'")

            if sample_size:
                rel = rel.limit(sample_size)

            return LazyRelation(rel, con)

        if verbose:
            logger.info(f'Loading CSV file ({fmt})')

        if fmt in ('polars', 'polars_lazy'):
            _lf = _scan_polars(file_path, 'csv', columns, filters, sample_size,
                               site_tz, time_unit, verbose)
            return _lf if fmt == 'polars_lazy' else _lf.collect()

        executor = _prepare_connection(duckdb_con, verbose)

        # Read raw UTC; _finalize relabels to site_tz. See docs/tz_dx.md.
        select_clause = "*" if not columns else ", ".join(columns)

        query = f"SELECT {select_clause} FROM read_csv_auto('{file_path}')"

        # Apply filters (shared compiler -- see _compile_filters_sql)
        filter_clauses = _compile_filters_sql(filters)
        if filter_clauses:
            query += " WHERE " + " AND ".join(filter_clauses)

        if sample_size:
            query += f" LIMIT {sample_size}"

        result = _finalize(executor.sql(query), fmt, site_tz, time_unit, verbose)

    elif table_format_type == 'parquet':
        result = load_parquet_with_tz(
            file_path, columns, filters, sample_size, site_tz, verbose,
            return_rel=False, lazy=False,
            return_format=fmt,
            duckdb_con=duckdb_con, time_unit=time_unit,
            _site_tz_explicit=_site_tz_explicit,
        )

    else:
        raise ValueError("Unsupported filetype. Only 'csv' and 'parquet' are supported.")

    filename = os.path.basename(file_path)
    if verbose:
        logger.info(f"Data loaded successfully from {filename}")

    return result

def convert_datetime_columns_to_site_tz(
    df: pd.DataFrame,
    site_tz_str: str,
    verbose: bool = True
) -> pd.DataFrame:
    """Relabel a DataFrame's ``*_dttm`` columns to a site timezone (pandas/pytz).

    This is the **pandas-level, tz-AWARE** timezone decoder — one of the two parallel
    tz mechanisms in this module (see the module-level "Timezone handling" note). It
    operates on an *already-materialized* ``DataFrame`` and, per ``*_dttm`` column:

    - **tz-aware input** → ``dt.tz_convert(site_tz)`` — a pure relabel; the absolute
      UTC instant is preserved, only the wall-clock/label changes.
    - **tz-naive input** → ``dt.tz_localize(site_tz)`` — *attaches* the zone, assuming
      the naive wall-clock is already in ``site_tz`` (logs a warning).

    Output columns are **tz-aware** in ``site_tz``. Used by the eager materialized load
    path (``load_data`` / ``load_parquet_with_tz`` with ``return_rel=False``) and by
    :func:`fetch_lazy_result`.

    Note
    ----
    Zone resolution uses **pytz**, whose DST transition table freezes at ~2037, so for
    de-identified far-future dates (e.g. CLIF-MIMIC's ~2180 shift) it applies a frozen
    offset. This is *intentionally* retained: pytz-decode is the correct inverse of
    CLIF-MIMIC's pytz encoding and recovers the intended clinical local time. See
    docs/tz_dx.md §11.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame (already materialized; e.g. from ``duckdb.sql(...).df()``).
    site_tz_str : str
        Target timezone string, e.g., "America/New_York" or "US/Central".
    verbose : bool
        Whether to log a per-run conversion summary (default: True).

    Returns
    -------
    pd.DataFrame
        The same DataFrame with every ``*_dttm`` column tz-aware in ``site_tz``.
    """
    site_tz = pytz.timezone(site_tz_str)

    # Identify datetime-related columns
    dttm_columns = [col for col in df.columns if 'dttm' in col]

    if not dttm_columns:
        logger.debug("No datetime columns found in DataFrame")
        return df

    # Track conversion statistics
    converted_cols = []
    already_correct_cols = []
    naive_cols = []
    problem_cols = []

    for col in dttm_columns:
        df[col] = pd.to_datetime(df[col], errors='coerce')
        if pd.api.types.is_datetime64tz_dtype(df[col]):
            current_tz = df[col].dt.tz
            # Compare timezone names/strings instead of timezone objects
            if str(current_tz) == str(site_tz):
                already_correct_cols.append(col)
                logger.debug(f"{col}: Already in timezone {current_tz}")
            else:
                null_before = df[col].isna().sum()
                df[col] = df[col].dt.tz_convert(site_tz)
                null_after = df[col].isna().sum()
                converted_cols.append(col)
                if null_before != null_after:
                    logger.warning(f"{col}: Null count changed during conversion ({null_before} -> {null_after})")
                logger.debug(f"{col}: Converted from {current_tz} to {site_tz}")
        elif pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.tz_localize(site_tz, ambiguous=True, nonexistent='shift_forward')
            naive_cols.append(col)
            logger.warning(f"{col}: Naive datetime localized to {site_tz}. Please verify this is correct.")
        else:
            problem_cols.append(col)
            logger.warning(f"{col}: Expected datetime but found {df[col].dtype}")

    # Log summary based on verbosity
    if verbose and (converted_cols or naive_cols or problem_cols):
        summary_parts = []
        if converted_cols:
            summary_parts.append(f"{len(converted_cols)} converted to {site_tz}")
        if already_correct_cols:
            summary_parts.append(f"{len(already_correct_cols)} already correct")
        if naive_cols:
            summary_parts.append(f"{len(naive_cols)} naive dates localized")
        if problem_cols:
            summary_parts.append(f"{len(problem_cols)} problematic")

        logger.info(f"Timezone processing complete: {', '.join(summary_parts)}")

        if logger.isEnabledFor(logging.DEBUG):
            if converted_cols:
                logger.debug(f"Converted columns: {', '.join(converted_cols)}")
            if naive_cols:
                logger.debug(f"Naive columns: {', '.join(naive_cols)}")
            if problem_cols:
                logger.debug(f"Problem columns: {', '.join(problem_cols)}")

    return df
