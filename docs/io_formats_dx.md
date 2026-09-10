# IO Output Formats: design record for `return_format` / `duckdb_con`

Companion to [`tz_dx.md`](tz_dx.md). That document diagnosed issue #144 and established the
timezone contract; this one records the decisions made when `clifpy/utils/io.py` gained
polars-first output formats, and — more importantly — **which alternatives were rejected and
why**.

> All code blocks here are `text`-fenced on purpose. The root `conftest.py` registers
> `Sybil(patterns=['**/*.md'], path='.')`, which executes every fenced `python` block and *runs*
> every fenced `bash` block via `subprocess`. Illustrative snippets must not be live tests.

---

## 1. Problem

`load_data()` selected its return type from two mutually-exclusive booleans (`return_rel`,
`lazy`) and defaulted to pandas. We are moving clifpy off pandas for loading. The replacement is a
single `return_format` argument with four values, defaulting to polars.

Inherited constraints:

- **`tz_dx.md` §11** — the `site_tz` decoder must behave like pytz, or CLIF-MIMIC timestamps
  shift by an hour (see §3.1).
- **`tz_dx.md` §12** — `return_rel` deliberately does *not* apply `site_tz` (see §4).
- **`tz_dx.md` §7** — prefer explicit connection discipline over bare `duckdb.sql()`.

---

## 2. Final shape

```text
load_data(
    table_name, table_path=None, table_format_type=None,
    sample_size=None, columns=None, filters=None, site_tz=None, verbose=False,
    return_rel=False, lazy=False,          # deprecated aliases
    config_path=None,
    *,
    return_format: Literal['polars','polars_lazy','duckdb','pandas'] = 'polars',
    duckdb_con: Optional[duckdb.DuckDBPyConnection] = None,
    time_unit: Optional[str] = None,       # None = keep source unit (us)
)
```

| `return_format` | returns | `site_tz` applied |
|---|---|---|
| `'polars'` *(default)* | `pl.DataFrame` | yes |
| `'polars_lazy'` | `pl.LazyFrame` | yes (deferred) |
| `'duckdb'` | `DuckDBPyRelation` | **no** — aware-UTC |
| `'pandas'` | `pd.DataFrame` + `DeprecationWarning` | yes |

One DuckDB SQL front-end builds the query (preserving the existing builders and their exact
emitted SQL); the four formats differ only in the terminal conversion.

---

## 3. Empirical findings

Each of these was measured, not reasoned about. Snippets are runnable if pasted into a REPL.

### 3.1 polars is pytz-equivalent at far-future dates — the load-bearing result

`tz_dx.md` §11 warns that `zoneinfo` and DuckDB's ICU project DST rules indefinitely while pytz
freezes its transition table at 2037, and that CLIF-MIMIC's ETL *encoded* with pytz — so only a
pytz decode recovers the intended local time. The question was whether polars' chrono-tz behaves
like pytz or like zoneinfo.

Tested against §11's canonical case, `2180-07-15 17:00 UTC -> US/Eastern`:

```text
pandas / pytz  .dt.tz_convert(pytz.timezone('US/Eastern'))  -> 12:00 EST   (baseline)
polars         .dt.convert_time_zone('US/Eastern')          -> 12:00 EST   MATCH
zoneinfo       .astimezone(ZoneInfo('US/Eastern'))          -> 13:00 EDT   diverges
duckdb ICU     (TIMESTAMPTZ ...)::VARCHAR under SET tz      -> 13:00 -05   diverges
```

Confirmed identical for `US/Eastern`, `America/New_York`, `US/Central`, `America/Chicago`,
`America/Los_Angeles`. **polars' chrono-tz freezes the same way pytz does**, so
`dt.convert_time_zone` is a safe replacement for the pandas decoder.

Measured with polars 1.40.1 / duckdb 1.5.2 / pandas 2.3.3 / pytz 2026.1.

**Caveat A — the agreement is engine-level only.** polars' Rust engine (display, `dt.hour()`,
`dt.replace_time_zone(None)`) says `12:00 EST`. The **Python-object export path** hands the
instant to `zoneinfo` and says `13:00 EDT`:

```text
s = pl.Series([datetime(2180,7,15,17, tzinfo=ZoneInfo('UTC'))]).dt.convert_time_zone('US/Eastern')
s.dt.hour().item()   -> 12     # Rust / chrono-tz  -- matches pytz
s.to_list()[0]       -> 13:00-04:00   # Python / zoneinfo -- does not
```

Column-level work is safe; per-row Python extraction is not. This is asserted in
`test_tz_parity.py` so nobody "fixes" the engine-level assertion to match the export path.

**Caveat B — no `shift_forward` in polars.** pandas uses
`tz_localize(..., nonexistent='shift_forward')`; polars' `replace_time_zone` offers only
`'raise'` / `'null'`. Naive spring-forward gap times therefore become null rather than shifting.
Documented, asserted, not hidden.

### 3.2 DuckDB relations cannot cross connections

This single fact determines the whole connection design.

```text
c1, c2 = duckdb.connect(), duckdb.connect()
labs   = c1.sql("SELECT 'a' AS hospitalization_id, 1 AS v")
vitals = c2.sql("SELECT 'a' AS hospitalization_id, 2 AS w")

duckdb.sql("FROM labs JOIN vitals USING (hospitalization_id) SELECT v, w")
  -> InvalidInputException: Python Object "labs" of type "DuckDBPyRelation" not suitable
     for replacement scan. The object was created by another Connection

c1.sql("FROM a JOIN b USING (hospitalization_id) SELECT v, w")   # both built on c1 -> works
duckdb.sql("FROM a JOIN b ...")                                  # a, b on c1 -> also fails
```

A private-connection relation is invisible to the global `duckdb.sql()` as well. Relations
compose only within one connection.

### 3.3 The tz label resolves at materialization, not at build

`tz_dx.md` §2 Fact A stated this; here it is measured across a re-pin:

```text
con.execute("SET timezone='UTC'")
rel   = con.sql("SELECT TIMESTAMPTZ '2024-07-01 12:00:00-00' AS t_dttm")
eager = rel.pl()               # materialized now
lazyf = rel.pl(lazy=True)      # deferred

con.execute("SET timezone='America/Chicago'")   # later re-pin

eager.schema            -> Datetime('us', 'UTC')             unchanged, detached
lazyf.collect().schema  -> Datetime('us', 'America/Chicago') FOLLOWED the connection
rel.df()  (after SET timezone='Asia/Tokyo')  -> 2024-07-01 21:00:00+09:00
```

The instant is preserved throughout (`12:00 UTC == 07:00 Chicago == 21:00 Tokyo`) — this is a
**label** hazard, never data corruption.

Consequence: `'polars'` and `'pandas'` are immune (they materialize immediately); `'polars_lazy'`
and `'duckdb'` are exposed. See §5.1 for how `'polars_lazy'` is immunized.

### 3.4 `rel.pl(lazy=True)` lifetime

```text
lf = make_lf()          # con + rel go out of scope, gc.collect()
lf.collect()            -> works; the LazyFrame holds a reference to the connection
lf.collect()            -> works again; re-collectable

con.close(); lf.collect()
  -> ComputeError: caught exception during execution of a Python source,
     exception: ConnectionException: Connection has already been closed
```

`explain()` shows `PYTHON SCAN []` — polars treats the relation as an opaque source, so a user's
*later* `.filter()` cannot be pushed into the parquet reader. Since `columns=` / `filters=` are
already pushed down in DuckDB, this was judged acceptable (see §4.3).

Practical rule: never close a connection behind a returned LazyFrame. This is precisely the
footgun `LazyRelation.close()` presents and `duckdb_con` avoids.

### 3.5 `pl.scan_csv` does not parse datetimes by default

```text
pl.scan_csv(path).collect_schema()                        -> event_dttm: String
pl.scan_csv(path, try_parse_dates=True).collect_schema()  -> Datetime('us','UTC')
duckdb read_csv_auto -> .pl()                             -> Datetime('us','UTC')
```

`io_polars.py:195` calls `pl.scan_csv` **without** `try_parse_dates=True`, so `*_dttm` comes back
as `String`; `datetime_polars._build_conversion_expr` then skips it with a "not a datetime
column" warning. CSV timestamps have therefore been silently unparsed on that path. Routing CSV
through the DuckDB reader fixes it — an intentional behaviour change, not a regression.

---

## 4. Options considered and rejected

The most valuable section. Each of these will look attractive again later.

### 4.1 Make every load use its own private connection

*Motivation:* `tz_dx.md` §7 asks for explicit connection discipline; the process-wide default
connection is shared mutable state.

*Rejected:* §3.2 — relations cannot cross connections. `sofa2/_core.py` loads 8 tables with
`return_rel=True` and joins them through global `duckdb.sql()`; downstream
`CLIF-SOFA2/code/sofa2_sa_common.py` does the same. Per-call isolation breaks all of it.

### 4.2 Route all loads through one module-level clifpy connection

*Motivation:* keeps relations mutually joinable (one connection) while isolating clifpy from the
process-wide default, so third-party `SET timezone` cannot reach us. Genuinely the "right" design.

*Rejected for now:* every `duckdb.sql()` that consumes a relation would have to become
`clifpy_con.sql()`. There are **152 such call sites in `clifpy/`**, 106 under `sofa2/`:

```text
_utils.py 23   _resp.py 16   _kidney.py 15   _cv.py 15   _core.py 13
_brain.py 9    rolling/_hemo.py 7   _perf.py 6   _liver.py 4   _hemo.py 4
```

plus `unit_converter.py` 19, `sofa.py` 6, `query.py` 4. And downstream repos would need the same
treatment. Kept as a follow-up; `duckdb_con` makes it *possible* incrementally.

### 4.3 Build `'polars_lazy'` on native `pl.scan_parquet` instead of `rel.pl(lazy=True)`

*Motivation:* true end-to-end laziness with real predicate/projection pushdown into the parquet
reader, and no DuckDB connection held.

*Rejected:* it requires a second implementation of `columns` / `filters` (polars expressions vs.
SQL) that can drift from the DuckDB path, plus `scan_csv(try_parse_dates=True)` handling (§3.5).
One filter/tz implementation across four formats was judged more valuable than pushdown on a
user's *later* filters. Users who want that can call `pl.scan_parquet` directly — the docstring
says so.

### 4.4 Apply `site_tz` to the `'duckdb'` relation

*Rejected:* there is nowhere to put it. `tz_dx.md` §2 Fact A — the label comes from the rendering
connection at materialization; Fact B — `timezone('X', ts)` returns a **naive** `TIMESTAMP`, which
was the original form of issue #144. A relation is an unevaluated plan with no values to label.
Pinning `SET timezone = site_tz` on the shared default is clobbered by the next load (§3.3).

`duckdb_con=new_duckdb_con(site_tz=...)` is the sanctioned path instead.

### 4.5 A "pytz-compat" mode that reproduces pytz wall-clocks inside polars

*Motivation:* guarantee byte-identical wall-clock output with the pandas path regardless of
tzdb drift.

*Rejected as impossible:* pandas stores a UTC instant plus a pytz `tzinfo`, so it gets a correct
instant *and* a pytz wall clock. polars' `Datetime` carries an IANA zone *name* resolved by
chrono-tz — to force a different wall clock you must shift the instant, breaking
instant-preservation, which `tz_dx.md` treats as the invariant that always holds. Moot in
practice: §3.1 shows the two already agree.

### 4.6 Name the argument `output_format` or `backend`

*Rejected:* `output_format` is taken with an incompatible meaning — `wide_dataset.py:110` and
`clif_orchestrator.py:414` use it for `'dataframe' | 'csv' | 'parquet'`, i.e. what file to write.
`backend` (`crosswalk.py:334`, `validator.py:59`) names the *engine*, not the return type.
`return_format` is unambiguous and reads as the successor to the `return_rel` it replaces.

### 4.7 Name the connection parameter `con`

*Rejected, narrowly.* Bare `con` is the codebase convention — `ase.py` uses
`con: duckdb.DuckDBPyConnection` at 7 sites, `sofa_v2_duckdb.py` at ~15. But those are
single-engine modules where a connection can only be a DuckDB connection. `load_data` is the
package's *multi-engine* entry point after this change, so `con=` invites "which engine?" and
pairs poorly with `return_format='duckdb'`. Local variables stay `con`; only the public parameter
is qualified.

---

## 5. Invariants

### 5.1 The unconditional `convert_time_zone` — do not "simplify" this

`_convert_dttm_cols_polars` always emits `dt.convert_time_zone(site_tz or 'UTC')`, even when
`site_tz` is `None` and the source is already UTC. **It looks like a no-op and is not.**

Because conversion is instant-preserving, it lands on the requested zone regardless of the base
label — which is what neutralizes the `'polars_lazy'` label float in §3.3. Skipping it when
`site_tz is None` leaves the label following whatever the connection says at `.collect()` time.

`test_label_float.py` fails if this call is removed. The same reasoning applies to the pandas
path, where `if site_tz:` became `site_tz or 'UTC'`.

### 5.2 Materialized formats are connection-independent

Following from 5.1: for `'polars'`, `'polars_lazy'` and `'pandas'`, a load on a caller-supplied
`duckdb_con` pinned to zone `X` produces the same result as one on the global connection. Only
`'duckdb'`, which relabels nothing, exposes the connection's zone.

| | `duckdb_con=None` (global, pinned UTC) | yours at UTC | yours at `X` |
|---|---|---|---|
| `'polars'` / `'polars_lazy'` | `convert_time_zone(site_tz or 'UTC')` | identical | identical |
| `'pandas'` | pytz `tz_convert(site_tz or 'UTC')` | identical | identical |
| `'duckdb'` | aware-UTC until re-pinned | aware-UTC | **aware in `X`** |

### 5.3 A caller-owned connection is never mutated

`load_data` applies `SET timezone` / `pandas_analyze_sample` only when `duckdb_con is None`.
For a supplied connection it reads `current_setting('TimeZone')` and warns if it is not UTC.
Silently re-pinning someone's connection is the exact bug `duckdb_con` exists to prevent.

### 5.4 Time unit

DuckDB yields `us`. `datetime_polars.standardize_datetime_columns` defaults to
`target_time_unit='ns'` and casts. `load_data(time_unit=None)` keeps the source unit; the
deprecated `io_polars` shims pass `time_unit='ns'` so their existing callers see no change.
This matters because `join_asof` requires exact dtype match — which is why
`ensure_datetime_precision_match` exists in the first place.

---

## 6. Deprecation ledger

Call sites counted across all 17 clifpy-importing repos in the CLIF workspace (paren-matched,
`.ipynb` sources included, vendored legacy `pyCLIF.py` and `archive/` excluded).

| surface | in-repo | downstream | removal stage |
|---|---|---|---|
| `lazy=` / `LazyRelation` / `fetch_lazy_result` / `close_lazy_relation` | 1 test | **0** | next release |
| `return_format='pandas'` (the default flip) | 3 pinned callers | **16** | after `BaseTable.df` migrates |
| `return_rel=` | 10 in `sofa2/_core.py`, ~20 in `dev/` | **26** | last |
| `io_polars.load_*` | 1 dead import | 0 | with pandas |

Downstream detail:

```text
CLIF-SOFA2                          return_rel 20   plain  0
CLIF-epi-of-sedation                return_rel  6   plain  1
CLIF-eligibility-for-mobilization   return_rel  0   plain  8
cco-mcb4arf                         return_rel  0   plain  7
                                    total      26   plain 16
```

`return_rel` outlives the pandas removal deliberately: nobody should have to fix 26 `return_rel`
calls in the same upgrade where they drop pandas. `CLIF-SOFA2` alone holds 20 of them.

The **16 plain calls** are the breaking ones — they silently change return type. One-line fix
(`return_format='pandas'`), but it must reach `cco-mcb4arf` and
`CLIF-eligibility-for-mobilization` directly, not just via release notes.

---

## 7. Follow-ups not in this change

1. **Migrate `BaseTable.df` to polars.** The real work: `base_table.py:104` annotates
   `Optional[pd.DataFrame]` and uses `.select_dtypes`, `.describe()`, `.groupby().nunique()`,
   `.memory_usage()`; propagates to `wide_dataset.py` (5 sites) and `clif_orchestrator.py` (11).
   Note `clif_orchestrator.py:621,658` already calls `df_pl.to_pandas()` — converting *back* — so
   a polars `self.df` removes work there. `validator.py:59` already has a polars backend.
2. **Thread `duckdb_con` through SOFA2** (§4.2). Suggested shape: optional `duckdb_con` on
   `calculate_sofa2` defaulting to global, then migrate modules incrementally. Failures are loud
   (`InvalidInputException`), never silent.
3. **`calculate_sofa2(return_rel=...)` name collision.** SOFA2 has its own public `return_rel`
   parameter (`_core.py:235`, `:722`), unrelated to io's. After this change the name means a live
   parameter in one place and a deprecated alias in another. Renaming is a public SOFA2 API break.
