# Test Update Memo — `unit_converter` weight-aware redesign

> **Audience**: human reviewer manually updating fixtures + tests in this directory.
> **Author**: AI agent implementing the upstream code changes in `clifpy/utils/unit_converter.py`.
> **Status**: code changes pending. Do NOT trust this directory's tests until both code and fixtures are updated and the test runs all green.

---

## 1. Pipeline contract (current state)

Two-stage conversion, weight-aware, DuckDB-native end-to-end.

### Stage 1: `_clean_unit → _base_unit`

- Normalizes amount (`mass → mcg`, `volume → ml`, `unit → u`) and time (`/hr → /min`).
- Collapses the weight qualifier `/lb` into the canonical `/kg` axis using a **constant** factor `KG_PER_LB = 2.20462`. Patient weight is never consulted here.
- `_base_unit`'s weight qualifier is in `{'/kg', ''}` only — `/lb` does not appear.
- Emits `_unit_class` (rate / amount / unrecognized).
- Identity in factor: when source already matches the canonical form, all factors are 1, dose passes through unchanged.

### Stage 2: `_base_unit → _preferred_unit`

- Normalizes amount and time from canonical (mcg / ml / u, /min) to the preferred unit's form.
- Applies the **weight transition factor** based on `(_base_wt, _pref_wt)`:

  | base_wt | pref_wt | factor                     | needs `weight_kg`? |
  |---------|---------|----------------------------|---------------------|
  | `/kg`   | `/kg`   | 1                          | no                  |
  | `/kg`   | `/lb`   | 1 / `KG_PER_LB`            | no                  |
  | `/kg`   | `''`    | `weight_kg`                | **yes**             |
  | `''`    | `/kg`   | 1 / `weight_kg`            | **yes**             |
  | `''`    | `/lb`   | 1 / (`weight_kg` × `KG_PER_LB`) | **yes**         |
  | `''`    | `''`    | 1                          | no                  |

- **`_needs_wt`**: 1 iff exactly one of `_base_wt` / `_pref_wt` is `''` (XOR). Drives the orchestrator's lazy weight join and the two failure-message split.
- **Identity short-circuit**: when `_clean_unit = _preferred_unit` and conversion is successful, `med_dose_converted = med_dose` exactly (bit-equal) — no multiplication.

### Schema-level columns emitted

- **Always** (default-visible): `_clean_unit`, `_unit_class`, `_convert_status`, `med_dose_converted`, `med_dose_unit_converted`.
- **Always** (default-hidden via `possible_cols_to_exclude`): `_base_unit`, `_base_dose`, `_preferred_unit`, `_unit_class_preferred`, `_unit_subclass`, `_unit_subclass_preferred`, `_base_wt`, `_pref_wt`, `_needs_wt`, `_weight_source`, `_weight_recorded_dttm`.
- **Only with `show_intermediate=True`**: `_amount_multiplier`, `_time_multiplier`, `_weight_multiplier` (stage 1), `_amount_multiplier_preferred`, `_time_multiplier_preferred`, `_weight_multiplier_preferred` (stage 2).

`_base_wt` and `_pref_wt` are guaranteed non-NULL (NULL inputs map to `''`).

### Orchestrator behavior

- **User-prefilled `weight_kg` is honored exactly**. If `'weight_kg' in med_df.columns`, the internal vitals lookup is skipped. NULLs in the supplied column stay NULL (rows that need weight will fail with the appropriate two-message status).
- **Lazy weight join**: when `weight_kg` is not in `med_df`, the orchestrator pre-computes `_needs_wt`. If no row needs weight, the vitals join is skipped entirely (only adds NULL placeholder columns). Otherwise it filters to needs-weight rows, ASOF-joins, and `UNION ALL BY NAME`s the rest.
- **`fallback_on_earliest: bool = False`**: when True, rows whose ASOF returns NULL (med admin precedes the first charted weight) fall back to the earliest charted weight for the same hospitalization. Surfaces in `_weight_source = 'earliest_fallback'`.
- **End-to-end `DuckDBPyRelation`**. No mid-pipeline `.to_df()`. Validation uses ANTI JOIN + `.fetchall()`. The `_needs_wt` gate uses `.fetchone()`. Final pandas materialization is gated by `return_rel`. Boundary 2a temp table (`_med_unit_input`) is created for pandas inputs and dropped via the temp-table registry on exit (when `return_rel=False`).

### Failure status messages

`_convert_status` values, in order of precedence:

- `'original unit is missing'`
- `'original unit <unit> is not recognized'`
- `'user-preferred unit <unit> is not recognized'`
- `'cannot convert <class> to <class_preferred>'` (e.g., `rate` to `amount`)
- `'cannot convert <subclass> to <subclass_preferred>'` (e.g., `mass` to `volume`)
- `'cannot convert weighted to unweighted: weight_kg is missing'` (source has weight qualifier, target does not, weight unavailable)
- `'cannot convert unweighted to weighted: weight_kg is missing'` (target has weight qualifier, source does not, weight unavailable)
- `'success'`

On any non-success, `med_dose_converted` falls back to `med_dose` (or `_base_dose` if `med_dose` not in scope) and `med_dose_unit_converted` falls back to `_clean_unit` (or `_base_unit`).

---

## 2. Inventory of this directory & what each file needs

(`tests/utils/med_unit_converter/`)

| File                                                             | Lines | Rows | Update scope |
|------------------------------------------------------------------|-------|------|--------------|
| `_clean_dose_unit_formats_test_data.csv`                         | 17    | 16   | **UNCHANGED**. Format cleaning is untouched. |
| `_clean_dose_unit_names_test_data.csv`                           | 43    | 42   | **UNCHANGED**. Name cleaning is untouched. |
| `_convert_clean_units_to_base_units_test_data.csv`               | 25    | 24   | **TARGETED**. ~13 weighted rows: `_base_unit` keeps weight qualifier; `_base_dose` no longer multiplied by `weight_kg`. |
| `test_unit_converter - standardize_dose_to_base_units.csv`       | 972   | 971  | **BULK**. Same edit as above, at scale. Every row matching `WEIGHT_REGEX` in `_clean_unit`. |
| `test_unit_converter - convert_dose_units_by_med_category.csv`   | 992   | 991  | **LARGEST**. `_base_unit` and `_base_dose` shifts as above + `_convert_status` two-message split + new `_needs_wt` column + APPEND new rows for 14 scenarios. |
| `vitals_weights.csv`                                             | 4     | 3    | **OPTIONAL ADDITION** for the `fallback_on_earliest` integration test. |
| `test_unit_converter.py`                                         | —     | —    | **PATH FIX + new test stubs** (see §6). |

**Suggested order** (smallest blast radius first):

1. File 3 (24 rows) — fastest to spot-check by hand. Builds your mental model.
2. File 4 (971 rows) — same edit pattern at scale.
3. File 5 (991 rows + new) — apply the pattern, then split `_convert_status` messages, then add `_needs_wt`, then append new rows.
4. File 6 (optional) — only if you want the fallback integration test.
5. File 7 — fix path, add new test stubs.

I can hand you a one-liner Python script to derive the new `_base_unit` / `_base_dose` / `_needs_wt` columns from `_clean_unit` and `_preferred_unit` once the code lands, so the bulk edits are computable rather than hand-typed. Just ask.

---

## 3. Concrete row-shape examples

### 3a. `_convert_clean_units_to_base_units_test_data.csv`

Stage 1 normalizes amount, time, and the weight axis. The weight axis collapses `/lb` into the canonical `/kg` via the constant `KG_PER_LB = 2.20462` (no patient weight). `_base_unit`'s weight qualifier is in `{'/kg', ''}` only.

Compute `_base_dose` as `med_dose × amount_factor × time_factor × weight_const_factor` where `weight_const_factor = KG_PER_LB` for `/lb` source, `1` otherwise.

Representative rows for `med_dose = 6`:

```
rn,case,med_dose,_clean_unit,weight_kg,_base_dose,_base_unit,note
0,valid,6,mcg/kg/hr,70,0.1,mcg/kg/min,time only: 6 × 1/60
4,valid,6,mcg/kg/min,74,6,mcg/kg/min,identity (factor 1)
5,valid,6,u/kg/hr,75,0.1,u/kg/min,time only
11,valid,6,ng/kg/min,81,0.006,mcg/kg/min,amount only: 6 × 1/1000
12,valid,6,mg/kg/min,82,6000,mcg/kg/min,amount only: 6 × 1000
13,valid,6,mcg/lb/hr,83,0.220462,mcg/kg/min,lb→kg via KG_PER_LB and hr→min
14,valid,6,l/lb/hr,84,220.462,ml/kg/min,lb→kg + L→ml + hr→min
```

`weight_kg` in the input is irrelevant to stage 1 — the test just asserts the column flows through.

### 3b. `test_unit_converter - standardize_dose_to_base_units.csv`

Same stage-1 rule across all rows. A row whose `_clean_unit` matches `/kg/` or `/lb/` will have `_base_unit` end in `/kg/min` (rate) or stay an unweighted form (amount). Rows whose only "failure" reason was missing `weight_kg` for stage 1 now succeed — flip those `case` from `invalid` → `valid` and update `_base_dose` / `_base_unit` accordingly.

### 3c. `test_unit_converter - convert_dose_units_by_med_category.csv`

Apply §3a rules first, then for stage 2 verify these invariants per row:

- **Joint consistency on success**: `med_dose_unit_converted = _preferred_unit`, and `med_dose_converted` is the dose value expressed in that unit. If the unit names a `/lb` form, the dose value must be the lb-form numerical result.
- **Multi-target medications need distinct `med_category` values**: `preferred_units` is a `{med_category: target_unit}` dict, so any row whose expected `_preferred_unit` differs from other rows of the same medication needs a unique `med_category` (e.g., `propofol_lb`, `propofol_unweighted`). Update the test's preferred-units dict to mirror.
- **Failure status precedence** (top to bottom): `'original unit is missing'` → `'original unit <u> is not recognized'` → `'user-preferred unit <u> is not recognized'` → `'cannot convert <class> to <class_preferred>'` → `'cannot convert <subclass> to <subclass_preferred>'` → `'cannot convert weighted to unweighted: weight_kg is missing'` → `'cannot convert unweighted to weighted: weight_kg is missing'` → `'success'`.
- **Failure fallbacks**: on any non-success, `med_dose_converted = med_dose` (or `_base_dose` if `med_dose` not in scope) and `med_dose_unit_converted = _clean_unit` (or `_base_unit`).
- **`_needs_wt` formula**: 1 iff exactly one of `_base_unit` / `_preferred_unit` contains `/kg/` or `/lb/` (XOR). 0 otherwise.

### 3d. `vitals_weights.csv`

Three rows today. Adding rows is needed for the `fallback_on_earliest` and `_weight_source` provenance tests in §6c:

```
hospitalization_id,recorded_dttm,vital_category,vital_value
H_LAGGED,2024-01-01 09:30,weight_kg,72.0
H_NORMAL,2024-01-01 08:00,weight_kg,70.0
H_NORMAL,2024-01-01 10:00,weight_kg,71.5
```

`H_LAGGED`'s earliest weight is recorded *after* its first med admin (09:00), so an ASOF lookup returns NULL; `fallback_on_earliest=True` recovers the 09:30 value.

---

## 4. New test scenarios to append (rows in `convert_dose_units_by_med_category.csv`)

All rows below should round-trip correctly under the new design. Use unique `rn` values continuing from the current max.

| #  | Scenario                                  | `med_dose_unit` | preferred (per `med_category`) | `weight_kg` | Expected `med_dose_converted`        | `_needs_wt` | Expected `_convert_status` |
|----|-------------------------------------------|-----------------|--------------------------------|-------------|--------------------------------------|-------------|----------------------------|
| 1  | Identity, weighted                        | `mcg/kg/min`    | `mcg/kg/min`                   | NULL        | `dose`                               | 0           | `success`                  |
| 2  | Identity after cleaning                   | `mcg/kg/minute` | `mcg/kg/min`                   | NULL        | `dose`                               | 0           | `success`                  |
| 3  | Weighted, time-only                       | `mcg/kg/hr`     | `mcg/kg/min`                   | NULL        | `dose / 60`                          | 0           | `success`                  |
| 4  | Weighted, amount-only                     | `mg/kg/min`     | `mcg/kg/min`                   | NULL        | `dose × 1000`                        | 0           | `success`                  |
| 5  | Weighted, amount + time                   | `mcg/kg/min`    | `mg/kg/hr`                     | NULL        | `dose × 0.06`                        | 0           | `success`                  |
| 6  | kg → lb                                   | `mcg/kg/min`    | `mcg/lb/min`                   | NULL        | `dose / 2.20462`                     | 0           | `success`                  |
| 7  | lb → kg                                   | `mcg/lb/min`    | `mcg/kg/min`                   | NULL        | `dose × 2.20462`                     | 0           | `success`                  |
| 8  | kg → lb, mixed amount/time                | `mg/kg/hr`      | `mcg/lb/min`                   | NULL        | `dose × 1000 / 60 / 2.20462`         | 0           | `success`                  |
| 9  | Weighted → unweighted, weight present     | `mcg/kg/min`    | `mcg/min`                      | 70          | `dose × 70`                          | 1           | `success`                  |
| 10 | Weighted → unweighted, weight NULL        | `mcg/kg/min`    | `mcg/min`                      | NULL        | `dose` (fallback)                    | 1           | `'cannot convert weighted to unweighted: weight_kg is missing'` |
| 11 | Unweighted → weighted, weight present     | `mcg/min`       | `mcg/kg/min`                   | 70          | `dose / 70`                          | 1           | `success`                  |
| 12 | Unweighted → weighted, weight NULL        | `mcg/min`       | `mcg/kg/min`                   | NULL        | `dose` (fallback)                    | 1           | `'cannot convert unweighted to weighted: weight_kg is missing'` |
| 13 | Same-class identity, unweighted           | `mcg/min`       | `mcg/min`                      | NULL        | `dose`                               | 0           | `success`                  |
| 14 | Amount unit identity                      | `mg`            | `mg`                           | NULL        | `dose`                               | 0           | `success`                  |

**Notes:**

- For #2, cleaning normalizes `mcg/kg/minute` → `mcg/kg/min`, identical to the preferred unit. The stage-2 identity short-circuit returns `med_dose` bit-exact.

- For #6 (`mcg/kg/min → mcg/lb/min`): the `1 / KG_PER_LB` factor is applied in **stage 2** (kg → lb is a forward transition, source already canonical).

- For #7 (`mcg/lb/min → mcg/kg/min`): the `KG_PER_LB` factor is applied in **stage 1** (lb is collapsed into kg before stage 2 sees the input). Stage 2 sees identity (`/kg → /kg`).

- For #6 and #7, expected values use the same `KG_PER_LB = 2.20462` literal as the implementation. Add the literal to the fixture's `note` column if you want bit-exact equality assertions.

- For #10 and #12, `med_dose_converted` falls back to the input `med_dose`, NOT `_base_dose`. `med_dose_unit_converted` falls back to `_clean_unit`.

---

## 5. Tests that should remain passing throughout my work (unchanged)

These cover code paths I'm not modifying:

- `test__clean_dose_unit_formats_duckdb`
- `test__clean_dose_unit_names_duckdb`
- `test__acceptable_rate_units`

I'll run only these (and ad-hoc smoke tests against my new code) to verify my work-in-progress without touching the under-update fixtures.

<!-- invisible-code-block: python
import pandas as pd
from pathlib import Path
-->

## 6. Tests that need updating in `test_unit_converter.py`

### 6a. Path fix (mandatory — fixtures moved)

```python
# Before (lines 34-38):
def _load(filename) -> pd.DataFrame:
    path = Path(__file__).parent.parent / 'fixtures' / 'unit_converter' / filename
    df = pd.read_csv(path)
    return df

# After (fixtures now colocated with the test file):
def _load(filename) -> pd.DataFrame:
    path = Path(__file__).parent / filename
    df = pd.read_csv(path)
    return df
```

### 6b. Existing tests to refresh (no logic change, just may need new imports)

- `test__convert_clean_units_to_base_units` — keep, will pass once File 3 is updated.
- `test_standardize_dose_to_base_units` — keep, will pass once File 4 is updated.
- `test__convert_base_units_to_preferred_units_inverse` — keep, will pass once File 4 is updated.
- `test__convert_base_units_to_preferred_units_new` — keep, will pass once File 5 is updated.
- `test_convert_dose_units_by_med_category` — keep, will pass once File 5 is updated.
- `test_convert_dose_units_by_med_category_return_rel` — keep, may need to assert `_needs_wt` in `result_df.columns`.

### 6c. New tests to add (eleven-test plan)

Tiered by priority. **Critical** = no current pytest coverage. **Important** = smoke-only today (in `/tmp/smoke_test_unit_converter.py`), needs formal coverage. **Lower** = ergonomic / surface-area checks.

#### Critical (3)

1. **`test_fallback_on_earliest_recovers_lagged_charting`** — fixture where the earliest charted weight is `recorded_dttm > admin_dttm` for some hospitalization. Assert:
   - With default `fallback_on_earliest=False`: `_convert_status` is the appropriate `weight_kg is missing` message; `_weight_source` is NULL.
   - With `fallback_on_earliest=True`: `_convert_status='success'`; `_weight_source='earliest_fallback'`; `med_dose_converted` is computed using the lagged weight value.

2. **`test_temp_table_cleanup_after_call`** — call `convert_dose_units_by_med_category(return_rel=False)`, then assert `duckdb.sql("SHOW TABLES").fetchall()` returns no `_med_unit_*` rows. Verifies the boundary-2a temp-table-registry cleanup contract.

3. **`test_identity_short_circuit_bit_exact`** — non-integer `weight_kg=73.4836`, `mcg/kg/min → mcg/kg/min`; assert `med_dose_converted == med_dose` exactly (`==`, not `assert_close`).

#### Important (7)

4. **`test_needs_wt_full_matrix`** — every combination of input qualifier `{/kg, /lb, ''}` × preferred qualifier `{/kg, /lb, ''}` (9 rows). Assert `_needs_wt` matches the XOR truth table. Note: `/lb` source rows are normalized to `/kg` in stage 1, so `_base_wt` is in `{'/kg', ''}` only.

5. **`test_kg_lb_constant_factor_no_weight`** — explicit kg↔lb conversion with `weight_kg=NULL`. Cover both directions: `mcg/kg/min → mcg/lb/min` (factor `1/KG_PER_LB`, applied in stage 2) and `mcg/lb/hr → mcg/kg/min` (factor `KG_PER_LB`, applied in stage 1).

6. **`test_user_prefilled_weight_kg_honored`** — pre-filled `weight_kg` column with mixed values + NULLs, `vitals_df=None`. Assert lookup is skipped, NULLs preserved, only `_needs_wt=1 AND weight_kg IS NULL` rows fail.

7. **`test_lazy_weight_join_skipped`** — preferred-units dict where every conversion is weight-compatible, no `weight_kg` in `med_df`, `vitals_df=None`. Assert no error, all rows succeed (verifies the orchestrator's `fetchone()` short-circuit).

8. **`test_anti_join_validation_unacceptable_preferred`** — pass an unacceptable preferred unit (e.g., `iu/hr` for some med). Assert `ValueError` (default) and warning when `override=True`.

9. **`test_anti_join_validation_extra_med_category`** — pass a `med_category` in `preferred_units` that is not in the `med_df`. Assert `ValueError` (default) and warning when `override=True`.

10. **`test_weight_source_provenance`** — three scenarios in one fixture: ASOF hit (`_weight_source='asof'`), earliest fallback (`_weight_source='earliest_fallback'`), no weight available (`_weight_source IS NULL`). Verifies `find_most_recent_weight` provenance column.

#### Lower priority (1)

11. **`test_show_intermediate_surfaces_qa_columns`** — assert `show_intermediate=True` exposes `_amount_multiplier_preferred`, `_time_multiplier_preferred`, `_weight_multiplier_preferred`; default hides them along with `_needs_wt`, `_base_wt`, `_pref_wt`, etc.

#### Required fixture additions

For tests 1 and 10 (`fallback_on_earliest`, `_weight_source` provenance), append to `vitals_weights.csv`:

```
H_LAGGED,2024-01-01 09:30,weight_kg,72.0
H_NORMAL,2024-01-01 08:00,weight_kg,70.0
H_NORMAL,2024-01-01 10:00,weight_kg,71.5
```

And to `convert_dose_units_by_med_category.csv`: two rows for `H_LAGGED` with `admin_dttm = 2024-01-01 09:00` — one expected to fail without fallback, one expected to succeed with fallback.

The remaining tests (3, 4, 5, 6, 7, 8, 9, 11) build their own minimal in-test fixtures (small `pd.DataFrame` literals) and do not depend on the colocated CSVs.

---

## 7. Workflow

1. I implement the code changes in `clifpy/utils/unit_converter.py`.
2. I run **only** the unchanged tests from §5 + ad-hoc smoke tests against my code.
3. I post a summary of the code changes here when done.
4. You manually update the fixtures and tests per §3 / §4 / §6.
5. You signal completion. I run the full test suite to verify correctness end-to-end.

If at any point you'd prefer me to generate the `_base_dose` / `_needs_wt` derivations as a helper script (so the bulk fixture edits are computable rather than hand-typed), say the word. I'll write it as a one-shot file under `dev/` that reads the old fixture and writes the new one.

---

# Round 2 — Weight-adjusted amount units

> **Status**: code changes landed in `clifpy/utils/unit_converter.py`. Fixtures + new test stubs still pending the user's manual update.
> **Smoke tests**: backward-compat against existing `_convert_clean_units_to_base_units_test_data.csv` (21 valid + 3 invalid rows) all pass; 7 new weighted-amount stage-1 cases + 8 end-to-end stage-1+2 cases all pass against the in-memory smoke fixture.

## R2.1 What changed in the converter

Three edits in `clifpy/utils/unit_converter.py`:

1. **Regex anchors fixed.** `KG_REGEX`, `LB_REGEX`, `WEIGHT_REGEX` now use trailing `(/|$)` so they match both rates (`/kg/min`) and weighted amounts (`mcg/kg` end-of-string). Without this, weighted amounts would be silently misclassified as unweighted.

2. **`ACCEPTABLE_AMOUNT_UNITS` is now a cartesian product** (8 → 24 entries) mirroring `_acceptable_rate_units`. The original 8-token vocabulary is preserved as `ACCEPTABLE_BASE_AMOUNT_UNITS` and is the source iterable for **both** rate and amount cartesian products. Without this rename, `_acceptable_rate_units` would have ballooned to 72 entries with garbage like `mcg/kg/kg/min`.

3. **Stage-1 amount branches** in `_convert_clean_units_to_base_units` (both `show_intermediate=True` and CTE-form `show_intermediate=False`) emit `'mcg' || base_weight_qual_expr` (and analogously `ml`, `u`) instead of bare `'mcg'`. For unweighted inputs, `base_weight_qual_expr = ''`, so the output is bit-identical to the prior behavior — strictly additive.

No stage-2 changes were needed: the existing 9-case `weight_factor_clause` handles all weight transitions transparently once stage 1 emits the right base unit.

## R2.2 What changed in the contract

### Updated stage-1 base unit set

`_base_unit` for amounts now joins the weight axis it already owned for rates:

- Mass amount: `mcg`, `mcg/kg` (with `/lb` collapsed to `/kg` via `KG_PER_LB`).
- Volume amount: `ml`, `ml/kg`.
- Unit amount: `u`, `u/kg`.

`/lb` never appears in `_base_unit` (collapsed in stage 1 by a constant factor, identical to the rate-side rule).

### Updated `_needs_wt` semantics

`_needs_wt = 1` iff exactly one of `_base_wt` / `_pref_wt` is `''` (XOR). Since amounts now carry a weight qualifier, this rule applies symmetrically to amount conversions: `mcg/kg → mcg` and `mcg → mcg/kg` both set `_needs_wt = 1` and trigger the lazy weight join.

### Failure status precedence (unchanged)

The two-message split for missing `weight_kg` (`'cannot convert weighted to unweighted: weight_kg is missing'` / `'cannot convert unweighted to weighted: weight_kg is missing'`) now also fires for amount→amount transitions when the weight qualifier flips and weight is unavailable.

## R2.3 Existing test that should keep passing

The user's pre-update `_convert_clean_units_to_base_units_test_data.csv` (rows 0–23) is fully backward-compatible. Verified by smoke test:

- All 21 `valid` rows produce the same `_base_dose` and `_base_unit` as before.
- All 3 `invalid` rows (`liter/kg/hr`, `liters/kg/hr`, `units`, `milli-unit`, `kg`) stay classified as `unrecognized` — they're raw uncleaned forms that don't match the new acceptable amount vocabulary.

## R2.4 Suggested fixture additions (`_convert_clean_units_to_base_units_test_data.csv`)

These rows exercise the new weighted-amount stage-1 path. `weight_kg` in the input is irrelevant to stage 1 — the test asserts the column flows through and that the constant `KG_PER_LB` is applied for `/lb`:

```
rn,case,med_dose,_clean_unit,weight_kg,_base_dose,_base_unit,note
25,valid,6,mcg/kg,70,6,mcg/kg,identity weighted mass amount
26,valid,6,mg/kg,75,6000,mcg/kg,amount only: 6 × 1000
27,valid,6,ng/kg,80,0.006,mcg/kg,amount only: 6 / 1000
28,valid,6,mcg/lb,82,13.22772,mcg/kg,lb→kg constant: 6 × 2.20462
29,valid,6,l/kg,84,6000,ml/kg,volume × 1000 (weighted)
30,valid,6,u/kg,85,6,u/kg,unit identity weighted
31,valid,6,mu/kg,86,0.006,u/kg,unit amount × 1/1000
32,valid,6,mg/lb,88,13227.72,mcg/kg,combined: 6 × 1000 × 2.20462
33,valid,6,g/kg,89,6000000,mcg/kg,grams weighted
```

## R2.5 Suggested fixture additions (`test_unit_converter - convert_dose_units_by_med_category.csv`)

The 14 round-2 scenarios. Use unique `med_category` values per row (`preferred_units` is a `{med_category: target_unit}` dict). Continue `rn` from current max (rough placeholders below).

| #  | Scenario                                          | `med_category` | `med_dose_unit` | preferred       | `weight_kg` | Expected `med_dose_converted`        | `_needs_wt` | Expected `_convert_status` |
|----|---------------------------------------------------|----------------|-----------------|-----------------|-------------|--------------------------------------|-------------|----------------------------|
| 1  | Identity, weighted mass amount                    | `r2_ident_mass`| `mcg/kg`        | `mcg/kg`        | NULL        | `dose`                               | 0           | `success`                  |
| 2  | Identity after cleaning (case/space)              | `r2_ident_clean`| `MG / KG`      | `mg/kg`         | NULL        | `dose`                               | 0           | `success`                  |
| 3  | Weighted amount, amount-only (mg→mcg)             | `r2_mg2mcg_kg` | `mg/kg`         | `mcg/kg`        | NULL        | `dose × 1000`                        | 0           | `success`                  |
| 4  | Weighted amount, amount-only (ng→mcg)             | `r2_ng2mcg_kg` | `ng/kg`         | `mcg/kg`        | NULL        | `dose / 1000`                        | 0           | `success`                  |
| 5  | Weighted amount, kg→lb (constant factor)          | `r2_kg2lb`     | `mcg/kg`        | `mcg/lb`        | NULL        | `dose / 2.20462`                     | 0           | `success`                  |
| 6  | Weighted amount, lb→kg (stage 1 collapse)         | `r2_lb2kg`     | `mg/lb`         | `mcg/kg`        | NULL        | `dose × 1000 × 2.20462`              | 0           | `success`                  |
| 7  | Weighted → unweighted, weight present             | `r2_wtd2u_w`   | `mcg/kg`        | `mcg`           | 70          | `dose × 70`                          | 1           | `success`                  |
| 8  | Weighted → unweighted, weight NULL                | `r2_wtd2u_n`   | `mcg/kg`        | `mcg`           | NULL        | `dose` (fallback)                    | 1           | `'cannot convert weighted to unweighted: weight_kg is missing'` |
| 9  | Unweighted → weighted, weight present             | `r2_u2wtd_w`   | `mcg`           | `mcg/kg`        | 70          | `dose / 70`                          | 1           | `success`                  |
| 10 | Unweighted → weighted, weight NULL                | `r2_u2wtd_n`   | `mcg`           | `mcg/kg`        | NULL        | `dose` (fallback)                    | 1           | `'cannot convert unweighted to weighted: weight_kg is missing'` |
| 11 | Weighted volume amount identity                   | `r2_ml_kg`     | `ml/kg`         | `ml/kg`         | NULL        | `dose`                               | 0           | `success`                  |
| 12 | Weighted unit amount identity                     | `r2_u_kg`      | `u/kg`          | `u/kg`          | NULL        | `dose`                               | 0           | `success`                  |
| 13 | Cross-class still fails (rate → weighted amount)  | `r2_xclass`    | `mcg/kg/min`    | `mcg/kg`        | NULL        | `dose` (fallback)                    | 0           | `'cannot convert rate to amount'` |
| 14 | Cross-subclass still fails (mass/kg → volume/kg)  | `r2_xsubclass` | `mcg/kg`        | `ml/kg`         | NULL        | `dose` (fallback)                    | 0           | `'cannot convert mass to volume'` |

**Verified expected values** (from smoke test with `dose=5`, `weight_kg=70`, `KG_PER_LB=2.20462`):

- Row 1: `5.0` exact (identity short-circuit fires bit-exact)
- Row 3: `5000.0`
- Row 5: `5 / 2.20462 ≈ 2.267965`
- Row 6: `5 × 1000 × 2.20462 = 11023.1`
- Row 7: `5 × 70 = 350.0`
- Row 9: `5 / 70 ≈ 0.071429`
- Row 8 / 10: fallback `med_dose_converted = med_dose = 5.0`; `med_dose_unit_converted = _clean_unit`

## R2.6 Suggested test stubs (`test_unit_converter.py`)

Tier matches §6c of the prior memo. Each test builds its own minimal `pd.DataFrame` literal — no shared CSV dependencies.

### Critical (3)

**R2-1. `test_weighted_amount_identity_short_circuit_bit_exact`**

```python
def test_weighted_amount_identity_short_circuit_bit_exact():
    """Identity short-circuit must apply to weighted amounts (no float drift)."""
    dose = 1.234567
    med_df = pd.DataFrame({
        'med_dose': [dose],
        'med_dose_unit': ['mcg/kg'],
        'med_category': ['x'],
        'weight_kg': [None],
    })
    result, _ = convert_dose_units_by_med_category(med_df, preferred_units={'x': 'mcg/kg'})
    assert result['med_dose_converted'].iloc[0] == dose  # bit-exact, not assert_close
    assert result['_convert_status'].iloc[0] == 'success'
```

**R2-2. `test_weighted_amount_kg_lb_constant_factor_no_weight`**

```python
def test_weighted_amount_kg_lb_constant_factor_no_weight():
    """kg↔lb conversions on weighted amounts use the constant KG_PER_LB; no weight needed."""
    from clifpy.utils.unit_converter import KG_PER_LB
    med_df = pd.DataFrame({
        'med_dose': [5.0, 5.0],
        'med_dose_unit': ['mcg/kg', 'mcg/lb'],
        'med_category': ['kg2lb', 'lb2kg'],
        'weight_kg': [None, None],
    })
    result, _ = convert_dose_units_by_med_category(
        med_df, preferred_units={'kg2lb': 'mcg/lb', 'lb2kg': 'mcg/kg'}
    )
    out = result.set_index('med_category')
    assert out.loc['kg2lb', 'med_dose_converted'] == pytest.approx(5.0 / KG_PER_LB)
    assert out.loc['lb2kg', 'med_dose_converted'] == pytest.approx(5.0 * KG_PER_LB)
    assert (out['_convert_status'] == 'success').all()
```

**R2-3. `test_weighted_amount_to_unweighted_requires_weight`**

```python
def test_weighted_amount_to_unweighted_requires_weight():
    """Weighted↔unweighted amount transitions require weight_kg; two-message split."""
    med_df = pd.DataFrame({
        'med_dose': [5.0, 5.0, 5.0, 5.0],
        'med_dose_unit': ['mcg/kg', 'mcg/kg', 'mcg', 'mcg'],
        'med_category': ['wtd2u_w', 'wtd2u_n', 'u2wtd_w', 'u2wtd_n'],
        'weight_kg': [70.0, None, 70.0, None],
    })
    pref = {'wtd2u_w': 'mcg', 'wtd2u_n': 'mcg', 'u2wtd_w': 'mcg/kg', 'u2wtd_n': 'mcg/kg'}
    result, _ = convert_dose_units_by_med_category(med_df, preferred_units=pref)
    out = result.set_index('med_category')
    assert out.loc['wtd2u_w', 'med_dose_converted'] == pytest.approx(350.0)
    assert out.loc['u2wtd_w', 'med_dose_converted'] == pytest.approx(5.0 / 70.0)
    assert out.loc['wtd2u_n', '_convert_status'] == \
        'cannot convert weighted to unweighted: weight_kg is missing'
    assert out.loc['u2wtd_n', '_convert_status'] == \
        'cannot convert unweighted to weighted: weight_kg is missing'
    # fallbacks
    assert out.loc['wtd2u_n', 'med_dose_converted'] == 5.0
    assert out.loc['u2wtd_n', 'med_dose_converted'] == 5.0
```

### Important (3)

**R2-4. `test_weighted_amount_classified_as_amount`** — assert `_unit_class = 'amount'` (not `'unrecognized'`) for `mcg/kg`, `mg/kg`, `u/kg`, `ml/kg`. Verifies `ACCEPTABLE_AMOUNT_UNITS` expansion took effect. Use `show_intermediate=True` to surface the column.

**R2-5. `test_weighted_amount_subclass_preserved`** — assert `_unit_subclass = 'mass'` for `mcg/kg`, `'volume'` for `ml/kg`, `'unit'` for `u/kg`. Confirms the `MASS_REGEX|VOLUME_REGEX|UNIT_REGEX` patterns still fire correctly with the trailing `/kg`.

**R2-6. `test_weighted_amount_cross_class_subclass_failures`** — `mcg/kg/min → mcg/kg` produces `'cannot convert rate to amount'`; `mcg/kg → ml/kg` produces `'cannot convert mass to volume'`. Verifies the precedence rules still fire when both sides carry weight qualifiers.

### Lower priority (2)

**R2-7. `test_acceptable_amount_units_cartesian_size`**

```python
def test_acceptable_amount_units_cartesian_size():
    from clifpy.utils.unit_converter import (
        ACCEPTABLE_AMOUNT_UNITS,
        ACCEPTABLE_BASE_AMOUNT_UNITS,
    )
    assert len(ACCEPTABLE_BASE_AMOUNT_UNITS) == 8
    assert len(ACCEPTABLE_AMOUNT_UNITS) == 24  # 8 × 3 (kg, lb, none)
    for u in {'mcg', 'mcg/kg', 'mcg/lb', 'ml/lb', 'u/kg', 'mg/kg', 'g/lb'}:
        assert u in ACCEPTABLE_AMOUNT_UNITS

def test_acceptable_rate_units_unchanged():
    from clifpy.utils.unit_converter import ACCEPTABLE_RATE_UNITS
    # 8 base × 3 weight × 2 time = 48 (must NOT have grown to 72)
    assert len(ACCEPTABLE_RATE_UNITS) == 48
    # spot-check no doubled weight qualifier
    for u in ACCEPTABLE_RATE_UNITS:
        assert u.count('/kg') <= 1
        assert u.count('/lb') <= 1
```

**R2-8. `test_acceptable_rate_units_unchanged`** — included as the second assertion block above. Defends against a refactor regression that doubles weight qualifiers via the cartesian product.

## R2.7 Verification workflow (post fixture-update)

1. **Backward-compat smoke** (already done by me, repeat after CSV updates land):
   ```bash
   uv run pytest tests/utils/med_unit_converter/test_unit_converter.py::test__convert_clean_units_to_base_units -v
   ```

2. **New cases**: run the 8 tests in R2.6 above (after adding them to `test_unit_converter.py`).

3. **Full suite**:
   ```bash
   uv run pytest tests/utils/med_unit_converter/ -v
   ```

4. **No regression in dependent callers**:
   ```bash
   rg -l "from clifpy.utils.unit_converter|from clifpy.utils import unit_converter" --type py
   ```
   and run the touching test files.

## R2.8 Cleaning fixture rows (optional but recommended)

You opted to add a few weighted-amount cleaning rows for defensive coverage. Suggested additions:

**`_clean_dose_unit_formats_test_data.csv`** (format step — case + whitespace):

```
input,expected
MCG/KG,mcg/kg
Mg / Kg,mg/kg
 U / KG ,u/kg
ml/LB,ml/lb
```

**`_clean_dose_unit_names_test_data.csv`** (name step — verbose forms):

```
_clean_unit,_clean_name_unit
micrograms/kg,mcg/kg
milligrams/kg,mg/kg
units/kg,u/kg
liters/lb,l/lb
µg/kg,mcg/kg
```

These exercise the existing `UNIT_NAMING_VARIANTS` patterns (`milli-?` → `m`, `g(rams|ram)?` → `g`, `^(u|µ|μ)g` → `mcg`, `u(nits|nit)?` → `u`, `l(iters|...)?` → `l`) in combination with the new weighted suffixes — no code change needed for these to pass.
