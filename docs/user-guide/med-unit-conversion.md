# Medication Unit Conversion

CLIFpy provides robust medication dose unit conversion functionality to standardize medication dosing across different unit systems. This is essential for clinical data analysis where medications may be recorded in various units across different systems.

There are two ways in: give the converter a dictionary of target units you
choose yourself, or point it at a CLIF mCIDE schema file and let it read the
targets from there.

## Standardize against the mCIDE schema

`med_category` target units are published in CLIF's mCIDE files. Point clifpy at
one and the whole table is standardized in a single call:

```python
from clifpy import ClifOrchestrator

SCHEMA = (
    "https://raw.githubusercontent.com/Common-Longitudinal-ICU-data-Format/"
    "CLIF/3.0/mCIDE/medication_admin_continuous/"
    "clif_medication_admin_continuous_med_categories.csv"
)

co = ClifOrchestrator(data_directory=..., filetype="parquet", timezone="US/Eastern")
co.standardize_dose_units_for_continuous_meds(SCHEMA)

converted = co.medication_admin_continuous.df_converted
counts = co.medication_admin_continuous.conversion_counts
```

Or without the orchestrator:

```python
from clifpy.utils.unit_converter import (
    load_dose_unit_targets,
    standardize_med_dose_units,
)

targets = load_dose_unit_targets(SCHEMA)      # {'norepinephrine': 'mcg/kg/min', ...}
converted, counts = standardize_med_dose_units(mac_df, targets, vitals_df=vitals_df)
```

Schema files are **always supplied externally and parsed** — clifpy vendors no
copy and assumes no default location, because the schemas change independently
of the library and a stale bundled copy would silently standardize data to the
wrong units.

Categories your data contains but the schema omits are **not** an error: they
keep their own base units and are reported per row in `_convert_status`. This is
why `standardize_med_dose_units` defaults to `override=True`, unlike the
lower-level converter.

### Other schema formats

Readers are registered per file extension (`.csv`, `.yaml`, `.yml`, `.json`,
`.parquet` built in). Adding one is a small function plus a registration:

```python
from clifpy.utils.unit_converter import register_target_reader
import pandas as pd, tomllib

def read_toml_targets(source):
    with open(source, "rb") as fh:
        data = tomllib.load(fh)
    return pd.DataFrame(data["targets"])

register_target_reader(".toml", read_toml_targets)
```

Every reader just returns a DataFrame; validation, NA filtering and
dict-building happen once inside `load_dose_unit_targets`.

The schema's own column names are separate parameters from your data's, because
they are different artefacts. To read the volume-infusion targets out of the
same continuous file:

```python
volume_targets = load_dose_unit_targets(
    SCHEMA, unit_col="volume_infusion_rate_units")
```

!!! warning "CLIF 3.0 lists two categories twice, with conflicting targets"
    `epoprostenol` appears as both `ng/kg/min` and `mcg/kg/min` (a factor of
    1000), and `terbutaline` as both `mg` and `mcg/kg/min` (different unit
    classes). `load_dose_unit_targets` keeps the **first** occurrence so the
    result never depends on row order, and logs a warning naming each conflict.
    Override deliberately if you need the other:

    ```python
    targets = load_dose_unit_targets(SCHEMA)
    targets["epoprostenol"] = "ng/kg/min"
    ```

## Standardize dose units by medication

In the most common use cases, we want to **standardize dose units by medication and pattern of administration** -- all propofol doses to be presented in mcg/kg/min in the continuous table and in mcg in the intermittent table, for example.

To achieve this, simply call one of the two `convert_dose_units_*` functions (one for continuous and one for intermittent) from the CLIF orchestrator and provide **a dictionary mapping of medication categories to their preferred units**:

```python
from clifpy.clif_orchestrator import ClifOrchestrator

co = ClifOrchestrator(config_path="config/config.yaml")

preferred_units_cont = {
    "propofol": "mcg/min",
    "fentanyl": "mcg/hr",
    "insulin": "u/hr",
    "midazolam": "mg/hr",
    "heparin": "u/min"
}

co.convert_dose_units_for_continuous_meds(preferred_units=preferred_units_cont)
```

### Returns

Under the hood, this function automatically loads and uses the medication and vitals tables to generate two dataframes that are saved to the corresponding medication table instance by default:

1. **`co.medication_admin_continuous.df_converted`** gives the updated medication table with the new columns appended:
   - `weight_kg`: the most recent weight relative to the `admin_dttm` pulled from the `vitals` table.
   - `_clean_unit`: cleaned source unit string where both 'U/h' and 'units / hour' would be standardized to 'u/hr', for example.
   - `_unit_class`: distinguishes where the source unit is an amount (e.g. 'mcg'), a 'rate' (e.g. 'mcg/hr'), or 'unrecognized'.
   - `_convert_status`: documents whether the conversion is a "success" or, in the case of failure, the reason for failure, e.g. 'cannot convert amount to rate' for rows of propofol in 'mcg' that the users want to convert to 'mcg/kg/min'.
   - `med_dose_converted`, `med_dose_unit_converted`: the converted results if the `_convert_status` is 'success', or fall back to the original `med_dose` and `_clean_unit` if failure.

*Note: the following demo output omits some rows and columns for display purposes*

2. **`co.medication_admin_continuous.conversion_counts`** shows an aggregated summary of which source units of which `med_category` are converted to which preferred units -- and their frequency counts. A useful quality check would be to filter for all the `_convert_status` that are not 'success.'

To access the results directly instead of from the table instance, turn off the `save_to_table` argument:

```python
cont_converted, cont_counts = co.convert_dose_units_for_continuous_meds(
    preferred_units=preferred_units_cont,
    save_to_table=False
)
```

### Override option

The function automatically parses whether the provided `med_categories` and preferred units in the dictionary are acceptable and return errors or warnings when they are not. To override any code-breaking error such as an unidentified `med_category` or preferred unit string, turn on the arg `override=True`:

```python
co.convert_dose_units_for_continuous_meds(
    preferred_units=preferred_units_cont,
    override=True
)
```

### Acceptable unit formatting

The unit strings in `preferred_units` dictionary need to be formatted a certain way for them to be accepted. (The original source unit strings in `med_dose_unit` do _not_ face such restrictions. Both 'mL' and 'milliliter' in `med_dose_unit` can be correctly parsed as 'ml', for example.)

For a list of acceptable preferred units:

- **amount:**
  - mass: `mcg`, `mg`, `ng`, `g`
  - volume: `ml`, `l`
  - unit: `mu`, `u`

- **weight:** `/kg`, `/lb`

- **time:** `/hr`, `/min`

- **rate:** a combination of amount, weight, and time, e.g. 'mcg/kg/min', 'u/hr'.
  - the unit can be either weight-adjusted or not -- that is, both 'mcg/kg/min' and 'mcg/min' are acceptable. When no weight is available from the `vitals` table to enable conversion between weight-adjusted and weight-less units, an error will be returned.

All strings should be in lower case with no whitespaces in between.


## Standardize to base units across medications

In rarer cases, one might prefer all applicable units of the same class be collapsed onto the same scale across medications, e.g. both 'mcg/kg/min' and 'mg/hour' would be converted to the same 'mcg/min' -- referred to here as the "base unit" -- across all medications applicable.

To enable this, turn on the `show_intermediate=True` argument:

```python
cont_converted_detailed, _ = co.convert_dose_units_for_continuous_meds(
    preferred_units=preferred_units_cont,
    save_to_table=False,
    show_intermediate=True
)
```

This would append a series of additional columns that were the intermediate results generated during the conversion, including the `_base_dose` and `_base_unit`.

The set of base units are:

- **amount**: `mcg`, `ml`, `u`
- **time**: `/min`
- **rate**: a combination of amount and time, e.g. `mcg/min`, `u/min`.
  - Note that all base units would be weight-less.

## Unit Classification System

### Unit Classes

- **`rate`**: Dose per time units (e.g., mcg/min, ml/hr, u/kg/hr)
- **`amount`**: Total dose units (e.g., mcg, ml, u)
- **`countable`**: Countable dosage forms (tablet, drop, puff, patch, dose). Understood, but *never convertible* — "1 tablet" carries no dose without the product strength, which CLIF does not record. The dose passes through untouched.
- **`unrecognized`**: Units that cannot be parsed or converted

A missing unit is not a class: an empty string, or one of `nan`, `None`,
`null`, `unspecified`, `*unspecified`, `unknown`, becomes NULL and reports as
`original unit is missing`. Site-local junk codes such as `asord` or `XX` stay
`unrecognized`, because collapsing them to "missing" would hide a real
data-quality signal.

### Unit Subclasses

- **`mass`**: mcg, mg, ng, g → base `mcg`
- **`volume`**: ml, l, mcl → base `ml`
- **`unit`**: u, mu (milli-units), mnu (million units) → base `u`
- **`equivalent`**: meq → base `meq`
- **`substance`**: mmol → base `mmol`
- **`cell_count`**: cells → base `cells` (CAR-T products)
- **`gas_fraction`**: ppm → base `ppm` (nitric oxide)
- **`unrecognized`**: Units that don't fit standard categories

`meq`, `mmol`, `cells` and `ppm` are each their own base. Converting mEq to mg
would need the ion's valence and molar mass, which the medication tables do not
carry, so cross-subclass conversion is refused rather than guessed. Within a
family the arithmetic is ordinary: `mEq/hr → mEq/min` works fine.

`ppm` and `cells` are also kept out of the weight and time axes — `ppm/kg/min`
is not a meaningful unit — so they convert only to themselves.

Unit class and subclass compatibility determines whether conversions are allowed. For example:

- ✅ `rate` → `rate` (same class)

- ✅ `mass` → `mass` (same subclass)

- ❌ `rate` → `amount` (different class)

- ❌ `mass` → `volume` (different subclass)

### Reference Table

| unit class | unit subclass | _clean_unit | acceptable source `med_dose_unit` examples | _base_unit |
|------------|---------------|-------------|----------------------|------------|
| **Amount Units** |
| amount | mass | mcg | MCG, µg, μg, ug | mcg |
| amount | mass | mg | MG, milligram | mcg |
| amount | mass | ng | NG, nanogram | mcg |
| amount | mass | g | G, gram, grams | mcg |
| amount | volume | ml | mL, milliliter, milliliters | ml |
| amount | volume | l | L, liter, liters, litre, litres | ml |
| amount | unit | u | U, unit, units | u |
| amount | unit | mu | MU, milliunit, milliunits, milli-unit, milli-units | u |
| **Rate Units** |
| rate | mass | mcg/min | MCG/MIN, µg/min, μg/min, mcg/minute, micrograms/minute | mcg/min |
| rate | mass | mcg/hr | MCG/HR, µg/hr, μg/hr, mcg/hour, micrograms/hour | mcg/min |
| rate | mass | mcg/kg/min | MCG/KG/MIN, µg/kg/min, mcg/kg/minute | mcg/min |
| rate | mass | mcg/kg/hr | MCG/KG/HR, µg/kg/hr, mcg/kg/hour | mcg/min |
| rate | mass | mcg/lb/min | MCG/LB/MIN, µg/lb/min, mcg/lb/minute | mcg/min |
| rate | mass | mcg/lb/hr | MCG/LB/HR, µg/lb/hr, mcg/lb/hour | mcg/min |
| rate | volume | ml/min | mL/min, ml/m, milliliter/minute | ml/min |
| rate | volume | ml/hr | mL/hr, ml/h, milliliter/hour, milliliters/hour, millilitres/hour | ml/min |
| rate | volume | ml/kg/min | mL/kg/min, milliliter/kg/minute | ml/min |
| rate | volume | ml/kg/hr | mL/kg/hr, milliliter/kg/hour | ml/min |
| rate | volume | ml/lb/min | mL/lb/min, milliliter/lb/minute | ml/min |
| rate | volume | ml/lb/hr | mL/lb/hr, milliliter/lb/hour | ml/min |
| rate | unit | u/min | U/min, units/minute, unit/minute | u/min |
| rate | unit | u/hr | U/hr, units/hour, unit/hour | u/min |
| rate | unit | u/kg/min | U/kg/min, units/kg/minute | u/min |
| rate | unit | u/kg/hr | U/kg/hr, u/kg/h, units/kg/hour | u/min |
| rate | unit | u/lb/min | U/lb/min, units/lb/minute | u/min |
| rate | unit | u/lb/hr | U/lb/hr, units/lb/hour, unit/lb/hr | u/min |
| **CLIF 3.0 additions** |
| amount | unit | mnu | million units, Million Units, MMU | u |
| amount | equivalent | meq | mEq, MEQ | meq |
| amount | substance | mmol | mmol, MMOL | mmol |
| amount | cell_count | cells | cells | cells |
| amount | gas_fraction | ppm | ppm, PPM | ppm |
| amount | volume | mcl | microliter, microlitre, µL, μL | ml |
| amount | mass | ng | nanogram, nanograms | mcg |
| rate | mass | mcg/kg/day | mcg/kg/day, mcg/kg/24hr | mcg/kg/min |
| rate | equivalent | meq/hr | mEq/hr, mEq/HR | meq/min |
| countable | — | tablet | tablet, Tablet., tab | *(not converted)* |
| countable | — | drop | drop, puff, patch, spray, dose, each, ... | *(not converted)* |

The time axis is `/min` (base), `/hr` and `/day`; the weight axis is `/kg`,
`/lb` or none. Any base token combines with both, so `meq/kg/day` and
`mmol/lb/hr` are accepted even though nothing in mCIDE targets them.

### Important Notes

- **_clean_unit**: The exact format you must use when specifying preferred units
- **Acceptable Variations**: Raw `med_dose_unit` strings in your original DataFrame that the converter can detect and clean (these are NOT acceptable formats for preferred units)
- **_base_unit**: The standardized unit all conversions target (mcg/min, ml/min, u/min for rates; mcg, ml, u for amounts)

### Unit family spelling

CLIF 3.0 mCIDE spells the unit family out (`units/hr`, `milli-units/min`) while
abbreviating mass and volume (`mcg`, `ml`). clifpy matches that:

- **Input** accepts both spellings. `units/kg/hr` and `u/kg/hr` are the same unit; existing code passing `u/min` is unaffected.
- **Output** echoes whatever *you* asked for. Request `u/min` and you get `u/min`; request `units/min` and you get `units/min`, so an mCIDE schema round-trips exactly.
- Where clifpy picks the unit itself — a category with no entry in `preferred_units` — it renders `units`, `milli-units` or `million-units`.

!!! warning "`MU` is never accepted as an abbreviation"
    In clinical use `MU` means million units and `mU` means milliunits,
    differing only by case — and the converter lowercases before it sees the
    string. Only the unambiguous spellings (`million units`, `MMU`) are read as
    million units; a bare `mu` always means milli-units. The two are a factor
    of 10⁹ apart.



## Converting a different column

The converter is not tied to `med_dose` / `med_dose_unit`. All six column names
are parameters, so the same machinery standardizes any dose-like pair — for
example `volume_infusion_rate`, which mCIDE targets at `ml/hr` for 72 of its 77
continuous categories:

```python
converted, counts = convert_dose_units_by_med_category(
    mac_df,
    preferred_units={cat: "ml/hr" for cat in categories},
    dose_col="volume_infusion_rate",
    unit_col="volume_infusion_rate_unit",
    converted_dose_col="volume_infusion_rate_converted",
    converted_unit_col="volume_infusion_rate_unit_converted",
)
```

`category_col` and `time_col` are available too. Defaults reproduce the previous
behaviour exactly.

Your other columns are safe: a `medication_admin_continuous` row carries both
`med_dose` and `volume_infusion_rate`, and converting one leaves the other
untouched. Column names must be plain SQL identifiers
(`[A-Za-z_][A-Za-z0-9_]*`); anything else is rejected rather than escaped.

## Extending the countable vocabulary

`_unit_class = 'countable'` is backed by 21 tokens covering 99.95% of countable
volume across the consortium. If your site uses others, pass them rather than
editing clifpy:

```python
from clifpy.utils.unit_converter import DEFAULT_COUNTABLE_UNITS

converted, counts = convert_dose_units_by_med_category(
    mac_df,
    preferred_units=prefs,
    countable_units=set(DEFAULT_COUNTABLE_UNITS) | {"troche", "lozenge"},
)
```

Matching is exact set membership, never a regex — `mg/kg/dose` is a prescribing
rate, not a dosage form, and a regex on `dose` would wrongly capture it.

## Error Handling

### The `_convert_status` Column

After conversion, each record includes a `_convert_status` field indicating the outcome:

Evaluated in this order — the first matching branch wins:

1. **`original unit is missing`**: no unit recorded (empty, or a placeholder such as `nan`, `*Unspecified`, `unknown`)
2. **`original unit [unit] is a countable dosage form; not convertible`**: a tablet, drop, puff, patch, dose, and similar
3. **`original unit [unit] is not recognized`**: input unit cannot be parsed
4. **`user-preferred unit [unit] is not recognized`**: target unit is invalid
5. **`cannot convert [class1] to [class2]`**: incompatible unit classes (e.g. rate → amount)
6. **`cannot convert [subclass1] to [subclass2]`**: incompatible subclasses (e.g. `cannot convert equivalent to mass` for mEq → mg)
7. **`cannot convert weighted to unweighted: weight_kg is missing`**
8. **`cannot convert unweighted to weighted: weight_kg is missing`**
9. **`success`**

!!! note "Statuses 7 and 8 replaced an older single message"
    Earlier versions reported `cannot convert to a weighted unit if weight_kg
    is missing` for both directions.

### Failure Handling

When conversion fails:

- `med_dose_converted` = original `med_dose` (or `_base_dose` if `med_dose` is absent)

- `med_dose_unit_converted` = `_clean_unit` (or `_base_unit` if cleaning failed)

The dose is never silently scaled on a failure path.

## Alternative: Direct Unit Converter Usage

For advanced users who need more control or want to use the unit converter directly without the ClifOrchestrator:

### Primary Function: `convert_dose_units_by_med_category()`

```python
from clifpy.utils.unit_converter import convert_dose_units_by_med_category
import pandas as pd

# Load your medication data
med_df = pd.read_parquet('clifpy/data/clif_demo/clif_medication_admin_continuous.parquet')

# Define preferred units for each medication
preferred_units = {
    'propofol': 'mcg/kg/min',
    'fentanyl': 'mcg/hr',
    'insulin': 'u/hr',
    'midazolam': 'mg/hr'
}

# Convert units
converted_df, summary_df = convert_dose_units_by_med_category(
    med_df=med_df,
    preferred_units=preferred_units,
    override=False
)
```

### Secondary Function: `standardize_dose_to_base_units()`

This function is for advanced users who need to standardize all units to a base set without medication-specific preferences.

```python
from clifpy.utils.unit_converter import standardize_dose_to_base_units

# Standardize to base units only
base_df, counts_df = standardize_dose_to_base_units(med_df)
```

## Best Practices

1. **Check conversion status** after processing to identify failed conversions
2. **Use exact _clean_unit formats** when specifying preferred units
3. **Review the conversion counts summary DataFrame** to understand conversion patterns and identify data quality issues
4. **Test with override=True** first to see all potential issues before requiring strict validation
5. **Validate your preferred_units dictionary** against the acceptable units table above

## Troubleshooting

### Common Issues

**Issue**: "Cannot convert rate to amount"
    - **Solution**: Ensure unit classes match (rate→rate, amount→amount)

**Issue**: "Cannot convert mass to volume"
    - **Solution**: Ensure unit subclasses match (mass→mass, volume→volume)

**Issue**: "User-preferred unit [unit] is not recognized"
    - **Solution**: Use exact `_clean_unit` format from the reference table above

**Issue**: Weight-based conversions failing
    - **Solution**: Ensure `weight_kg` column exists in your DataFrame or is available in vitals data

**Issue**: "Cannot convert to a weighted unit if weight_kg is missing"
    - **Solution**: Provide patient weights in the vitals table or med_df

### Getting Help

If you encounter units not in the reference table or unexpected conversion failures:

1. Check the `_convert_status` column for specific error messages
2. Review the summary DataFrame for patterns in failed conversions
3. Use `override=True` to see warnings instead of stopping on errors
4. Consult the API reference for detailed function documentation

## Example Analysis Workflow

```python
# 1. Basic conversion
converted_df, summary_df = co.convert_dose_units_for_continuous_meds(
    preferred_units=preferred_units_cont,
    save_to_table=False
)

# 2. Check conversion success
print(f"Total records: {len(converted_df)}")
print(f"Successful conversions: {(converted_df['_convert_status'] == 'success').sum()}")

# 3. Analyze conversion patterns
summary_analysis = summary_df.groupby(['med_category', '_convert_status'])['count'].sum()
print("Conversion summary by medication:")
print(summary_analysis)

# 4. Check for problematic units
problematic_units = summary_df[summary_df['_convert_status'] != 'success']
print("\\nUnits requiring attention:")
print(problematic_units[['med_dose_unit', '_convert_status', 'count']])
```