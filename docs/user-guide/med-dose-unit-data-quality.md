# Medication dose unit data quality

This page documents dose-unit problems that clifpy's converter **deliberately
does not fix**, and why. Each one is something only the contributing site can
resolve; converting them anyway would replace a visible problem with an
invisible one.

The findings below come from a consortium-wide inventory of every
`med_dose_unit` string across 11 sites — 3,783 `(med_category, med_dose_unit)`
pairs covering **387,795,229 observations**.

A machine-readable version, with per-site attribution, is at
`dev/med-unit-converter/unit_quality_findings.csv`:

| column | meaning |
|---|---|
| `table` | `continuous` or `intermittent` |
| `med_category`, `med_dose_unit` | the pair as reported |
| `issue_type` | one of the categories below |
| `observations`, `median_value` | scale and typical magnitude |
| `n_sites`, `sites` | which sites report it |
| `recommendation` | what to check |

---

## What the converter does handle

For context, of those 387.8M observations:

| outcome | share |
|---|---:|
| converted | 95.08% |
| unit not recorded (`nan`, `None`, `*Unspecified`, `unknown`) | 3.58% |
| countable dosage form (`tablet`, `dose`, `drop`, `puff`) | 1.25% |
| **genuinely unrecognized** | **0.09%** |

Excluding rows with no recorded unit, **98.61%** convert. The 0.09% remainder is
what this page is about.

### Countable dosage forms are not a defect

`tablet`, `capsule`, `drop`, `puff`, `patch`, `dose` and similar are reported as
`_unit_class = 'countable'` with the status *"is a countable dosage form; not
convertible"*. Across every countable token in the inventory the median value is
**1** — these count things, they don't measure drug. "1 tablet" carries no dose
without the product strength, which CLIF does not record.

No action is needed for these. They are called out separately from unparseable
strings so that a genuine parsing gap is not buried among 4.8M tablets.

---

## Findings requiring site action

### 1. Suspected `med_category` mapping errors — highest priority

**9 pairs, 6,538 observations.** These are not unit problems at all: the unit
names an ingredient that is not part of the category it is attached to, which
usually means the wrong `med_category` was assigned upstream.

| category | unit | obs | median | sites |
|---|---|---:|---:|---|
| acetaminophen | `mg of hydrocodone` | 5,274 | 7.5 | UCSF |
| sulfamethoxazole | `mg of trimethoprim` | 1,175 | 240.0 | JHU |
| acetaminophen | `mg of codeine` | 36 | 30.0 | UCSF |
| diphenhydramine | `mg of hydrocodone` | 31 | 37.5 | Emory |
| daptomycin | `units of lipase` | 11 | 3000.0 | Emory |
| sulfamethoxazole | `mg/kg/24hr of trimethoprim` | 6 | 0.0 | JHU |
| morphine | `mg of elemental zinc (Zn)` | 2 | 5.0 | UCSF |
| morphine | `mg of amoxicillin` | 2 | 5.0 | UCSF |
| fentanyl | `mcg of opiate` | 1 | 25.0 | UCMC |

Take the first row. A hydrocodone-acetaminophen product has been mapped to
`acetaminophen`, but the dose recorded is the **hydrocodone** component — median
7.5 mg. A real acetaminophen dose is 325–1000 mg. Anyone analysing acetaminophen
exposure at this site is reading a number roughly **50× too small**.

This is precisely why the converter refuses these. Stripping `of hydrocodone` and
treating the value as plain `mg` would yield "7.5 mg of acetaminophen" — a
number that sits in no outlier filter's danger zone and looks entirely
plausible. The odd unit string is the only surviving evidence that anything is
wrong.

**Action:** review the `med_category` assignment for these products at the source.

### 2. Moiety-qualified units on combination products

**28 pairs, 152,568 observations.** Here the mapping is right, but the dose
refers to one ingredient of a combination product rather than the whole product.

| category | unit | obs | median | plain `mg` median | sites |
|---|---|---:|---:|---:|---|
| trimethoprim_sulfamethoxazole | `mg of trimethoprim` | 97,737 | 190.6 | 320.0 | Emory, JHU, UCSF, UMN |
| imipenem | `mg of imipenem` | 19,793 | 500.0 | 492.7 | JHU, UCSF |
| piperacillin_tazobactam | `mg of piperacillin` | 16,261 | 3985.0 | 2700.0 | 6 sites |
| amoxicillin_clavulanate | `mg of amoxicillin` | 5,853 | 976.9 | 500.0 | JHU, UCSF |
| ampicillin_sulbactam | `mg of ampicillin` | 3,210 | 2000.0 | 3000.0 | JHU, UCSF, UMN |

The problem is that the **same `med_category` carries both conventions**. For
SMX-TMP the moiety-qualified median is 190.6 and the plain-`mg` median is 320.0 —
not comparable, though nothing in the data says so once the unit string is
discarded.

Converting these to plain `mg` would make two incompatible conventions look
identical, so clifpy leaves them flagged.

**Action:** decide per site whether to report the total product dose or the
component dose, and use one convention consistently within a `med_category`.

### 3. Concentrations recorded as dose units

**68 pairs, 17,128 observations.** `mg/ml`, `mcg/ml`, `units/ml`, `mEq/ml`,
`mg/L`, `%`. These describe how concentrated the preparation is, not how much
was given — the administered amount cannot be recovered from this field alone.

**Action:** record the administered amount in `med_dose`, and put concentration
elsewhere.

### 4. Length as a dose unit

**20 pairs, 47,673 observations.** Almost entirely `inch`, for topical
ointments (nitroglycerin especially). A ribbon length is not a dose without the
product's strength per unit length.

### 5. Body-surface-area indexed units

**12 pairs, 5,175 observations.** `mg/m2`, `ml/m2/hr`, `ml/m2/day`. Converting
these needs body surface area, which CLIF does not carry. Note that `/kg`-indexed
units **are** supported, since weight is available from `vitals`.

### 6. Radioactivity units

**3 pairs, 5,527 observations.** `millicurie`, `mci`, `microcurie` — a dimension
outside the converter's unit families.

### 7. Unparseable local codes and free text

**17 pairs, 3,239 observations.** Site-local codes (`asord`, `zzbag`, `XX`,
`mgy`) and free text that leaked into the unit field:

- `unit marking on an u-100 insulin syringe` (2,668 obs)
- `mg/kg only on day(s) in admin instructions` (1 obs)
- `Dose Level`, `kg patient weight`

These deliberately stay `unrecognized` rather than being folded in with missing
values — a junk code is a real signal that the field needs attention, and
collapsing it to NULL would hide it.

---

## A note on the schema itself

Two `med_category` values are listed **twice with conflicting target units** in
the CLIF 3.0 mCIDE continuous file:

| category | targets listed | apart by |
|---|---|---|
| `epoprostenol` | `ng/kg/min` and `mcg/kg/min` | factor of 1000 |
| `terbutaline` | `mg` and `mcg/kg/min` | different unit *classes* |

`load_dose_unit_targets()` keeps the **first** occurrence, so results do not
depend on row order, and logs a warning naming each category and its competing
units. Pass an explicit dict to override:

```python
targets = load_dose_unit_targets(SCHEMA_URL)
targets['epoprostenol'] = 'ng/kg/min'   # decide deliberately
```

This should be resolved upstream in the mCIDE files.

---

## Reproducing these findings

```bash
python dev/med-unit-converter/gen_quality_findings.py
```

reads the two DQA inventory exports in `dev/med-unit-converter/` and writes
`unit_quality_findings.csv` beside them.

Both the inventory exports and the generated findings are CSVs, which this repo
gitignores, so the findings file is produced on demand rather than committed.
The generator itself is tracked, so anyone with the inventory exports can
rebuild it — and rerunning it against a fresh export is how you check whether a
site has fixed something.

## See also

- [Medication Unit Conversion](med-unit-conversion.md) — what the converter *does* convert
