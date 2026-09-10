"""Tests for the CLIF 3.0 unit-converter expansion.

Covers the work that widened the grammar to the mCIDE target vocabulary, added
the `countable` class and placeholder handling, made column names parameters,
and added schema-driven standardization.

The two regression tests worth reading first are
:func:`test_preferred_unit_normalized_milli_units` and
:func:`test_million_units_not_confused_with_milliunits`. Both guard *silent*
failures -- cases where the converter reported success while returning a dose
off by 1000x or 1e9 -- which is why they assert on values rather than status.

Style follows tests/utils/med_unit_converter/test_unit_converter.py: CSV
fixtures co-located in this directory, module-level constants over magic
numbers.
"""

from pathlib import Path

import duckdb
import pandas as pd
import pytest

from clifpy.utils.unit_converter import (
    ALL_ACCEPTABLE_UNITS,
    DEFAULT_COUNTABLE_UNITS,
    SUBCLASS_SPEC,
    convert_dose_units_by_med_category,
    load_dose_unit_targets,
    register_target_reader,
    standardize_med_dose_units,
    validate_column_name,
    _clean_dose_unit_formats_duckdb,
    _clean_dose_unit_names_duckdb,
)

FIXTURE_DIR = Path(__file__).parent

# 1 kg = 2.20462 lb; mirrors clifpy.utils.unit_converter.KG_PER_LB
WEIGHT_KG = 70.0
TS = pd.Timestamp('2024-01-01', tz='UTC')


@pytest.fixture
def captured_warnings():
    """Collect WARNING records from clifpy's converter logger.

    Not `caplog`: clifpy configures its own handlers, and once another test has
    initialised logging the records no longer propagate to the root logger that
    caplog attaches to -- so caplog passes in isolation and fails in a full
    run. Attaching directly to the named logger is order-independent.
    """
    import logging

    records = []

    class _Capture(logging.Handler):
        def emit(self, record):
            records.append(record.getMessage())

    handler = _Capture()
    lg = logging.getLogger('clifpy.utils.unit_converter')
    previous_level = lg.level
    lg.addHandler(handler)
    lg.setLevel(logging.WARNING)
    try:
        yield records
    finally:
        lg.removeHandler(handler)
        lg.setLevel(previous_level)


def _frame(rows):
    """Build a minimal med frame from (category, dose, unit) triples."""
    return pd.DataFrame({
        'hospitalization_id': [f'H{i}' for i in range(len(rows))],
        'med_category': [r[0] for r in rows],
        'med_dose': [r[1] for r in rows],
        'med_dose_unit': [r[2] for r in rows],
        'weight_kg': [WEIGHT_KG] * len(rows),
        'admin_dttm': [TS] * len(rows),
    })


def _convert(rows, preferred_units, **kwargs):
    kwargs.setdefault('override', True)
    out, counts = convert_dose_units_by_med_category(
        _frame(rows), preferred_units=preferred_units, **kwargs)
    return out.set_index('med_category'), counts


def _normalize(unit):
    """Push a unit string through the converter's own cleaning chain."""
    df = pd.DataFrame({'u': [unit]})
    rel = _clean_dose_unit_formats_duckdb(df, col='u', out_col='c')
    rel = _clean_dose_unit_names_duckdb(rel, col='c')
    return rel.to_df()['c'].iloc[0]


# ===========================================
# Silent-failure regressions
# ===========================================

@pytest.mark.unit_conversion
def test_preferred_unit_normalized_milli_units():
    """mCIDE's `milli-units/min` must behave identically to `mu/min`.

    The caller's preferred unit used to be matched against the factor regexes
    raw. `milli-units/min` starts "mi", so it failed `MU_REGEX = ^(mu)`, the
    multiplier fell through to 1, and the dose came out 1000x wrong while
    `_convert_status` still said 'success'.
    """
    out, _ = _convert(
        [('spelled', 1.0, 'units/min'), ('abbrev', 1.0, 'units/min')],
        {'spelled': 'milli-units/min', 'abbrev': 'mu/min'},
    )
    assert out.loc['spelled', 'med_dose_converted'] == pytest.approx(1000.0)
    assert out.loc['abbrev', 'med_dose_converted'] == pytest.approx(1000.0)
    assert (out.loc['spelled', 'med_dose_converted']
            == out.loc['abbrev', 'med_dose_converted'])
    assert out.loc['spelled', '_convert_status'] == 'success'


@pytest.mark.unit_conversion
def test_million_units_not_confused_with_milliunits():
    """`million units` and `milli-units` must stay 1e9 apart.

    Clinically `MU` is million units and `mU` is milliunits, differing only by
    case -- and case is gone by the time the converter sees the string. Mapping
    million-units onto the existing `mu` token would be a 1e9 error on
    oxytocin, penicillin and heparin doses.
    """
    out, _ = _convert(
        [('mega', 1.0, 'million units'), ('milli', 1.0, 'milli-units')],
        {'mega': 'u', 'milli': 'u'},
    )
    assert out.loc['mega', 'med_dose_converted'] == pytest.approx(1e6)
    assert out.loc['milli', 'med_dose_converted'] == pytest.approx(1e-3)
    ratio = (out.loc['mega', 'med_dose_converted']
             / out.loc['milli', 'med_dose_converted'])
    assert ratio == pytest.approx(1e9)


@pytest.mark.unit_conversion
def test_bare_mu_is_never_read_as_million_units():
    """A bare `mu` means milli-units, never million units."""
    assert _normalize('MU') == 'mu'
    assert _normalize('mu') == 'mu'
    assert _normalize('million units') == 'mnu'
    assert _normalize('MMU') == 'mnu'


# ===========================================
# clifpy#153
# ===========================================

@pytest.mark.unit_conversion
def test_issue_153_partial_preferred_units():
    """An unconvertible drug outside `preferred_units` must not abort the call.

    Requesting vasopressor units on a table that also held sodium bicarbonate
    charted in mEq/min used to raise, naming a unit the caller never asked for.
    """
    med_df = _frame([
        ('norepinephrine', 5.0, 'mcg/min'),
        ('sodium bicarbonate', 50.0, 'not-a-unit'),
    ])
    out, _ = convert_dose_units_by_med_category(
        med_df,
        preferred_units={'norepinephrine': 'mcg/kg/min'},
        override=False,          # must NOT raise
    )
    out = out.set_index('med_category')
    assert out.loc['norepinephrine', '_convert_status'] == 'success'
    assert out.loc['norepinephrine', 'med_dose_converted'] == pytest.approx(5.0 / WEIGHT_KG)
    # the untouched drug passes through and is reported per row
    assert out.loc['sodium bicarbonate', 'med_dose_converted'] == 50.0
    assert 'not recognized' in out.loc['sodium bicarbonate', '_convert_status']


@pytest.mark.unit_conversion
def test_bad_caller_unit_still_raises_and_names_the_category():
    """A genuinely bad unit the caller DID supply must still raise."""
    with pytest.raises(ValueError) as exc:
        convert_dose_units_by_med_category(
            _frame([('norepinephrine', 5.0, 'mcg/min')]),
            preferred_units={'norepinephrine': 'iu/hr'},
            override=False,
        )
    assert 'norepinephrine' in str(exc.value)


# ===========================================
# New unit families
# ===========================================

@pytest.mark.unit_conversion
@pytest.mark.parametrize('dose,src,tgt,expected', [
    (60.0, 'mEq/hr', 'meq/min', 1.0),
    (1.0, 'meq/min', 'meq/hr', 60.0),
    (5.0, 'mmol', 'mmol', 5.0),
    (20.0, 'ppm', 'ppm', 20.0),
    (2.0, 'cells', 'cells', 2.0),
    (1440.0, 'mcg/kg/day', 'mcg/kg/min', 1.0),
    (1.0, 'mcg/kg/min', 'mcg/kg/day', 1440.0),
    (5.0, 'nanogram/kg/min', 'ng/kg/min', 5.0),
    (1000.0, 'microliter/min', 'ml/min', 1.0),
    (1.0, 'units', 'million-units', 1e-6),
])
def test_new_family_conversions(dose, src, tgt, expected):
    out, _ = _convert([('drug', dose, src)], {'drug': tgt})
    assert out.loc['drug', '_convert_status'] == 'success'
    assert out.loc['drug', 'med_dose_converted'] == pytest.approx(expected)


@pytest.mark.unit_conversion
@pytest.mark.parametrize('src,tgt,message', [
    ('mEq', 'mg', 'cannot convert equivalent to mass'),
    ('mmol', 'mg', 'cannot convert substance to mass'),
    ('ppm', 'mcg', 'cannot convert gas_fraction to mass'),
    ('cells', 'mg', 'cannot convert cell_count to mass'),
])
def test_cross_family_conversion_is_refused(src, tgt, message):
    """mEq -> mg needs ion valence and molar mass the table does not carry.

    The existing subclass guard blocks this with no family-specific code; the
    dose must pass through untouched rather than being silently scaled.
    """
    out, _ = _convert([('drug', 10.0, src)], {'drug': tgt})
    assert out.loc['drug', '_convert_status'] == message
    assert out.loc['drug', 'med_dose_converted'] == 10.0


@pytest.mark.unit_conversion
def test_standalone_units_have_no_weight_or_time_forms():
    """`ppm/kg/min` and `cells/kg` are meaningless and must not be accepted."""
    for bad in ('ppm/kg/min', 'ppm/hr', 'ppm/kg', 'cells/kg/min', 'cells/hr'):
        assert bad not in ALL_ACCEPTABLE_UNITS
    for good in ('ppm', 'cells'):
        assert good in ALL_ACCEPTABLE_UNITS


@pytest.mark.unit_conversion
def test_mcide_targets_all_acceptable():
    """Every CLIF 3.0 mCIDE target unit must normalize to an accepted unit.

    This is the acceptance test for the whole expansion. The external schema is
    a better adversary than fixtures written against our own implementation --
    it is what surfaced the `milli-units/min` bug in the first place.
    """
    spec = pd.read_csv(FIXTURE_DIR / 'mcide_target_units.csv')
    assert len(spec) == 25, 'fixture should list all 25 distinct mCIDE targets'
    # row counts must still add up to the published schema sizes
    assert spec.loc[spec.table == 'continuous', 'n_categories'].sum() == 77
    assert spec.loc[spec.table == 'intermittent', 'n_categories'].sum() == 271

    rejected = [u for u in spec['target_unit'] if _normalize(u) not in ALL_ACCEPTABLE_UNITS]
    assert not rejected, f'mCIDE targets not accepted: {rejected}'


# ===========================================
# Grammar invariants
# ===========================================

@pytest.mark.unit_conversion
def test_subclass_spec_regexes_longest_first():
    """Longer tokens must win over shorter ones that prefix them.

    Regex alternation is leftmost-match, so `^(mg|mcg)` matches only `mg`
    against "mcg". Guards a future token added to SUBCLASS_SPEC from silently
    breaking precedence.
    """
    from clifpy.utils.unit_converter import SUBCLASS_REGEX

    for name, spec in SUBCLASS_SPEC.items():
        rx = SUBCLASS_REGEX[name]
        for token in spec['tokens']:
            matched = duckdb.sql(
                f"SELECT regexp_extract('{token}', '{rx}', 1) AS m"
            ).fetchone()[0]
            assert matched == token, (
                f'{name}: {token!r} matched as {matched!r}; alternation is not '
                f'longest-first'
            )


@pytest.mark.unit_conversion
def test_canonical_output_spelling():
    """Explicit requests echo the caller; clifpy's own choice uses mCIDE."""
    out, _ = _convert(
        [('short', 1.0, 'units/min'),
         ('long', 1.0, 'units/min'),
         ('none', 1.0, 'units/min')],
        {'short': 'u/min', 'long': 'units/min'},
    )
    assert out.loc['short', 'med_dose_unit_converted'] == 'u/min'
    assert out.loc['long', 'med_dose_unit_converted'] == 'units/min'
    # no preferred unit -> clifpy picks, so it spells it the mCIDE way
    assert out.loc['none', 'med_dose_unit_converted'] == 'units/min'


# ===========================================
# countable class and placeholders
# ===========================================

@pytest.mark.unit_conversion
@pytest.mark.parametrize('raw', ['tablet', 'Tablet.', 'drop', 'puff', 'dose', 'each'])
def test_countable_class_passthrough(raw):
    """Countable forms are understood, reported as such, and never scaled."""
    out, _ = _convert([('drug', 3.0, raw)], None)
    assert out.loc['drug', '_unit_class'] == 'countable'
    assert out.loc['drug', 'med_dose_converted'] == 3.0
    assert 'countable dosage form' in out.loc['drug', '_convert_status']


@pytest.mark.unit_conversion
def test_mg_kg_dose_is_not_countable():
    """`mg/kg/dose` is a prescribing rate, not a dosage form.

    Guards the choice of exact set membership over a regex: a regex on 'dose'
    would swallow this.
    """
    out, _ = _convert([('drug', 3.0, 'mg/kg/dose')], None)
    assert out.loc['drug', '_unit_class'] == 'unrecognized'
    assert 'not recognized' in out.loc['drug', '_convert_status']


@pytest.mark.unit_conversion
def test_countable_units_override():
    """Callers can extend the countable vocabulary without editing clifpy."""
    default, _ = _convert([('drug', 1.0, 'troche')], None)
    assert default.loc['drug', '_unit_class'] == 'unrecognized'

    extended, _ = _convert([('drug', 1.0, 'troche')], None,
                           countable_units=set(DEFAULT_COUNTABLE_UNITS) | {'troche'})
    assert extended.loc['drug', '_unit_class'] == 'countable'


@pytest.mark.unit_conversion
@pytest.mark.parametrize('raw', ['nan', 'None', '*Unspecified', 'unknown', ''])
def test_placeholder_units_report_as_missing(raw):
    """Placeholders mean "no unit recorded", not "unit we cannot parse"."""
    out, _ = _convert([('drug', 1.0, raw)], None)
    assert out.loc['drug', '_convert_status'] == 'original unit is missing'


@pytest.mark.unit_conversion
@pytest.mark.parametrize('raw', ['asord', 'zzbag', 'XX'])
def test_site_junk_codes_stay_unrecognized(raw):
    """Junk codes are a real data-quality signal; folding them into "missing"
    would hide it."""
    out, _ = _convert([('drug', 1.0, raw)], None)
    assert 'not recognized' in out.loc['drug', '_convert_status']


@pytest.mark.unit_conversion
@pytest.mark.parametrize('raw,expected', [
    ('g (central catheter)', 'g'),
    ('mg (central catheter)', 'mg'),
    ('ml given', 'ml'),
    ('Tablet.', 'tablet'),
])
def test_trailing_noise_is_stripped(raw, expected):
    assert _normalize(raw) == expected


# ===========================================
# Column-name parameters
# ===========================================

@pytest.mark.unit_conversion
def test_custom_column_names_and_collision():
    """Converting volume_infusion_rate must not disturb med_dose.

    A medication_admin_continuous row carries both, so the collision is the
    normal case rather than an edge case.
    """
    df = _frame([('sodium_chloride', 100.0, 'ml/hr')])
    df['volume_infusion_rate'] = [2.0]
    df['volume_infusion_rate_unit'] = ['l/hr']

    out, _ = convert_dose_units_by_med_category(
        df,
        preferred_units={'sodium_chloride': 'ml/hr'},
        override=True,
        dose_col='volume_infusion_rate',
        unit_col='volume_infusion_rate_unit',
        converted_dose_col='volume_infusion_rate_converted',
        converted_unit_col='volume_infusion_rate_unit_converted',
    )
    assert out['volume_infusion_rate_converted'].iloc[0] == pytest.approx(2000.0)
    assert out['volume_infusion_rate_unit_converted'].iloc[0] == 'ml/hr'
    # the parked columns came back intact
    assert out['med_dose'].iloc[0] == 100.0
    assert out['med_dose_unit'].iloc[0] == 'ml/hr'
    assert not any(c.startswith('_saved__') for c in out.columns)


@pytest.mark.unit_conversion
@pytest.mark.parametrize('bad', [
    'x"; DROP TABLE y --', 'a b', '1col', 'col;', "o'brien", '*', 'a-b',
])
def test_column_name_validation_rejects_injection(bad):
    with pytest.raises(ValueError):
        validate_column_name(bad, 'dose_col')


@pytest.mark.unit_conversion
def test_missing_column_raises_with_available_listed():
    with pytest.raises(ValueError) as exc:
        convert_dose_units_by_med_category(
            _frame([('drug', 1.0, 'mg')]),
            preferred_units=None, override=True, dose_col='no_such_column',
        )
    assert 'not found' in str(exc.value)


# ===========================================
# Schema-driven standardization
# ===========================================

@pytest.mark.unit_conversion
def test_load_dose_unit_targets_csv(tmp_path):
    p = tmp_path / 'targets.csv'
    p.write_text('med_category,med_dose_unit\nnorepinephrine,mcg/kg/min\npitocin,milli-units/min\n')
    assert load_dose_unit_targets(p) == {
        'norepinephrine': 'mcg/kg/min', 'pitocin': 'milli-units/min'}


@pytest.mark.unit_conversion
def test_load_dose_unit_targets_drops_blank_and_na(tmp_path):
    p = tmp_path / 'targets.csv'
    p.write_text('med_category,med_dose_unit\na,mg\nb,NA\nc,\nd,mcg\n')
    assert load_dose_unit_targets(p) == {'a': 'mg', 'd': 'mcg'}


@pytest.mark.unit_conversion
def test_duplicate_schema_targets_keep_first_and_warn(tmp_path, captured_warnings):
    """CLIF 3.0 lists epoprostenol twice, as ng/kg/min and mcg/kg/min.

    That is a factor of 1000, so the choice must be deterministic (first wins,
    not row-order dependent) and must be surfaced rather than silently made.
    """
    p = tmp_path / 'targets.csv'
    p.write_text('med_category,med_dose_unit\n'
                 'epoprostenol,ng/kg/min\nepoprostenol,mcg/kg/min\n'
                 'terbutaline,mg\nterbutaline,mcg/kg/min\n')
    targets = load_dose_unit_targets(p)

    assert targets == {'epoprostenol': 'ng/kg/min', 'terbutaline': 'mg'}
    text = '\n'.join(captured_warnings)
    assert 'conflicting target units' in text
    # the warning must name both competing units, not just say "duplicate"
    assert 'epoprostenol' in text and 'ng/kg/min' in text and 'mcg/kg/min' in text
    assert 'terbutaline' in text


@pytest.mark.unit_conversion
def test_register_target_reader(tmp_path):
    """A new schema format is a small function plus one registration."""
    p = tmp_path / 'targets.myfmt'
    p.write_text('norepinephrine=mcg/kg/min\n')

    def _reader(source):
        rows = [ln.split('=', 1) for ln in Path(source).read_text().splitlines() if ln.strip()]
        return pd.DataFrame(rows, columns=['med_category', 'med_dose_unit'])

    register_target_reader('.myfmt', _reader)
    assert load_dose_unit_targets(p) == {'norepinephrine': 'mcg/kg/min'}


@pytest.mark.unit_conversion
def test_unknown_schema_format_raises(tmp_path):
    with pytest.raises(ValueError) as exc:
        load_dose_unit_targets(tmp_path / 'targets.unknownext')
    assert 'No dose-unit target reader' in str(exc.value)


@pytest.mark.unit_conversion
def test_standardize_med_dose_units_end_to_end(tmp_path):
    """Schema in, standardized table out -- including categories it omits."""
    schema = tmp_path / 'targets.csv'
    schema.write_text('med_category,med_dose_unit\n'
                      'norepinephrine,mcg/kg/min\npitocin,milli-units/min\n')
    med = _frame([
        ('norepinephrine', 5.0, 'mcg/min'),
        ('pitocin', 1.0, 'units/min'),
        ('not_in_schema', 7.0, 'mg'),
    ])
    out, counts = standardize_med_dose_units(med, schema)
    out = out.set_index('med_category')

    assert out.loc['norepinephrine', 'med_dose_converted'] == pytest.approx(5.0 / WEIGHT_KG)
    assert out.loc['pitocin', 'med_dose_converted'] == pytest.approx(1000.0)
    assert out.loc['pitocin', 'med_dose_unit_converted'] == 'milli-units/min'
    # a category the schema omits keeps its base unit instead of aborting
    assert out.loc['not_in_schema', '_convert_status'] == 'success'
    assert len(counts) > 0


@pytest.mark.unit_conversion
def test_standardize_accepts_a_dict_schema():
    med = _frame([('norepinephrine', 5.0, 'mcg/min')])
    out, _ = standardize_med_dose_units(med, {'norepinephrine': 'mcg/kg/min'})
    assert out['med_dose_converted'].iloc[0] == pytest.approx(5.0 / WEIGHT_KG)


@pytest.mark.unit_conversion
def test_standardize_rejects_an_empty_schema(tmp_path):
    schema = tmp_path / 'targets.csv'
    schema.write_text('med_category,med_dose_unit\n')
    with pytest.raises(ValueError) as exc:
        standardize_med_dose_units(_frame([('a', 1.0, 'mg')]), schema)
    assert 'no usable targets' in str(exc.value)
