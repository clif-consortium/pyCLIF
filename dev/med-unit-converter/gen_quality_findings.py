"""Generate dev/med-unit-converter/unit_quality_findings.csv.

Emits the per-(category, unit) data-quality findings that the converter
deliberately does NOT convert, with site attribution, so they can be routed
back to the contributing sites.

Run: python .dev/_scratch/gen_quality_findings.py
"""
import csv
import re
from pathlib import Path

INVENTORY = Path('dev/med-unit-converter')
OUT = INVENTORY / 'unit_quality_findings.csv'
FILES = {
    'continuous': 'medication_admin_continuous_dose_by_category_and_unit.csv',
    'intermittent': 'medication_admin_intermittent_dose_by_category_and_unit.csv',
}
SITES = ['Emory', 'JHU', 'MIMIC', 'NU', 'OHSU', 'RUSH', 'Sunnybrook',
         'UCMC', 'UCSF', 'UMN', 'UPenn']

# "<x> of <y>" strings that are dosage forms, not moiety qualifiers.
_FORM_PHRASES = {'piece of gum', 'tablet of each'}

# Words carrying no drug identity, stripped before matching a moiety against
# its med_category.
_MOIETY_STOPWORDS = {'elemental', 'the', 'an', 'a'}


def _is_ingredient_of(moiety: str, category: str) -> bool:
    """Does the named moiety plausibly belong to this med_category?

    Derived from the strings themselves rather than a hand-maintained combo
    list, so a category nobody anticipated is still judged correctly. A
    category like `piperacillin_tazobactam` contains its ingredients' names, so
    substring matching in either direction is a good proxy.
    """
    cat = re.sub(r'[^a-z]+', ' ', category.lower())
    cat_words = set(cat.split())
    for word in re.sub(r'[^a-z]+', ' ', moiety.lower()).split():
        if word in _MOIETY_STOPWORDS or len(word) < 4:
            continue
        if word in cat_words or word in cat.replace(' ', ''):
            return True
        # catch abbreviated category names, e.g. 'trimethoprim' vs 'tmp'
        if any(word.startswith(cw) or cw.startswith(word) for cw in cat_words if len(cw) >= 4):
            return True
    return False


def num(x):
    try:
        return float(x)
    except Exception:
        return 0.0


def classify(category, unit):
    """Return (issue_type, recommendation) or None if not a finding."""
    u = unit.strip()
    low = u.lower()

    if ' of ' in low and low not in _FORM_PHRASES:
        moiety = low.split(' of ', 1)[1].strip().rstrip('.')
        moiety = re.sub(r'\s*\(.*\)$', '', moiety)
        if not _is_ingredient_of(moiety, category):
            return ('suspected_category_mapping_error',
                    f'unit names "{moiety}", which is not an ingredient of '
                    f'"{category}" -- check the source med_category mapping; '
                    f'the dose is likely for a different drug')
        return ('moiety_qualified',
                'dose refers to one ingredient of a combination product; not '
                'comparable with the total-product dose reported elsewhere')

    if re.search(r'/m2\b', low):
        return ('bsa_indexed',
                'body-surface-area indexed; clifpy has no BSA to convert with')

    if low == '%' or re.search(r'^(m?c?g|ng|mg|meq|units?|mcg)/(ml|l)$', low):
        return ('concentration_as_dose_unit',
                'a concentration, not a dose; the administered amount is not '
                'recoverable from this field alone')

    if low in {'asord', 'zzbag', 'xx', 'mgy'} or 'marking on' in low or \
            'admin instructions' in low or low in {'dose level', 'kg patient weight'}:
        return ('unparseable_local_code',
                'site-local code or free text in the unit field')

    if low in {'inch', 'cm', 'm'}:
        return ('length_as_dose_unit',
                'a length (e.g. ointment ribbon); no dose without product strength')

    if 'curie' in low or low in {'mci', 'mci.'}:
        return ('radioactivity_unit',
                'radioactivity, outside the converter\'s unit families')

    return None


def main():
    rows = []
    for table, fn in FILES.items():
        for r in csv.DictReader((INVENTORY / fn).open()):
            cat = r['med_category']
            unit = r['med_dose_unit']
            found = classify(cat, unit)
            if not found:
                continue
            obs = num(r['total_obs__ALL'])
            if obs < 1:
                continue
            sites = [s for s in SITES if num(r.get(f'total_obs__{s}') or 0) > 0]
            rows.append({
                'table': table,
                'med_category': cat,
                'med_dose_unit': unit,
                'issue_type': found[0],
                'observations': int(obs),
                'median_value': r.get('median__ALL', ''),
                'n_sites': len(sites),
                'sites': ';'.join(sites),
                'recommendation': found[1],
            })

    rows.sort(key=lambda d: (d['issue_type'], -d['observations']))
    with OUT.open('w', newline='') as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

    print(f'wrote {OUT} with {len(rows)} findings')
    by_type = {}
    for r in rows:
        t = by_type.setdefault(r['issue_type'], [0, 0])
        t[0] += 1
        t[1] += r['observations']
    print(f"{'issue_type':36s} {'rows':>5s} {'observations':>13s}")
    for t, (n, obs) in sorted(by_type.items(), key=lambda kv: -kv[1][1]):
        print(f'{t:36s} {n:5d} {obs:13,d}')


if __name__ == '__main__':
    main()
