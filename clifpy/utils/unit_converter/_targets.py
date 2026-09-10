"""Reading per-category target dose units from an external schema.

CLIF's mCIDE files define the target unit for each `med_category`. Those files
are **always supplied externally and parsed** -- never vendored into clifpy --
because they change independently of the library and a stale bundled copy would
silently standardise data to the wrong units.

The format is therefore expected to change too. Readers are registered per file
extension, so supporting a new one is a small function plus a
:func:`register_target_reader` call, with nothing else in the package touched:

    >>> def _read_targets_toml(source):          # doctest: +SKIP
    ...     import tomllib
    ...     with open(source, 'rb') as fh:
    ...         data = tomllib.load(fh)
    ...     return pd.DataFrame(data['targets'])
    >>> register_target_reader('.toml', _read_targets_toml)   # doctest: +SKIP

Every reader returns a DataFrame; validation, filtering and dict-building are
done once in :func:`load_dose_unit_targets`, so a reader never repeats them.
"""

import json
from pathlib import Path
from typing import Callable, Dict, Union

import pandas as pd

from clifpy.utils.logging_config import get_logger

logger = get_logger('utils.unit_converter')

TargetReader = Callable[[Union[str, Path]], pd.DataFrame]

# Values that mean "no target defined for this category". The CLIF 3.0
# continuous file uses the literal string 'NA' in volume_infusion_rate_units
# for 5 of its 77 rows.
_NA_TOKENS = {'', 'na', 'n/a', 'nan', 'none', 'null'}


def _read_targets_csv(source) -> pd.DataFrame:
    """Read a CSV schema. Accepts a local path or an http(s) URL."""
    return pd.read_csv(source, dtype=str, keep_default_na=False)


def _read_targets_yaml(source) -> pd.DataFrame:
    """Read a YAML schema.

    Accepts either a list of records or a mapping of category -> unit; the
    mapping form is normalised into two columns named as the caller expects.
    """
    import yaml

    text = _read_text(source)
    data = yaml.safe_load(text)
    return _records_to_frame(data)


def _read_targets_json(source) -> pd.DataFrame:
    """Read a JSON schema (list of records, or a category -> unit mapping)."""
    return _records_to_frame(json.loads(_read_text(source)))


def _read_targets_parquet(source) -> pd.DataFrame:
    """Read a Parquet schema."""
    return pd.read_parquet(source)


def _read_text(source) -> str:
    """Read a local path or an http(s) URL as text."""
    s = str(source)
    if s.startswith(('http://', 'https://')):
        from urllib.request import urlopen

        with urlopen(s) as fh:  # noqa: S310 - caller-supplied schema location
            return fh.read().decode('utf-8')
    return Path(s).read_text()


def _records_to_frame(data) -> pd.DataFrame:
    """Normalise a parsed document into a DataFrame.

    A mapping of category -> unit is the natural hand-written shape, so it is
    accepted alongside the list-of-records shape a table export produces.
    """
    if isinstance(data, dict):
        # Allow a nested {'<key>': {...}} wrapper around the mapping.
        if len(data) == 1:
            (only,) = data.values()
            if isinstance(only, (dict, list)):
                data = only
    if isinstance(data, dict):
        return pd.DataFrame(
            {'med_category': list(data.keys()),
             'med_dose_unit': [str(v) for v in data.values()]}
        )
    return pd.DataFrame(data)


_TARGET_READERS: Dict[str, TargetReader] = {
    '.csv': _read_targets_csv,
    '.yaml': _read_targets_yaml,
    '.yml': _read_targets_yaml,
    '.json': _read_targets_json,
    '.parquet': _read_targets_parquet,
}


def register_target_reader(suffix: str, reader: TargetReader) -> None:
    """Register a reader for a schema file extension.

    Parameters
    ----------
    suffix : str
        File extension including the leading dot, e.g. `'.toml'`. Matched
        case-insensitively.
    reader : TargetReader
        Callable taking the source path/URL and returning a DataFrame that
        contains the category and unit columns.

    Notes
    -----
    Replaces any existing reader for that suffix, so the built-ins can be
    overridden.
    """
    _TARGET_READERS[suffix.lower()] = reader
    logger.debug("Registered dose-unit target reader for %s", suffix)


def available_target_formats() -> list:
    """List the schema file extensions currently registered."""
    return sorted(_TARGET_READERS)


def load_dose_unit_targets(
    source: Union[str, Path],
    *,
    reader: TargetReader = None,
    category_col: str = 'med_category',
    unit_col: str = 'med_dose_unit',
) -> Dict[str, str]:
    """Parse an external schema into ``{med_category: target_unit}``.

    Parameters
    ----------
    source : str or Path
        Local path or http(s) URL of the schema file. The canonical CLIF 3.0
        files live under `mCIDE/medication_admin_{continuous,intermittent}/`
        in the Common-Longitudinal-ICU-data-Format/CLIF repository.
    reader : callable, optional
        Override the reader. When omitted, one is chosen by file extension.
    category_col : str, default 'med_category'
        Column holding the medication category.
    unit_col : str, default 'med_dose_unit'
        Column holding the target unit. Pass e.g.
        `'volume_infusion_rate_units'` to read the volume targets from the
        same file.

    Returns
    -------
    dict
        Mapping of category to target unit, suitable as `preferred_units`.
        Rows whose target is blank or `NA` are dropped.

    Raises
    ------
    ValueError
        If no reader is registered for the extension, or a requested column is
        absent from the parsed schema.

    Examples
    --------
    >>> targets = load_dose_unit_targets(              # doctest: +SKIP
    ...     'mCIDE/medication_admin_continuous/'
    ...     'clif_medication_admin_continuous_med_categories.csv')
    >>> targets['norepinephrine']                      # doctest: +SKIP
    'mcg/kg/min'
    """
    if reader is None:
        suffix = Path(str(source)).suffix.lower()
        reader = _TARGET_READERS.get(suffix)
        if reader is None:
            raise ValueError(
                f"No dose-unit target reader registered for {suffix!r}. "
                f"Registered: {available_target_formats()}. "
                f"Pass reader= explicitly, or call register_target_reader()."
            )

    df = reader(source)
    if not isinstance(df, pd.DataFrame):
        raise ValueError(
            f"target reader must return a pandas DataFrame; got {type(df).__name__}"
        )

    missing = {category_col, unit_col} - set(df.columns)
    if missing:
        raise ValueError(
            f"schema is missing column(s) {sorted(missing)}. "
            f"Found: {sorted(df.columns)}"
        )

    targets = {}
    conflicts = {}
    dropped = 0
    for cat, unit in zip(df[category_col], df[unit_col]):
        cat = str(cat).strip()
        unit = str(unit).strip()
        if not cat or cat.lower() in _NA_TOKENS or unit.lower() in _NA_TOKENS:
            dropped += 1
            continue
        if cat in targets:
            if targets[cat] != unit:
                # Keep the FIRST occurrence, so the result does not depend on
                # row order, and surface the conflict rather than resolving it
                # silently.
                conflicts.setdefault(cat, [targets[cat]]).append(unit)
            continue
        targets[cat] = unit

    if conflicts:
        # NOTE: this is a clifpy limitation, not a schema defect. CLIF 3.0
        # keys its targets on (med_category, med_group), and the same drug is
        # legitimately dosed differently by route -- epoprostenol is ng/kg/min
        # as an IV infusion and mcg/kg/min inhaled, a factor of 1000;
        # terbutaline is mg inhaled and mcg/kg/min otherwise, not even the same
        # unit class. clifpy currently keys on med_category alone, so it can
        # only carry one target per category.
        #
        # Until composite-key support lands, keep the FIRST occurrence, so the
        # result never depends on row order, and say plainly which rows were
        # collapsed. See docs/user-guide/med-dose-unit-data-quality.md.
        detail = '; '.join(
            f"{cat}: {' vs '.join(units)} (using {targets[cat]})"
            for cat, units in sorted(conflicts.items())
        )
        logger.warning(
            "Schema %s defines more than one target unit for %d med_category "
            "value(s), most likely distinguished by med_group (route). clifpy "
            "keys on med_category alone and is keeping the first occurrence of "
            "each. If the other applies to your cohort, pass an explicit dict "
            "to override: %s",
            source, len(conflicts), detail,
        )

    logger.info(
        "Loaded %d dose-unit targets from %s (%d row(s) dropped as blank/NA, "
        "%d conflicting duplicate(s))",
        len(targets), source, dropped, len(conflicts),
    )
    return targets
