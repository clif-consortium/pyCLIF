"""Unit converter for standardizing medication dose units.

This package provides utilities for converting medication dose units between
different formats and standardizing them to a common base set. It handles
weight-based dosing, time unit conversions, and various unit name variants.

In general, convert both rate and amount indiscriminately and report them
as well as unrecognized units.

The implementation is split by concern -- `_grammar` (unit vocabulary),
`_clean` (string normalisation), `_sql` (CASE builders), `_base` (stage 1),
`_weight` (patient-weight lookup), `_preferred` (stage 2), `_counts` (summary
table) and `_convert` (orchestration) -- but the public surface is re-exported
here, so `from clifpy.utils.unit_converter import ...` is unchanged.
"""

from ._grammar import (
    KG_PER_LB,
    UNIT_NAMING_VARIANTS,
    CANONICAL_UNIT_SPELLING,
    DEFAULT_COUNTABLE_UNITS,
    MISSING_UNIT_PLACEHOLDERS,
    STANDALONE_AMOUNT_UNITS,
    SUBCLASS_SPEC,
    SUBCLASS_REGEX,
    STANDALONE_SUBCLASSES,
    TOKEN_TO_BASE_FACTOR,
    TIME_TO_BASE_FACTOR,
    REGEX_TO_FACTOR_MAPPER,
    AMOUNT_ENDER,
    MASS_REGEX,
    VOLUME_REGEX,
    UNIT_REGEX,
    HR_REGEX,
    MU_REGEX,
    MG_REGEX,
    NG_REGEX,
    G_REGEX,
    L_REGEX,
    LB_REGEX,
    KG_REGEX,
    WEIGHT_REGEX,
    AMOUNT_FACTOR_PATTERNS,
    TIME_FACTOR_PATTERNS,
    ACCEPTABLE_WEIGHT_UNITS,
    ACCEPTABLE_TIME_UNITS,
    ACCEPTABLE_BASE_AMOUNT_UNITS,
    ACCEPTABLE_AMOUNT_UNITS,
    ACCEPTABLE_RATE_UNITS,
    ALL_ACCEPTABLE_UNITS,
    RATE_UNITS_STR,
    AMOUNT_UNITS_STR,
    _tokens_alternation,
    _token_regex,
    _weight_qual_clause,
    _acceptable_amount_units,
    _acceptable_rate_units,
    _convert_set_to_str_for_sql,
)
from ._clean import (
    _clean_dose_unit_formats,
    _clean_dose_unit_formats_duckdb,
    _clean_dose_unit_names,
    _clean_dose_unit_names_duckdb,
)
from ._sql import (
    _concat_builders_by_patterns,
    _pattern_to_factor_builder_for_base,
    _pattern_to_factor_builder_for_preferred,
)
from ._counts import _create_unit_conversion_counts_table
from ._columns import (
    validate_column_name,
    build_rename_map,
    rename_to_internal,
    rename_from_internal,
)
from ._weight import find_most_recent_weight
from ._base import (
    _convert_clean_units_to_base_units,
    standardize_dose_to_base_units,
)
from ._preferred import _convert_base_units_to_preferred_units
from ._convert import (
    convert_dose_units_by_med_category,
    standardize_med_dose_units,
)
from ._targets import (
    load_dose_unit_targets,
    register_target_reader,
    available_target_formats,
    TargetReader,
)

__all__ = [
    "convert_dose_units_by_med_category",
    "standardize_med_dose_units",
    "load_dose_unit_targets",
    "register_target_reader",
    "available_target_formats",
    "standardize_dose_to_base_units",
    "find_most_recent_weight",
    "ALL_ACCEPTABLE_UNITS",
    "ACCEPTABLE_AMOUNT_UNITS",
    "ACCEPTABLE_RATE_UNITS",
    "ACCEPTABLE_BASE_AMOUNT_UNITS",
    "SUBCLASS_SPEC",
    "UNIT_NAMING_VARIANTS",
    "REGEX_TO_FACTOR_MAPPER",
    "KG_PER_LB",
]
