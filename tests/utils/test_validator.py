"""Tests for the DQA validator module (clifpy.utils.validator).

Covers conformance checks, completeness checks, relational integrity,
orchestration runners, and the CLIF-TableOne compatibility layer.
Both Polars and DuckDB code paths are exercised.
"""

import pytest
import pandas as pd
import polars as pl
import os
import logging
from pathlib import Path

from clifpy.utils.validator import (
    # Schema loader
    _load_schema,
    _load_validation_rules,
    _get_default_conditions,
    # Result containers
    DQAConformanceResult,
    DQACompletenessResult,
    DQAPlausibilityResult,
    # Conformance checks
    check_table_exists,
    check_table_presence,
    check_table_presence_polars,
    check_required_columns,
    check_required_columns_polars,
    check_column_dtypes,
    check_column_dtypes_polars,
    check_datetime_format,
    check_datetime_format_polars,
    check_lab_reference_units,
    check_lab_reference_units_polars,
    check_categorical_values,
    check_categorical_values_polars,
    check_category_group_mapping,
    check_category_group_mapping_polars,
    check_medication_dose_units,
    check_medication_dose_units_polars,
    # Completeness checks
    check_missingness,
    check_missingness_polars,
    check_conditional_requirements,
    check_conditional_requirements_polars,
    check_mcide_value_coverage,
    check_mcide_value_coverage_polars,
    # Relational integrity
    check_relational_integrity,
    check_relational_integrity_polars,
    # Plausibility checks
    check_chronological_order,
    check_chronological_order_polars,
    check_numeric_range_plausibility,
    check_numeric_range_plausibility_polars,
    check_field_plausibility,
    check_field_plausibility_polars,
    check_medication_dose_unit_consistency,
    check_medication_dose_unit_consistency_polars,
    check_cross_table_temporal_plausibility,
    check_cross_table_temporal_plausibility_polars,
    check_overlapping_periods,
    check_overlapping_periods_polars,
    check_category_temporal_consistency,
    check_category_temporal_consistency_polars,
    check_duplicate_composite_keys,
    check_duplicate_composite_keys_polars,
    # Orchestration
    run_conformance_checks,
    run_completeness_checks,
    run_relational_integrity_checks,
    run_plausibility_checks,
    run_cross_table_plausibility_checks,
    run_full_dqa,
    # Compatibility layer
    validate_dataframe,
    format_clifpy_error,
    determine_validation_status,
    classify_errors_by_status_impact,
    get_validation_summary,
)


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def patient_schema():
    """Minimal patient-like schema for testing."""
    return {
        "table_name": "patient",
        "required_columns": ["patient_id", "sex_category"],
        "columns": [
            {"name": "patient_id", "data_type": "VARCHAR", "required": True},
            {"name": "sex_category", "data_type": "VARCHAR", "required": True,
             "is_category_column": True,
             "permissible_values": ["Male", "Female", "Unknown"]},
            {"name": "age", "data_type": "INTEGER", "required": False},
        ],
        "category_columns": ["sex_category"],
    }


@pytest.fixture
def labs_schema():
    """Minimal labs-like schema with lab_reference_units."""
    return {
        "table_name": "labs",
        "required_columns": ["hospitalization_id", "lab_category", "reference_unit", "lab_value"],
        "columns": [
            {"name": "hospitalization_id", "data_type": "VARCHAR", "required": True},
            {"name": "lab_category", "data_type": "VARCHAR", "required": True,
             "is_category_column": True,
             "permissible_values": ["albumin", "creatinine"]},
            {"name": "reference_unit", "data_type": "VARCHAR", "required": True},
            {"name": "lab_value", "data_type": "VARCHAR", "required": True},
        ],
        "category_columns": ["lab_category"],
        "lab_reference_units": {
            "albumin": ["g/dL"],
            "creatinine": ["mg/dL"],
        },
    }


@pytest.fixture
def labs_schema_canonical():
    """Labs schema in the canonical-key + allowed_unit_variants format."""
    return {
        "table_name": "labs",
        "required_columns": ["hospitalization_id", "lab_category", "reference_unit", "lab_value"],
        "columns": [
            {"name": "hospitalization_id", "data_type": "VARCHAR", "required": True},
            {"name": "lab_category", "data_type": "VARCHAR", "required": True,
             "is_category_column": True,
             "permissible_values": ["albumin", "creatinine", "inr"]},
            {"name": "reference_unit", "data_type": "VARCHAR", "required": True},
            {"name": "lab_value", "data_type": "VARCHAR", "required": True},
        ],
        "category_columns": ["lab_category"],
        "lab_reference_units": {
            "albumin": "g/dl",
            "creatinine": "mg/dl",
            "inr": "(no units)",
        },
        "allowed_unit_variants": {
            "g/dl": ["g/dl", "g per dl", "gram/dl", "gm/dl"],
            "mg/dl": ["mg/dl", "mg per dl", "mg.dl"],
            "(no units)": ["(no units)", "no units", "none", "unitless", "ratio"],
        },
    }


@pytest.fixture
def adt_schema():
    """Minimal ADT-like schema for conditional-requirement tests."""
    return {
        "table_name": "adt",
        "required_columns": ["hospitalization_id", "location_category"],
        "columns": [
            {"name": "hospitalization_id", "data_type": "VARCHAR", "required": True},
            {"name": "location_category", "data_type": "VARCHAR", "required": True,
             "is_category_column": True,
             "permissible_values": ["ed", "ward", "icu"]},
            {"name": "location_type", "data_type": "VARCHAR", "required": False},
        ],
        "category_columns": ["location_category"],
    }


# ---------------------------------------------------------------------------
# 1. Result containers
# ---------------------------------------------------------------------------

class TestDQAConformanceResult:

    def test_initial_state(self):
        r = DQAConformanceResult("test_check", "my_table")
        assert r.check_type == "test_check"
        assert r.table_name == "my_table"
        assert r.passed is True
        assert r.errors == []
        assert r.warnings == []
        assert r.info == []
        assert r.metrics == {}

    def test_add_error_sets_passed_false(self):
        r = DQAConformanceResult("chk", "tbl")
        r.add_error("something wrong", {"col": "x"})
        assert r.passed is False
        assert len(r.errors) == 1
        assert r.errors[0]["message"] == "something wrong"

    def test_add_warning_keeps_passed_true(self):
        r = DQAConformanceResult("chk", "tbl")
        r.add_warning("heads up")
        assert r.passed is True
        assert len(r.warnings) == 1

    def test_to_dict(self):
        r = DQAConformanceResult("chk", "tbl")
        r.add_info("note")
        d = r.to_dict()
        assert d["check_type"] == "chk"
        assert d["passed"] is True
        assert len(d["info"]) == 1


class TestDQACompletenessResult:

    def test_initial_state(self):
        r = DQACompletenessResult("miss", "tbl")
        assert r.passed is True

    def test_add_error(self):
        r = DQACompletenessResult("miss", "tbl")
        r.add_error("bad")
        assert r.passed is False

    def test_to_dict(self):
        r = DQACompletenessResult("miss", "tbl")
        r.metrics["total_rows"] = 100
        d = r.to_dict()
        assert d["metrics"]["total_rows"] == 100


# ---------------------------------------------------------------------------
# 3. _to_pandas helper
# ---------------------------------------------------------------------------

@pytest.mark.skip(reason="_to_pandas helper was removed from validator module")
class TestToPandas:

    def test_passthrough_pandas(self):
        pass

    def test_from_polars_dataframe(self):
        pass

    def test_from_polars_lazyframe(self):
        pass

    def test_unsupported_type_raises(self):
        pass


# ---------------------------------------------------------------------------
# 4. _load_schema
# ---------------------------------------------------------------------------

class TestLoadSchema:

    def test_load_existing_schema(self):
        schema = _load_schema("patient")
        assert schema is not None
        assert schema["table_name"] == "patient"
        assert "columns" in schema

    def test_load_nonexistent_returns_none(self):
        assert _load_schema("nonexistent_table_xyz") is None

    def test_load_with_custom_dir(self, tmp_path):
        import yaml
        schema_file = tmp_path / "demo_schema.yaml"
        schema_file.write_text(yaml.dump({"table_name": "demo", "columns": []}))
        loaded = _load_schema("demo", str(tmp_path))
        assert loaded["table_name"] == "demo"


# ---------------------------------------------------------------------------
# 5. check_table_exists
# ---------------------------------------------------------------------------

class TestCheckTableExists:

    def test_existing_file(self, tmp_path):
        (tmp_path / "patient.parquet").write_bytes(b"fake")
        result = check_table_exists(str(tmp_path), "patient")
        assert result.passed is True
        assert result.metrics["file_path"] == str(tmp_path / "patient.parquet")

    def test_missing_file(self, tmp_path):
        result = check_table_exists(str(tmp_path), "patient")
        assert result.passed is False
        assert len(result.errors) == 1

    def test_custom_filetype(self, tmp_path):
        (tmp_path / "labs.csv").write_text("a,b\n1,2\n")
        result = check_table_exists(str(tmp_path), "labs", filetype="csv")
        assert result.passed is True


# ---------------------------------------------------------------------------
# 6. check_required_columns  (Polars + DuckDB)
# ---------------------------------------------------------------------------

class TestCheckRequiredColumns:

    def test_all_present_polars(self, patient_schema):
        lf = pl.LazyFrame({"patient_id": ["p1"], "sex_category": ["Male"], "age": [30]})
        result = check_required_columns_polars(lf, patient_schema, "patient")
        assert result.passed is True
        assert result.metrics["total_missing"] == 0

    def test_missing_column_polars(self, patient_schema):
        lf = pl.LazyFrame({"patient_id": ["p1"]})
        result = check_required_columns_polars(lf, patient_schema, "patient")
        assert result.passed is False
        assert result.metrics["total_missing"] == 1

    def test_dispatcher_returns_result(self, patient_schema):
        lf = pl.LazyFrame({"patient_id": ["p1"], "sex_category": ["Male"]})
        result = check_required_columns(lf, patient_schema, "patient")
        assert isinstance(result, DQAConformanceResult)


# ---------------------------------------------------------------------------
# 7. check_column_dtypes  (Polars + DuckDB)
# ---------------------------------------------------------------------------

class TestCheckColumnDtypes:

    def test_correct_types_polars(self, patient_schema):
        lf = pl.LazyFrame({
            "patient_id": ["p1", "p2"],
            "sex_category": ["Male", "Female"],
            "age": [30, 40],
        })
        result = check_column_dtypes_polars(lf, patient_schema, "patient")
        assert result.passed is True

    def test_wrong_type_polars(self):
        schema = {
            "columns": [
                {"name": "val", "data_type": "INTEGER"},
            ]
        }
        lf = pl.LazyFrame({"val": ["not", "an", "int"]})
        result = check_column_dtypes_polars(lf, schema, "test")
        # Should either error or warn about the mismatch
        assert len(result.errors) > 0 or len(result.warnings) > 0

    def test_dispatcher(self, patient_schema):
        lf = pl.LazyFrame({"patient_id": ["p1"], "sex_category": ["Male"], "age": [1]})
        result = check_column_dtypes(lf, patient_schema, "patient")
        assert isinstance(result, DQAConformanceResult)


# ---------------------------------------------------------------------------
# 8. check_datetime_format  (Polars + DuckDB)
# ---------------------------------------------------------------------------

class TestCheckDatetimeFormat:

    def _make_schema(self):
        return {
            "columns": [
                {"name": "ts", "data_type": "DATETIME"},
                {"name": "name", "data_type": "VARCHAR"},
            ]
        }

    def test_proper_datetime_polars(self):
        from datetime import datetime
        lf = pl.LazyFrame({"ts": [datetime(2024, 1, 1)], "name": ["a"]})
        result = check_datetime_format_polars(lf, self._make_schema(), "test")
        assert result.passed is True

    def test_non_datetime_warns_polars(self):
        lf = pl.LazyFrame({"ts": ["2024-01-01"], "name": ["a"]})
        result = check_datetime_format_polars(lf, self._make_schema(), "test")
        # String column in place of datetime should produce a warning
        assert len(result.warnings) > 0

    def test_dispatcher(self):
        from datetime import datetime
        lf = pl.LazyFrame({"ts": [datetime(2024, 1, 1)], "name": ["a"]})
        result = check_datetime_format(lf, self._make_schema(), "test")
        assert isinstance(result, DQAConformanceResult)


# ---------------------------------------------------------------------------
# 9. check_lab_reference_units  (Polars + DuckDB)
# ---------------------------------------------------------------------------

@pytest.fixture
def clifpy_caplog(caplog):
    """caplog that still sees clifpy records after setup_logging() has run.

    Constructing any table calls setup_logging(), which sets
    logging.getLogger('clifpy').propagate = False. caplog's handler sits on the
    root logger, so once an earlier test in the session has built a table these
    records stop arriving. Attach directly to the clifpy logger instead.
    """
    clifpy_logger = logging.getLogger('clifpy')
    previous = clifpy_logger.propagate
    clifpy_logger.addHandler(caplog.handler)
    clifpy_logger.propagate = True
    try:
        yield caplog
    finally:
        clifpy_logger.removeHandler(caplog.handler)
        clifpy_logger.propagate = previous


class TestCheckLabReferenceUnits:

    def test_valid_units_polars(self, labs_schema):
        lf = pl.LazyFrame({
            "hospitalization_id": ["h1", "h2"],
            "lab_category": ["albumin", "creatinine"],
            "reference_unit": ["g/dL", "mg/dL"],
            "lab_value": ["3.5", "1.1"],
        })
        result = check_lab_reference_units_polars(lf, labs_schema, "labs")
        assert result.passed is True
        assert result.metrics["invalid_unit_categories"] == 0

    def test_invalid_units_polars(self, labs_schema):
        lf = pl.LazyFrame({
            "hospitalization_id": ["h1"],
            "lab_category": ["albumin"],
            "reference_unit": ["mg/L"],  # wrong unit for albumin
            "lab_value": ["3.5"],
        })
        result = check_lab_reference_units_polars(lf, labs_schema, "labs")
        assert len(result.warnings) > 0

    def test_no_lab_units_in_schema(self):
        schema = {"lab_reference_units": {}}
        lf = pl.LazyFrame({"lab_category": ["x"], "reference_unit": ["y"]})
        result = check_lab_reference_units_polars(lf, schema, "labs")
        assert result.passed is True

    def test_missing_columns_polars(self, labs_schema):
        lf = pl.LazyFrame({"hospitalization_id": ["h1"]})
        result = check_lab_reference_units_polars(lf, labs_schema, "labs")
        assert result.passed is False

    def test_dispatcher(self, labs_schema):
        lf = pl.LazyFrame({
            "hospitalization_id": ["h1"],
            "lab_category": ["albumin"],
            "reference_unit": ["g/dL"],
            "lab_value": ["3.5"],
        })
        result = check_lab_reference_units(lf, labs_schema, "labs")
        assert isinstance(result, DQAConformanceResult)

    # --- canonical-key + allowed_unit_variants format ---

    def test_canonical_exact_match_polars(self, labs_schema_canonical):
        lf = pl.LazyFrame({
            "hospitalization_id": ["h1", "h2"],
            "lab_category": ["albumin", "creatinine"],
            "reference_unit": ["g/dL", "mg/dL"],
            "lab_value": ["3.5", "1.1"],
        })
        result = check_lab_reference_units_polars(lf, labs_schema_canonical, "labs")
        assert result.metrics["invalid_unit_categories"] == 0, result.warnings

    def test_canonical_variant_match_polars(self, labs_schema_canonical):
        # Data carries non-canonical but schema-accepted spellings.
        lf = pl.LazyFrame({
            "hospitalization_id": ["h1", "h2", "h3"],
            "lab_category": ["albumin", "albumin", "creatinine"],
            "reference_unit": ["g per dL", "GRAM/DL", "mg.dl"],
            "lab_value": ["3.5", "3.6", "1.1"],
        })
        result = check_lab_reference_units_polars(lf, labs_schema_canonical, "labs")
        assert result.metrics["invalid_unit_categories"] == 0, result.warnings

    def test_canonical_invalid_variant_polars(self, labs_schema_canonical):
        lf = pl.LazyFrame({
            "hospitalization_id": ["h1"],
            "lab_category": ["albumin"],
            "reference_unit": ["mg/L"],  # not in albumin's variant set
            "lab_value": ["3.5"],
        })
        result = check_lab_reference_units_polars(lf, labs_schema_canonical, "labs")
        assert len(result.warnings) > 0
        assert result.metrics["invalid_unit_categories"] == 1

    def test_canonical_no_units_sentinel_polars(self, labs_schema_canonical):
        # '(no units)' canonical accepts empty reference_unit values.
        lf = pl.LazyFrame({
            "hospitalization_id": ["h1", "h2"],
            "lab_category": ["inr", "inr"],
            "reference_unit": ["", "ratio"],
            "lab_value": ["1.0", "1.1"],
        })
        result = check_lab_reference_units_polars(lf, labs_schema_canonical, "labs")
        assert result.metrics["invalid_unit_categories"] == 0, result.warnings

    def test_canonical_variant_logs_and_details_polars(self, labs_schema_canonical, clifpy_caplog):
        """Schema-summary INFO log + per-category details capture variant-lookup state."""
        lf = pl.LazyFrame({
            "hospitalization_id": ["h1", "h2", "h3", "h4"],
            "lab_category": ["albumin", "albumin", "creatinine", "albumin"],
            "reference_unit": ["g/dL", "g per dL", "mg.dl", "mg/L"],  # 1 canonical, 2 via variant, 1 bad
            "lab_value": ["3.5", "3.6", "1.1", "99"],
        })
        with clifpy_caplog.at_level(logging.INFO, logger="clifpy.utils.validator"):
            result = check_lab_reference_units_polars(lf, labs_schema_canonical, "labs")

        # Summary INFO line fires and mentions the variants map.
        summary_msgs = [r.getMessage() for r in clifpy_caplog.records if "allowed_unit_variants present" in r.getMessage()]
        assert summary_msgs, "expected INFO summary about allowed_unit_variants"

        # albumin entry: mixed canonical + variant + invalid.
        albumin_details = next(
            (w['details'] for w in result.warnings if w['details'].get('column') == 'albumin'),
            None,
        )
        assert albumin_details is not None, "albumin should be flagged (one invalid unit)"
        assert albumin_details['canonical_unit'] == 'g/dl'
        assert albumin_details['variant_lookup_used'] is True
        assert albumin_details['accepted_variant_count'] >= 4
        assert 'g/dl' in albumin_details['accepted_variants_sample']
        assert albumin_details['matched_canonical_records'] == 1
        assert albumin_details['matched_via_variant_records'] == 1
        assert albumin_details['invalid_records'] == 1

    def test_summary_log_when_no_variants_map(self, labs_schema, clifpy_caplog):
        """INFO log indicates when the schema has no allowed_unit_variants map."""
        lf = pl.LazyFrame({
            "hospitalization_id": ["h1"],
            "lab_category": ["albumin"],
            "reference_unit": ["g/dL"],
            "lab_value": ["3.5"],
        })
        with clifpy_caplog.at_level(logging.INFO, logger="clifpy.utils.validator"):
            check_lab_reference_units_polars(lf, labs_schema, "labs")
        summary_msgs = [r.getMessage() for r in clifpy_caplog.records if "allowed_unit_variants NOT present" in r.getMessage()]
        assert summary_msgs, "expected INFO summary noting absence of variants map"

    def test_canonical_fallback_without_variants_map_polars(self):
        # Canonical key is a string but schema has no allowed_unit_variants;
        # accepted set should collapse to the canonical key itself.
        schema = {
            "lab_reference_units": {"albumin": "g/dl"},
        }
        lf = pl.LazyFrame({
            "hospitalization_id": ["h1", "h2"],
            "lab_category": ["albumin", "albumin"],
            "reference_unit": ["g/dL", "g per dL"],
            "lab_value": ["3.5", "3.6"],
        })
        result = check_lab_reference_units_polars(lf, schema, "labs")
        # 'g/dL' matches canonical after lowercasing; 'g per dL' does not.
        assert result.metrics["invalid_unit_categories"] == 1


# ---------------------------------------------------------------------------
# 10. check_categorical_values  (Polars + DuckDB)
# ---------------------------------------------------------------------------

class TestCheckCategoricalValues:

    def test_all_valid_polars(self, patient_schema):
        lf = pl.LazyFrame({"patient_id": ["p1"], "sex_category": ["Male"], "age": [25]})
        result = check_categorical_values_polars(lf, patient_schema, "patient")
        assert result.passed is True

    def test_invalid_value_polars(self, patient_schema):
        lf = pl.LazyFrame({"patient_id": ["p1"], "sex_category": ["Alien"], "age": [25]})
        result = check_categorical_values_polars(lf, patient_schema, "patient")
        # mCIDE non-conformance is reported as a warning (non-blocking),
        # so `passed` stays True and the finding lives in `warnings`.
        assert len(result.warnings) > 0
        assert result.warnings[0]['details']['column'] == 'sex_category'

    def test_dispatcher(self, patient_schema):
        lf = pl.LazyFrame({"patient_id": ["p1"], "sex_category": ["Male"], "age": [25]})
        result = check_categorical_values(lf, patient_schema, "patient")
        assert isinstance(result, DQAConformanceResult)


# ---------------------------------------------------------------------------
# 10b. check_category_group_mapping  (Polars + DuckDB)
# ---------------------------------------------------------------------------

class TestCheckCategoryGroupMapping:

    @pytest.fixture
    def mapping_schema(self):
        """Minimal schema with a category-to-group mapping."""
        return {
            "table_name": "test_table",
            "test_category_to_group_mapping": {
                "albumin": "fluids_electrolytes",
                "amiodarone": "cardiac",
                "argatroban": "anticoagulation",
            },
            "columns": [],
            "required_columns": [],
        }

    def test_valid_mapping_polars(self, mapping_schema):
        lf = pl.LazyFrame({
            "test_category": ["albumin", "amiodarone", "argatroban"],
            "test_group": ["fluids_electrolytes", "cardiac", "anticoagulation"],
        })
        result = check_category_group_mapping_polars(lf, mapping_schema, "test_table")
        assert result.passed is True
        assert len(result.warnings) == 0
        assert result.metrics["test_category_to_group_mapping_mismatch_count"] == 0

    def test_mismatched_mapping_polars(self, mapping_schema):
        lf = pl.LazyFrame({
            "test_category": ["albumin", "amiodarone"],
            "test_group": ["fluids_electrolytes", "WRONG_GROUP"],
        })
        result = check_category_group_mapping_polars(lf, mapping_schema, "test_table")
        assert result.passed is True  # warnings only, not errors
        assert len(result.warnings) > 0
        assert result.metrics["test_category_to_group_mapping_mismatch_count"] == 1

    def test_no_mapping_in_schema_polars(self):
        schema = {"table_name": "patient", "columns": [], "required_columns": []}
        lf = pl.LazyFrame({"patient_id": ["p1"]})
        result = check_category_group_mapping_polars(lf, schema, "patient")
        assert result.passed is True
        assert len(result.info) > 0

    def test_dispatcher(self, mapping_schema):
        lf = pl.LazyFrame({
            "test_category": ["albumin"],
            "test_group": ["fluids_electrolytes"],
        })
        result = check_category_group_mapping(lf, mapping_schema, "test_table")
        assert isinstance(result, DQAConformanceResult)

    def test_run_conformance_includes_key(self):
        schema = _load_schema("patient")
        lf = pl.LazyFrame({
            "patient_id": ["p1"],
            "birth_date": ["2000-01-01"],
            "death_dttm": ["2024-01-01T00:00:00"],
            "race_category": ["White"],
            "ethnicity_category": ["Non-Hispanic"],
            "sex_category": ["Male"],
            "language_category": ["English"],
        })
        results = run_conformance_checks(lf, schema, "patient")
        assert "category_group_mapping" in results


# ---------------------------------------------------------------------------
# 11. check_missingness  (Polars + DuckDB)
# ---------------------------------------------------------------------------

class TestCheckMissingness:

    def test_no_nulls_polars(self, patient_schema):
        lf = pl.LazyFrame({"patient_id": ["p1", "p2"], "sex_category": ["Male", "Female"]})
        result = check_missingness_polars(lf, patient_schema, "patient")
        assert result.passed is True

    def test_high_missingness_polars(self, patient_schema):
        lf = pl.LazyFrame({
            "patient_id": ["p1", None, None, None],
            "sex_category": ["Male", None, None, None],
        })
        result = check_missingness_polars(lf, patient_schema, "patient",
                                          error_threshold=50.0, warning_threshold=10.0)
        assert result.passed is False
        assert result.metrics["total_rows"] == 4

    def test_empty_dataframe_polars(self, patient_schema):
        lf = pl.LazyFrame({"patient_id": [], "sex_category": []},
                          schema={"patient_id": pl.Utf8, "sex_category": pl.Utf8})
        result = check_missingness_polars(lf, patient_schema, "patient")
        assert result.passed is False  # empty DF is an error

    def test_warning_threshold(self, patient_schema):
        # 1 out of 5 null = 20% — above warning (10%) but below error (50%)
        lf = pl.LazyFrame({
            "patient_id": ["p1", "p2", "p3", "p4", None],
            "sex_category": ["Male", "Female", "Male", "Female", "Male"],
        })
        result = check_missingness_polars(lf, patient_schema, "patient",
                                          error_threshold=50.0, warning_threshold=10.0)
        assert result.passed is True  # only warnings, no errors
        assert len(result.warnings) > 0

    def test_dispatcher(self, patient_schema):
        lf = pl.LazyFrame({"patient_id": ["p1"], "sex_category": ["Male"]})
        result = check_missingness(lf, patient_schema, "patient")
        assert isinstance(result, DQACompletenessResult)


# ---------------------------------------------------------------------------
# 12. check_conditional_requirements  (Polars + DuckDB)
# ---------------------------------------------------------------------------

class TestCheckConditionalRequirements:

    def _conditions(self):
        return [
            {
                "when_column": "location_category",
                "when_value": ["icu"],
                "then_required": ["location_type"],
                "description": "ICU locations must have location_type",
            }
        ]

    def test_satisfied_polars(self):
        lf = pl.LazyFrame({
            "location_category": ["icu", "ward"],
            "location_type": ["general_icu", None],
        })
        result = check_conditional_requirements_polars(lf, "adt", self._conditions())
        assert result.passed is True
        assert len(result.warnings) == 0

    def test_violated_polars(self):
        lf = pl.LazyFrame({
            "location_category": ["icu", "icu"],
            "location_type": [None, None],
        })
        result = check_conditional_requirements_polars(lf, "adt", self._conditions())
        assert len(result.warnings) > 0

    def test_default_conditions_for_adt(self):
        """Passing conditions=None for adt should use built-in defaults."""
        lf = pl.LazyFrame({
            "location_category": ["icu"],
            "location_type": ["general_icu"],
        })
        result = check_conditional_requirements_polars(lf, "adt", None)
        assert isinstance(result, DQACompletenessResult)

    def test_no_conditions_for_unknown_table(self):
        lf = pl.LazyFrame({"a": [1]})
        result = check_conditional_requirements_polars(lf, "unknown_table", None)
        assert result.passed is True
        assert len(result.info) > 0  # "No conditional requirements defined"

    def test_dispatcher(self):
        lf = pl.LazyFrame({
            "location_category": ["icu"],
            "location_type": ["general_icu"],
        })
        result = check_conditional_requirements(lf, "adt")
        assert isinstance(result, DQACompletenessResult)

    def test_validation_rules_yaml_loads(self):
        """Verify _load_validation_rules returns the centralised rules file."""
        rules = _load_validation_rules()
        assert "conditional_requirements" in rules
        assert "adt" in rules["conditional_requirements"]
        assert "respiratory_support" in rules["conditional_requirements"]

    def test_conditions_loaded_from_rules_yaml(self):
        """Verify _get_default_conditions loads rules from validation_rules.yaml."""
        conditions = _get_default_conditions("adt")
        assert len(conditions) == 1
        assert conditions[0]["when_column"] == "location_category"
        assert conditions[0]["when_value"] == ["icu"]
        assert conditions[0]["then_required"] == ["location_type"]

        conditions_resp = _get_default_conditions("respiratory_support")
        assert len(conditions_resp) >= 2
        assert conditions_resp[0]["when_column"] == "device_category"
        # Verify compound conditions are loaded
        compound = [c for c in conditions_resp if 'and_column' in c]
        assert len(compound) > 0, "Should have compound (and_column) rules"

    def test_default_conditions_polars_loads_from_rules(self):
        """Polars backend picks up rules from validation_rules.yaml when conditions=None."""
        lf = pl.LazyFrame({
            "location_category": ["icu", "icu"],
            "location_type": [None, None],
        })
        result = check_conditional_requirements_polars(lf, "adt")
        assert len(result.warnings) > 0

    def test_no_conditions_for_unlisted_table(self):
        """A table not in validation_rules.yaml returns empty conditions."""
        conditions = _get_default_conditions("nonexistent_table_xyz")
        assert conditions == []

    def test_compound_condition_satisfied_polars(self):
        """Compound and_column/and_value condition satisfied → no warnings."""
        conditions = [{
            "when_column": "device_category",
            "when_value": ["IMV"],
            "and_column": "mode_category",
            "and_value": ["Assist Control-Volume Control"],
            "then_required": ["tidal_volume_set"],
            "description": "IMV AC-VC requires tidal_volume_set",
        }]
        lf = pl.LazyFrame({
            "device_category": ["IMV", "IMV", "CPAP"],
            "mode_category": ["Assist Control-Volume Control", "Pressure Control", "Pressure Support/CPAP"],
            "tidal_volume_set": [500.0, None, None],
        })
        result = check_conditional_requirements_polars(lf, "respiratory_support", conditions)
        assert result.passed is True
        assert len(result.warnings) == 0

    def test_compound_condition_violated_polars(self):
        """Compound and_column/and_value condition violated → warning."""
        conditions = [{
            "when_column": "device_category",
            "when_value": ["IMV"],
            "and_column": "mode_category",
            "and_value": ["Assist Control-Volume Control"],
            "then_required": ["tidal_volume_set"],
            "description": "IMV AC-VC requires tidal_volume_set",
        }]
        lf = pl.LazyFrame({
            "device_category": ["IMV", "IMV"],
            "mode_category": ["Assist Control-Volume Control", "Assist Control-Volume Control"],
            "tidal_volume_set": [None, None],
        })
        result = check_conditional_requirements_polars(lf, "respiratory_support", conditions)
        assert len(result.warnings) > 0
        assert "IMV AC-VC" in result.warnings[0]["message"]

# ---------------------------------------------------------------------------
# 13. check_mcide_value_coverage  (Polars + DuckDB)
# ---------------------------------------------------------------------------

class TestCheckMcideValueCoverage:

    def test_full_coverage_polars(self, patient_schema):
        lf = pl.LazyFrame({
            "patient_id": ["p1", "p2", "p3"],
            "sex_category": ["Male", "Female", "Unknown"],
            "age": [1, 2, 3],
        })
        result = check_mcide_value_coverage_polars(lf, patient_schema, "patient")
        assert result.passed is True
        cov = result.metrics["coverage_by_column"]["sex_category"]
        assert cov["coverage_percent"] == 100.0

    def test_partial_coverage_polars(self, patient_schema):
        lf = pl.LazyFrame({
            "patient_id": ["p1"],
            "sex_category": ["Male"],
            "age": [1],
        })
        result = check_mcide_value_coverage_polars(lf, patient_schema, "patient")
        cov = result.metrics["coverage_by_column"]["sex_category"]
        assert cov["coverage_percent"] < 100.0
        assert "Female" in cov["missing_values"]

    def test_dispatcher(self, patient_schema):
        lf = pl.LazyFrame({"patient_id": ["p1"], "sex_category": ["Male"], "age": [1]})
        result = check_mcide_value_coverage(lf, patient_schema, "patient")
        assert isinstance(result, DQACompletenessResult)


# ---------------------------------------------------------------------------
# 14. check_relational_integrity  (Polars + DuckDB backends + bidirectional)
# ---------------------------------------------------------------------------

class TestCheckRelationalIntegrity:

    # -- Backend-specific (unidirectional) tests --

    def test_no_orphans_polars(self):
        source = pl.LazyFrame({"patient_id": ["p1", "p2"]})
        ref = pl.LazyFrame({"patient_id": ["p1", "p2", "p3"]})
        result = check_relational_integrity_polars(source, ref, "labs", "patient", "patient_id")
        assert result.passed is True
        assert result.metrics["orphan_ids"] == 0

    def test_orphans_detected_polars(self):
        source = pl.LazyFrame({"patient_id": ["p1", "p2", "p99"]})
        ref = pl.LazyFrame({"patient_id": ["p1", "p2"]})
        result = check_relational_integrity_polars(source, ref, "labs", "patient", "patient_id")
        assert result.metrics["orphan_ids"] == 1
        assert len(result.warnings) > 0

    # -- Bidirectional dispatcher tests --

    def test_perfect_bidirectional_coverage(self):
        """100% coverage both ways — every ID appears in both tables."""
        hosp = pl.LazyFrame({"hospitalization_id": ["H1", "H2", "H3"]})
        labs = pl.LazyFrame({"hospitalization_id": ["H1", "H2", "H3"]})
        result = check_relational_integrity(
            labs, hosp, "labs", "hospitalization", "hospitalization_id"
        )
        assert result.metrics["forward_coverage_percent"] == 100.0
        assert result.metrics["reverse_coverage_percent"] == 100.0
        assert result.metrics["forward_orphan_ids"] == 0
        assert result.metrics["reverse_orphan_ids"] == 0
        assert result.passed is True

    def test_partial_forward_coverage(self):
        """Some hospitalization IDs have no labs (forward orphans)."""
        hosp = pl.LazyFrame({"hospitalization_id": ["H1", "H2", "H3"]})
        labs = pl.LazyFrame({"hospitalization_id": ["H1"]})
        result = check_relational_integrity(
            labs, hosp, "labs", "hospitalization", "hospitalization_id"
        )
        # Forward: 2 of 3 hosp IDs not in labs
        assert result.metrics["forward_orphan_ids"] == 2
        assert result.metrics["forward_coverage_percent"] < 100.0
        # Reverse: all lab IDs are valid
        assert result.metrics["reverse_orphan_ids"] == 0
        assert result.metrics["reverse_coverage_percent"] == 100.0

    def test_partial_reverse_coverage(self):
        """Orphan IDs in the target table (reverse orphans)."""
        hosp = pl.LazyFrame({"hospitalization_id": ["H1", "H2"]})
        labs = pl.LazyFrame({"hospitalization_id": ["H1", "H2", "H99"]})
        result = check_relational_integrity(
            labs, hosp, "labs", "hospitalization", "hospitalization_id"
        )
        # Forward: all hosp IDs are in labs
        assert result.metrics["forward_orphan_ids"] == 0
        assert result.metrics["forward_coverage_percent"] == 100.0
        # Reverse: H99 is orphan
        assert result.metrics["reverse_orphan_ids"] == 1
        assert result.metrics["reverse_coverage_percent"] < 100.0

    def test_both_directions_partial(self):
        """Orphans in both directions. The FK integrity direction (rev)
        uses thresholds: >10% orphans → error, >1% → warning. Here labs
        has 1 orphan of 2 (50%), which crosses the error threshold.
        """
        hosp = pl.LazyFrame({"hospitalization_id": ["H1", "H2", "H3"]})
        labs = pl.LazyFrame({"hospitalization_id": ["H1", "H99"]})
        result = check_relational_integrity(
            labs, hosp, "labs", "hospitalization", "hospitalization_id"
        )
        # Forward (reverse-cardinality): H2, H3 not in labs — informational.
        assert result.metrics["forward_orphan_ids"] == 2
        # Reverse (FK integrity): H99 not in hosp.
        assert result.metrics["reverse_orphan_ids"] == 1
        # 50% orphans in the integrity direction > 10% threshold → error.
        assert result.passed is False
        assert len(result.errors) >= 1

    def test_patient_id_key(self):
        """Works with patient_id key (e.g., code_status table)."""
        patient = pl.LazyFrame({"patient_id": ["P1", "P2", "P3"]})
        code_status = pl.LazyFrame({"patient_id": ["P1", "P2"]})
        result = check_relational_integrity(
            code_status, patient, "code_status", "patient", "patient_id"
        )
        assert result.metrics["forward_orphan_ids"] == 1  # P3 not in code_status
        assert result.metrics["reverse_orphan_ids"] == 0
        assert result.metrics["reverse_coverage_percent"] == 100.0

    def test_metadata(self):
        """check_type and table_name are set correctly."""
        hosp = pl.LazyFrame({"hospitalization_id": ["H1"]})
        labs = pl.LazyFrame({"hospitalization_id": ["H1"]})
        result = check_relational_integrity(
            labs, hosp, "labs", "hospitalization", "hospitalization_id"
        )
        assert result.check_type == "relational_integrity"
        assert result.table_name == "labs<->hospitalization"
        assert isinstance(result, DQACompletenessResult)


# ---------------------------------------------------------------------------
# 14b. Auto-detected relational integrity checks
# ---------------------------------------------------------------------------

class MockTable:
    """Lightweight stand-in for BaseTable in tests."""
    def __init__(self, table_name, df):
        self.table_name = table_name
        self.df = df


class TestRunRelationalIntegrityChecks:

    def test_auto_detects_hospitalization_id(self):
        """labs with hospitalization_id auto-checks vs hospitalization."""
        hosp = MockTable("hospitalization", pl.LazyFrame({
            "hospitalization_id": ["H1", "H2"],
        }))
        labs = MockTable("labs", pl.LazyFrame({
            "hospitalization_id": ["H1", "H2"],
            "lab_value": ["1.0", "2.0"],
        }))
        results = run_relational_integrity_checks([labs, hosp])
        assert "labs" in results
        assert "hospitalization_id" in results["labs"]
        assert isinstance(results["labs"]["hospitalization_id"], DQACompletenessResult)

    def test_auto_detects_patient_id(self):
        """code_status with patient_id auto-checks vs patient."""
        patient = MockTable("patient", pl.LazyFrame({
            "patient_id": ["P1", "P2"],
        }))
        code_status = MockTable("code_status", pl.LazyFrame({
            "patient_id": ["P1", "P2"],
        }))
        results = run_relational_integrity_checks([code_status, patient])
        assert "code_status" in results
        assert "patient_id" in results["code_status"]

    def test_multi_fk_table(self):
        """microbiology_culture runs hosp_id & patient_id, skips organism_id (self-ref)."""
        hosp = MockTable("hospitalization", pl.LazyFrame({
            "hospitalization_id": ["H1"],
        }))
        patient = MockTable("patient", pl.LazyFrame({
            "patient_id": ["P1"],
        }))
        micro = MockTable("microbiology_culture", pl.LazyFrame({
            "hospitalization_id": ["H1"],
            "patient_id": ["P1"],
            "organism_id": ["O1"],
        }))
        results = run_relational_integrity_checks([micro, hosp, patient])
        mc = results.get("microbiology_culture", {})
        assert "hospitalization_id" in mc
        assert "patient_id" in mc
        # organism_id -> microbiology_culture is a self-ref, must be skipped
        assert "organism_id" not in mc

    def test_skips_missing_reference(self):
        """No error when the reference table is not loaded."""
        labs = MockTable("labs", pl.LazyFrame({
            "hospitalization_id": ["H1"],
        }))
        # hospitalization not provided
        results = run_relational_integrity_checks([labs])
        assert results == {} or "hospitalization_id" not in results.get("labs", {})

    def test_self_reference_skip(self):
        """hospitalization doesn't check hospitalization_id against itself."""
        hosp = MockTable("hospitalization", pl.LazyFrame({
            "hospitalization_id": ["H1", "H2"],
            "patient_id": ["P1", "P2"],
        }))
        patient = MockTable("patient", pl.LazyFrame({
            "patient_id": ["P1", "P2"],
        }))
        results = run_relational_integrity_checks([hosp, patient])
        hosp_checks = results.get("hospitalization", {})
        # hospitalization_id should be skipped (self-ref)
        assert "hospitalization_id" not in hosp_checks
        # patient_id should be checked
        assert "patient_id" in hosp_checks

    def test_returns_nested_dict(self):
        """Return structure is {table_name: {fk_column: DQACompletenessResult}}."""
        hosp = MockTable("hospitalization", pl.LazyFrame({
            "hospitalization_id": ["H1"],
        }))
        labs = MockTable("labs", pl.LazyFrame({
            "hospitalization_id": ["H1"],
        }))
        results = run_relational_integrity_checks([labs, hosp])
        assert isinstance(results, dict)
        for tbl, checks in results.items():
            assert isinstance(tbl, str)
            assert isinstance(checks, dict)
            for col, res in checks.items():
                assert isinstance(col, str)
                assert isinstance(res, DQACompletenessResult)

    def test_accepts_pandas_input(self):
        """Pandas DataFrames are auto-converted when Polars backend is active."""
        hosp = MockTable("hospitalization", pd.DataFrame({
            "hospitalization_id": ["H1", "H2"],
        }))
        labs = MockTable("labs", pd.DataFrame({
            "hospitalization_id": ["H1", "H2"],
            "lab_value": ["1.0", "2.0"],
        }))
        results = run_relational_integrity_checks([labs, hosp])
        assert "labs" in results
        assert "hospitalization_id" in results["labs"]
        res = results["labs"]["hospitalization_id"]
        assert res.metrics["forward_coverage_percent"] == 100.0
        assert res.metrics["reverse_coverage_percent"] == 100.0


class TestRunFullDqa:

    def test_result_structure(self, patient_schema):
        """Verify keys: table_name, backend, conformance, completeness, relational."""
        lf = pl.LazyFrame({"patient_id": ["p1"], "sex_category": ["Male"]})
        result = run_full_dqa(lf, patient_schema, "patient")
        assert result["table_name"] == "patient"
        assert "backend" in result
        assert "conformance" in result
        assert "completeness" in result
        assert "relational" in result

    def test_with_and_without_tables(self, patient_schema):
        """Relational section is empty without tables, populated with tables."""
        patient_df = pl.LazyFrame({
            "patient_id": ["P1", "P2"],
            "sex_category": ["Male", "Female"],
        })
        # Without tables — relational should be empty
        result_no_tables = run_full_dqa(patient_df, patient_schema, "patient")
        assert result_no_tables["relational"] == {}

        # With tables — code_status should get a patient_id relational check
        code_schema = {
            "table_name": "code_status",
            "required_columns": ["patient_id"],
            "columns": [
                {"name": "patient_id", "data_type": "VARCHAR", "required": True},
            ],
        }
        code_df = pl.LazyFrame({"patient_id": ["P1"]})
        patient_tbl = MockTable("patient", patient_df)
        code_tbl = MockTable("code_status", code_df)
        result_with = run_full_dqa(
            code_df, code_schema, "code_status",
            tables=[code_tbl, patient_tbl],
        )
        assert "patient_id" in result_with["relational"]


# ---------------------------------------------------------------------------
# 14c. Table presence check (DataFrame-level)
# ---------------------------------------------------------------------------

class TestCheckTablePresence:

    def test_non_empty_df_passes(self):
        lf = pl.LazyFrame({"a": [1, 2], "b": [3, 4]})
        result = check_table_presence(lf, "test_table")
        assert result.passed is True
        assert result.metrics["row_count"] == 2
        assert result.metrics["column_count"] == 2

    def test_empty_df_fails(self):
        lf = pl.LazyFrame({"a": [], "b": []}).cast({"a": pl.Int64, "b": pl.Int64})
        result = check_table_presence(lf, "test_table")
        assert result.passed is False
        assert result.metrics["row_count"] == 0
        assert any("0 rows" in e["message"] for e in result.errors)

# ---------------------------------------------------------------------------
# 15. Orchestration: run_conformance_checks / run_completeness_checks
# ---------------------------------------------------------------------------

class TestRunConformanceChecks:

    def test_returns_all_check_keys(self, patient_schema):
        lf = pl.LazyFrame({"patient_id": ["p1"], "sex_category": ["Male"], "age": [25]})
        results = run_conformance_checks(lf, patient_schema, "patient")
        assert "table_presence" in results
        assert "required_columns" in results
        assert "column_dtypes" in results
        assert "datetime_format" in results
        assert "categorical_values" in results

    def test_includes_lab_units_for_labs(self, labs_schema):
        lf = pl.LazyFrame({
            "hospitalization_id": ["h1"],
            "lab_category": ["albumin"],
            "reference_unit": ["g/dL"],
            "lab_value": ["3.5"],
        })
        results = run_conformance_checks(lf, labs_schema, "labs")
        assert "lab_reference_units" in results

    def test_accepts_pandas_input(self, patient_schema):
        df = pd.DataFrame({"patient_id": ["p1"], "sex_category": ["Male"], "age": [25]})
        results = run_conformance_checks(df, patient_schema, "patient")
        assert "required_columns" in results


class TestRunCompletenessChecks:

    def test_returns_all_check_keys(self, patient_schema):
        lf = pl.LazyFrame({"patient_id": ["p1"], "sex_category": ["Male"]})
        results = run_completeness_checks(lf, patient_schema, "patient")
        assert "missingness" in results
        assert "conditional_requirements" in results
        assert "mcide_value_coverage" in results


# ---------------------------------------------------------------------------
# 16. CLIF-TableOne compatibility layer: validate_dataframe
# ---------------------------------------------------------------------------

class TestValidateDataframeCompat:

    def test_clean_data_no_errors(self, patient_schema):
        lf = pl.LazyFrame({
            "patient_id": ["p1", "p2"],
            "sex_category": ["Male", "Female"],
            "age": [30, 40],
        })
        errors = validate_dataframe(lf, patient_schema, "patient")
        # Should have zero or only informational entries
        critical = [e for e in errors if e.get("severity") != "warning"]
        # No missing-columns or dtype errors expected
        assert not any("Missing Required" in e["type"] for e in critical)

    def test_missing_column_produces_error(self, patient_schema):
        lf = pl.LazyFrame({"patient_id": ["p1"]})
        errors = validate_dataframe(lf, patient_schema, "patient")
        types = [e["type"] for e in errors]
        assert "Missing Required Columns" in types

    def test_returns_list(self, patient_schema):
        lf = pl.LazyFrame({"patient_id": ["p1"], "sex_category": ["Male"]})
        result = validate_dataframe(lf, patient_schema, "patient")
        assert isinstance(result, list)


# ---------------------------------------------------------------------------
# 17. format_clifpy_error
# ---------------------------------------------------------------------------

class TestFormatClifpyError:

    def test_basic_formatting(self):
        err = {"type": "Missing Required Columns", "description": "col x missing",
               "category": "schema", "details": {"missing_columns": ["x"]}}
        formatted = format_clifpy_error(err, 100, "patient")
        assert formatted["table_name"] == "patient"
        assert formatted["row_count"] == 100
        assert formatted["type"] == "Missing Required Columns"

    def test_severity_preserved(self):
        err = {"type": "warn", "description": "d", "category": "dq", "severity": "warning"}
        formatted = format_clifpy_error(err, 10, "t")
        assert formatted["severity"] == "warning"


# ---------------------------------------------------------------------------
# 18. determine_validation_status
# ---------------------------------------------------------------------------

class TestDetermineValidationStatus:

    def test_no_errors_is_complete(self):
        assert determine_validation_status([]) == "complete"

    def test_missing_columns_is_incomplete(self):
        errors = [{"type": "Missing Required Columns", "category": "schema",
                    "details": {"missing_columns": ["x"]}}]
        assert determine_validation_status(errors) == "incomplete"

    def test_non_castable_dtype_is_incomplete(self):
        errors = [{"type": "Data Type Mismatch", "category": "schema",
                    "details": {"castable": False}}]
        assert determine_validation_status(errors) == "incomplete"

    def test_100pct_missing_required_is_incomplete(self):
        errors = [{"type": "High Missingness", "category": "data_quality",
                    "details": {"percent_missing": 100, "column": "patient_id"}}]
        assert determine_validation_status(
            errors, required_columns=["patient_id"]
        ) == "incomplete"

    def test_warnings_only_is_complete(self):
        errors = [{"type": "some_check", "severity": "warning"}]
        assert determine_validation_status(errors) == "complete"

    def test_non_critical_errors_is_partial(self):
        errors = [{"type": "High Missingness", "category": "data_quality",
                    "details": {"percent_missing": 60, "column": "age"}}]
        assert determine_validation_status(errors) == "partial"


# ---------------------------------------------------------------------------
# 19. classify_errors_by_status_impact
# ---------------------------------------------------------------------------

class TestClassifyErrorsByStatusImpact:

    def test_warnings_are_informational(self):
        errors = {
            "schema_errors": [],
            "data_quality_issues": [
                {"type": "check", "description": "d", "details": {}, "severity": "warning"}
            ],
            "other_errors": [],
        }
        classified = classify_errors_by_status_impact(errors, [], "patient")
        assert len(classified["informational"]["data_quality_issues"]) == 1
        assert len(classified["status_affecting"]["data_quality_issues"]) == 0

    def test_mcide_coverage_is_informational(self):
        errors = {
            "schema_errors": [],
            "data_quality_issues": [
                {"type": "mcide coverage gap", "description": "d", "details": {}}
            ],
            "other_errors": [],
        }
        classified = classify_errors_by_status_impact(errors, [], "patient")
        assert len(classified["informational"]["data_quality_issues"]) == 1

    def test_optional_column_is_informational(self):
        errors = {
            "schema_errors": [],
            "data_quality_issues": [
                {"type": "check", "description": "d", "details": {"column": "race"}}
            ],
            "other_errors": [],
        }
        classified = classify_errors_by_status_impact(errors, [], "patient")
        assert len(classified["informational"]["data_quality_issues"]) == 1

    def test_real_error_is_status_affecting(self):
        errors = {
            "schema_errors": [
                {"type": "missing columns", "description": "d", "details": {}}
            ],
            "data_quality_issues": [],
            "other_errors": [],
        }
        classified = classify_errors_by_status_impact(errors, [], "patient")
        assert len(classified["status_affecting"]["schema_errors"]) == 1


# ---------------------------------------------------------------------------
# 20. get_validation_summary
# ---------------------------------------------------------------------------

class TestGetValidationSummary:

    def test_complete_status(self):
        vr = {"status": "complete", "errors": {}}
        summary = get_validation_summary(vr)
        assert "COMPLETE" in summary

    def test_incomplete_with_counts(self):
        vr = {
            "status": "incomplete",
            "errors": {
                "schema_errors": [{"type": "x"}],
                "data_quality_issues": [{"type": "y"}, {"type": "z"}],
                "other_errors": [],
            },
        }
        summary = get_validation_summary(vr)
        assert "INCOMPLETE" in summary
        assert "3 total" in summary

    def test_no_issues(self):
        vr = {"status": "complete", "errors": {}}
        summary = get_validation_summary(vr)
        assert "No issues found" in summary


# ---------------------------------------------------------------------------
# 21. Integration: real schema files
# ---------------------------------------------------------------------------

class TestRealSchemaIntegration:
    """Smoke tests using actual schema files shipped with clifpy."""

    def test_patient_schema_loads(self):
        schema = _load_schema("patient")
        assert schema is not None
        assert "patient_id" in schema["required_columns"]

    def test_labs_schema_loads(self):
        schema = _load_schema("labs")
        assert schema is not None
        assert "lab_reference_units" in schema

    def test_conformance_on_minimal_patient_df(self):
        schema = _load_schema("patient")
        lf = pl.LazyFrame({
            "patient_id": ["p1"],
            "birth_date": ["2000-01-01"],
            "death_dttm": ["2024-01-01T00:00:00"],
            "race_category": ["White"],
            "ethnicity_category": ["Non-Hispanic"],
            "sex_category": ["Male"],
            "language_category": ["English"],
        })
        results = run_conformance_checks(lf, schema, "patient")
        assert "required_columns" in results
        assert results["required_columns"].passed is True

    def test_completeness_on_minimal_patient_df(self):
        schema = _load_schema("patient")
        lf = pl.LazyFrame({
            "patient_id": ["p1"],
            "birth_date": ["2000-01-01"],
            "death_dttm": ["2024-01-01T00:00:00"],
            "race_category": ["White"],
            "ethnicity_category": ["Non-Hispanic"],
            "sex_category": ["Male"],
            "language_category": ["English"],
        })
        results = run_completeness_checks(lf, schema, "patient")
        assert "missingness" in results
        assert results["missingness"].passed is True


class TestCheckFieldPlausibility:
    """Field plausibility: '' must count as absent, with its own advisory."""

    def _rules(self):
        return [{
            "when_column": "location_category",
            "when_not_value": ["icu"],
            "then_null_or_absent": ["location_type"],
            "description": "Non-ICU locations should not have ICU location_type",
        }]

    def _run(self, df):
        return check_field_plausibility_polars(
            pl.from_pandas(df).lazy(), "adt", self._rules())

    def test_empty_strings_are_absent_not_violations(self):
        df = pd.DataFrame({
            "location_category": ["ward", "ed"],
            "location_type": ["", ""],
        })
        result = self._run(df)
        assert result.metrics["violations_by_rule"] == {}
        assert result.metrics["empty_string_columns"] == {"location_type": 2}
        messages = [w["message"] for w in result.warnings]
        assert any("convert '' to null" in m for m in messages)
        assert not any("plausibility violation" in m for m in messages)

    def test_real_values_still_violate(self):
        df = pd.DataFrame({
            "location_category": ["ward", "ward", "icu"],
            "location_type": ["general_icu", "", "general_icu"],
        })
        result = self._run(df)
        stats = result.metrics["violations_by_rule"][
            "Non-ICU locations should not have ICU location_type"]
        assert stats["violations"] == 1
        assert stats["total_applicable"] == 2

    def test_nulls_pass_clean(self):
        df = pd.DataFrame({
            "location_category": ["ward", "ed"],
            "location_type": [None, None],
        })
        result = self._run(df)
        assert result.metrics["violations_by_rule"] == {}
        assert result.metrics["empty_string_columns"] == {}
        assert len(result.warnings) == 0

    def test_when_not_null_variant_ignores_empty_when_values(self):
        rules = [{
            "when_column": "discharge_dttm",
            "when_not_null": True,
            "then_column": "discharge_category",
            "then_not_value": ["Still Admitted"],
            "description": "Discharged patients should not have "
                           "discharge_category 'Still Admitted'",
        }]
        df = pd.DataFrame({
            "discharge_dttm": ["2024-01-01", "", None],
            "discharge_category": ["Still Admitted", "Still Admitted", "Still Admitted"],
        })
        result = check_field_plausibility_polars(
            pl.from_pandas(df).lazy(), "hospitalization", rules)
        stats = result.metrics["violations_by_rule"][rules[0]["description"]]
        assert stats["total_applicable"] == 1
        assert stats["violations"] == 1


# ---------------------------------------------------------------------------
# 22. Medication dose units (per-category expected units from mCIDE)
# ---------------------------------------------------------------------------

class TestCheckMedicationDoseUnits:
    """Conformance check of med_dose_unit against the schema's
    med_category_to_dose_unit_mapping, on both backends."""

    CONT_SCHEMA = {
        "med_category_to_dose_unit_mapping": {
            "norepinephrine": "mcg",
            "sodium_bicarbonate": "ml/hr",
            "nitric_oxide": "ppm",
            "epoprostenol": ["ng", "mcg"],
        },
        "expected_volume_infusion_rate_unit": "ml/hr",
    }
    INT_SCHEMA = {
        "med_category_to_dose_unit_mapping": {
            "acetaminophen": "mg",
            "vancomycin": "g",
        },
    }

    def _run(self, df, schema, table="medication_admin_continuous"):
        return check_medication_dose_units_polars(
            pl.from_pandas(df).lazy(), schema, table)

    def _warnings_for(self, result, category):
        return [w for w in result.warnings
                if w["details"].get("med_category") == category]

    def test_continuous_numerator_match_passes(self):
        df = pd.DataFrame({
            "med_category": ["norepinephrine", "norepinephrine"],
            "med_dose_unit": ["mcg/kg/min", " MCG/HR "],
        })
        result = self._run(df, self.CONT_SCHEMA)
        assert self._warnings_for(result, "norepinephrine") == []
        assert result.metrics["categories_with_mismatch"] == 0
        assert result.metrics["mismatched_records"] == 0

    def test_full_rate_expected_requires_exact_unit(self):
        # sodium_bicarbonate expects the full rate 'ml/hr': bare 'ml' mismatches
        df = pd.DataFrame({
            "med_category": ["sodium_bicarbonate", "sodium_bicarbonate"],
            "med_dose_unit": ["ml/hr", "ml"],
        })
        result = self._run(df, self.CONT_SCHEMA)
        warns = self._warnings_for(result, "sodium_bicarbonate")
        assert len(warns) == 1
        details = warns[0]["details"]
        assert details["matched_records"] == 1
        assert details["mismatched_records"] == 1
        assert details["top_mismatched_units"][0]["med_dose_unit"] == "ml"

    def test_no_slash_expected_unit_passes(self):
        df = pd.DataFrame({
            "med_category": ["nitric_oxide"],
            "med_dose_unit": ["ppm"],
        })
        result = self._run(df, self.CONT_SCHEMA)
        assert self._warnings_for(result, "nitric_oxide") == []

    def test_continuous_numerator_mismatch_warns(self):
        df = pd.DataFrame({
            "med_category": ["norepinephrine"] * 3,
            "med_dose_unit": ["mcg/kg/min", "mg/hr", "mg/hr"],
        })
        result = self._run(df, self.CONT_SCHEMA)
        warns = self._warnings_for(result, "norepinephrine")
        assert len(warns) == 1
        details = warns[0]["details"]
        assert details["matched_records"] == 1
        assert details["mismatched_records"] == 2
        assert result.metrics["categories_with_mismatch"] == 1

    def test_multi_unit_list_accepts_any(self):
        # epoprostenol (IV vs inhaled formulations) accepts ng or mcg
        df = pd.DataFrame({
            "med_category": ["epoprostenol"] * 3,
            "med_dose_unit": ["ng/kg/min", "mcg/kg/min", "mg/hr"],
        })
        result = self._run(df, self.CONT_SCHEMA)
        warns = self._warnings_for(result, "epoprostenol")
        assert len(warns) == 1
        assert warns[0]["details"]["matched_records"] == 2
        assert warns[0]["details"]["mismatched_records"] == 1

    def test_intermittent_requires_exact_match(self):
        df = pd.DataFrame({
            "med_category": ["acetaminophen"] * 3,
            "med_dose_unit": ["mg", "mg/hr", "g"],
        })
        result = self._run(df, self.INT_SCHEMA,
                           table="medication_admin_intermittent")
        warns = self._warnings_for(result, "acetaminophen")
        assert len(warns) == 1
        assert warns[0]["details"]["matched_records"] == 1
        assert warns[0]["details"]["mismatched_records"] == 2

    def test_unknown_and_absent_categories(self):
        df = pd.DataFrame({
            "med_category": ["mystery_med"],
            "med_dose_unit": ["mg"],
        })
        result = self._run(df, self.CONT_SCHEMA)
        # unknown data category: metric + info, never a warning
        assert result.metrics["unknown_categories"] == [
            {"med_category": "mystery_med", "count": 1}]
        assert not any("mystery_med" in w["message"] for w in result.warnings)
        # mapping categories absent from data: info only
        infos = [i["message"] for i in result.info]
        assert any("'norepinephrine': not present in data" in m for m in infos)
        assert result.metrics["categories_checked"] == 0

    def test_null_category_rows_are_skipped(self):
        df = pd.DataFrame({
            "med_category": [None, "norepinephrine"],
            "med_dose_unit": ["mg", "mcg/kg/min"],
        })
        result = self._run(df, self.CONT_SCHEMA)
        assert result.errors == [] and result.warnings == []
        assert result.metrics["total_records"] == 1
        assert "unknown_categories" not in result.metrics

    def test_schema_without_mapping_degrades_gracefully(self):
        df = pd.DataFrame({
            "med_category": ["norepinephrine"],
            "med_dose_unit": ["mcg/kg/min"],
        })
        result = self._run(df, {})
        assert result.atomic_total == 0
        assert result.warnings == [] and result.errors == []
        assert any("No med_category dose unit mapping" in i["message"]
                   for i in result.info)

    def test_null_and_empty_units_excluded_with_advisory(self):
        df = pd.DataFrame({
            "med_category": ["norepinephrine"] * 3,
            "med_dose_unit": ["mcg/kg/min", "", None],
        })
        result = self._run(df, self.CONT_SCHEMA)
        assert self._warnings_for(result, "norepinephrine") == []
        assert result.metrics["total_records"] == 1
        empties = [w for w in result.warnings
                   if w["details"].get("column") == "med_dose_unit"]
        assert len(empties) == 1
        assert empties[0]["details"]["empty_string_rows"] == 1

    def test_volume_infusion_rate_unit_subcheck(self):
        df = pd.DataFrame({
            "med_category": ["norepinephrine"] * 3,
            "med_dose_unit": ["mcg/kg/min"] * 3,
            "volume_infusion_rate_unit": ["ml/hr", "l/hr", None],
        })
        result = self._run(df, self.CONT_SCHEMA)
        vol = result.metrics["volume_infusion_rate_unit"]
        assert vol["matched_records"] == 1
        assert vol["mismatched_records"] == 1
        assert vol["top_mismatched_units"][0]["volume_infusion_rate_unit"] == "l/hr"
        # counts as one extra atomic item
        assert result.atomic_total == len(
            self.CONT_SCHEMA["med_category_to_dose_unit_mapping"]) + 1

    def test_volume_column_absent_is_skipped(self):
        df = pd.DataFrame({
            "med_category": ["norepinephrine"],
            "med_dose_unit": ["mcg/kg/min"],
        })
        result = self._run(df, self.CONT_SCHEMA)
        assert "volume_infusion_rate_unit" not in result.metrics
        assert result.atomic_total == len(
            self.CONT_SCHEMA["med_category_to_dose_unit_mapping"])

    def test_intermittent_ignores_volume_key(self):
        schema = dict(self.INT_SCHEMA,
                      expected_volume_infusion_rate_unit="ml/hr")
        df = pd.DataFrame({
            "med_category": ["acetaminophen"],
            "med_dose_unit": ["mg"],
            "volume_infusion_rate_unit": ["l/hr"],
        })
        result = self._run(df, schema,
                           table="medication_admin_intermittent")
        assert "volume_infusion_rate_unit" not in result.metrics
        assert result.warnings == []

    def test_report_enrichment_shows_units_and_rule_code(self):
        # The report layer must render rule code C.8, the category as the
        # column field, and expected-vs-observed units in the finding text.
        from clifpy.utils.rule_codes import enrich_issue
        df = pd.DataFrame({
            "med_category": ["norepinephrine"],
            "med_dose_unit": ["mg/hr"],
        })
        result = self._run(df, self.CONT_SCHEMA)
        warn = self._warnings_for(result, "norepinephrine")[0]
        issue = enrich_issue({
            "category": "conformance", "check_type": "medication_dose_units",
            "severity": "warning", "message": warn["message"],
            "details": warn["details"],
        })
        assert issue["rule_code"] == "C.8"
        assert issue["rule_description"] == "Medication dose unit validation"
        assert issue["column_field"] == "norepinephrine"
        assert "Expected 'mcg'" in issue["finding"]
        assert "'mg/hr' (1 rows)" in issue["finding"]

    def test_run_conformance_checks_includes_med_dose_units(self):
        schema = _load_schema("medication_admin_continuous", clif_version="3.0")
        lf = pl.LazyFrame({
            "hospitalization_id": ["h1"],
            "med_category": ["norepinephrine"],
            "med_dose_unit": ["mcg/kg/min"],
        })
        results = run_conformance_checks(lf, schema, "medication_admin_continuous")
        assert "medication_dose_units" in results
        assert results["medication_dose_units"].atomic_total > 0


class TestMedicationDoseUnitConsistencyExemptions:
    """P.4 shape check: exempt_units (validation_rules.yaml) lets valid
    non-rate units like nitric_oxide's 'ppm' through for continuous meds."""

    def _run(self, df, table="medication_admin_continuous"):
        return check_medication_dose_unit_consistency_polars(
            pl.from_pandas(df).lazy(), table)

    def test_ppm_exempt_for_continuous(self):
        df = pd.DataFrame({
            "med_category": ["nitric_oxide", "norepinephrine"],
            "med_dose_unit": ["ppm", "mcg/kg/min"],
        })
        result = self._run(df)
        assert result.metrics["unit_pattern_violations"] == 0

    def test_non_exempt_discrete_unit_still_flagged(self):
        df = pd.DataFrame({
            "med_category": ["norepinephrine", "norepinephrine"],
            "med_dose_unit": ["mg", "mcg/kg/min"],
        })
        result = self._run(df)
        assert result.metrics["unit_pattern_violations"] == 1

    def test_intermittent_unaffected_by_exemptions(self):
        df = pd.DataFrame({
            "med_category": ["acetaminophen", "acetaminophen"],
            "med_dose_unit": ["mg", "mg/hr"],
        })
        result = self._run(df, table="medication_admin_intermittent")
        assert result.metrics["unit_pattern_violations"] == 1
