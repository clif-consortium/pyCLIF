"""Tests for CLIF schema versioning (3.0 default + 2.1 support).

Covers the schema registry, version threading through BaseTable and
ClifOrchestrator, the ecmo_mcs -> mcs rename, and integrity of the 3.0
schema set. These tests are self-contained (no data files needed) and
avoid the legacy validator-spec fixtures.
"""

import glob
import os

import pandas as pd
import pytest
import yaml

from clifpy.schemas import (
    DEFAULT_CLIF_VERSION,
    SUPPORTED_CLIF_VERSIONS,
    load_schema,
    resolve_schema_filename,
    schema_dir,
)
from clifpy import EcmoMcs, Mcs, Patient, Vitals, ClifOrchestrator
from clifpy.clif_orchestrator import TABLE_CLASSES
from clifpy.utils.validator import run_full_dqa, validate_dataframe

SCHEMAS_ROOT = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'clifpy', 'schemas')


# --------------------------------------------------------------------------- #
# Registry
# --------------------------------------------------------------------------- #

def test_default_and_supported_versions():
    assert DEFAULT_CLIF_VERSION == "3.0"
    assert set(SUPPORTED_CLIF_VERSIONS) == {"2.1", "3.0"}


def test_load_schema_defaults_to_30():
    s = load_schema("patient")
    assert s is not None
    assert s["version"] == "3.0"
    assert s["table_name"] == "patient"


@pytest.mark.parametrize("version", ["2.1", "3.0"])
def test_load_schema_returns_requested_version(version):
    s = load_schema("vitals", version)
    assert s is not None
    assert s["version"] == version


def test_unsupported_version_raises():
    with pytest.raises(ValueError, match="Unsupported CLIF version"):
        load_schema("patient", "9.9")
    with pytest.raises(ValueError):
        schema_dir("nope")


def test_missing_schema_returns_none():
    # 'input' is a 3.0-only table; it does not exist in 2.1
    assert load_schema("input", "2.1") is None
    assert load_schema("input", "3.0") is not None


# --------------------------------------------------------------------------- #
# ecmo_mcs -> mcs rename
# --------------------------------------------------------------------------- #

def test_ecmo_mcs_rename_override():
    # The 2.1 table 'ecmo_mcs' was renamed to 'mcs' in 3.0.
    assert resolve_schema_filename("ecmo_mcs", "2.1") == "ecmo_mcs_schema.yaml"
    assert resolve_schema_filename("ecmo_mcs", "3.0") == "mcs_schema.yaml"
    # Loading the 2.1 table name under 3.0 yields the redesigned mcs schema
    s = load_schema("ecmo_mcs", "3.0")
    assert s is not None and s["table_name"] == "mcs"


def test_ecmo_mcs_class_versions():
    em_21 = EcmoMcs(data=pd.DataFrame(), clif_version="2.1")
    assert em_21.schema["table_name"] == "ecmo_mcs"
    assert em_21.schema["version"] == "2.1"

    em_30 = EcmoMcs(data=pd.DataFrame(), clif_version="3.0")
    assert em_30.schema["table_name"] == "mcs"  # override

    mcs = Mcs(data=pd.DataFrame(), clif_version="3.0")
    assert mcs.schema["table_name"] == "mcs"


# --------------------------------------------------------------------------- #
# BaseTable version threading
# --------------------------------------------------------------------------- #

def test_basetable_defaults_to_30():
    p = Patient(data=pd.DataFrame())
    assert p.clif_version == "3.0"
    assert p.schema["version"] == "3.0"


def test_basetable_honors_version():
    v = Vitals(data=pd.DataFrame(), clif_version="3.0")
    assert v.clif_version == "3.0"
    assert v.schema["version"] == "3.0"
    # 3.0 adds intracranial_pressure + pulse_pressure_variation
    cats = next(c for c in v.schema["columns"] if c["name"] == "vital_category")["permissible_values"]
    assert "intracranial_pressure" in cats
    assert "pulse_pressure_variation" in cats


def test_none_version_coerced_to_default():
    p = Patient(data=pd.DataFrame(), clif_version=None)
    assert p.clif_version == "3.0"


# --------------------------------------------------------------------------- #
# Orchestrator version threading
# --------------------------------------------------------------------------- #

def test_orchestrator_default_version(tmp_path):
    co = ClifOrchestrator(data_directory=str(tmp_path), filetype="parquet", timezone="UTC")
    assert co.clif_version == "3.0"


def test_orchestrator_explicit_version(tmp_path):
    co = ClifOrchestrator(data_directory=str(tmp_path), filetype="parquet",
                          timezone="UTC", clif_version="3.0")
    assert co.clif_version == "3.0"


def test_orchestrator_version_from_config(tmp_path):
    cfg = tmp_path / "config.yaml"
    cfg.write_text(
        f"data_directory: {tmp_path}\nfiletype: parquet\ntimezone: UTC\nclif_version: '3.0'\n"
    )
    co = ClifOrchestrator(config_path=str(cfg))
    assert co.clif_version == "3.0"


# --------------------------------------------------------------------------- #
# Integrity of the full 3.0 schema set
# --------------------------------------------------------------------------- #

def test_30_schema_set_parses_and_is_consistent():
    files = sorted(glob.glob(os.path.join(SCHEMAS_ROOT, "3.0", "*_schema.yaml")))
    # The CLIF 3.0.0 data dictionary defines exactly 40 tables: 15 beta,
    # 13 alpha, 10 concept and 2 future-proposed.
    assert len(files) == 40
    for fp in files:
        table = os.path.basename(fp).replace("_schema.yaml", "")
        s = yaml.safe_load(open(fp))
        assert s["version"] == "3.0", table
        assert s["table_name"] == table, table
        cols = s.get("columns") or []
        # per-column flags must agree with the summary lists
        req_flags = {c["name"] for c in cols if c.get("required")}
        cat_flags = {c["name"] for c in cols if c.get("is_category_column")}
        grp_flags = {c["name"] for c in cols if c.get("is_group_column")}
        assert req_flags == set(s.get("required_columns") or []), table
        assert cat_flags == set(s.get("category_columns") or []), table
        assert grp_flags == set(s.get("group_columns") or []), table


@pytest.mark.parametrize("table,expected_count", [
    ("medication_admin_continuous", 75),
    ("medication_admin_intermittent", 271),
])
def test_30_med_dose_unit_mappings_consistent(table, expected_count):
    # The mCIDE-derived dose unit mapping must stay in lockstep with the
    # med_category permissible values.
    s = yaml.safe_load(open(os.path.join(SCHEMAS_ROOT, "3.0", f"{table}_schema.yaml")))
    mapping = s["med_category_to_dose_unit_mapping"]
    assert len(mapping) == expected_count
    cats = next(c for c in s["columns"] if c["name"] == "med_category")["permissible_values"]
    assert set(mapping) == set(cats)
    for cat, units in mapping.items():
        units = units if isinstance(units, list) else [units]
        assert units, cat
        assert all(isinstance(u, str) and u.strip() for u in units), cat
    if table == "medication_admin_continuous":
        assert s["expected_volume_infusion_rate_unit"] == "ml/hr"


def test_30_lab_reference_units_consistent():
    # Every lab_category needs a reference unit, and every reference unit needs
    # an allowed_unit_variants key, or only its exact spelling is accepted.
    s = yaml.safe_load(open(os.path.join(SCHEMAS_ROOT, "3.0", "labs_schema.yaml"), encoding="utf-8"))
    units = s["lab_reference_units"]
    cats = next(c for c in s["columns"] if c["name"] == "lab_category")["permissible_values"]
    assert set(units) == set(cats)
    missing = {cat: u for cat, u in units.items() if u not in s["allowed_unit_variants"]}
    assert not missing, missing


def test_all_30_tables_registered():
    files = {os.path.basename(f).replace("_schema.yaml", "")
             for f in glob.glob(os.path.join(SCHEMAS_ROOT, "3.0", "*_schema.yaml"))}
    missing = files - set(TABLE_CLASSES)
    assert not missing, f"3.0 tables missing from TABLE_CLASSES: {missing}"


def test_21_schemas_relocated():
    # The 18 original schemas now live under schemas/2.1/, not at the root.
    root_schemas = glob.glob(os.path.join(SCHEMAS_ROOT, "*_schema.yaml"))
    assert root_schemas == []
    v21 = glob.glob(os.path.join(SCHEMAS_ROOT, "2.1", "*_schema.yaml"))
    assert len(v21) == 18
    # shared configs remain at the root
    for shared in ("validation_rules.yaml", "outlier_config.yaml", "wide_tables_config.yaml"):
        assert os.path.exists(os.path.join(SCHEMAS_ROOT, shared))


# --------------------------------------------------------------------------- #
# Version surfaced as an attribute through DQA validation
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("version", ["2.1", "3.0"])
def test_validate_dataframe_stamps_version_on_errors(version):
    schema = load_schema("labs", version)
    # Empty frame guarantees some required-column errors to stamp.
    errors = validate_dataframe(pd.DataFrame({"hospitalization_id": ["1"]}), schema)
    assert errors, "expected validation errors to stamp"
    assert {e.get("clif_version") for e in errors} == {version}


def test_validate_dataframe_clif_version_arg_overrides_when_schema_unversioned():
    schema = dict(load_schema("labs", "3.0"))
    schema.pop("version", None)  # hand-built schema with no version key
    errors = validate_dataframe(
        pd.DataFrame({"hospitalization_id": ["1"]}), schema, clif_version="3.0"
    )
    assert {e.get("clif_version") for e in errors} == {"3.0"}


@pytest.mark.parametrize("version", ["2.1", "3.0"])
def test_run_full_dqa_reports_version(version):
    result = run_full_dqa(
        pd.DataFrame({"hospitalization_id": ["1"]}),
        table_name="labs",
        clif_version=version,
    )
    assert result["clif_version"] == version
    # every result object across check families carries the version
    for family in ("conformance", "completeness", "plausibility"):
        for check in result[family].values():
            assert check["clif_version"] == version


@pytest.mark.parametrize("version", ["2.1", "3.0"])
def test_basetable_validate_stamps_version(version):
    t = Vitals(data=pd.DataFrame({"hospitalization_id": ["1"]}), clif_version=version)
    t.validate()
    assert t.clif_version == version
    assert {e.get("clif_version") for e in t.errors} == {version}


# --------------------------------------------------------------------------- #
# Bundled CLIF 3.0 demo data (produced by the 2.1->3.0 crosswalk)
# --------------------------------------------------------------------------- #

DEMO_30_DIR = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "clifpy", "data", "clif_demo", "3.0",
)
DEMO_30_FILES = sorted(glob.glob(os.path.join(DEMO_30_DIR, "clif_*.parquet")))


def _demo_table_name(path):
    return os.path.basename(path)[len("clif_"):-len(".parquet")]


DEMO_21_FILES = sorted(glob.glob(os.path.join(
    os.path.dirname(DEMO_30_DIR), "clif_*.parquet")))


def test_30_demo_data_present():
    # The crosswalk converts the 14 bundled 2.1 demo tables to 3.0.
    assert len(DEMO_30_FILES) == 14


@pytest.mark.parametrize("path", DEMO_21_FILES + DEMO_30_FILES,
                         ids=lambda p: f"{os.path.basename(os.path.dirname(p))}/{_demo_table_name(p)}")
def test_demo_data_has_no_leaked_index_column(path):
    # Guard against the pandas __index_level_0__ artifact creeping back into
    # the bundled demo data (2.1 source or 3.0 crosswalk output).
    cols = pd.read_parquet(path).columns
    leaked = [c for c in cols if c.startswith("__index")]
    assert not leaked, f"{path} leaks index column(s): {leaked}"


@pytest.mark.parametrize("path", DEMO_30_FILES, ids=_demo_table_name)
def test_30_demo_data_validates_with_version_stamp(path):
    """Every bundled 3.0 demo table runs through the full DQA suite and the
    3.0 version is stamped on the top-level result and every check object."""
    table = _demo_table_name(path)
    df = pd.read_parquet(path)
    result = run_full_dqa(df, table_name=table, clif_version="3.0")
    assert result["clif_version"] == "3.0"
    checks = [
        check
        for family in ("conformance", "completeness", "plausibility")
        for check in result[family].values()
    ]
    assert checks, f"no DQA checks ran for {table}"
    assert all(c["clif_version"] == "3.0" for c in checks)


def test_30_demo_data_loads_through_table_class():
    """A converted 3.0 demo table loads via the table API against the 3.0
    schema and stamps the version onto its validation errors."""
    df = pd.read_parquet(os.path.join(DEMO_30_DIR, "clif_vitals.parquet"))
    t = Vitals(data=df, clif_version="3.0")
    t.validate()
    assert t.clif_version == "3.0"
    assert t.schema["version"] == "3.0"
    # errors are expected (e.g. mCIDE coverage gaps); each must carry the version
    assert all(e.get("clif_version") == "3.0" for e in t.errors)


# --------------------------------------------------------------------------- #
# Expected-check-count denominators must track the version being validated
# --------------------------------------------------------------------------- #

def test_check_counts_honour_clif_version():
    """``get_schema_check_counts`` must compute against the requested version.

    It previously had no ``clif_version`` parameter and always resolved the
    default schema set, so a 3.0 run was scored against 2.1 denominators.
    """
    from clifpy.utils.validator import get_schema_check_counts

    c21 = get_schema_check_counts("medication_admin_continuous", clif_version="2.1")
    c30 = get_schema_check_counts("medication_admin_continuous", clif_version="3.0")
    assert c21 != c30, "2.1 and 3.0 must not produce identical denominators"

    # A table that only exists in 3.0 must count there and be empty at 2.1,
    # rather than silently returning zeros for both.
    rrt30 = get_schema_check_counts("renal_replacement_therapy", clif_version="3.0")
    rrt21 = get_schema_check_counts("renal_replacement_therapy", clif_version="2.1")
    assert all(v > 0 for v in rrt30.values()), rrt30
    assert rrt21 == {"conformance": 0, "completeness": 0, "plausibility": 0}


@pytest.mark.parametrize("version", ["2.1", "3.0"])
@pytest.mark.parametrize(
    "table", ["medication_admin_continuous", "medication_admin_intermittent"]
)
def test_conformance_denominator_covers_every_check_that_runs(table, version):
    """Every check ``run_conformance_checks`` executes must be counted.

    ``medication_dose_units`` runs for the medication-admin tables but had no
    term in ``get_schema_check_counts``, so on 3.0 the denominator understated
    the work actually done by the whole dose-unit mapping.
    """
    from clifpy.utils.validator import (
        get_schema_check_counts,
        run_conformance_checks,
    )

    df = pd.DataFrame({
        "hospitalization_id": ["h1"],
        "med_category": ["norepinephrine"],
        "med_dose": [5.0],
        "med_dose_unit": ["mcg/min"],
        "admin_dttm": pd.to_datetime(["2024-01-01 00:00:00"]),
        "volume_infusion_rate_unit": ["ml/hr"],
    })
    schema = load_schema(table, version)
    results = run_conformance_checks(df, schema, table)
    actual = sum(r.atomic_total or 0 for r in results.values())
    expected = get_schema_check_counts(table, clif_version=version)["conformance"]
    assert actual == expected, (
        f"{table} @{version}: {actual} atoms ran but the denominator is {expected}"
    )


def test_absent_table_result_reports_its_version():
    """The absent-table stand-in must say which version its 0/N is measured against."""
    from clifpy.utils.validator import build_absent_table_dqa_result

    r = build_absent_table_dqa_result("renal_replacement_therapy", clif_version="3.0")
    assert r["clif_version"] == "3.0"
    assert r["expected_check_counts"]["conformance"] > 0
