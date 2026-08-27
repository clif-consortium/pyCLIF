"""The silent-failure defences around the pandas -> polars default flip.

polars supports df['col'], len(df) and boolean masks, so a caller that silently
receives polars where it expected pandas can look fine and diverge later. The defences:
pin every in-package caller, and guard the pandas boundary with a clear TypeError.
"""

import logging

import pandas as pd
import polars as pl
import pytest

from clifpy.tables.patient import Patient
from clifpy.utils.io import load_data


def test_base_table_accepts_a_polars_frame():
    """The table layer stores polars now, so a polars frame is taken as-is."""
    df = pl.DataFrame({"patient_id": ["1"], "sex_category": ["female"]})
    patient = Patient(data=df)
    assert isinstance(patient.data, pl.DataFrame)
    assert patient.data.equals(df)


def test_base_table_rejects_an_unsupported_type():
    """The boundary guard still names the accepted types."""
    with pytest.raises(TypeError, match="pandas DataFrame, polars"):
        Patient(data=[{"patient_id": "1"}])


def test_base_table_accepts_pandas():
    """pandas input is converted on the way in; .df hands pandas back."""
    df = pd.DataFrame({"patient_id": ["1"], "sex_category": ["female"]})
    patient = Patient(data=df)
    assert isinstance(patient.data, pl.DataFrame)
    assert isinstance(patient.df, pd.DataFrame)


def test_df_still_yields_pandas(demo_dir):
    """.df stays pandas for the ~20 modules that read it that way."""
    patient = Patient.from_file(data_directory=demo_dir, filetype="parquet",
                                timezone="US/Eastern")
    assert isinstance(patient.data, pl.DataFrame)
    assert isinstance(patient.df, pd.DataFrame)


def test_df_conversion_is_cached_and_invalidated(demo_dir):
    """The pandas view is built once, and rebuilt after .df is reassigned."""
    patient = Patient.from_file(data_directory=demo_dir, filetype="parquet",
                                timezone="US/Eastern")
    assert patient.df is patient.df                      # cached
    patient.df = pl.DataFrame({"patient_id": ["9"]})     # setter invalidates
    assert patient.df["patient_id"].tolist() == ["9"]


def test_pandas_format_matches_the_historic_default(demo_dir):
    """return_format='pandas' reproduces what a bare load_data() used to return."""
    df = load_data("vitals", demo_dir, "parquet", sample_size=50,
                   site_tz="US/Eastern", return_format="pandas")
    assert isinstance(df, pd.DataFrame)
    assert str(df["recorded_dttm"].dt.tz) == "US/Eastern"
    assert str(df["hospitalization_id"].dtype) == "string"


def test_bare_load_data_now_returns_polars(demo_dir):
    """The flip itself, asserted so the change is explicit rather than incidental."""
    assert isinstance(load_data("vitals", demo_dir, "parquet", sample_size=5),
                      pl.DataFrame)


def test_in_place_df_edits_do_not_reach_the_stored_data():
    """`.df` is a conversion: editing it in place never reaches `.data`.

    This is the pattern docs/user-guide/timezones.md used to teach. The edit
    lands on the cached pandas copy only, so validate() and every summary
    method -- all of which read `_data` -- would still see the original data.
    """
    patient = Patient(data=pd.DataFrame({"patient_id": ["1"], "sex_category": ["female"]}))
    patient.df["site"] = "site_a"
    assert "site" not in patient.data.columns


def test_assigning_back_through_df_does_reach_the_stored_data():
    """The supported write path: edit, then assign back through the setter."""
    patient = Patient(data=pd.DataFrame({"patient_id": ["1"], "sex_category": ["female"]}))
    edited = patient.df
    edited["site"] = "site_a"
    patient.df = edited
    assert "site" in patient.data.columns
    assert list(patient.df.columns) == list(patient.data.columns)


class _CaptureLogs(logging.Handler):
    """Attach to the table's own logger.

    ``clifpy``'s logger sets ``propagate = False`` (logging_config.py:150), so
    ``caplog`` -- which hooks the root logger -- never sees these records.
    """

    def __init__(self):
        super().__init__()
        self.messages = []

    def emit(self, record):
        self.messages.append(record.getMessage())


def _validate_capturing_logs(table):
    handler = _CaptureLogs()
    table.logger.addHandler(handler)
    try:
        table.validate()
    finally:
        table.logger.removeHandler(handler)
    return handler.messages


def test_validate_warns_when_the_pandas_view_diverged():
    """validate() must not silently run against data the caller thinks it edited."""
    patient = Patient(data=pd.DataFrame({"patient_id": ["1"], "sex_category": ["female"]}))
    patient.df["site"] = "site_a"
    assert any("modified in place" in m for m in _validate_capturing_logs(patient))


def test_validate_does_not_warn_when_the_view_is_untouched():
    """The guard compares shape, so merely reading .df must not trip it."""
    patient = Patient(data=pd.DataFrame({"patient_id": ["1"], "sex_category": ["female"]}))
    _ = patient.df
    assert not any("modified in place" in m for m in _validate_capturing_logs(patient))


def test_data_setter_accepts_polars_pandas_and_lazy():
    """`.data` is the canonical write path and takes all three frame types."""
    patient = Patient(data=pd.DataFrame({"patient_id": ["1"], "sex_category": ["female"]}))

    patient.data = pl.DataFrame({"patient_id": ["2"]})
    assert patient.data["patient_id"].to_list() == ["2"]

    patient.data = pl.LazyFrame({"patient_id": ["3"]})
    assert isinstance(patient.data, pl.DataFrame)      # collected on the way in
    assert patient.data["patient_id"].to_list() == ["3"]

    patient.data = pd.DataFrame({"patient_id": ["4"]})
    assert isinstance(patient.data, pl.DataFrame)      # converted on the way in
    assert patient.data["patient_id"].to_list() == ["4"]

    patient.data = None
    assert patient.data is None


def test_data_setter_rejects_an_unsupported_type():
    patient = Patient(data=pd.DataFrame({"patient_id": ["1"], "sex_category": ["female"]}))
    with pytest.raises(TypeError, match="pandas DataFrame, polars"):
        patient.data = [{"patient_id": "1"}]


def test_df_setter_is_an_alias_for_the_data_setter():
    """`.df = ...` stays supported and must behave identically to `.data = ...`."""
    a = Patient(data=pd.DataFrame({"patient_id": ["1"], "sex_category": ["female"]}))
    b = Patient(data=pd.DataFrame({"patient_id": ["1"], "sex_category": ["female"]}))
    frame = pl.DataFrame({"patient_id": ["7"], "sex_category": ["male"]})
    a.df = frame
    b.data = frame
    assert a.data.equals(b.data)


def test_writing_invalidates_the_cached_pandas_view():
    """Either setter must drop a twin built before the write."""
    for write in ("df", "data"):
        patient = Patient(data=pd.DataFrame({"patient_id": ["1"], "sex_category": ["female"]}))
        _ = patient.df                       # build the twin
        assert patient._df_pandas is not None
        setattr(patient, write, pl.DataFrame({"patient_id": ["9"]}))
        assert patient._df_pandas is None, f".{write} setter left a stale twin"
        assert patient.df["patient_id"].tolist() == ["9"]
