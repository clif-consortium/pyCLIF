from typing import Optional, Dict
import pandas as pd
import polars as pl
from .base_table import BaseTable


class HospitalDiagnosis(BaseTable):
    """
    Hospital diagnosis table wrapper inheriting from BaseTable.

    This class handles hospital diagnosis-specific data and validations while
    leveraging the common functionality provided by BaseTable. Hospital diagnosis
    codes are finalized billing diagnosis codes for hospital reimbursement,
    appropriate for calculation of comorbidity scores but should not be used
    as input features into a prediction model for an inpatient event.
    """

    def __init__(
        self,
        data_directory: str = None,
        filetype: str = None,
        timezone: str = "UTC",
        output_directory: Optional[str] = None,
        data: Optional[pd.DataFrame] = None,
        clif_version: Optional[str] = None
    ):
        """
        Initialize the hospital diagnosis table.

        Parameters
        ----------
        data_directory : str
            Path to the directory containing data files
        filetype : str
            Type of data file (csv, parquet, etc.)
        timezone : str
            Timezone for datetime columns
        output_directory : str, optional
            Directory for saving output files and logs
        data : pd.DataFrame, optional
            Pre-loaded data to use instead of loading from file
        """
        super().__init__(
            data_directory=data_directory,
            filetype=filetype,
            timezone=timezone,
            output_directory=output_directory,
            data=data,
            clif_version=clif_version
        )

        # Auto-load data if not provided
        if data is None and data_directory is not None and filetype is not None:
            self.load_table()

    def load_table(self):
        """Load hospital diagnosis table data from the configured data directory."""
        from ..utils.io import load_data

        if self.data_directory is None or self.filetype is None:
            raise ValueError("data_directory and filetype must be set to load data")

        # Assigned through .data, the canonical write path: load_data's native
        # polars output is stored as-is, with no conversion in either direction.
        self.data = load_data(
            self.table_name,
            self.data_directory,
            self.filetype,
            site_tz=self.timezone,
            return_format='polars',
        )

        if self.logger:
            self.logger.info(f"Loaded {self._data.height} rows from {self.table_name} table")

    @staticmethod
    def _value_counts(data, col):
        """value_counts().to_dict() with pandas' semantics.

        pandas excludes NaN; polars keeps null as its own category.
        """
        vc = data.get_column(col).drop_nulls().value_counts(sort=True)
        return dict(zip(vc.get_column(col).to_list(), vc.get_column('count').to_list()))

    @staticmethod
    def _n_unique(data, col):
        """nunique() with pandas' semantics: NaN is not counted."""
        return data.get_column(col).drop_nulls().n_unique()

    def get_diagnosis_summary(self) -> Dict:
        """Return comprehensive summary statistics for hospital diagnosis data."""
        if self.data is None:
            return {}

        data = self.data          # bound once; the table is never converted
        cols = data.columns

        stats = {
            'total_diagnoses': data.height,
            'unique_hospitalizations': (
                self._n_unique(data, 'hospitalization_id')
                if 'hospitalization_id' in cols else 0
            ),
            'unique_diagnosis_codes': (
                self._n_unique(data, 'diagnosis_code') if 'diagnosis_code' in cols else 0
            ),
        }

        # Diagnosis code format distribution
        if 'diagnosis_code_format' in cols:
            stats['diagnosis_format_counts'] = self._value_counts(data, 'diagnosis_code_format')

        # Primary vs secondary diagnosis distribution
        if 'diagnosis_primary' in cols:
            primary_counts = self._value_counts(data, 'diagnosis_primary')
            stats['primary_diagnosis_counts'] = {
                'primary': primary_counts.get(1, 0),
                'secondary': primary_counts.get(0, 0)
            }

        # Present on admission distribution
        if 'poa_present' in cols:
            poa_counts = self._value_counts(data, 'poa_present')
            stats['poa_counts'] = {
                'present_on_admission': poa_counts.get(1, 0),
                'not_present_on_admission': poa_counts.get(0, 0)
            }

        return stats

    def get_primary_diagnosis_counts(self) -> pd.DataFrame:
        """Return DataFrame with counts of primary diagnoses by diagnosis code."""
        if self.data is None or 'diagnosis_primary' not in self.data.columns:
            return pd.DataFrame()

        primary = self.data.filter(pl.col('diagnosis_primary') == 1)
        if primary.is_empty():
            return pd.DataFrame()

        # One row per (code, format); only that aggregate becomes pandas.
        # drop_nulls on the group keys matches pandas' groupby(dropna=True).
        return (
            primary
            .drop_nulls(subset=['diagnosis_code', 'diagnosis_code_format'])
            .group_by(['diagnosis_code', 'diagnosis_code_format'])
            .agg(pl.len().alias('count'))
            .sort('count', descending=True, maintain_order=True)
            .to_pandas()
        )

    def get_poa_statistics(self) -> Dict:
        """Calculate present on admission statistics by diagnosis type."""
        if self.data is None or 'poa_present' not in self.data.columns or 'diagnosis_primary' not in self.data.columns:
            return {}

        data = self.data
        stats = {}

        def _poa_block(frame):
            total = frame.height
            present = int(frame.get_column('poa_present').eq(1).sum())
            absent = int(frame.get_column('poa_present').eq(0).sum())
            return {
                'total_diagnoses': total,
                'poa_present_count': present,
                'poa_not_present_count': absent,
                'poa_present_rate': (present / total * 100) if total > 0 else 0,
            }

        stats['overall'] = _poa_block(data)

        for diagnosis_type, diagnosis_value in [('primary', 1), ('secondary', 0)]:
            subset = data.filter(pl.col('diagnosis_primary') == diagnosis_value)
            if not subset.is_empty():
                stats[diagnosis_type] = _poa_block(subset)

        return stats

    def get_diagnosis_by_format(self) -> Dict:
        """Group diagnoses by format (ICD9/ICD10) and return summary statistics."""
        if self.data is None or 'diagnosis_code_format' not in self.data.columns:
            return {}

        data = self.data
        cols = data.columns
        format_stats = {}

        # maintain_order reproduces pandas' unique() ordering. pandas' unique()
        # keeps NaN, and the original looped over it, so nulls are kept here too.
        for format_type in data.get_column('diagnosis_code_format').unique(maintain_order=True).to_list():
            if format_type is None:
                subset = data.filter(pl.col('diagnosis_code_format').is_null())
            else:
                subset = data.filter(pl.col('diagnosis_code_format') == format_type)

            format_stats[format_type] = {
                'total_diagnoses': subset.height,
                'unique_diagnosis_codes': (
                    self._n_unique(subset, 'diagnosis_code') if 'diagnosis_code' in cols else 0
                ),
                'unique_hospitalizations': (
                    self._n_unique(subset, 'hospitalization_id') if 'hospitalization_id' in cols else 0
                ),
            }

            if 'diagnosis_primary' in cols:
                primary_counts = self._value_counts(subset, 'diagnosis_primary')
                format_stats[format_type]['primary_count'] = primary_counts.get(1, 0)
                format_stats[format_type]['secondary_count'] = primary_counts.get(0, 0)

            if 'poa_present' in cols:
                poa_counts = self._value_counts(subset, 'poa_present')
                format_stats[format_type]['poa_present_count'] = poa_counts.get(1, 0)
                format_stats[format_type]['poa_not_present_count'] = poa_counts.get(0, 0)

        return format_stats

    def get_hospitalization_diagnosis_counts(self) -> pd.DataFrame:
        """Return DataFrame with diagnosis counts per hospitalization."""
        if self.data is None or 'hospitalization_id' not in self.data.columns:
            return pd.DataFrame()

        has_poa = 'poa_present' in self.data.columns

        # The pandas version used groupby().agg() with lambdas; these are the
        # same three aggregations expressed natively, so no per-group Python
        # callback runs and only the per-hospitalization result becomes pandas.
        return (
            self.data
            .drop_nulls(subset=['hospitalization_id'])
            .group_by('hospitalization_id')
            .agg(
                # count() and sum() yield u32. total_diagnoses counts non-null
                # codes while primary_diagnoses counts matching rows, so the
                # subtraction below can legitimately go negative -- pandas
                # returned -1 there, unsigned arithmetic returns 4294967295.
                # Cast to a signed type to preserve the original result.
                pl.col('diagnosis_code').count().cast(pl.Int64).alias('total_diagnoses'),
                pl.col('diagnosis_primary').eq(1).sum().cast(pl.Int64).alias('primary_diagnoses'),
                (pl.col('poa_present').eq(1).sum().cast(pl.Int64) if has_poa
                 else pl.lit(0, dtype=pl.Int64)).alias('poa_present_diagnoses'),
            )
            .with_columns(
                (pl.col('total_diagnoses') - pl.col('primary_diagnoses')).alias('secondary_diagnoses')
            )
            # pandas' sort_values is stable, so ties keep their prior order.
            .sort('total_diagnoses', descending=True, maintain_order=True)
            .to_pandas()
        )
