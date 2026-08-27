from typing import Optional, Dict
import pandas as pd
import polars as pl
from .base_table import BaseTable


class Hospitalization(BaseTable):
    """
    Hospitalization table wrapper inheriting from BaseTable.
    
    This class handles hospitalization-specific data and validations while
    leveraging the common functionality provided by BaseTable.
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
        Initialize the hospitalization table.
        
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
        # For backward compatibility, handle the old signature
        if data_directory is None and filetype is None and data is not None:
            # Old signature: hospitalization(data)
            # Use dummy values for required parameters
            data_directory = "."
            filetype = "parquet"
        
        super().__init__(
            data_directory=data_directory,
            filetype=filetype,
            timezone=timezone,
            output_directory=output_directory,
            data=data,
            clif_version=clif_version
        )
    
    # ------------------------------------------------------------------
    # Hospitalization Specific Methods
    # ------------------------------------------------------------------

    def calculate_length_of_stay(self) -> pd.DataFrame:
        """Calculate length of stay for each hospitalization and return DataFrame with LOS column."""
        if self.data is None:
            return pd.DataFrame()

        required_cols = ['admission_dttm', 'discharge_dttm']
        if not all(col in self.data.columns for col in required_cols):
            print(f"Missing required columns: {[col for col in required_cols if col not in self.data.columns]}")
            return pd.DataFrame()

        # Computed on the polars frame; converted once at the return because the
        # documented return type is pandas. The pd.to_datetime() calls the pandas
        # version made are unnecessary here -- the loader already types these as
        # Datetime, and a non-datetime column would be a schema error validation
        # reports rather than something to coerce silently.
        return (
            self.data
            .with_columns(
                (
                    (pl.col('discharge_dttm') - pl.col('admission_dttm'))
                    .dt.total_seconds() / (24 * 3600)
                ).alias('length_of_stay_days')
            )
            .to_pandas()
        )

    @staticmethod
    def _round(value, digits):
        """round(), but None-tolerant.

        polars' Series.std() returns None when there are fewer than two
        non-null values; pandas returned NaN there, and round(NaN) is NaN.
        Without this, a one-row table raises TypeError out of get_summary_stats.
        """
        return float('nan') if value is None else round(value, digits)

    def _length_of_stay_days(self) -> 'pl.Series':
        """LOS in days as a polars Series, for the summary stats."""
        return (
            self.data
            .select(
                (
                    (pl.col('discharge_dttm') - pl.col('admission_dttm'))
                    .dt.total_seconds() / (24 * 3600)
                ).alias('los')
            )
            .drop_nulls()
            .get_column('los')
        )

    def get_mortality_rate(self) -> float:
        """Calculate in-hospital mortality rate."""
        if self.data is None or 'discharge_category' not in self.data.columns:
            return 0.0

        total_hospitalizations = self.data.height
        if total_hospitalizations == 0:
            return 0.0

        expired_count = (
            self.data.get_column('discharge_category').eq('Expired').sum()
        )
        return (expired_count / total_hospitalizations) * 100


    def get_summary_stats(self) -> Dict:
        """Return comprehensive summary statistics for hospitalization data."""
        if self.data is None:
            return {}

        # Bound once. Every stat below reads this polars frame, so the table is
        # never converted to pandas -- the pandas version made 19 separate .df
        # reads, each of which was a full conversion once .df stops caching.
        data = self.data
        cols = data.columns

        def _counts(col):
            """value_counts().to_dict(), matching pandas' descending-count order."""
            if col not in cols:
                return {}
            vc = (
                data.get_column(col)
                .drop_nulls()          # pandas' value_counts() excludes NaN;
                .value_counts(sort=True)   # polars' keeps null as a category
            )
            return dict(zip(vc.get_column(col).to_list(),
                            vc.get_column('count').to_list()))

        def _minmax(col, fn):
            return getattr(data.get_column(col), fn)() if col in cols else None

        stats = {
            'total_hospitalizations': data.height,
            # drop_nulls before n_unique matches pandas' nunique(), which does
            # not count NaN; polars' n_unique() counts null as a value.
            'unique_patients': (
                data.get_column('patient_id').drop_nulls().n_unique()
                if 'patient_id' in cols else 0
            ),
            'discharge_category_counts': _counts('discharge_category'),
            'admission_type_counts': _counts('admission_type_category'),
            'date_range': {
                'earliest_admission': _minmax('admission_dttm', 'min'),
                'latest_admission': _minmax('admission_dttm', 'max'),
                'earliest_discharge': _minmax('discharge_dttm', 'min'),
                'latest_discharge': _minmax('discharge_dttm', 'max'),
            }
        }

        # Age statistics
        if 'age_at_admission' in cols:
            age_data = data.get_column('age_at_admission').drop_nulls()
            if len(age_data):
                stats['age_stats'] = {
                    'mean': self._round(age_data.mean(), 1),
                    'median': age_data.median(),
                    'min': age_data.min(),
                    'max': age_data.max(),
                    'std': self._round(age_data.std(), 1)
                }

        # Length of stay statistics
        if all(col in cols for col in ['admission_dttm', 'discharge_dttm']):
            los_data = self._length_of_stay_days()
            if len(los_data):
                stats['length_of_stay_stats'] = {
                    'mean_days': self._round(los_data.mean(), 1),
                    'median_days': self._round(los_data.median(), 1),
                    'min_days': self._round(los_data.min(), 1),
                    'max_days': self._round(los_data.max(), 1),
                    'std_days': self._round(los_data.std(), 1)
                }

        # Mortality rate
        stats['mortality_rate_percent'] = round(self.get_mortality_rate(), 2)

        return stats

    def get_patient_hospitalization_counts(self) -> pd.DataFrame:
        """Return DataFrame with hospitalization counts per patient."""
        if self.data is None or 'patient_id' not in self.data.columns:
            return pd.DataFrame()

        # One row per patient, so only the aggregate is converted to pandas.
        # drop_nulls on the group key matches pandas' groupby(dropna=True).
        return (
            self.data
            .drop_nulls(subset=['patient_id'])
            .group_by('patient_id')
            .agg(
                pl.col('hospitalization_id').count().alias('hospitalization_count'),
                pl.col('admission_dttm').min().alias('first_admission'),
                pl.col('admission_dttm').max().alias('last_admission'),
            )
            .with_columns(
                (
                    (pl.col('last_admission') - pl.col('first_admission'))
                    .dt.total_seconds() / (24 * 3600)
                ).alias('care_span_days')
            )
            .sort('hospitalization_count', descending=True)
            .to_pandas()
        )