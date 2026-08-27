from typing import Optional, List, Dict
from datetime import datetime
import pandas as pd
import polars as pl
import json
import os
from .base_table import BaseTable


class Vitals(BaseTable):
    """
    Vitals table wrapper inheriting from BaseTable.
    
    This class handles vitals-specific data and validations including
    range validation for vital signs.
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
        Initialize the vitals table.
        
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
        # Initialize range validation errors list
        self.range_validation_errors: List[dict] = []
        
        # Load vital ranges and units from schema
        self._vital_units = None
        self._vital_ranges = None
        
        super().__init__(
            data_directory=data_directory,
            filetype=filetype,
            timezone=timezone,
            output_directory=output_directory,
            data=data,
            clif_version=clif_version
        )
        
        # Load vital-specific schema data
        self._load_vitals_schema_data()

    def _load_vitals_schema_data(self):
        """Load vital units and ranges from the YAML schema."""
        if self.schema:
            self._vital_units = self.schema.get('vital_units', {})
            self._vital_ranges = self.schema.get('vital_ranges', {})

    @property
    def vital_units(self) -> Dict[str, str]:
        """Get the vital units mapping from the schema."""
        return self._vital_units.copy() if self._vital_units else {}

    @property
    def vital_ranges(self) -> Dict[str, Dict[str, float]]:
        """Get the vital ranges from the schema."""
        return self._vital_ranges.copy() if self._vital_ranges else {}

    def isvalid(self) -> bool:
        """Return ``True`` if the last validation finished without errors."""
        return not self.errors and not self.range_validation_errors

    def _run_table_specific_validations(self):
        """
        Run vitals-specific validations including range validation.

        This overrides the base class method to add vitals-specific validation.
        """
        self.validate_vital_ranges()

    def validate_vital_ranges(self):
        """Validate vital values against the schema's expected ranges.

        Results land in :attr:`range_validation_errors` (not ``errors``), which
        :meth:`isvalid` checks separately. Each out-of-range category produces a
        single entry carrying the observed min/max and a human-readable list of
        ``issues``, rather than one entry per bound.
        """
        self.range_validation_errors = []

        if self.data is None or not self._vital_ranges:
            return

        required_columns = ['vital_category', 'vital_value']
        missing = [col for col in required_columns if col not in self.data.columns]
        if missing:
            self.range_validation_errors.append({
                'error_type': 'missing_columns_for_range_validation',
                'columns': missing,
                'message': (
                    f"Missing column(s) {', '.join(missing)}: "
                    "cannot perform range validation."
                ),
            })
            return

        # Reads the stored polars frame. Going through .df here would convert the
        # whole table to pandas and leave that copy cached on the object for the
        # rest of the session -- on the validation path, which is the one users
        # run over full-size data.
        #
        # cast(strict=False) is polars' pd.to_numeric(errors='coerce'): unparseable
        # values become null. Non-numeric values are a dtype problem, which schema
        # validation reports separately. Nulls in vital_category are dropped to
        # match pandas' groupby(dropna=True) default.
        vital_stats = (
            self.data
            .select(required_columns)
            .with_columns(pl.col('vital_value').cast(pl.Float64, strict=False))
            .drop_nulls(subset=required_columns)
            .group_by('vital_category')
            .agg(
                pl.col('vital_value').min().alias('min'),
                pl.col('vital_value').max().alias('max'),
                pl.col('vital_value').mean().alias('mean'),
                pl.col('vital_value').count().alias('count'),
            )
            # group_by does not order its output; pandas' groupby sorted keys
            # ascending, and range_validation_errors is compared as a list.
            .sort('vital_category')
        )
        if vital_stats.is_empty():
            return

        for row in vital_stats.iter_rows(named=True):
            vital_category = row['vital_category']
            min_val, max_val = row['min'], row['max']
            count = int(row['count'])

            if vital_category not in self._vital_ranges:
                self.range_validation_errors.append({
                    'error_type': 'unknown_vital_category',
                    'vital_category': vital_category,
                    'affected_rows': count,
                    'message': (
                        f"Unknown vital category '{vital_category}' "
                        f"affects {count} rows"
                    ),
                })
                continue

            expected_range = self._vital_ranges[vital_category]
            expected_min = expected_range.get('min')
            expected_max = expected_range.get('max')

            issues = []
            if expected_min is not None and min_val < expected_min:
                issues.append(
                    f"minimum value {min_val} below expected {expected_min}"
                )
            if expected_max is not None and max_val > expected_max:
                issues.append(
                    f"maximum value {max_val} above expected {expected_max}"
                )

            if issues:
                self.range_validation_errors.append({
                    'error_type': 'values_out_of_range',
                    'vital_category': vital_category,
                    'affected_rows': count,
                    'min_value': min_val,
                    'max_value': max_val,
                    'mean_value': round(row['mean'], 2),
                    'expected_range': expected_range,
                    'issues': issues,
                    'message': (
                        f"Vital '{vital_category}' has values out of expected "
                        f"range: {'; '.join(issues)}"
                    ),
                })

        if self.range_validation_errors:
            self.logger.warning(
                f"Validation completed with {len(self.range_validation_errors)} "
                "range validation error(s)."
            )

    # ------------------------------------------------------------------
    # Vitals Specific Methods
    # ------------------------------------------------------------------
    def filter_by_vital_category(self, vital_category: str) -> pd.DataFrame:
        """Return all records for a specific vital category (e.g., 'heart_rate', 'temp_c')."""
        if self.data is None or 'vital_category' not in self.data.columns:
            return pd.DataFrame()
        # Filtered in polars; only the matching subset becomes pandas.
        return self.data.filter(pl.col('vital_category') == vital_category).to_pandas()

    def get_vital_categories(self) -> List[str]:
        """Return the unique vital categories present in the data."""
        if self.data is None or 'vital_category' not in self.data.columns:
            return []
        # maintain_order reproduces pandas' unique(), which returns values in
        # order of first appearance; polars' unique() is unordered by default.
        return (
            self.data.get_column('vital_category')
            .drop_nulls().unique(maintain_order=True).to_list()
        )

    def filter_by_hospitalization(self, hospitalization_id: str) -> pd.DataFrame:
        """Return all vital records for one hospitalization."""
        if self.data is None or 'hospitalization_id' not in self.data.columns:
            return pd.DataFrame()
        return self.data.filter(pl.col('hospitalization_id') == hospitalization_id).to_pandas()

    def filter_by_date_range(
        self, start_date: datetime, end_date: datetime
    ) -> pd.DataFrame:
        """Return records recorded within ``[start_date, end_date]`` inclusive."""
        if self.data is None or 'recorded_dttm' not in self.data.columns:
            return pd.DataFrame()
        return self.data.filter(
            pl.col('recorded_dttm').is_between(start_date, end_date, closed='both')
        ).to_pandas()

    def get_summary_stats(self) -> Dict:
        """Return summary statistics for the vitals data as a dict.

        Distinct from :meth:`get_vital_summary_stats`, which returns a
        per-category DataFrame; this is a single overview mapping.
        """
        if self.data is None:
            return {}

        data = self.data          # bound once; the table is never converted
        cols = data.columns
        has_category = 'vital_category' in cols

        vc_counts = {}
        if has_category:
            # pandas' value_counts() excludes NaN; polars' keeps null.
            vc = data.get_column('vital_category').drop_nulls().value_counts(sort=True)
            vc_counts = dict(zip(vc.get_column('vital_category').to_list(),
                                 vc.get_column('count').to_list()))

        stats = {
            'total_records': data.height,
            'unique_hospitalizations': (
                # drop_nulls before n_unique matches pandas' nunique()
                data.get_column('hospitalization_id').drop_nulls().n_unique()
                if 'hospitalization_id' in cols else 0
            ),
            'vital_category_counts': vc_counts,
            'date_range': {
                'earliest': (
                    data.get_column('recorded_dttm').min()
                    if 'recorded_dttm' in cols else None
                ),
                'latest': (
                    data.get_column('recorded_dttm').max()
                    if 'recorded_dttm' in cols else None
                ),
            },
        }

        if has_category and 'vital_value' in cols:
            vital_value_stats = {}
            # One grouped pass replaces the per-category filter loop, which
            # called filter_by_vital_category once per category.
            grouped = (
                data
                .drop_nulls(subset=['vital_category'])
                .with_columns(pl.col('vital_value').cast(pl.Float64, strict=False))
                .drop_nulls(subset=['vital_value'])
                .group_by('vital_category')
                .agg(
                    pl.len().cast(pl.Int64).alias('count'),
                    pl.col('vital_value').mean().alias('mean'),
                    pl.col('vital_value').min().alias('min'),
                    pl.col('vital_value').max().alias('max'),
                    pl.col('vital_value').std().alias('std'),
                )
            )
            by_cat = {r['vital_category']: r for r in grouped.iter_rows(named=True)}
            # Every category is reported, including ones whose values are all
            # non-numeric -- the pandas version emitted a zero/None block there.
            for vital_cat in self.get_vital_categories():
                row = by_cat.get(vital_cat)
                if row is None:
                    vital_value_stats[vital_cat] = {
                        'count': 0, 'mean': None, 'min': None, 'max': None, 'std': None,
                    }
                else:
                    vital_value_stats[vital_cat] = {
                        'count': row['count'],
                        'mean': round(row['mean'], 2),
                        'min': row['min'],
                        'max': row['max'],
                        # std() is None for a single value where pandas gave NaN.
                        'std': None if row['std'] is None else round(row['std'], 2),
                    }
            stats['vital_value_stats'] = vital_value_stats

        return stats

    def get_range_validation_report(self) -> pd.DataFrame:
        """Return :attr:`range_validation_errors` as a DataFrame.

        Reflects the last :meth:`validate` / :meth:`validate_vital_ranges` run;
        it does not trigger validation itself.
        """
        if not self.range_validation_errors:
            return pd.DataFrame(
                columns=['error_type', 'vital_category', 'affected_rows', 'message']
            )
        return pd.DataFrame(self.range_validation_errors)

    def get_vital_summary_stats(self) -> pd.DataFrame:
        """Return summary statistics for each vital category."""
        if self.data is None or 'vital_value' not in self.data.columns:
            return pd.DataFrame()

        # One row per category, so only the aggregate becomes pandas. The index
        # is restored because the pandas version returned a groupby result keyed
        # by vital_category.
        return (
            self.data
            .drop_nulls(subset=['vital_category'])
            .with_columns(pl.col('vital_value').cast(pl.Float64, strict=False))
            .group_by('vital_category')
            .agg(
                pl.col('vital_value').count().cast(pl.Int64).alias('count'),
                pl.col('vital_value').mean().alias('mean'),
                pl.col('vital_value').std().alias('std'),
                pl.col('vital_value').min().alias('min'),
                pl.col('vital_value').max().alias('max'),
                pl.col('vital_value').quantile(0.25, interpolation='linear').alias('q1'),
                pl.col('vital_value').quantile(0.50, interpolation='linear').alias('median'),
                pl.col('vital_value').quantile(0.75, interpolation='linear').alias('q3'),
            )
            .sort('vital_category')
            .to_pandas()
            .set_index('vital_category')
            .round(2)
        )
