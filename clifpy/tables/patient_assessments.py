from typing import Any, Dict, List, Optional
import pandas as pd
import polars as pl
from .base_table import BaseTable


class PatientAssessments(BaseTable):
    """
    Patient assessments table wrapper inheriting from BaseTable.
    
    This class handles patient assessment data and validations while
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
        Initialize the patient_assessments table.
        
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
            # Old signature: patient_assessments(data)
            # Use dummy values for required parameters
            data_directory = "."
            filetype = "parquet"
        
        # Initialize assessment mappings
        self._assessment_category_to_group = None
        self._assessment_score_ranges = None
        
        super().__init__(
            data_directory=data_directory,
            filetype=filetype,
            timezone=timezone,
            output_directory=output_directory,
            data=data,
            clif_version=clif_version
        )
        
        # Load assessment-specific schema data
        self._load_assessment_schema_data()

    def _load_assessment_schema_data(self):
        """Load assessment category to group mappings from the YAML schema."""
        if self.schema:
            self._assessment_category_to_group = self.schema.get('assessment_category_to_group_mapping', {})
        self._assessment_score_ranges = self._load_assessment_score_ranges()

    def _load_assessment_score_ranges(self) -> Dict[str, Dict[str, float]]:
        """Read per-category score ranges from the outlier configuration.

        These ranges used to live in the table schema. They now sit in
        ``outlier_config.yaml`` under ``patient_assessments.numerical_value``,
        which is the single source the outlier pipeline also reads, so the two
        cannot drift apart.
        """
        config = self.outlier_config or {}
        tables = config.get('tables', config)
        table_config = tables.get('patient_assessments') or {}
        return table_config.get('numerical_value') or {}

    @property
    def assessment_category_to_group_mapping(self) -> Dict[str, str]:
        """Get the assessment category to group mapping from the schema."""
        return self._assessment_category_to_group.copy() if self._assessment_category_to_group else {}

    @property
    def assessment_score_ranges(self) -> Dict[str, Dict[str, float]]:
        """Get the expected numerical score range for each assessment category."""
        return self._assessment_score_ranges.copy() if self._assessment_score_ranges else {}
    
    @staticmethod
    def _value_counts(data, col):
        """value_counts().to_dict() with pandas' semantics.

        pandas excludes NaN and orders by descending count; polars keeps null
        as its own category, so nulls are dropped explicitly.
        """
        vc = data.get_column(col).drop_nulls().value_counts(sort=True)
        return dict(zip(vc.get_column(col).to_list(), vc.get_column('count').to_list()))

    def get_assessment_categories(self) -> List[str]:
        """Return the unique assessment categories present in the data."""
        if self.data is None or 'assessment_category' not in self.data.columns:
            return []
        # maintain_order reproduces pandas' unique(), which returns values in
        # order of first appearance; polars' unique() is unordered by default.
        return (
            self.data.get_column('assessment_category')
            .drop_nulls().unique(maintain_order=True).to_list()
        )

    def filter_by_assessment_category(
        self, assessment_category: str
    ) -> pd.DataFrame:
        """Return all records for one assessment category."""
        if self.data is None or 'assessment_category' not in self.data.columns:
            return pd.DataFrame()
        # Filtered in polars; only the matching subset becomes pandas.
        return self.data.filter(
            pl.col('assessment_category') == assessment_category
        ).to_pandas()

    def get_summary_stats(self) -> Dict[str, Any]:
        """Return summary statistics for the patient assessments data."""
        if self.data is None:
            return {}

        data = self.data          # bound once; the table is never converted
        cols = data.columns
        has_category = 'assessment_category' in cols

        stats = {
            'total_records': data.height,
            'unique_hospitalizations': (
                # drop_nulls before n_unique matches pandas' nunique()
                data.get_column('hospitalization_id').drop_nulls().n_unique()
                if 'hospitalization_id' in cols else 0
            ),
            'assessment_category_counts': (
                self._value_counts(data, 'assessment_category') if has_category else {}
            ),
            'assessment_group_counts': (
                self._value_counts(data, 'assessment_group')
                if 'assessment_group' in cols else {}
            ),
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

        if has_category and 'numerical_value' in cols:
            numerical_stats = {}
            # One grouped pass replaces the per-category filter loop.
            # cast(strict=False) is polars' pd.to_numeric(errors='coerce').
            grouped = (
                data
                .drop_nulls(subset=['assessment_category'])
                .with_columns(pl.col('numerical_value').cast(pl.Float64, strict=False))
                .drop_nulls(subset=['numerical_value'])
                .group_by('assessment_category')
                .agg(
                    pl.len().alias('count'),
                    pl.col('numerical_value').mean().alias('mean'),
                    pl.col('numerical_value').min().alias('min'),
                    pl.col('numerical_value').max().alias('max'),
                    pl.col('numerical_value').std().alias('std'),
                )
            )
            by_cat = {r['assessment_category']: r for r in grouped.iter_rows(named=True)}
            # Iterating get_assessment_categories() keeps the original order.
            for assessment_cat in self.get_assessment_categories():
                row = by_cat.get(assessment_cat)
                if row is not None:
                    numerical_stats[assessment_cat] = {
                        'count': row['count'],
                        'mean': round(row['mean'], 2),
                        'min': row['min'],
                        'max': row['max'],
                        # std() is None for a single value where pandas gave
                        # NaN, and round(None) raises.
                        'std': float('nan') if row['std'] is None else round(row['std'], 2),
                    }
            stats['numerical_value_stats'] = numerical_stats

        return stats

    # Patient assessments-specific methods can be added here if needed
    # The base functionality (validate, isvalid, from_file) is inherited from BaseTable