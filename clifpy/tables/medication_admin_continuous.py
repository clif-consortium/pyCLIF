from typing import Any, Dict, List, Optional, Set, Tuple, Union
import pandas as pd
import polars as pl
from pyarrow import BooleanArray
from .base_table import BaseTable
import duckdb

class MedicationAdminContinuous(BaseTable):
    """
    Medication administration continuous table wrapper inheriting from BaseTable.
    
    This class handles medication administration continuous data and validations
    while leveraging the common functionality provided by BaseTable.
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
        Initialize the MedicationAdminContinuous table.
        
        This class handles continuous medication administration data, including validation,
        dose unit standardization, and unit conversion capabilities.
        
        Parameters
        ----------
        data_directory : str, optional
            Path to the directory containing data files. If None and data is provided,
            defaults to current directory.
        filetype : str, optional
            Type of data file (csv, parquet, etc.). If None and data is provided,
            defaults to 'parquet'.
        timezone : str, default="UTC"
            Timezone for datetime columns. Used for proper timestamp handling.
        output_directory : str, optional
            Directory for saving output files and logs. If not specified, outputs
            are saved to the current working directory.
        data : pd.DataFrame, optional
            Pre-loaded DataFrame to use instead of loading from file. Supports
            backward compatibility with direct DataFrame initialization.
        
        Notes
        -----
        The class supports two initialization patterns:
        1. Loading from file: provide data_directory and filetype
        2. Direct DataFrame: provide data parameter (legacy support)
        
        Upon initialization, the class loads medication schema data including
        category-to-group mappings from the YAML schema.
        """
        # For backward compatibility, handle the old signature
        if data_directory is None and filetype is None and data is not None:
            # Old signature: medication_admin_continuous(data)
            # Use dummy values for required parameters
            data_directory = "."
            filetype = "parquet"
        
        # Load medication mappings
        self._med_category_to_group = None
        
        super().__init__(
            data_directory=data_directory,
            filetype=filetype,
            timezone=timezone,
            output_directory=output_directory,
            data=data,
            clif_version=clif_version
        )
        
        # Load medication-specific schema data
        self._load_medication_schema_data()

    def _load_medication_schema_data(self):
        """
        Load medication-specific schema data from the YAML configuration.
        
        This method extracts medication category to group mappings from the loaded
        schema, which are used for medication classification and grouping operations.
        The mappings define relationships between medication categories (e.g., 'Antibiotics')
        and their broader therapeutic groups (e.g., 'Antimicrobials').
        
        The method is called automatically during initialization after the base
        schema is loaded.
        """
        if self.schema:
            self._med_category_to_group = self.schema.get('med_category_to_group_mapping', {})

    @property
    def med_category_to_group_mapping(self) -> Dict[str, str]:
        """
        Get the medication category to group mapping from the schema.
        
        Returns
        -------
        Dict[str, str]
            A dictionary mapping medication categories to their therapeutic groups.
            Returns a copy to prevent external modification of the internal mapping.
            Returns an empty dict if no mappings are loaded.
        
        Examples
        --------
        >>> mac = MedicationAdminContinuous(data)
        >>> mappings = mac.med_category_to_group_mapping
        >>> mappings['Antibiotics']
        'Antimicrobials'
        """
        return self._med_category_to_group.copy() if self._med_category_to_group else {}
    
    # Medication-specific methods can be added here if needed
    # The base functionality (validate, isvalid, from_file) is inherited from BaseTable
    
    @property
    def _acceptable_dose_unit_patterns(self) -> Set[str]:
        pass
    
    def resolve_mar_action_duplicates(self) -> pd.DataFrame:
        pass

    # ------------------------------------------------------------------
    # Medication Admin Continuous Specific Methods
    # ------------------------------------------------------------------
    @staticmethod
    def _value_counts(data, col):
        """value_counts().to_dict() with pandas' semantics.

        pandas excludes NaN and orders by descending count; polars keeps null
        as its own category, so the nulls are dropped explicitly.
        """
        vc = data.get_column(col).drop_nulls().value_counts(sort=True)
        return dict(zip(vc.get_column(col).to_list(), vc.get_column('count').to_list()))

    def get_med_categories(self) -> List[str]:
        """Return the unique medication categories present in the data."""
        if self.data is None or 'med_category' not in self.data.columns:
            return []
        # maintain_order reproduces pandas' unique(), which returns values in
        # order of first appearance; polars' unique() is unordered by default.
        return (
            self.data.get_column('med_category')
            .drop_nulls().unique(maintain_order=True).to_list()
        )

    def get_med_groups(self) -> List[str]:
        """Return the unique medication groups present in the data."""
        if self.data is None or 'med_group' not in self.data.columns:
            return []
        return (
            self.data.get_column('med_group')
            .drop_nulls().unique(maintain_order=True).to_list()
        )

    def filter_by_med_group(self, med_group: str) -> pd.DataFrame:
        """Return all records for one medication group."""
        if self.data is None or 'med_group' not in self.data.columns:
            return pd.DataFrame()
        # Filtered in polars; only the matching subset becomes pandas.
        return self.data.filter(pl.col('med_group') == med_group).to_pandas()

    def get_summary_stats(self) -> Dict[str, Any]:
        """Return summary statistics for the continuous medication data."""
        if self.data is None:
            return {}

        data = self.data          # bound once; the table is never converted
        cols = data.columns

        stats = {
            'total_records': data.height,
            'unique_hospitalizations': (
                # drop_nulls before n_unique matches pandas' nunique()
                data.get_column('hospitalization_id').drop_nulls().n_unique()
                if 'hospitalization_id' in cols else 0
            ),
            'med_category_counts': (
                self._value_counts(data, 'med_category') if 'med_category' in cols else {}
            ),
            'med_group_counts': (
                self._value_counts(data, 'med_group') if 'med_group' in cols else {}
            ),
            'date_range': {
                'earliest': (
                    data.get_column('admin_dttm').min() if 'admin_dttm' in cols else None
                ),
                'latest': (
                    data.get_column('admin_dttm').max() if 'admin_dttm' in cols else None
                ),
            },
        }

        if 'med_group' in cols and 'med_dose' in cols:
            dose_stats = {}
            # One grouped pass replaces the per-group filter loop, which called
            # filter_by_med_group twice per group.
            grouped = (
                data
                .drop_nulls(subset=['med_group'])
                .with_columns(pl.col('med_dose').cast(pl.Float64, strict=False))
                .drop_nulls(subset=['med_dose'])
                .group_by('med_group')
                .agg(
                    pl.len().alias('count'),
                    pl.col('med_dose').mean().alias('mean_dose'),
                    pl.col('med_dose').min().alias('min_dose'),
                    pl.col('med_dose').max().alias('max_dose'),
                )
            )
            by_group = {r['med_group']: r for r in grouped.iter_rows(named=True)}
            # Iterating get_med_groups() keeps the original insertion order.
            for group in self.get_med_groups():
                row = by_group.get(group)
                if row is not None:
                    dose_stats[group] = {
                        'count': row['count'],
                        'mean_dose': round(row['mean_dose'], 3),
                        'min_dose': row['min_dose'],
                        'max_dose': row['max_dose'],
                    }
            stats['dose_stats_by_group'] = dose_stats

        return stats
