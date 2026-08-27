from typing import Dict, Optional, Set, Tuple
import pandas as pd
from .base_table import BaseTable


class MicrobiologyNonculture(BaseTable):
    """
    Microbiology non-culture table wrapper inheriting from BaseTable.
    
    This class handles microbiology non-culture test data including PCR
    and other molecular diagnostic results while leveraging the common 
    functionality provided by BaseTable.
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
        Initialize the microbiology non-culture table.
        
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

    # ------------------------------------------------------------------
    # Schema-derived category vocabularies
    # ------------------------------------------------------------------

    _RESULT_STANDARDIZATION = {
        'detected': 'positive',
        'not_detected': 'negative',
        'indeterminate': 'indeterminate',
    }

    def _permissible_values(self, column: str) -> Set[str]:
        """Return the permissible values the schema declares for ``column``."""
        if not self.schema:
            return set()
        for col in self.schema.get('columns', []):
            if col.get('name') == column:
                return set(col.get('permissible_values') or [])
        return set()

    @property
    def _acceptable_organism_categories(self) -> Set[str]:
        """Permissible ``organism_category`` values for this CLIF version."""
        return self._permissible_values('organism_category')

    @property
    def _acceptable_result_categories(self) -> Set[str]:
        """Permissible ``result_category`` values for this CLIF version."""
        return self._permissible_values('result_category')

    @property
    def _acceptable_fluid_categories(self) -> Set[str]:
        """Permissible ``fluid_category`` values for this CLIF version."""
        return self._permissible_values('fluid_category')

    # ------------------------------------------------------------------
    # Normalization
    # ------------------------------------------------------------------

    def _resolve_frame(self, df: Optional[pd.DataFrame]) -> pd.DataFrame:
        """Return a working copy of ``df``, falling back to the table's own data."""
        if df is None:
            # The normalization helpers below are pandas-native and callers may
            # pass their own pandas frame, so this converts from .data rather
            # than reading .df, which would cache a pandas copy on the table.
            df = None if self.data is None else self.data.to_pandas()
            if df is None:
                raise ValueError("No data provided")
            return df          # already a fresh frame; no copy needed
        return df.copy()

    def _normalize_category_column(
        self,
        df: Optional[pd.DataFrame],
        column: str
    ) -> Tuple[pd.DataFrame, Dict[str, int]]:
        """Lowercase/strip one category column and report values off the mCIDE list.

        Returns ``(frame, unrecognized)`` where ``unrecognized`` maps each value
        that is not permissible to the number of rows carrying it. An empty dict
        means every non-null value is recognized.
        """
        frame = self._resolve_frame(df)
        if column not in frame.columns:
            return frame, {}

        normalized = frame[column].astype('string').str.strip().str.lower()
        frame[column] = normalized

        acceptable = self._permissible_values(column)
        if not acceptable:
            return frame, {}

        offending = normalized[normalized.notna() & ~normalized.isin(acceptable)]
        unrecognized = {str(k): int(v) for k, v in offending.value_counts().items()}
        return frame, unrecognized

    def _normalize_organism_names(
        self, df: Optional[pd.DataFrame] = None
    ) -> Tuple[pd.DataFrame, Dict[str, int]]:
        """Normalize ``organism_category`` and report unrecognized organisms."""
        return self._normalize_category_column(df, 'organism_category')

    def _normalize_result_categories(
        self, df: Optional[pd.DataFrame] = None
    ) -> Tuple[pd.DataFrame, Dict[str, int]]:
        """Normalize ``result_category`` and report unrecognized results."""
        return self._normalize_category_column(df, 'result_category')

    # ------------------------------------------------------------------
    # Standardization
    # ------------------------------------------------------------------

    def _organism_group_lookup(self) -> Dict[str, str]:
        """Build ``{organism_category: organism_group}`` from the loaded table.

        The CLIF schema declares both vocabularies but not the mapping between
        them, so the table's own data is the only available source. Frames that
        already carry ``organism_group`` never consult this.
        """
        source = self.data
        if source is None:
            return {}
        if not {'organism_category', 'organism_group'}.issubset(source.columns):
            return {}
        # Distinct pairs only -- a handful of rows -- so the lookup is built
        # without converting the table to pandas at all.
        pairs = (
            source
            .select(['organism_category', 'organism_group'])
            .drop_nulls()
            .unique()
        )
        return dict(zip(pairs['organism_category'].to_list(),
                        pairs['organism_group'].to_list()))

    def standardize_test_results(
        self, df: Optional[pd.DataFrame] = None
    ) -> pd.DataFrame:
        """Normalize categories and add ``organism_group`` / ``standardized_result``.

        ``standardized_result`` collapses the mCIDE result vocabulary onto
        ``positive`` / ``negative``; anything the schema does not recognize
        becomes ``unknown`` rather than being dropped, so unmapped values stay
        visible downstream.

        Parameters
        ----------
        df : pd.DataFrame, optional
            Frame to standardize. Defaults to the table's own data.

        Returns
        -------
        pd.DataFrame
            A copy with ``organism_group`` and ``standardized_result`` added.
        """
        frame, unrecognized_organisms = self._normalize_organism_names(df)
        frame, unrecognized_results = self._normalize_result_categories(frame)

        if unrecognized_organisms:
            self.logger.warning(
                f"Organism validation issues found: {unrecognized_organisms}"
            )
        if unrecognized_results:
            self.logger.warning(
                f"Result validation issues found: {unrecognized_results}"
            )

        if 'organism_group' not in frame.columns:
            if 'organism_category' in frame.columns:
                lookup = self._organism_group_lookup()
                frame['organism_group'] = (
                    frame['organism_category'].map(lookup).fillna('unknown')
                )
            else:
                frame['organism_group'] = 'unknown'

        if 'result_category' in frame.columns:
            frame['standardized_result'] = (
                frame['result_category']
                .map(self._RESULT_STANDARDIZATION)
                .fillna('unknown')
            )
        else:
            frame['standardized_result'] = 'unknown'

        return frame

    # ------------------------------------------------------------------
    # Summaries
    # ------------------------------------------------------------------

    def get_test_summary_by_organism_group(
        self, df: Optional[pd.DataFrame] = None
    ) -> pd.DataFrame:
        """Summarize positive/negative test counts per ``organism_group``.

        ``total_tests`` counts every row in the group, so it exceeds
        ``positive_tests + negative_tests`` whenever a group contains
        indeterminate or unrecognized results. ``positive_rate`` is a fraction
        of ``total_tests``.

        Returns
        -------
        pd.DataFrame
            Columns ``organism_group``, ``total_tests``, ``positive_tests``,
            ``negative_tests``, ``positive_rate``; empty in, empty out.
        """
        columns = [
            'organism_group', 'total_tests', 'positive_tests',
            'negative_tests', 'positive_rate',
        ]

        frame = self.standardize_test_results(df)
        if frame.empty:
            return pd.DataFrame(columns=columns)

        flags = frame.assign(
            _positive=(frame['standardized_result'] == 'positive').astype(int),
            _negative=(frame['standardized_result'] == 'negative').astype(int),
        )

        summary = (
            flags.groupby('organism_group', dropna=False)
            .agg(
                total_tests=('standardized_result', 'size'),
                positive_tests=('_positive', 'sum'),
                negative_tests=('_negative', 'sum'),
            )
            .reset_index()
        )
        summary['positive_rate'] = (
            summary['positive_tests'] / summary['total_tests']
        ).where(summary['total_tests'] > 0, 0.0)

        return summary[columns].sort_values('organism_group').reset_index(drop=True)
