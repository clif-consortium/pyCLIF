from typing import Optional, Dict, List, Union, Any
import os
import re
import pandas as pd
import polars as pl
from .base_table import BaseTable

# Pre-compiled regex patterns for unit normalization (compiled once at import)
_BRACKET_RE = re.compile(r'[\[\]\(\)]')
_CALC_RE = re.compile(r'\s*calc\s*$')
_UNIT_REPLACEMENTS = [
    (re.compile(r'\s+'), ''),           # Remove whitespace
    (re.compile(r'μ|µ'), 'u'),          # Greek mu to u
    (re.compile(r'\^'), ''),            # Remove caret
    (re.compile(r'\*'), ''),            # Remove asterisk
    (re.compile(r'hours?'), 'hr'),      # hour/hours -> hr
    (re.compile(r'seconds?'), 'sec'),   # second/seconds -> sec
    (re.compile(r'^s$'), 'sec'),        # lone 's' -> sec
    (re.compile(r'minutes?'), 'min'),   # minute/minutes -> min
    (re.compile(r'iu'), 'u'),           # iU -> U
    (re.compile(r'\bgrams?\b'), 'g'),   # gram/grams -> g
    (re.compile(r'\bgm\b'), 'g'),       # gm -> g
    (re.compile(r'k/ul'), '103/ul'),    # k/uL -> 10^3/uL
    (re.compile(r'10e3'), '103'),       # 10e3 -> 10^3
    (re.compile(r'x10e3'), '103'),      # x10E3 -> 10^3
    (re.compile(r'x103'), '103'),       # x10^3 -> 10^3
    (re.compile(r'\bpg/ml\b'), 'ng/l'),  # if it's only 'pg/ml', then it becomes 'ng/l'
    (re.compile(r','), ''),             # Remove commas
]


class Labs(BaseTable):
    """
    Labs table wrapper inheriting from BaseTable.
    
    This class handles laboratory data and validations including
    reference unit validation while leveraging the common functionality
    provided by BaseTable.
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
        Initialize the labs table.
        
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
            # Old signature: labs(data)
            # Use dummy values for required parameters
            data_directory = "."
            filetype = "parquet"
        
        # Initialize lab reference units
        self._lab_reference_units = None
        self._allowed_unit_variants = {}

        super().__init__(
            data_directory=data_directory,
            filetype=filetype,
            timezone=timezone,
            output_directory=output_directory,
            data=data,
            clif_version=clif_version
        )

        # Load lab-specific schema data
        self._load_labs_schema_data()

    def _load_labs_schema_data(self):
        """Load lab reference units from the YAML schema."""
        if self.schema:
            self._lab_reference_units = self.schema.get('lab_reference_units', {})
            # Normalize variant-map keys to lowercased+stripped for robust lookup.
            variants_map = self.schema.get('allowed_unit_variants') or {}
            self._allowed_unit_variants = {
                str(k).lower().strip(): v for k, v in variants_map.items()
            }

    @property
    def lab_reference_units(self) -> Dict[str, Any]:
        """Get the schema-declared reference units mapping.

        This is the units the CLIF schema *expects* per ``lab_category``. It is
        distinct from :meth:`get_lab_reference_units`, which reports the units
        actually *observed* in the loaded data.
        """
        return self._lab_reference_units.copy() if self._lab_reference_units else {}

    def _resolve_target_groups(self, entry) -> list:
        """Return ``[(preferred_canonical, accepted_spellings), ...]`` for one
        ``lab_reference_units`` entry, one group per reference unit.

        ``entry`` is a canonical unit key, or a list of them for analytes CLIF
        reports in more than one unit (albumin is g/dl in blood, mg/dl in
        urine). Each key is expanded via ``allowed_unit_variants`` (always with
        the canonical itself first). A list in a schema without that map is the
        legacy format: pre-expanded spellings of one unit, preferring the first.
        """
        if isinstance(entry, str):
            entry = [entry]
        if not isinstance(entry, (list, tuple)) or not entry:
            return []
        if not self._allowed_unit_variants:
            return [(entry[0], list(entry))]
        groups = []
        for canonical in entry:
            variants = self._allowed_unit_variants.get(canonical.lower().strip()) or []
            groups.append((canonical, list(dict.fromkeys([canonical, *variants]))))
        return groups

    def _validate_required_columns(self, required: set) -> set:
        """Check for required columns, return set of missing columns."""
        return required - set(self.data.columns)

    def _to_lazy_frame(self) -> 'pl.LazyFrame':
        """Return the stored frame as a Polars LazyFrame."""
        # ``self.data`` is always a polars DataFrame (BaseTable converts on the way
        # in), so this is a free .lazy() rather than the polars -> pandas -> polars
        # round-trip that reading .df used to force. That round-trip also left a
        # cached pandas copy of the whole table on the object as a side effect.
        return self.data.lazy()

    def _get_lab_reference_units_polars(self) -> 'pl.DataFrame':
        """
        Polars-optimized implementation for get_lab_reference_units.

        Uses lazy evaluation and streaming to efficiently process large datasets
        without loading everything into memory at once.

        Returns
        -------
        pl.DataFrame
            Aggregated counts by (lab_category, reference_unit).
        """

        required = {'lab_category', 'reference_unit'}
        missing = self._validate_required_columns(required)
        if missing:
            self.logger.warning(f"Missing columns: {missing} - cannot compute reference units")
            return pl.DataFrame(schema={'lab_category': pl.Utf8, 'reference_unit': pl.Utf8, 'count': pl.UInt32})

        cols = ['lab_category', 'reference_unit']
        lf = self._to_lazy_frame().select(cols)

        # Build and execute query with streaming
        return (
            lf
            .group_by(['lab_category', 'reference_unit'])
            .agg(pl.len().alias('count'))
            .sort(['lab_category', 'reference_unit'])
            .collect(engine="streaming")
        )

    def _get_lab_reference_units_pandas(self) -> 'pd.DataFrame':
        """
        Pandas implementation for get_lab_reference_units.

        Fallback for systems where Polars is not available.

        Returns
        -------
        pd.DataFrame
            Aggregated counts by (lab_category, reference_unit).
        """
        required = {'lab_category', 'reference_unit'}
        missing = self._validate_required_columns(required)
        if missing:
            self.logger.warning(f"Missing columns: {missing} - cannot compute reference units")
            return pd.DataFrame(columns=['lab_category', 'reference_unit', 'count'])

        # One row per (category, unit) pair; only that aggregate becomes pandas.
        # drop_nulls on the group keys matches pandas' groupby(dropna=True).
        return (
            self.data
            .drop_nulls(subset=['lab_category', 'reference_unit'])
            .group_by(['lab_category', 'reference_unit'])
            .agg(pl.len().cast(pl.Int64).alias('count'))
            .sort(['lab_category', 'reference_unit'])
            .to_pandas()
        )

    def get_lab_reference_units(
        self,
        save: bool = False,
        output_directory: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Get all unique reference units observed in the data,
        grouped by lab_category along with their counts.

        Uses Polars for efficient processing of large datasets, with automatic
        fallback to pandas if Polars is unavailable or fails.

        Parameters
        ----------
        save : bool, default False
            If True, save the results to the output directory as a CSV file.
        output_directory : str, optional
            Directory to save results. If None, uses self.output_directory.

        Returns
        -------
        pd.DataFrame
            DataFrame with columns: ['lab_category', 'reference_unit', 'count']
        """
        if self.data is None:
            raise ValueError("No data")

        # Try Polars first (more efficient for large data), fall back to pandas
        try:
            result_pl = self._get_lab_reference_units_polars()
            result_df = result_pl.to_pandas()
            self.logger.debug("Used Polars for get_lab_reference_units")
        except Exception as e:
            self.logger.debug(f"Polars failed ({e}), falling back to pandas")
            result_df = self._get_lab_reference_units_pandas()

        if save:
            save_dir = output_directory if output_directory is not None else self.output_directory
            os.makedirs(save_dir, exist_ok=True)
            csv_path = os.path.join(save_dir, 'lab_reference_units.csv')
            result_df.to_csv(csv_path, index=False)
            self.logger.info(f"Saved lab reference units to {csv_path}")

        return result_df


    def _normalize_unit(self, unit: str) -> str:
        """
        Normalize a unit string for comparison by removing special characters,
        standardizing common variations, and lowercasing.
        """
        if not isinstance(unit, str):
            return ""

        normalized = unit.lower().strip()
        normalized = _BRACKET_RE.sub('', normalized)
        normalized = _CALC_RE.sub('', normalized)

        for pattern, repl in _UNIT_REPLACEMENTS:
            normalized = pattern.sub(repl, normalized)

        return normalized

    def _find_matching_target_unit(
        self,
        source_unit: str,
        target_units: List[str],
        preferred: Optional[str] = None,
    ) -> Optional[str]:
        """
        Find the best matching target unit for a source unit using normalized comparison.

        Parameters
        ----------
        source_unit : str
            The unit string from the data
        target_units : List[str]
            List of acceptable target units from schema.
        preferred : str, optional
            The canonical unit to emit on a normalized match. Defaults to the
            first element of ``target_units`` (legacy behavior).

        Returns
        -------
        Optional[str]
            The matching target unit, or None if no match found
        """
        if not source_unit or not target_units:
            return None

        if preferred is None:
            preferred = target_units[0]

        normalized_source = self._normalize_unit(source_unit)

        # Check for exact match first
        if source_unit in target_units:
            return source_unit

        # Check normalized matches against all target units
        for target in target_units:
            if self._normalize_unit(target) == normalized_source:
                return preferred

        return None

    def _build_unit_mapping(
        self,
        unique_combos_df: pd.DataFrame,
        lowercase: bool
    ) -> tuple:
        """
        Build unit mapping dictionary from unique lab_category + reference_unit combinations.

        Returns tuple of (unit_mapping dict, mappings_applied list, unmatched_units list)
        """
        unit_mapping = {}
        mappings_applied = []
        unmatched_units = []
        loggable_mappings = []  # Batch logging at the end

        for lab_cat, source_unit in unique_combos_df.itertuples(index=False):
            if pd.isna(source_unit):
                continue

            groups = self._resolve_target_groups(self._lab_reference_units.get(lab_cat))
            if not groups:
                continue

            # Relabel to the unit whose spellings matched, so a urine albumin
            # in mg/dL becomes mg/dl rather than being relabelled g/dl.
            matched_target = None
            for canonical, accepted in groups:
                matched_target = self._find_matching_target_unit(
                    source_unit, accepted, preferred=canonical,
                )
                if matched_target:
                    break

            if matched_target:
                final_target = matched_target.lower() if lowercase else matched_target

                if final_target != source_unit:
                    unit_mapping[(lab_cat, source_unit)] = final_target

                    # Check if change is cosmetic (mu char or case only)
                    is_mu_only_diff = source_unit.replace('µ', 'μ') == matched_target
                    is_case_only_diff = source_unit.lower() == matched_target.lower()
                    is_silent = is_mu_only_diff or (lowercase and is_case_only_diff)

                    mappings_applied.append({
                        'lab_category': lab_cat,
                        'source_unit': source_unit,
                        'target_unit': final_target,
                        'silent': is_silent
                    })

                    if not is_silent:
                        loggable_mappings.append((source_unit, final_target, lab_cat))

            else:
                unmatched_units.append({
                    'lab_category': lab_cat,
                    'source_unit': source_unit,
                    'expected_units': [u for _, accepted in groups for u in accepted]
                })

        # Batch log all mappings at once
        for source, target, lab in loggable_mappings:
            self.logger.info(f"Mapping '{source}' -> '{target}' for {lab}")

        return unit_mapping, mappings_applied, unmatched_units

    def _standardize_reference_units_polars(
        self,
        unit_mapping: Dict,
        lowercase: bool
    ) -> 'pl.DataFrame':
        """
        Polars-optimized implementation for standardize_reference_units.

        Uses join-based mapping for O(n) performance instead of O(n*k) chained conditions.
        Always returns a new DataFrame; caller handles inplace assignment.
        """

        lf = self._to_lazy_frame()

        # Apply mappings using join (O(n) hash join vs O(n*k) chained conditions)
        if unit_mapping:
            mapping_df = pl.DataFrame({
                'lab_category': [k[0] for k in unit_mapping.keys()],
                '_source_unit': [k[1] for k in unit_mapping.keys()],
                '_target_unit': list(unit_mapping.values())
            }).lazy()

            lf = (
                lf
                .join(
                    mapping_df,
                    left_on=['lab_category', 'reference_unit'],
                    right_on=['lab_category', '_source_unit'],
                    how='left'
                )
                .with_columns(
                    pl.coalesce('_target_unit', 'reference_unit').alias('reference_unit')
                )
                .drop('_target_unit')
            )

        if lowercase:
            lf = lf.with_columns(
                pl.col('reference_unit').str.to_lowercase()
            )

        return lf.collect(engine="streaming")

    def _standardize_reference_units_pandas(
        self,
        unit_mapping: Dict,
        lowercase: bool
    ) -> pd.DataFrame:
        """
        Pandas implementation for standardize_reference_units.

        Uses merge-based mapping for O(n) performance instead of O(n*k) row-wise apply.
        Always returns a new DataFrame; caller handles inplace assignment.
        """
        # This fallback is a pandas merge pipeline, so it stays pandas.
        # Converting from .data explicitly gives the same frame without
        # populating -- and then permanently holding -- the table's cached
        # pandas view. to_pandas() already returns a fresh frame.
        df = self.data.to_pandas()

        # Apply mappings using merge (O(n) vs O(n*k) for apply)
        if unit_mapping:
            mapping_df = pd.DataFrame([
                {'lab_category': k[0], '_source_unit': k[1], '_target_unit': v}
                for k, v in unit_mapping.items()
            ])

            df = df.merge(
                mapping_df,
                left_on=['lab_category', 'reference_unit'],
                right_on=['lab_category', '_source_unit'],
                how='left'
            )
            df['reference_unit'] = df['_target_unit'].fillna(df['reference_unit'])
            df = df.drop(columns=['_source_unit', '_target_unit'])

        if lowercase:
            df['reference_unit'] = df['reference_unit'].str.lower()

        return df

    def standardize_reference_units(
        self,
        inplace: bool = True,
        save: bool = False,
        lowercase: bool = False,
        output_directory: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        Standardize reference unit strings to match the schema's target units.

        Uses Polars for efficient processing of large datasets, with automatic
        fallback to pandas if Polars is unavailable or fails.

        Uses fuzzy matching to detect similar unit strings (e.g., 'mmhg' -> 'mmHg',
        '10*3/ul' -> '10^3/μL', 'hr' -> 'hour') and converts them to the preferred
        target unit defined in the schema.

        This does NOT perform value conversions between different unit types
        (e.g., mg/dL to mmol/L). Units that don't match any target will be logged
        as warnings.

        Parameters
        ----------
        inplace : bool, default True
            If True, modify the table's data in place. If False, return a copy.
        save : bool, default False
            If True, save a CSV of the unit mappings applied to the output directory.
        lowercase : bool, default False
            If True, convert all reference units to lowercase instead of using
            the schema's original casing (e.g., 'mg/dl' instead of 'mg/dL').
        output_directory : str, optional
            Directory to save results. If None, uses self.output_directory.

        Returns
        -------
        Optional[pd.DataFrame]
            If inplace=False, returns the modified DataFrame. Otherwise None.
        """
        if self.data is None:
            raise ValueError(
                "No data loaded. Please provide data using one of these methods:\n"
                "  1. Labs.from_file(data_directory=..., filetype=..., timezone=...)\n"
                "  2. Labs(data=your_dataframe)"
            )

        # Check for required columns
        missing = self._validate_required_columns({'lab_category', 'reference_unit'})
        if missing:
            raise ValueError(f"Required columns not found: {missing}")

        if not self._lab_reference_units:
            self.logger.warning("No lab reference units defined in schema")
            return None

        # Only the distinct (lab_category, reference_unit) pairs cross into pandas
        # for _build_unit_mapping -- a handful of rows, not the table.
        unique_combos_df = (
            self.data
            .select(['lab_category', 'reference_unit'])
            .unique()
            .to_pandas()
        )

        # Build mapping dictionary (shared logic)
        unit_mapping, mappings_applied, unmatched_units = self._build_unit_mapping(
            unique_combos_df, lowercase
        )

        # Try Polars first, fall back to pandas
        try:
            result_df = self._standardize_reference_units_polars(unit_mapping, lowercase)
            self.logger.debug("Used Polars for standardize_reference_units")
        except Exception as e:
            self.logger.debug(f"Polars failed ({e}), falling back to pandas")
            result_df = self._standardize_reference_units_pandas(unit_mapping, lowercase)

        # Handle inplace at API level. result_df is polars from the primary
        # path and pandas from the fallback; the .data setter takes either.
        if inplace:
            self.data = result_df

        # Log results
        if unit_mapping:
            actual_mappings = [m for m in mappings_applied if not m.get('silent')]
            if actual_mappings:
                self.logger.info(f"Applied {len(actual_mappings)} unit standardizations")
        elif not lowercase:
            self.logger.info("No unit standardizations needed")

        # Warn about unmatched units
        for item in unmatched_units:
            self.logger.warning(
                f"Unmatched unit '{item['source_unit']}' for {item['lab_category']}. "
                f"Expected one of: {item['expected_units']}"
            )

        # Save mapping if requested
        if save and mappings_applied:
            save_dir = output_directory if output_directory is not None else self.output_directory
            os.makedirs(save_dir, exist_ok=True)
            mapping_df = pd.DataFrame(mappings_applied)
            csv_path = os.path.join(save_dir, 'lab_reference_unit_standardized.csv')
            mapping_df.to_csv(csv_path, index=False)
            self.logger.info(f"Saved unit mappings to {csv_path}")

        if not inplace:
            # Convert to pandas if returning
            try:
                if isinstance(result_df, pl.DataFrame):
                    return result_df.to_pandas()
            except ImportError:
                pass
            return result_df

        return None

    # ------------------------------------------------------------------
    # Labs Specific Methods
    # ------------------------------------------------------------------
    def get_lab_category_stats(self) -> pd.DataFrame:
        """Return summary statistics for each lab category, including missingness and unique hospitalization_id counts."""
        if (
            self.data is None
            or 'lab_value_numeric' not in self.data.columns
            or 'hospitalization_id' not in self.data.columns        # remove this line if hosp-id is optional
        ):
            return {"status": "Missing columns"}
        
        # Aggregated in polars -- one row per category -- so the table itself
        # is never converted. drop_nulls on the group key matches pandas'
        # groupby(dropna=True); drop_nulls before n_unique matches nunique().
        stats = (
            self.data
            .drop_nulls(subset=['lab_category'])            .group_by('lab_category')
            .agg(
                pl.col('lab_value_numeric').count().cast(pl.Int64).alias('count'),
                pl.col('hospitalization_id').drop_nulls().n_unique().cast(pl.Int64).alias('unique'),
                (100 * pl.col('lab_value_numeric').is_null().mean()).alias('missing_pct'),
                pl.col('lab_value_numeric').mean().alias('mean'),
                pl.col('lab_value_numeric').std().alias('std'),
                pl.col('lab_value_numeric').min().alias('min'),
                pl.col('lab_value_numeric').quantile(0.25, interpolation='linear').alias('q1'),
                pl.col('lab_value_numeric').median().alias('median'),
                pl.col('lab_value_numeric').quantile(0.75, interpolation='linear').alias('q3'),
                pl.col('lab_value_numeric').max().alias('max'),
            )
            .sort('lab_category')
            .to_pandas()
            .set_index('lab_category')
            .round(2)
        )

        return stats
    
    def get_lab_specimen_stats(self) -> pd.DataFrame:
        """Return summary statistics for each lab category, including missingness and unique hospitalization_id counts."""
        if (
            self.data is None
            or 'lab_value_numeric' not in self.data.columns
            or 'hospitalization_id' not in self.data.columns 
            or 'lab_speciment_category' not in self.data.columns       # remove this line if hosp-id is optional
        ):
            return {"status": "Missing columns"}
        
        stats = (
            self.data
            .drop_nulls(subset=['lab_specimen_category'])            .group_by('lab_specimen_category')
            .agg(
                pl.col('lab_value_numeric').count().cast(pl.Int64).alias('count'),
                pl.col('hospitalization_id').drop_nulls().n_unique().cast(pl.Int64).alias('unique'),
                (100 * pl.col('lab_value_numeric').is_null().mean()).alias('missing_pct'),
                pl.col('lab_value_numeric').mean().alias('mean'),
                pl.col('lab_value_numeric').std().alias('std'),
                pl.col('lab_value_numeric').min().alias('min'),
                pl.col('lab_value_numeric').quantile(0.25, interpolation='linear').alias('q1'),
                pl.col('lab_value_numeric').median().alias('median'),
                pl.col('lab_value_numeric').quantile(0.75, interpolation='linear').alias('q3'),
                pl.col('lab_value_numeric').max().alias('max'),
            )
            .sort('lab_specimen_category')
            .to_pandas()
            .set_index('lab_specimen_category')
            .round(2)
        )

        return stats