"""
BaseTable class for pyCLIF tables.

This module provides the base class that all pyCLIF table classes inherit from.
It handles common functionality including data loading, validation, and reporting.
"""

import os
import logging
import pandas as pd
import polars as pl
import pyarrow as pa
import yaml
import numpy as np
from typing import Optional, List, Dict, Any, Tuple
from pathlib import Path
from datetime import datetime

from ..utils.io import load_data
from ..utils import validator
from ..utils.outlier_handler import _load_outlier_config
from ..utils.config import get_config_or_params
from ..utils.logging_config import setup_logging
from ..schemas import DEFAULT_CLIF_VERSION, load_schema


logger = logging.getLogger(__name__)


def _coerce_unconvertible_object_columns(
    frame: pd.DataFrame,
) -> Tuple[pd.DataFrame, List[str]]:
    """Cast pandas ``object`` columns that Arrow cannot convert to text.

    Arrow allows one type per column, while a pandas ``object`` column is an array
    of Python pointers and may hold a different type in every cell -- e.g. a stray
    ``'invalid_number'`` among floats. Casting such a column to text keeps the bad
    values visible so the DQA dtype checks can *report* them, instead of the
    conversion failing at construction and hiding the very problem DQA exists to
    find.

    Columns are tested one at a time so only genuinely unconvertible ones are
    touched -- a benign object column in the same frame keeps its dtype. Nulls stay
    null: pandas' nullable ``string`` dtype maps them to ``pd.NA``, whereas a bare
    ``.astype(str)`` would write the literal ``"nan"``.

    Returns the frame -- copied only if something changed -- and the names of the
    columns that were cast.
    """
    offenders = []
    for col in frame.select_dtypes(include='object').columns:
        try:
            pl.from_pandas(frame[[col]])
        except pa.ArrowInvalid:
            offenders.append(col)

    if not offenders:
        return frame, []

    coerced = frame.copy()
    for col in offenders:
        coerced[col] = coerced[col].astype('string')
    return coerced, offenders


class BaseTable:
    """
    Base class for all pyCLIF table classes.
    
    Provides common functionality for loading data, running validations,
    and generating reports. All table-specific classes should inherit from this.
    
    Attributes
    ----------
    data_directory : str
        Path to the directory containing data files
    filetype : str
        Type of data file (csv, parquet, etc.)
    timezone : str
        Timezone for datetime columns
    output_directory : str
        Directory for saving output files and logs
    table_name : str
        Name of the table (from class name)
    df : pd.DataFrame
        The loaded data, converted from the stored polars frame on access
    data : pl.DataFrame
        The loaded data as stored (polars)
    schema : dict
        The YAML schema for this table
    errors : List[dict]
        Validation errors from last validation run
    logger : logging.Logger
        Logger for this table
    """
    
    def __init__(
        self, 
        data_directory: str,
        filetype: str,
        timezone: str,
        output_directory: Optional[str] = None,
        data: Optional[pd.DataFrame] = None,
        clif_version: str = DEFAULT_CLIF_VERSION
    ):
        """
        Initialize the BaseTable.

        Parameters
        ----------
        data_directory : str
            Path to the directory containing data files
        filetype : str
            Type of data file (csv, parquet, etc.)
        timezone : str
            Timezone for datetime columns
        output_directory : str, optional
            Directory for saving output files and logs.
            If not provided, creates an 'output' directory in the current working directory.
        data : pd.DataFrame, optional
            Pre-loaded data to use instead of loading from file
        clif_version : str, optional
            CLIF schema version to validate against (e.g. "2.1", "3.0").
            Defaults to the package default (3.0).
        """
        # Store configuration
        self.data_directory = data_directory
        self.filetype = filetype
        self.timezone = timezone
        self.clif_version = clif_version or DEFAULT_CLIF_VERSION
        
        # Set output directory
        if output_directory is None:
            output_directory = os.path.join(os.getcwd(), 'output')
        self.output_directory = output_directory
        os.makedirs(self.output_directory, exist_ok=True)

        # Initialize centralized logging
        setup_logging(output_directory=self.output_directory)

        # Derive snake_case table name from PascalCase class name
        # Example: Adt -> adt, RespiratorySupport -> respiratory_support
        self.table_name = ''.join(['_' + c.lower() if c.isupper() else c for c in self.__class__.__name__]).lstrip('_')

        # Data is stored as polars (see the ``df`` property below); pandas and
        # polars are both accepted here and normalized on the way in.
        self._data: Optional[pl.DataFrame] = None
        self._df_pandas: Optional[pd.DataFrame] = None
        self.data = data

        self.errors: List[Dict[str, Any]] = []
        self.warnings: List[Dict[str, Any]] = []
        self.schema: Optional[Dict[str, Any]] = None
        self.outlier_config: Optional[Dict[str, Any]] = None
        self._validated: bool = False

        # Setup table-specific logging
        self._setup_logging()

        # Load schema
        self._load_schema()

        # Load outlier config
        self._load_outlier_config()


    # ------------------------------------------------------------------
    # Data access
    # ------------------------------------------------------------------
    # The canonical frame is polars (``self._data``), read and written through
    # ``data``. ``df`` remains as the pandas view: its getter converts once and
    # caches, because modules outside clifpy/tables/ and downstream analyses
    # still read it that way; its setter is a pure alias for ``data``'s.
    # Code that never touches ``.df`` never builds the pandas copy.

    @property
    def df(self) -> Optional[pd.DataFrame]:
        """The table as a pandas DataFrame (converted from polars on first access).

        Prefer :attr:`data` when writing new code -- it is the stored frame and
        needs no conversion.
        """
        if self._data is None:
            return None
        if self._df_pandas is None:
            self._df_pandas = self._data.to_pandas()
        return self._df_pandas

    @df.setter
    def df(self, value) -> None:
        """Write through to :attr:`data`.

        Kept because ``self.df = <frame>`` is the historical write path and is
        still used across the package and by downstream callers. It converts
        nothing on the way in -- the work is in the ``data`` setter -- so this
        is a pure alias and is unaffected by ``.df``'s pandas-returning getter.
        """
        self.data = value

    @property
    def data(self) -> Optional[pl.DataFrame]:
        """The table as a polars DataFrame -- the stored representation."""
        return self._data

    @data.setter
    def data(self, value) -> None:
        """Store a frame, accepting pandas, polars, or a polars LazyFrame.

        This is the canonical write path. Assigning polars stores it as-is;
        pandas is converted once here so everything downstream is polars.
        """
        if value is None:
            self._data = None
        elif isinstance(value, pl.DataFrame):
            self._data = value
        elif isinstance(value, pl.LazyFrame):
            self._data = value.collect()
        elif isinstance(value, pd.DataFrame):
            try:
                self._data = pl.from_pandas(value)
            except pa.ArrowInvalid:
                # A mixed-type object column. Cast the offenders to text so the DQA
                # dtype checks can report them -- crashing here would hide exactly
                # the kind of dirty data validation is meant to surface. Anything
                # else Arrow rejects is still a hard error.
                coerced, offenders = _coerce_unconvertible_object_columns(value)
                if not offenders:
                    raise
                logger.warning(
                    "%s: column(s) %s hold more than one type and were read as "
                    "text so validation can report them; the original dtype of "
                    "those columns is lost.",
                    self.__class__.__name__, ', '.join(offenders),
                )
                self._data = pl.from_pandas(coerced)
        else:
            raise TypeError(
                f"{self.__class__.__name__}.data accepts a pandas DataFrame, polars "
                f"DataFrame, or polars LazyFrame; got {type(value).__name__}."
            )
        self._df_pandas = None  # invalidate the cached conversion

    def _warn_if_pandas_view_diverged(self) -> None:
        """Warn when the cached pandas view has been edited in place.

        ``.df`` hands back a cached pandas *conversion*. Editing that frame --
        ``table.df['site'] = site`` -- changes only the conversion; ``_data``,
        which every validation and summary path reads, never sees it. The edit
        then silently does not exist as far as validation is concerned.

        Comparing column names and row counts catches the shape-changing edits
        that pattern produces. It cannot catch an in-place value edit, so this
        narrows the failure rather than closing it; the real fix is to assign
        back (``table.data = edited``), which routes through the setter.
        """
        twin = self._df_pandas
        if twin is None or self._data is None:
            return
        if list(twin.columns) == list(self._data.columns) and len(twin) == self._data.height:
            return
        self.logger.warning(
            "The pandas view returned by .df has been modified in place; those "
            "changes are NOT in the data being validated. .df is a cached "
            "conversion -- assign back with `%s.data = <edited frame>` to make "
            "an edit take effect, or work on .data (polars) directly.",
            self.table_name,
        )

    def _setup_logging(self):
        """Set up table-specific logging (supplementary to centralized logs)."""
        # Get logger from centralized system
        self.logger = logging.getLogger(f'clifpy.tables.{self.table_name}')

        # Add supplementary file handler for table-specific validation logs
        # These go to output/logs/validation_log_{table}.log in addition to main logs
        log_dir = os.path.join(self.output_directory, 'logs')
        os.makedirs(log_dir, exist_ok=True)

        log_file = os.path.join(log_dir, f'validation_log_{self.table_name}.log')

        # Check if this handler already exists (avoid duplicates)
        existing_handlers = [h for h in self.logger.handlers
                           if isinstance(h, logging.FileHandler) and h.baseFilename == log_file]

        if not existing_handlers:
            file_handler = logging.FileHandler(log_file, mode='w')
            file_handler.setLevel(logging.INFO)
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)

        # Log initialization
        self.logger.info(f"Initialized {self.table_name} table")
        self.logger.info(f"Data directory: {self.data_directory}")
        self.logger.info(f"File type: {self.filetype}")
        self.logger.info(f"Timezone: {self.timezone}")
        self.logger.info(f"Output directory: {self.output_directory}")
    
    def _load_schema(self):
        """Load the YAML schema for this table at the configured CLIF version."""
        try:
            self.schema = load_schema(self.table_name, self.clif_version)
            if self.schema is None:
                self.logger.warning(
                    f"Schema file not found for table '{self.table_name}' "
                    f"(CLIF {self.clif_version})"
                )
            else:
                self.logger.info(
                    f"Loaded schema for '{self.table_name}' (CLIF {self.clif_version})"
                )
        except Exception as e:
            self.logger.error(f"Error loading schema: {str(e)}")
            self.schema = None

    def _load_outlier_config(self):
        """Load the outlier configuration for validation."""
        try:
            self.outlier_config = _load_outlier_config()
            if self.outlier_config:
                self.logger.info("Loaded outlier configuration")
            else:
                self.logger.warning("Could not load outlier configuration")
        except Exception as e:
            self.logger.error(f"Error loading outlier config: {str(e)}")
            self.outlier_config = None
    
    @classmethod
    def from_file(
        cls,
        data_directory: Optional[str] = None,
        filetype: Optional[str] = None,
        timezone: Optional[str] = None,
        config_path: Optional[str] = None,
        output_directory: Optional[str] = None,
        sample_size: Optional[int] = None,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        verbose: bool = False,
        clif_version: Optional[str] = None
    ) -> 'BaseTable':
        """
        Load data from file and create a table instance.

        Parameters
        ----------
        data_directory : str, optional
            Path to the directory containing data files
        filetype : str, optional
            Type of data file (csv, parquet, etc.)
        timezone : str, optional
            Timezone for datetime columns
        config_path : str, optional
            Path to configuration JSON file
        output_directory : str, optional
            Directory for saving output files and logs
        sample_size : int, optional
            Number of rows to load
        columns : List[str], optional
            Specific columns to load
        filters : Dict, optional
            Filters to apply when loading
        verbose : bool, optional
            If True, show detailed loading messages. Default is False
        clif_version : str, optional
            CLIF schema version to validate against. Overrides any ``clif_version``
            in the config file. If neither is set, the package default (3.0) is used.

        Notes
        -----
        Loading priority:
            1. If all required params provided → use them
            2. If config_path provided → load from that path, allow param overrides
            3. If no params and no config_path → auto-detect config.json
            4. Parameters override config file values when both are provided
            
        Returns
        -------
        BaseTable
            Instance of the table class with loaded data
        """
        # Get configuration from config file or parameters
        config = get_config_or_params(
            config_path=config_path,
            data_directory=data_directory,
            filetype=filetype,
            timezone=timezone,
            output_directory=output_directory
        )
        
        # Resolve CLIF version: explicit param > config file > package default
        resolved_version = clif_version or config.get('clif_version', DEFAULT_CLIF_VERSION)

        # Derive snake_case table name from PascalCase class name
        table_name = ''.join(['_' + c.lower() if c.isupper() else c for c in cls.__name__]).lstrip('_')

        # Load data using existing io utility. The table layer stores polars now
        # (see the ``df`` property), so this takes load_data's native default and
        # no longer needs the pandas pin.
        data = load_data(
            table_name,
            config['data_directory'],
            config['filetype'],
            sample_size=sample_size,
            columns=columns,
            filters=filters,
            site_tz=config['timezone'],
            verbose=verbose,
            return_format='polars',
        )

        # Create instance with loaded data
        return cls(
            data_directory=config['data_directory'],
            filetype=config['filetype'],
            timezone=config['timezone'],
            output_directory=config.get('output_directory', output_directory),
            data=data,
            clif_version=resolved_version
        )
    
    def validate(self):
        """
        Run comprehensive validation on the data.

        This method runs all validation checks including:

        - Schema validation (required columns, data types, categories)
        - Missing data analysis
        - Duplicate checking
        - Statistical analysis
        - Table-specific validations (if overridden in child class)
        """
        if self._data is None:
            self.logger.warning("No dataframe to validate.")
            return

        self._warn_if_pandas_view_diverged()

        self.logger.info("Starting validation")
        self.errors = []
        self.warnings = []
        self._validated = True

        try:
            # Run basic schema validation. The stored frame is polars and the DQA
            # checks are polars-native, so this hands over the frame directly
            # instead of round-tripping through pandas.
            if self.schema:
                self.logger.info("Running schema validation")
                findings = validator.validate_dataframe(
                    self._data, self.schema, clif_version=self.clif_version
                )

                # Only conformance findings mean the data does not match the
                # schema. Completeness and plausibility findings (an mCIDE value
                # this site never records, a shifted distribution, a mostly-null
                # column) describe the shape of the dataset -- real DQA output,
                # but not grounds for calling the table invalid. Folding them in
                # made isvalid() False for every site that does not exercise
                # every permissible value.
                for finding in findings:
                    if finding.get('check_group', 'conformance') == 'conformance':
                        self.errors.append(finding)
                    else:
                        self.warnings.append(finding)

                if self.errors:
                    self.logger.warning(f"Schema validation found {len(self.errors)} errors")
                else:
                    self.logger.info("Schema validation passed")
                if self.warnings:
                    self.logger.info(
                        f"Data quality checks raised {len(self.warnings)} finding(s). "
                        "See `warnings` attribute."
                    )

            # Run enhanced validations (these will be implemented in Phase 3)
            self._run_enhanced_validations()

            # Run table-specific validations (can be overridden in child classes)
            self._run_table_specific_validations()

            # Log validation results
            if not self.errors:
                self.logger.info("Validation completed successfully.")
            else:
                self.logger.warning(f"Validation completed with {len(self.errors)} error(s). See `errors` attribute.")

            # Persist both, so the DQA findings are not lost just because the
            # table conforms.
            self._save_validation_errors()

        except Exception as e:
            self.logger.error(f"Error during validation: {str(e)}")
            self.errors.append({
                "type": "validation_error",
                "message": str(e)
            })
    
    def _run_tz_validation(self):

        datetime_columns = [
            col['name'] for col in self.schema.get('columns', [])
            if col.get('data_type') == 'DATETIME' and col['name'] in self._data.columns and col['name'] != 'birth_date'
        ]
        if datetime_columns:
            self.logger.info(f"Validating timezone for datetime columns: {datetime_columns}")
            tz_results = validator.validate_datetime_timezone(self._data, datetime_columns)
            for result in tz_results:
                if result.get('status') in ['warning', 'error']:
                    self.errors.append(result)

    def _run_enhanced_validations(self):
        """No-op shim.

        Historically ran a chain of helpers (``check_for_duplicates``,
        ``validate_datetime_timezone``, ``calculate_missing_stats``,
        ``validate_units``, etc.) that no longer exist — those checks have
        moved to the DQA pipeline (``run_full_dqa`` in ``utils.validator``).
        The body here is intentionally empty; calling ``.validate()`` still
        runs the ``validator.validate_dataframe`` compat shim which routes to
        the DQA backend. Subclasses that need additional per-table checks
        should override ``_run_table_specific_validations``.
        """
        return
    
    def _run_table_specific_validations(self):
        """
        Run table-specific validations.
        
        This method should be overridden in child classes to implement
        table-specific validation logic (e.g., range validation for vitals).
        """
        pass
    
    def _save_validation_errors(self):
        """Save validation errors and data quality findings to a CSV file."""
        findings = self.errors + self.warnings
        if not findings:
            return

        try:
            # Convert findings to DataFrame
            errors_df = pd.DataFrame(findings)
            
            # Save to CSV
            error_file = os.path.join(
                self.output_directory,
                f'validation_errors_{self.table_name}.csv'
            )
            errors_df.to_csv(error_file, index=False)
            
            self.logger.info(f"Saved validation errors to {error_file}")
            
        except Exception as e:
            self.logger.error(f"Error saving validation errors: {str(e)}")
    
    def isvalid(self) -> bool:
        """
        Check if the data is valid based on the last validation run.

        "Valid" means the data conforms to the CLIF schema. Data quality
        findings from the completeness and plausibility checks live in
        ``warnings`` and do not affect this result.

        Returns:
            bool: True if validation has been run and no errors were found,
                  False if validation found errors or hasn't been run yet
        """
        if not self._validated:
            self.logger.warning("Validation has not been run yet. Please call validate() first.")
            return False
        return not self.errors
    
    def get_summary(self) -> Dict[str, Any]:
        """
        Get a summary of the table data.
        
        Returns:
            dict: Summary statistics and information about the table
        """
        if self._data is None:
            return {"status": "No data loaded"}

        data = self._data
        summary = {
            "table_name": self.table_name,
            "num_rows": data.height,
            "num_columns": data.width,
            "columns": list(data.columns),
            "memory_usage_mb": data.estimated_size() / 1024 / 1024,
            "validation_run": self._validated,
            "validation_errors": len(self.errors) if self._validated else None,
            "data_quality_findings": len(self.warnings) if self._validated else None,
            "is_valid": self.isvalid()
        }

        # Add basic statistics for numeric columns. Built from explicit aggregations
        # rather than polars' describe(), whose row-oriented output does not match
        # the {column: {stat: value}} shape pandas' describe().to_dict() produced.
        numeric_cols = [c for c, dt in data.schema.items() if dt.is_numeric()]
        if numeric_cols:
            summary["numeric_columns"] = list(numeric_cols)
            summary["numeric_stats"] = {
                c: {
                    "count": float(data.height - data[c].null_count()),
                    "mean": data[c].mean(),
                    "std": data[c].std(),
                    "min": data[c].min(),
                    "25%": data[c].quantile(0.25),
                    "50%": data[c].quantile(0.50),
                    "75%": data[c].quantile(0.75),
                    "max": data[c].max(),
                }
                for c in numeric_cols
            }

        # Add missing data summary
        missing_counts = {
            c: n for c, n in zip(data.columns, data.null_count().row(0)) if n > 0
        }
        if missing_counts:
            summary["missing_data"] = missing_counts

        return summary
    
    def save_summary(self):
        """Save table summary to a JSON file."""
        try:
            import json
            
            summary = self.get_summary()
            
            # Save to JSON
            summary_file = os.path.join(
                self.output_directory,
                f'summary_{self.table_name}.json'
            )
            
            with open(summary_file, 'w') as f:
                json.dump(summary, f, indent=2, default=str)
            
            self.logger.info(f"Saved summary to {summary_file}")
            
        except Exception as e:
            self.logger.error(f"Error saving summary: {str(e)}")

    def analyze_categorical_distributions(self, save: bool = True) -> Dict[str, pd.DataFrame]:
        """
        Analyze distributions of categorical variables.

        For each categorical variable, returns the distribution of categories
        based on unique hospitalization_id (or patient_id if hospitalization_id is not present).

        Parameters
        ----------
        save : bool, default=True
            If True, saves distribution data to CSV files in the output directory.

        Returns
        -------
        Dict[str, pd.DataFrame]
            Dictionary where keys are categorical column names and values are
            DataFrames with category distributions (unique ID counts and %).
        """
        if self._data is None:
            self.logger.warning("No dataframe to analyze")
            return {}

        if not self.schema:
            self.logger.warning("No schema available for categorical analysis")
            return {}

        data = self._data

        # Determine ID column to use (prefer hospitalization_id)
        if 'hospitalization_id' in data.columns:
            id_col = 'hospitalization_id'
        elif 'patient_id' in data.columns:
            id_col = 'patient_id'
        else:
            self.logger.warning("No hospitalization_id or patient_id column found")
            return {}

        # Get categorical columns from schema
        categorical_columns = [
            col['name'] for col in self.schema.get('columns', [])
            if col.get('is_category_column', False) and col['name'] in data.columns
        ]

        if not categorical_columns:
            self.logger.info("No categorical columns found in schema")
            return {}

        results = {}
        # drop_nulls before n_unique mirrors pandas' nunique(), which does not
        # count NaN. Null *categories* are still kept -- group_by includes them,
        # matching the previous groupby(dropna=False).
        total_unique_ids = data[id_col].drop_nulls().n_unique()

        for col in categorical_columns:
            try:
                # Count unique IDs per category, most common first
                id_counts = (
                    data.group_by(col)
                    .agg(pl.col(id_col).drop_nulls().n_unique().alias('count'))
                    .sort('count', descending=True)
                )
                distribution_df = id_counts.select(
                    pl.col(col).alias('category'),
                    pl.col('count'),
                    (pl.col('count') / total_unique_ids * 100).round(2).alias('%'),
                ).to_pandas()

                results[col] = distribution_df

                # Save to CSV if requested
                if save:
                    csv_filename = f'categorical_dist_{self.table_name}_{col}.csv'
                    csv_path = os.path.join(self.output_directory, csv_filename)
                    distribution_df.to_csv(csv_path, index=False)
                    self.logger.info(f"Saved distribution data to {csv_path}")

                self.logger.info(f"Analyzed categorical distribution for {col}")

            except Exception as e:
                self.logger.error(f"Error analyzing categorical distribution for {col}: {str(e)}")
                continue

        return results

    def plot_categorical_distributions(self, columns: Optional[List[str]] = None, figsize: Tuple[int, int] = (10, 6), save: bool = True, dpi: int = 300):
        """
        Create bar plots for categorical variable distributions.

        Counts unique hospitalization_id (or patient_id if hospitalization_id is not present)
        for each category.

        Parameters
        ----------
        columns : List[str], optional
            Specific categorical columns to plot. If None, plots all categorical columns.
        figsize : Tuple[int, int], default=(10, 6)
            Figure size for each plot (width, height).
        save : bool, default=True
            If True, saves plots to output directory as PNG files.
        dpi : int, default=300
            Resolution for saved plots (dots per inch).

        Returns
        -------
        Dict[str, Figure]
            Dictionary where keys are categorical column names and values are
            matplotlib Figure objects.
        """
        import matplotlib.pyplot as plt

        if self._data is None:
            self.logger.warning("No dataframe to plot")
            return {}

        if not self.schema:
            self.logger.warning("No schema available for categorical plotting")
            return {}

        data = self._data

        # Determine ID column to use (prefer hospitalization_id)
        if 'hospitalization_id' in data.columns:
            id_col = 'hospitalization_id'
        elif 'patient_id' in data.columns:
            id_col = 'patient_id'
        else:
            self.logger.warning("No hospitalization_id or patient_id column found")
            return {}

        # Get categorical columns from schema
        categorical_columns = [
            col['name'] for col in self.schema.get('columns', [])
            if col.get('is_category_column', False) and col['name'] in data.columns
        ]

        if not categorical_columns:
            self.logger.info("No categorical columns found in schema")
            return {}

        # Filter to requested columns if specified
        if columns is not None:
            categorical_columns = [col for col in categorical_columns if col in columns]

        if not categorical_columns:
            self.logger.warning("No matching categorical columns found")
            return {}

        plots = {}

        for col in categorical_columns:
            try:
                # Count unique IDs per category, most common first
                id_counts = (
                    data.group_by(col)
                    .agg(pl.col(id_col).drop_nulls().n_unique().alias('count'))
                    .sort('count', descending=True)
                )
                categories = id_counts.get_column(col).to_list()
                counts = id_counts.get_column('count').to_list()

                # Create modern bar plot
                fig, ax = plt.subplots(figsize=figsize, facecolor='white')

                # Use colorblind-friendly color palette (cividis)
                colors = plt.cm.cividis(np.linspace(0.3, 0.9, len(counts)))
                bars = ax.bar(range(len(counts)), counts, color=colors, edgecolor='white', linewidth=1.5)

                # Styling
                ax.set_xlabel('Category', fontsize=12, fontweight='bold', color='#333333')
                ax.set_ylabel(f'Unique {id_col} counts', fontsize=12, fontweight='bold', color='#333333')
                ax.set_title(f'Distribution of {col}', fontsize=14, fontweight='bold', pad=20, color='#1a1a1a')
                ax.set_xticks(range(len(counts)))
                ax.set_xticklabels([str(x) for x in categories], rotation=45, ha='right', fontsize=10)

                # Remove top and right spines
                ax.spines['top'].set_visible(False)
                ax.spines['right'].set_visible(False)
                ax.spines['left'].set_color('#cccccc')
                ax.spines['bottom'].set_color('#cccccc')

                # Add grid for readability
                ax.yaxis.grid(True, linestyle='--', alpha=0.3, color='#cccccc')
                ax.set_axisbelow(True)

                # Add value labels on top of bars (adjust font size and rotation based on number of categories)
                num_categories = len(counts)
                if num_categories <= 10:
                    label_fontsize = 9
                    label_rotation = 0
                elif num_categories <= 20:
                    label_fontsize = 7
                    label_rotation = 45
                else:
                    label_fontsize = 6
                    label_rotation = 90

                for i, (bar, value) in enumerate(zip(bars, counts)):
                    height = bar.get_height()
                    ax.text(bar.get_x() + bar.get_width()/2., height,
                           f'{int(value)}',
                           ha='center', va='bottom', fontsize=label_fontsize,
                           color='#333333', rotation=label_rotation)

                plt.tight_layout()

                # Save plot if requested
                if save:
                    plot_filename = f'categorical_dist_{self.table_name}_{col}.png'
                    plot_path = os.path.join(self.output_directory, plot_filename)
                    fig.savefig(plot_path, dpi=dpi, bbox_inches='tight')
                    self.logger.info(f"Saved plot to {plot_path}")

                plots[col] = fig

                self.logger.info(f"Created plot for {col}")

            except Exception as e:
                self.logger.error(f"Error creating plot for {col}: {str(e)}")
                continue

        return plots

    def calculate_stratified_ecdf(
        self,
        value_column: str,
        category_column: str,
        category_values: Optional[List[str]] = None,
        save: bool = True
    ) -> Optional[List['pl.DataFrame']]:
        """
        Calculate ECDF for a continuous variable stratified by categories using loaded DataFrame (self.df).
    
        Parameters
        ----------
        value_column : str
            Name of the continuous/numeric column to calculate ECDF for.
        category_column : str
            Name of the categorical column to stratify by.
        category_values : List[str], optional
            Specific category values to include. If None, uses permissible values from schema,
            or all unique values in the data if schema doesn't specify permissible values.
        save : bool, default=True
            If True, saves stratified ECDF data to CSV files (one per category).
    
        Returns
        -------
        List[pl.DataFrame] or None
            List of DataFrames (one per category), each with x-values and their corresponding cumulative probabilities.
            If save=True, saves the resulting DataFrame to CSV.
        """
        import polars as pl
    
        # Check if data is loaded
        if self._data is None:
            self.logger.error("Loaded dataframe is not available.")
            return None

        # Already polars -- no conversion needed.
        df_pl = self._data


        # Check if columns exist
        columns = df_pl.columns
        if value_column not in columns:
            self.logger.error(f"Value column '{value_column}' not found in dataframe")
            return None
        if category_column not in columns:
            self.logger.error(f"Category column '{category_column}' not found in dataframe")
            return None
    
        # Determine which category values to use
        if category_values is None:
            # Try permissible values from schema
            category_values = None
            if self.schema:
                for col_def in self.schema.get('columns', []):
                    if col_def.get('name') == category_column:
                        category_values = col_def.get('permissible_values')
                        if category_values:
                            self.logger.info(f"Using permissible values from schema for {category_column}")
                        break
            # Otherwise use all unique values from data
            if not category_values:
                category_values = (
                    df_pl
                    .select(pl.col(category_column).drop_nulls().unique())
                    .to_series()
                    .to_list()
                )
                self.logger.info(f"Using all unique values from data for {category_column}")
    
        all_ecdf_rows = []
    
        for category in category_values:
            try:
                # Filter data for this category
                cat_df = (
                    df_pl
                    .filter(pl.col(category_column) == category)
                    .select([pl.col(value_column)])
                    .drop_nulls()
                    .sort(value_column)
                )
    
                n = cat_df.shape[0]
                if n == 0:
                    self.logger.warning(f"No valid data for category '{category}'")
                    continue
    
                # Calculate ECDF: each value gets rank = position, cumulative_probability = rank/n
                ecdf_df = cat_df.with_columns([
                    (pl.arange(1, n + 1) / n).alias('cumulative_probability'),
                ])
                # Add category for later clarity
                ecdf_df = ecdf_df.with_columns([
                    pl.lit(category).alias(category_column)
                ])
    
                all_ecdf_rows.append(ecdf_df)
    
                self.logger.info(f"Calculated ECDF for {category_column}={category} with {n} measurements")
    
            except Exception as e:
                self.logger.error(f"Error calculating ECDF for category '{category}': {str(e)}")
                continue
    
        if not all_ecdf_rows:
            self.logger.warning("No valid ECDF data for any category.")
            return None
    
        # Concatenate all
        all_ecdf_pl = pl.concat(all_ecdf_rows)
    
        if save:
            csv_filename = f'ecdf_{self.table_name}_{value_column}_by_{category_column}.csv'
            csv_path = os.path.join(self.output_directory, csv_filename)
            try:
                all_ecdf_pl.write_csv(csv_path)
                self.logger.info(f"Saved ECDF data for all categories to {csv_path}")
            except Exception as e:
                self.logger.error(f"Failed to save ECDF CSV: {str(e)}")
    
        return all_ecdf_rows
