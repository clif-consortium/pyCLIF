"""
Wide dataset creation utilities for CLIF data.

This module provides functionality to create wide time-series datasets
by joining multiple CLIF tables with automatic pivoting and hourly aggregation
using DuckDB for high performance.
"""

import pandas as pd
import duckdb
import numpy as np
from datetime import datetime
import os
import re
import yaml
from typing import List, Dict, Optional, Union
from tqdm import tqdm
import logging

# Set up logging - use centralized logger
logger = logging.getLogger('clifpy.utils.wide_dataset')

# Global config cache
_WIDE_TABLES_CONFIG = None


def _load_wide_tables_config() -> Dict:
    """
    Load wide tables configuration from YAML file.
    Config is cached globally to avoid repeated file I/O.

    Returns
    -------
    Dict
        Configuration dictionary with table metadata
    """
    global _WIDE_TABLES_CONFIG

    if _WIDE_TABLES_CONFIG is None:
        # Get path to config file relative to this module
        config_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            'schemas',
            'wide_tables_config.yaml'
        )

        if not os.path.exists(config_path):
            raise FileNotFoundError(
                f"Wide tables config not found at: {config_path}\n"
                "Please ensure clifpy/schemas/wide_tables_config.yaml exists."
            )

        with open(config_path, 'r', encoding='utf-8') as f:
            _WIDE_TABLES_CONFIG = yaml.safe_load(f)

    return _WIDE_TABLES_CONFIG


def _get_supported_tables(table_type: Optional[str] = None) -> List[str]:
    """
    Get list of supported tables from config.

    Parameters
    ----------
    table_type : str, optional
        Filter by table type: 'pivot', 'wide', 'base', or None for all

    Returns
    -------
    List[str]
        List of supported table names
    """
    config = _load_wide_tables_config()
    tables = config.get('tables', {})

    supported = []
    for table_name, table_config in tables.items():
        if table_config.get('supported', False):
            if table_type is None or table_config.get('type') == table_type:
                supported.append(table_name)

    return supported


def _get_table_config(table_name: str) -> Optional[Dict]:
    """
    Get configuration for a specific table.

    Parameters
    ----------
    table_name : str
        Name of the table

    Returns
    -------
    Dict or None
        Table configuration dictionary, or None if not found
    """
    config = _load_wide_tables_config()
    return config.get('tables', {}).get(table_name)


def create_wide_dataset(
    clif_instance,
    optional_tables: Optional[List[str]] = None,
    category_filters: Optional[Dict[str, List[str]]] = None,
    sample: bool = False,
    hospitalization_ids: Optional[List[str]] = None,
    cohort_df: Optional[pd.DataFrame] = None,
    output_format: str = 'dataframe',
    save_to_data_location: bool = False,
    output_filename: Optional[str] = None,
    return_dataframe: bool = True,
    base_table_columns: Optional[Dict[str, List[str]]] = None,
    batch_size: int = 1000,
    memory_limit: Optional[str] = None,
    threads: Optional[int] = None,
    show_progress: bool = True
) -> Optional[pd.DataFrame]:
    """
    Create a wide dataset by joining multiple CLIF tables with pivoting support.
    
    Parameters
    ----------
    clif_instance
        CLIF object with loaded data
    optional_tables : List[str], optional
        DEPRECATED - use category_filters to specify tables
    category_filters : Dict[str, List[str]], optional
        Dict specifying filtering/selection for each table. Behavior differs by table type:

        **PIVOT TABLES** (narrow to wide conversion):
        - Values are **category values** to filter and pivot into columns
        - Example: {'vitals': ['heart_rate', 'sbp', 'spo2'],
                    'labs': ['hemoglobin', 'sodium', 'creatinine']}
        - Acceptable values come from the category column's permissible values
          defined in each table's schema file (clifpy/schemas/<clif_version>/*_schema.yaml)

        **WIDE TABLES** (already in wide format):
        - Values are **column names** to keep from the table
        - Example: {'respiratory_support': ['device_category', 'fio2_set', 'peep_set']}
        - Acceptable values are any column names from the table schema

        **Supported tables and their types are defined in:**
        clifpy/schemas/wide_tables_config.yaml

        Table presence in this dict determines if it will be loaded.
        For complete lists of acceptable category values, see:
        - Table schemas: clifpy/schemas/<clif_version>/*_schema.yaml
        - Wide dataset config: clifpy/schemas/wide_tables_config.yaml
    sample : bool, default=False
        if True, randomly select 20 hospitalizations
    hospitalization_ids : List[str], optional
        List of specific hospitalization IDs to filter
    cohort_df : pd.DataFrame, optional
        DataFrame with columns ['hospitalization_id', 'start_time', 'end_time']
        If provided, data will be filtered to only include events within the specified
        time windows for each hospitalization
    output_format : str, default='dataframe'
        'dataframe', 'csv', or 'parquet'
    save_to_data_location : bool, default=False
        save output to data directory
    output_filename : str, optional
        Custom filename (default: 'wide_dataset_YYYYMMDD_HHMMSS')
    return_dataframe : bool, default=True
        return DataFrame even when saving to file
    base_table_columns : Dict[str, List[str]], optional
        DEPRECATED - columns are selected automatically
    batch_size : int, default=1000
        Number of hospitalizations to process in each batch
    memory_limit : str, optional
        DuckDB memory limit (e.g., '8GB')
    threads : int, optional
        Number of threads for DuckDB to use
    show_progress : bool, default=True
        Show progress bars for long operations
    
    Returns
    -------
    pd.DataFrame or None
        DataFrame if return_dataframe=True, None otherwise
    """


    logger.info("Phase 4: Wide Dataset Processing (utility function)")
    logger.debug("  4.1: Starting wide dataset creation")

    # Validate cohort_df if provided
    if cohort_df is not None:
        required_cols = ['hospitalization_id', 'start_time', 'end_time']
        missing_cols = [col for col in required_cols if col not in cohort_df.columns]
        if missing_cols:
            raise ValueError(f"cohort_df must contain columns: {required_cols}. Missing: {missing_cols}")

        # Ensure hospitalization_id is string type to match with other tables
        cohort_df['hospitalization_id'] = cohort_df['hospitalization_id'].astype(str)

        # Ensure time columns are datetime
        for time_col in ['start_time', 'end_time']:
            if not pd.api.types.is_datetime64_any_dtype(cohort_df[time_col]):
                cohort_df[time_col] = pd.to_datetime(cohort_df[time_col])

        logger.info("  === SPECIAL: COHORT TIME WINDOW FILTERING ===")
        logger.info(f"       - Processing {len(cohort_df)} hospitalizations with time windows")
        logger.debug(f"       - Ensuring datetime types for start_time, end_time")
    
    # Get table types from config
    PIVOT_TABLES = _get_supported_tables(table_type='pivot')
    WIDE_TABLES = _get_supported_tables(table_type='wide')

    # Determine which tables to load from category_filters
    if category_filters is None:
        category_filters = {}
    
    # For backward compatibility with optional_tables
    if optional_tables and not category_filters:
        logger.warning("optional_tables parameter is deprecated. Converting to category_filters format")
        category_filters = {table: [] for table in optional_tables}
    
    tables_to_load = list(category_filters.keys())
    
    # Create DuckDB connection with optimized settings
    conn_config = {
        'preserve_insertion_order': 'false'
    }
    
    if memory_limit:
        conn_config['memory_limit'] = memory_limit
    if threads:
        conn_config['threads'] = str(threads)
    
    # Use context manager for connection
    with duckdb.connect(':memory:', config=conn_config) as conn:
        # Preserve timezone from clif_instance configuration
        conn.execute(f"SET timezone = '{clif_instance.timezone}'")
        # Set additional optimization settings
        conn.execute("SET preserve_insertion_order = false")
        
        # Get hospitalization IDs to process
        hospitalization_df = clif_instance.hospitalization.df.copy()

        if hospitalization_ids is not None:
            logger.info(f"Filtering to specific hospitalization IDs: {len(hospitalization_ids)} encounters")
            required_ids = hospitalization_ids
        elif cohort_df is not None:
            # Use hospitalization IDs from cohort_df
            required_ids = cohort_df['hospitalization_id'].unique().tolist()
            logger.info(f"Using {len(required_ids)} hospitalization IDs from cohort_df")
        elif sample:
            logger.info("Sampling 20 random hospitalizations")
            all_ids = hospitalization_df['hospitalization_id'].unique()
            required_ids = np.random.choice(all_ids, size=min(20, len(all_ids)), replace=False).tolist()
            logger.info(f"Selected {len(required_ids)} hospitalizations for sampling")
        else:
            required_ids = hospitalization_df['hospitalization_id'].unique().tolist()
            logger.info(f"Processing all {len(required_ids)} hospitalizations")

        # Filter all base tables by required IDs immediately
        logger.info("Loading and filtering base tables")
        # Only keep required columns from hospitalization table
        hosp_required_cols = ['hospitalization_id', 'patient_id', 'age_at_admission']
        hosp_available_cols = [col for col in hosp_required_cols if col in hospitalization_df.columns]
        hospitalization_df = hospitalization_df[hosp_available_cols]
        hospitalization_df = hospitalization_df[hospitalization_df['hospitalization_id'].isin(required_ids)]
        patient_df = clif_instance.patient.df[['patient_id']].copy()
        
        # Get ADT with selected columns
        adt_df = clif_instance.adt.df.copy()
        adt_df = adt_df[adt_df['hospitalization_id'].isin(required_ids)]
        
        # Apply time filtering to ADT if cohort_df is provided
        if cohort_df is not None and 'in_dttm' in adt_df.columns:
            pre_filter_count = len(adt_df)
            # Merge with cohort_df to get time windows
            adt_df = pd.merge(
                adt_df,
                cohort_df[['hospitalization_id', 'start_time', 'end_time']],
                on='hospitalization_id',
                how='inner'
            )

            # Ensure in_dttm column is datetime
            if not pd.api.types.is_datetime64_any_dtype(adt_df['in_dttm']):
                adt_df['in_dttm'] = pd.to_datetime(adt_df['in_dttm'])

            # Filter to time window
            adt_df = adt_df[
                (adt_df['in_dttm'] >= adt_df['start_time']) &
                (adt_df['in_dttm'] <= adt_df['end_time'])
            ].copy()

            # Drop the time window columns
            adt_df = adt_df.drop(columns=['start_time', 'end_time'])

            logger.info(f"  ADT time filtering: {pre_filter_count} -> {len(adt_df)} records")
        
        # Remove duplicate columns and _name columns
        adt_cols = [col for col in adt_df.columns if not col.endswith('_name') and col != 'patient_id']
        adt_df = adt_df[adt_cols]

        logger.info(f"       - Base tables filtered - Hospitalization: {len(hospitalization_df)}, Patient: {len(patient_df)}, ADT: {len(adt_df)}")

        logger.info("  4.2: Determining processing mode")
        # Process in batches to avoid memory issues
        if batch_size > 0 and len(required_ids) > batch_size:
            logger.info(f"       - Batch mode: {len(required_ids)} hospitalizations in {len(required_ids)//batch_size + 1} batches of {batch_size}")
            logger.info("  4.B: === BATCH PROCESSING MODE ===")
            return _process_in_batches(
                conn, clif_instance, required_ids, patient_df, hospitalization_df, adt_df,
                tables_to_load, category_filters, PIVOT_TABLES, WIDE_TABLES,
                batch_size, show_progress, save_to_data_location, output_filename,
                output_format, return_dataframe, cohort_df
            )
        else:
            logger.info(f"       - Single mode: Processing all {len(required_ids)} hospitalizations at once")
            logger.info("  4.S: === SINGLE PROCESSING MODE ===")
            # Process all at once for small datasets
            return _process_hospitalizations(
                conn, clif_instance, required_ids, patient_df, hospitalization_df, adt_df,
                tables_to_load, category_filters, PIVOT_TABLES, WIDE_TABLES,
                show_progress, cohort_df
            )


def convert_wide_to_hourly(
    wide_df: pd.DataFrame,
    aggregation_config: Dict[str, List[str]],
    id_name: str = 'hospitalization_id',
    hourly_window: int = 1,
    fill_gaps: bool = False,
    memory_limit: str = '4GB',
    temp_directory: Optional[str] = None,
    batch_size: Optional[int] = None,
    timezone: str = 'UTC'
) -> pd.DataFrame:
    """
    Convert a wide dataset to temporal aggregation with user-defined aggregation methods.

    This function uses DuckDB for high-performance aggregation with event-based windowing.

    Parameters
    ----------
    wide_df : pd.DataFrame
        Wide dataset DataFrame from create_wide_dataset()
    aggregation_config : Dict[str, List[str]]
        Dict mapping aggregation methods to list of columns
        Example: {
            'max': ['map', 'temp_c', 'sbp'],
            'mean': ['heart_rate', 'respiratory_rate'],
            'min': ['spo2'],
            'median': ['glucose'],
            'first': ['gcs_total', 'rass'],
            'last': ['assessment_value'],
            'boolean': ['norepinephrine', 'propofol'],
            'one_hot_encode': ['medication_name', 'assessment_category']
        }
    id_name : str, default='hospitalization_id'
        Column name to use for grouping aggregation
        - 'hospitalization_id': Group by individual hospitalizations (default)
        - 'encounter_block': Group by encounter blocks (after encounter stitching)
        - Any other ID column present in the wide dataset
    hourly_window : int, default=1
        Time window for aggregation in hours (1-72).

        Windows are event-based (relative to each group's first event):
        - Window 0: [first_event, first_event + hourly_window hours)
        - Window 1: [first_event + hourly_window, first_event + 2*hourly_window)
        - Window N: [first_event + N*hourly_window, ...)

        Common values: 1 (hourly), 2 (bi-hourly), 6 (quarter-day), 12 (half-day),
                       24 (daily), 72 (3-day - maximum)
    fill_gaps : bool, default=False
        Whether to create rows for time windows with no data.

        - False (default): Sparse output - only windows with actual data appear
        - True: Dense output - create ALL windows from 0 to max_window per group,
                filling gaps with NaN values (no forward-filling)

        Example with events at window 0, 1, 5:
        - fill_gaps=False: Output has 3 rows (windows 0, 1, 5)
        - fill_gaps=True: Output has 6 rows (windows 0, 1, 2, 3, 4, 5)
                          Windows 2, 3, 4 have NaN for all aggregated columns
    memory_limit : str, default='4GB'
        DuckDB memory limit (e.g., '4GB', '8GB')
    temp_directory : str, optional
        Directory for temporary files (default: system temp)
    batch_size : int, optional
        Process in batches if dataset is large (auto-determined if None)
    timezone : str, default='UTC'
        Timezone for datetime operations in DuckDB (e.g., 'UTC', 'America/New_York')

    Returns
    -------
    pd.DataFrame
        Aggregated dataset with columns:

        **Group & Window Identifiers:**
        - {id_name}: Group identifier (hospitalization_id or encounter_block)
        - window_number: Sequential window index (0-indexed, starts at 0 for each group)
        - window_start_dttm: Window start timestamp (inclusive)
        - window_end_dttm: Window end timestamp (exclusive)

        **Context Columns:**
        - patient_id: Patient identifier
        - day_number: Day number within hospitalization

        **Aggregated Columns:**
        - All columns specified in aggregation_config with appropriate suffixes
          (_max, _min, _mean, _median, _first, _last, _boolean, one-hot encoded)

        **Notes:**
        - Windows are relative to each group's first event, not calendar boundaries
        - window_end_dttm - window_start_dttm = hourly_window hours (always)
        - When fill_gaps=True, gap windows contain NaN (not forward-filled)
        - When fill_gaps=False, only windows with data appear (sparse output)
    """

    # Validate hourly_window parameter
    if not isinstance(hourly_window, int):
        raise ValueError(f"hourly_window must be an integer, got: {type(hourly_window).__name__}")
    if hourly_window < 1 or hourly_window > 72:
        raise ValueError(f"hourly_window must be between 1 and 72 hours, got: {hourly_window}")

    # Validate fill_gaps parameter
    if not isinstance(fill_gaps, bool):
        raise ValueError(f"fill_gaps must be a boolean, got: {type(fill_gaps).__name__}")

    # Strip timezone from datetime columns (no conversion, just remove tz metadata)
    wide_df = wide_df.copy()
    for col in wide_df.columns:
        if pd.api.types.is_datetime64_any_dtype(wide_df[col]):
            if hasattr(wide_df[col].dtype, 'tz') and wide_df[col].dtype.tz is not None:
                wide_df[col] = wide_df[col].dt.tz_localize(None)

    # Update log statements
    window_label = "hourly" if hourly_window == 1 else f"{hourly_window}-hour"
    gap_handling = "with gap filling" if fill_gaps else "sparse (no gap filling)"
    logger.info(f"Starting optimized {window_label} aggregation using DuckDB {gap_handling}")
    logger.info(f"Input dataset shape: {wide_df.shape}")
    logger.debug(f"Memory limit: {memory_limit}")
    logger.debug(f"Aggregation window: {hourly_window} hour(s)")
    logger.debug(f"Gap filling: {'enabled' if fill_gaps else 'disabled'}")
    
    # Validate input
    required_columns = ['event_time', id_name, 'day_number']
    for col in required_columns:
        if col not in wide_df.columns:
            raise ValueError(f"wide_df must contain '{col}' column")
    
    # Auto-determine batch size for very large datasets
    if batch_size is None:
        n_rows = len(wide_df)
        n_ids = wide_df[id_name].nunique()
        
        # Use batching if dataset is very large
        if n_rows > 1_000_000 or n_ids > 10_000:
            batch_size = min(5000, n_ids // 4)
            logger.info(f"Large dataset detected ({n_rows:,} rows, {n_ids:,} {id_name}s)")
            logger.info(f"Will process in batches of {batch_size} {id_name}s")
        else:
            batch_size = 0  # Process all at once
    
    # Configure DuckDB connection
    config = {
        'memory_limit': memory_limit,
        'temp_directory': temp_directory or '/tmp/duckdb_temp',
        'preserve_insertion_order': 'false',
        'threads': '4'
    }
    
    # Remove None values from config
    config = {k: v for k, v in config.items() if v is not None}
    
    try:
        # Create DuckDB connection with error handling
        with duckdb.connect(':memory:', config=config) as conn:
            # Use timezone from parameter (passed from orchestrator)
            conn.execute(f"SET timezone = '{timezone}'")
            # Set additional optimization settings
            conn.execute("SET preserve_insertion_order = false")

            if batch_size > 0:
                return _process_hourly_in_batches(conn, wide_df, aggregation_config, id_name, batch_size, hourly_window, fill_gaps)
            else:
                return _process_hourly_single_batch(conn, wide_df, aggregation_config, id_name, hourly_window, fill_gaps)

    except Exception as e:
        logger.error(f"DuckDB processing failed: {str(e)}")
        raise


def _find_alternative_timestamp(table_name: str, columns: List[str]) -> Optional[str]:
    """Find alternative timestamp column if the default is not found (from config)."""
    table_config = _get_table_config(table_name)

    if table_config:
        alternatives = table_config.get('alternative_timestamps', [])
        for alt_col in alternatives:
            if alt_col in columns:
                return alt_col

    return None


def _save_dataset(
    df: pd.DataFrame,
    data_dir: str,
    output_filename: Optional[str],
    output_format: str
):
    """Save the dataset to file."""

    if output_filename is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_filename = f'wide_dataset_{timestamp}'

    output_path = os.path.join(data_dir, f'{output_filename}.{output_format}')

    if output_format == 'csv':
        df.to_csv(output_path, index=False)
    elif output_format == 'parquet':
        df.to_parquet(output_path, index=False)

    logger.info(f"Wide dataset saved to: {output_path}")


def _get_timestamp_column(table_name: str) -> Optional[str]:
    """Get the timestamp column name for each table type from config."""
    table_config = _get_table_config(table_name)
    if table_config:
        return table_config.get('timestamp_column')
    return None


def _process_hourly_single_batch(
    conn: duckdb.DuckDBPyConnection,
    wide_df: pd.DataFrame,
    aggregation_config: Dict[str, List[str]],
    id_name: str = 'hospitalization_id',
    hourly_window: int = 1,
    fill_gaps: bool = False
) -> pd.DataFrame:
    """Process entire dataset in a single batch with progress tracking by aggregation type."""
    
    try:
        # Register the DataFrame
        conn.register('wide_data', wide_df)

        # Create window assignments with event-based windowing
        window_label = "hourly" if hourly_window == 1 else f"{hourly_window}-hour"
        logger.info(f"Creating event-based {window_label} windows")
        logger.debug("Calculating window boundaries")
        conn.execute(f"""
            CREATE OR REPLACE TABLE windowed_data AS
            WITH first_events AS (
                SELECT
                    {id_name},
                    MIN(event_time) AS first_event_time
                FROM wide_data
                GROUP BY {id_name}
            )
            SELECT
                wd.*,
                fe.first_event_time,
                CAST(FLOOR((EPOCH(wd.event_time) - EPOCH(fe.first_event_time)) / ({hourly_window} * 3600)) AS INTEGER) AS window_number
            FROM wide_data wd
            JOIN first_events fe ON wd.{id_name} = fe.{id_name}
        """)
        
        # Build separate queries for each aggregation type
        agg_queries = _build_aggregation_query_duckdb(conn, aggregation_config, wide_df.columns, id_name, hourly_window)

        # Execute base query first
        logger.info("Processing aggregations by type:")
        logger.debug("- Extracting base columns")
        base_df = conn.execute(agg_queries['base']).df()

        # Process each aggregation type separately and merge properly
        result_df = base_df

        # Define the order of operations for better user feedback
        agg_order = ['max', 'min', 'mean', 'median', 'first', 'last', 'boolean', 'one_hot_encode']

        for agg_type in agg_order:
            if agg_type in agg_queries and agg_type != 'base':
                logger.info(f"- Processing {agg_type} aggregation")
                try:
                    agg_result = conn.execute(agg_queries[agg_type]).df()
                    # Merge on the keys to ensure proper row alignment
                    result_df = result_df.merge(
                        agg_result,
                        on=[id_name, 'window_number'],
                        how='left'
                    )
                    logger.info(f"  [ok] {agg_type} complete ({agg_result.shape[1] - 2} columns)")
                except Exception as e:
                    logger.error(f"  [fail] {agg_type} failed: {str(e)}")

        # Merge all results
        logger.debug("Merging aggregation results")

        # Fill gaps if requested
        if fill_gaps:
            logger.info("Filling gaps in window sequence")
            result_df = _fill_window_gaps(conn, result_df, id_name, hourly_window)

        # Sort by id_name and window_number
        result_df = result_df.sort_values([id_name, 'window_number']).reset_index(drop=True)

        # Strip timezone from datetime columns (DuckDB to_timestamp adds it back)
        for col in result_df.columns:
            if pd.api.types.is_datetime64_any_dtype(result_df[col]):
                if hasattr(result_df[col].dtype, 'tz') and result_df[col].dtype.tz is not None:
                    result_df[col] = result_df[col].dt.tz_localize(None)

        window_label = "hourly" if hourly_window == 1 else f"{hourly_window}-hour"
        logger.info(f"{window_label.capitalize()} aggregation complete: {len(result_df)} records")
        logger.info(f"Columns in dataset: {len(result_df.columns)}")

        return result_df

    except Exception as e:
        logger.error(f"Single batch processing failed: {str(e)}")
        raise


def _fill_window_gaps(
    conn: duckdb.DuckDBPyConnection,
    aggregated_df: pd.DataFrame,
    id_name: str,
    hourly_window: int
) -> pd.DataFrame:
    """
    Fill gaps in window sequence by creating rows for missing windows with NaN values.

    Parameters
    ----------
    conn : duckdb.DuckDBPyConnection
        Active DuckDB connection
    aggregated_df : pd.DataFrame
        Aggregated data with window_number, window_start_dttm, window_end_dttm
    id_name : str
        Grouping column name
    hourly_window : int
        Window size in hours

    Returns
    -------
    pd.DataFrame
        Complete window sequence with gaps filled as NaN
    """
    logger.debug(f"  - Generating complete window sequences per {id_name}")

    # Register aggregated data
    conn.register('aggregated_data', aggregated_df)

    # Create complete window sequence
    complete_df = conn.execute(f"""
        WITH window_ranges AS (
            SELECT
                {id_name},
                MIN(window_number) AS min_window,
                MAX(window_number) AS max_window
            FROM aggregated_data
            GROUP BY {id_name}
        ),
        first_event_times AS (
            SELECT
                {id_name},
                window_start_dttm
            FROM aggregated_data
            WHERE window_number = 0
        ),
        all_windows AS (
            SELECT
                wr.{id_name},
                unnest(generate_series(wr.min_window, wr.max_window, 1)) AS window_number
            FROM window_ranges wr
        ),
        window_timestamps AS (
            SELECT
                aw.{id_name},
                aw.window_number,
                fe.window_start_dttm + (aw.window_number * {hourly_window}) * INTERVAL '1' HOUR AS window_start_dttm,
                fe.window_start_dttm + ((aw.window_number + 1) * {hourly_window}) * INTERVAL '1' HOUR AS window_end_dttm
            FROM all_windows aw
            LEFT JOIN first_event_times fe ON aw.{id_name} = fe.{id_name}
        )
        SELECT
            wt.{id_name},
            wt.window_number,
            wt.window_start_dttm,
            wt.window_end_dttm,
            ad.* EXCLUDE ({id_name}, window_number, window_start_dttm, window_end_dttm)
        FROM window_timestamps wt
        LEFT JOIN aggregated_data ad
            ON wt.{id_name} = ad.{id_name}
            AND wt.window_number = ad.window_number
        ORDER BY wt.{id_name}, wt.window_number
    """).df()

    # Cleanup
    conn.unregister('aggregated_data')

    # Report stats
    original_rows = len(aggregated_df)
    filled_rows = len(complete_df)
    gap_rows = filled_rows - original_rows

    logger.info(f"  - Original: {original_rows} windows, Filled: {filled_rows} windows (+{gap_rows} gaps filled with NaN)")

    return complete_df


def _process_hourly_in_batches(
    conn: duckdb.DuckDBPyConnection,
    wide_df: pd.DataFrame,
    aggregation_config: Dict[str, List[str]],
    id_name: str,
    batch_size: int,
    hourly_window: int = 1,
    fill_gaps: bool = False
) -> pd.DataFrame:
    """Process dataset in batches to manage memory usage with progress tracking."""

    logger.info(f"Processing in batches of {batch_size} {id_name}s")

    # Get unique IDs
    unique_ids = wide_df[id_name].unique()
    n_batches = (len(unique_ids) + batch_size - 1) // batch_size
    
    batch_results = []
    
    # Use tqdm for batch-level progress
    batch_iterator = tqdm(range(0, len(unique_ids), batch_size),
                         desc="Processing batches",
                         total=n_batches,
                         unit="batch")

    for i in batch_iterator:
        batch_ids = unique_ids[i:i + batch_size]
        batch_num = (i // batch_size) + 1
        
        batch_iterator.set_description(f"Processing batch {batch_num}/{n_batches}")
        
        try:
            # Filter to current batch
            batch_df = wide_df[wide_df[id_name].isin(batch_ids)].copy()

            logger.debug(f"--- Batch {batch_num}/{n_batches} ({len(batch_ids)} {id_name}s) ---")

            # Process this batch
            batch_result = _process_hourly_single_batch(conn, batch_df, aggregation_config.copy(), id_name, hourly_window, fill_gaps)

            if len(batch_result) > 0:
                batch_results.append(batch_result)
                logger.info(f"Batch {batch_num} completed: {len(batch_result)} records")
            
            # Clean up batch-specific tables
            try:
                conn.execute("DROP TABLE IF EXISTS windowed_data")
                conn.unregister('wide_data')
            except:
                pass
            
            # Explicit garbage collection between batches
            import gc
            gc.collect()


        except Exception as e:
            logger.error(f"Error processing batch {batch_num}: {str(e)}")
            continue

    if batch_results:
        logger.info(f"Combining {len(batch_results)} batch results")
        final_df = pd.concat(batch_results, ignore_index=True)
        final_df = final_df.sort_values([id_name, 'window_number']).reset_index(drop=True)

        window_label = "hourly" if hourly_window == 1 else f"{hourly_window}-hour"
        logger.info(f"Final {window_label} dataset: {len(final_df)} records from {len(batch_results)} batches")
        return final_df
    else:
        raise ValueError("No batches processed successfully")


def _build_aggregation_query_duckdb(
    conn: duckdb.DuckDBPyConnection,
    aggregation_config: Dict[str, List[str]],
    all_columns: List[str],
    id_name: str = 'hospitalization_id',
    hourly_window: int = 1
) -> Dict[str, str]:
    """Build separate DuckDB aggregation queries by type for better performance and progress tracking."""
    
    # Group by columns
    group_cols = [id_name, 'window_number', 'window_start_dttm', 'window_end_dttm']

    # Get columns not in aggregation config
    all_agg_columns = []
    for columns_list in aggregation_config.values():
        all_agg_columns.extend(columns_list)

    non_agg_columns = [col for col in all_columns
                      if col not in all_agg_columns
                      and col not in group_cols
                      and col not in ['patient_id', 'day_number', 'first_event_time', 'event_time', 'window_number']]
    
    if non_agg_columns:
        logger.info(f"Columns not in aggregation_config, defaulting to 'first' with '_c' postfix: {', '.join(non_agg_columns[:5])}")
        if len(non_agg_columns) > 5:
            logger.debug(f"  ... and {len(non_agg_columns) - 5} more")
        if 'first' not in aggregation_config:
            aggregation_config['first'] = []
        aggregation_config['first'].extend(non_agg_columns)
    
    # Build separate queries for each aggregation type
    queries = {}
    
    # Base columns query with window calculations
    base_query = f"""
    WITH window_aggregates AS (
        SELECT
            {id_name},
            window_number,
            MIN(first_event_time) AS first_event_time
        FROM windowed_data
        GROUP BY {id_name}, window_number
    )
    SELECT
        wa.{id_name},
        wa.window_number,
        wa.first_event_time + (wa.window_number * {hourly_window}) * INTERVAL '1' HOUR AS window_start_dttm,
        wa.first_event_time + ((wa.window_number + 1) * {hourly_window}) * INTERVAL '1' HOUR AS window_end_dttm,
        FIRST(wd.patient_id ORDER BY wd.event_time) AS patient_id,
        FIRST(wd.day_number ORDER BY wd.event_time) AS day_number
    FROM windowed_data wd
    JOIN window_aggregates wa
        ON wd.{id_name} = wa.{id_name}
        AND wd.window_number = wa.window_number
    GROUP BY wa.{id_name}, wa.window_number, wa.first_event_time
    """
    queries['base'] = base_query
    
    # Process each aggregation type separately
    for agg_method, columns in aggregation_config.items():
        if agg_method == 'one_hot_encode':
            continue  # Handle separately
            
        valid_columns = [col for col in columns if col in all_columns]
        if not valid_columns:
            continue

        select_parts = [id_name, 'window_number']

        for col in valid_columns:
            if agg_method == 'max':
                select_parts.append(f"MAX({col}) AS {col}_max")
            elif agg_method == 'min':
                select_parts.append(f"MIN({col}) AS {col}_min")
            elif agg_method == 'mean':
                select_parts.append(f"AVG({col}) AS {col}_mean")
            elif agg_method == 'median':
                select_parts.append(f"MEDIAN({col}) AS {col}_median")
            elif agg_method == 'first':
                if col in non_agg_columns:
                    select_parts.append(f"FIRST({col} ORDER BY event_time) AS {col}_c")
                else:
                    select_parts.append(f"FIRST({col} ORDER BY event_time) AS {col}_first")
            elif agg_method == 'last':
                select_parts.append(f"LAST({col} ORDER BY event_time) AS {col}_last")
            elif agg_method == 'boolean':
                select_parts.append(f"CASE WHEN COUNT({col}) > 0 THEN 1 ELSE 0 END AS {col}_boolean")

        query = f"""
        SELECT
            {', '.join(select_parts)}
        FROM windowed_data
        GROUP BY {id_name}, window_number
        """
        queries[agg_method] = query
    
    # Handle one-hot encoding separately
    if 'one_hot_encode' in aggregation_config:
        one_hot_query = _build_one_hot_encoding_query_duckdb(
            conn, aggregation_config['one_hot_encode'], all_columns, id_name
        )
        if one_hot_query:
            queries['one_hot_encode'] = one_hot_query
    
    return queries


def _build_one_hot_encoding_query_duckdb(
    conn: duckdb.DuckDBPyConnection,
    one_hot_columns: List[str],
    all_columns: List[str],
    id_name: str = 'hospitalization_id'
) -> Optional[str]:
    """Build a separate query for one-hot encoding."""
    
    valid_columns = [col for col in one_hot_columns if col in all_columns]
    if not valid_columns:
        return None

    select_parts = [id_name, 'window_number']

    for col in valid_columns:
        # Get unique values for this column
        unique_vals_query = f"""
        SELECT DISTINCT {col}
        FROM windowed_data
        WHERE {col} IS NOT NULL
        ORDER BY {col}
        LIMIT 100  -- Limit to prevent too many columns
        """
        
        try:
            unique_vals_result = conn.execute(unique_vals_query).fetchall()
            
            if len(unique_vals_result) > 50:
                logger.warning(f"{col} has {len(unique_vals_result)} unique values. One-hot encoding may create many columns")
            
            # Create conditional aggregation for each unique value
            for (val,) in unique_vals_result:
                # Clean column name
                clean_val = re.sub(r'[^a-zA-Z0-9_]', '_', str(val))
                col_name = f"{col}_{clean_val}"
                
                # Handle string values with proper escaping
                if isinstance(val, str):
                    val_escaped = val.replace("'", "''")
                    select_parts.append(f"MAX(CASE WHEN {col} = '{val_escaped}' THEN 1 ELSE 0 END) AS {col_name}")
                else:
                    select_parts.append(f"MAX(CASE WHEN {col} = {val} THEN 1 ELSE 0 END) AS {col_name}")


        except Exception as e:
            logger.warning(f"Could not create one-hot encoding for {col}: {str(e)}")


    if len(select_parts) > 2:  # More than just the group by columns
        query = f"""
        SELECT
            {', '.join(select_parts)}
        FROM windowed_data
        GROUP BY {id_name}, window_number
        """
        return query

    return None


def _process_hospitalizations(
    conn: duckdb.DuckDBPyConnection,
    clif_instance,
    required_ids: List[str],
    patient_df: pd.DataFrame,
    hospitalization_df: pd.DataFrame,
    adt_df: pd.DataFrame,
    tables_to_load: List[str],
    category_filters: Dict[str, List[str]],
    pivot_tables: List[str],
    wide_tables: List[str],
    show_progress: bool,
    cohort_df: Optional[pd.DataFrame] = None
) -> Optional[pd.DataFrame]:
    """Process hospitalizations with pivot-first approach."""
    
    logger.debug("    4.S.1: Loading and filtering base tables")

    # Create base cohort
    base_cohort = pd.merge(hospitalization_df, patient_df, on='patient_id', how='inner')
    logger.info(f"           - Base cohort created with {len(base_cohort)} records")
    
    # Register base tables as proper tables, not views
    conn.register('temp_base', base_cohort)
    conn.execute("CREATE OR REPLACE TABLE base_cohort AS SELECT * FROM temp_base")
    conn.unregister('temp_base')
    
    conn.register('temp_adt', adt_df)
    conn.execute("CREATE OR REPLACE TABLE adt AS SELECT * FROM temp_adt")
    conn.unregister('temp_adt')
    
    # Dictionaries to store table info
    event_time_queries = []
    pivoted_table_names = {}
    raw_table_names = {}
    
    # Add ADT event times
    if 'in_dttm' in adt_df.columns:
        event_time_queries.append("""
            SELECT DISTINCT hospitalization_id, in_dttm AS event_time 
            FROM adt 
            WHERE in_dttm IS NOT NULL
        """)
    
    logger.info("    4.S.3: Processing tables")

    # Process tables to load
    for table_name in tables_to_load:

        logger.info(f"           - Processing {table_name}")

        # Get table data - fix: use 'labs' instead of 'lab'
        table_attr = table_name  # Use table_name directly
        table_obj = getattr(clif_instance, table_attr, None)

        if table_obj is None:
            logger.warning(f"{table_name} not loaded in CLIF instance, skipping")
            continue
            
        # Filter by hospitalization IDs immediately
        # Check if this is medication table with converted data (from config)
        table_config = _get_table_config(table_name)
        if table_config and table_config.get('supports_unit_conversion', False):
            # Check if converted data exists
            if hasattr(table_obj, 'df_converted') and table_obj.df_converted is not None:
                logger.info(f"           === SPECIAL: USING CONVERTED MEDICATION DATA ===")
                # Use all converted data (both successful and failed conversions)
                all_data = table_obj.df_converted[table_obj.df_converted['hospitalization_id'].isin(required_ids)]
                table_df = all_data.copy()

                # Report conversion statistics
                success_count = (all_data['_convert_status'] == 'success').sum()
                failed_count = len(all_data) - success_count

                if failed_count > 0:
                    percentage = (failed_count / len(all_data)) * 100
                    logger.info(f"           - Including all {len(all_data):,} rows: {success_count:,} successful conversions, {failed_count:,} ({percentage:.1f}%) fallback to original units")
                else:
                    logger.info(f"           - All {len(table_df):,} conversions successful")
            else:
                # Fallback to original behavior
                logger.info(f"           - No converted data found, using original medication data")
                table_df = table_obj.df[table_obj.df['hospitalization_id'].isin(required_ids)].copy()
        else:
            # Original behavior for other tables
            table_df = table_obj.df[table_obj.df['hospitalization_id'].isin(required_ids)].copy()

        if len(table_df) == 0:
            logger.warning(f"No data found in {table_name} for selected hospitalizations")
            continue
        
        # For wide tables (non-pivot), filter columns based on category_filters
        if table_name in wide_tables and table_name in category_filters:
            # For respiratory_support, category_filters contains column names to keep
            required_cols = ['hospitalization_id']  # Always keep hospitalization_id
            timestamp_col = _get_timestamp_column(table_name)
            if timestamp_col:
                required_cols.append(timestamp_col)
            
            # Add the columns specified in category_filters
            specified_cols = category_filters[table_name]
            required_cols.extend(specified_cols)
            
            # Filter to only available columns
            available_cols = [col for col in required_cols if col in table_df.columns]
            missing_cols = [col for col in required_cols if col not in table_df.columns]

            if missing_cols:
                logger.warning(f"Columns not found in {table_name}: {missing_cols}")

            if available_cols:
                table_df = table_df[available_cols].copy()
                logger.debug(f"Filtered {table_name} to {len(available_cols)} columns: {available_cols}")

        logger.info(f"Loaded {len(table_df)} records from {table_name}")

        # Get timestamp column
        timestamp_col = _get_timestamp_column(table_name)
        if timestamp_col and timestamp_col not in table_df.columns:
            timestamp_col = _find_alternative_timestamp(table_name, table_df.columns)

        if not timestamp_col or timestamp_col not in table_df.columns:
            logger.warning(f"No timestamp column found for {table_name}, skipping")
            continue
        
        # Apply time filtering if cohort_df is provided
        if cohort_df is not None:

            logger.info("           === SPECIAL: TIME FILTERING ===")
            pre_filter_count = len(table_df)
            logger.debug(f"           - Applying cohort time windows to {table_name}")
            # Merge with cohort_df to get time windows
            table_df = pd.merge(
                table_df,
                cohort_df[['hospitalization_id', 'start_time', 'end_time']],
                on='hospitalization_id',
                how='inner'
            )

            # Ensure timestamp column is datetime
            if not pd.api.types.is_datetime64_any_dtype(table_df[timestamp_col]):
                table_df[timestamp_col] = pd.to_datetime(table_df[timestamp_col])

            # Filter to time window
            table_df = table_df[
                (table_df[timestamp_col] >= table_df['start_time']) &
                (table_df[timestamp_col] <= table_df['end_time'])
            ].copy()

            # Drop the time window columns
            table_df = table_df.drop(columns=['start_time', 'end_time'])

            logger.info(f"           - {table_name}: {pre_filter_count} -> {len(table_df)} records after filtering")

        # Register raw table as a proper table, not a view
        raw_table_name = f"{table_name}_raw"
        # First register the DataFrame temporarily
        conn.register('temp_df', table_df)
        # Create a proper table from it
        conn.execute(f"CREATE OR REPLACE TABLE {raw_table_name} AS SELECT * FROM temp_df")
        # Clean up the temporary registration
        conn.unregister('temp_df')
        raw_table_names[table_name] = raw_table_name
        
        # Process based on table type
        if table_name in pivot_tables:

            logger.info(f"           === PIVOTING {table_name.upper()} ===")
            if table_name in category_filters and category_filters[table_name]:
                logger.debug(f"           - Categories to pivot: {category_filters[table_name]}")
            # Pivot the table first
            pivoted_name = _pivot_table_duckdb(conn, table_name, table_df, timestamp_col, category_filters)
            if pivoted_name:
                pivoted_table_names[table_name] = pivoted_name
                # Add event times from the RAW table (not pivoted)
                event_time_queries.append(f"""
                    SELECT DISTINCT hospitalization_id, {timestamp_col} AS event_time
                    FROM {raw_table_name}
                    WHERE {timestamp_col} IS NOT NULL
                """)
        else:

            logger.info(f"           === WIDE TABLE {table_name.upper()} ===")
            if table_name in category_filters and category_filters[table_name]:
                logger.debug(f"           - Keeping columns: {category_filters[table_name]}")
            # Wide table - just add event times
            event_time_queries.append(f"""
                SELECT DISTINCT hospitalization_id, {timestamp_col} AS event_time
                FROM {raw_table_name}
                WHERE {timestamp_col} IS NOT NULL
            """)
    
    # Now create the union and join
    if event_time_queries:
        logger.info("    4.S.4: Creating wide dataset")
        logger.debug("           - Building event time union from {} tables".format(len(event_time_queries)))
        logger.debug("           - Creating combo_id keys")
        logger.debug("           - Executing main join query")
        final_df = _create_wide_dataset(
            conn, base_cohort, event_time_queries,
            pivoted_table_names, raw_table_names,
            tables_to_load, pivot_tables,
            category_filters, cohort_df
        )
        return final_df
    else:
        logger.warning("           - No event times found, returning base cohort only")
        return base_cohort


def _pivot_table_duckdb(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    table_df: pd.DataFrame,
    timestamp_col: str,
    category_filters: Dict[str, List[str]]
) -> Optional[str]:
    """Pivot a table and return the pivoted table name."""

    # Get column mappings from config
    table_config = _get_table_config(table_name)

    if not table_config:
        logger.warning(f"No configuration found for {table_name}")
        return None

    category_col = table_config.get('category_column')
    value_col = table_config.get('value_column')

    # Check if this is medication table with converted data
    has_converted_meds = False
    unit_col = None
    if table_config.get('supports_unit_conversion', False):
        # Check if converted columns exist in the dataframe
        converted_value_col = table_config.get('converted_value_column')
        converted_unit_col = table_config.get('converted_unit_column')

        if (converted_value_col and converted_unit_col and
                converted_value_col in table_df.columns and converted_unit_col in table_df.columns):
            has_converted_meds = True
            value_col = converted_value_col
            unit_col = converted_unit_col
            logger.debug(f"           - Using converted medication columns: {value_col}, {unit_col}")
        else:
            logger.debug(f"           - Using original medication column: {value_col}")

    if not category_col or not value_col:
        logger.warning(f"No pivot configuration for {table_name}")
        return None

    if category_col not in table_df.columns or value_col not in table_df.columns:
        logger.warning(f"Required columns {category_col} or {value_col} not found in {table_name}")
        return None
    
    # Build filter clause if categories specified
    filter_clause = ""
    if table_name in category_filters and category_filters[table_name]:
        categories_list = "','".join(category_filters[table_name])
        filter_clause = f"AND {category_col} IN ('{categories_list}')"
        logger.debug(f"Filtering {table_name} categories to: {category_filters[table_name]}")

    # Create pivot query
    pivoted_table_name = f"{table_name}_pivoted"

    if has_converted_meds:
        # Special pivot for medications with units
        logger.info(f"           - Creating unit-aware pivot with columns: category_unit format")
        logger.debug(f"           - Including both successful conversions and original units for failed conversions")
        pivot_query = f"""
        CREATE OR REPLACE TABLE {pivoted_table_name} AS
        WITH pivot_data AS (
            SELECT DISTINCT
                {value_col} as value,
                {category_col} || '_' ||
                REPLACE(REPLACE(REPLACE(REPLACE({unit_col}, '/', '_'), '-', '_'), ' ', '_'), '.', '_')
                AS category_for_pivot,
                hospitalization_id || '_' || strftime({timestamp_col}, '%Y%m%d%H%M') AS combo_id
            FROM {table_name}_raw
            WHERE {timestamp_col} IS NOT NULL {filter_clause}
        )
        PIVOT pivot_data
        ON category_for_pivot
        USING first(value)
        GROUP BY combo_id
        """
    else:
        # Original pivot query for other tables
        pivot_query = f"""
        CREATE OR REPLACE TABLE {pivoted_table_name} AS
        WITH pivot_data AS (
            SELECT DISTINCT
                {value_col},
                {category_col},
                hospitalization_id || '_' || strftime({timestamp_col}, '%Y%m%d%H%M') AS combo_id
            FROM {table_name}_raw
            WHERE {timestamp_col} IS NOT NULL {filter_clause}
        )
        PIVOT pivot_data
        ON {category_col}
        USING first({value_col})
        GROUP BY combo_id
        """
    
    try:
        conn.execute(pivot_query)
        
        # Get stats
        count = conn.execute(f"SELECT COUNT(*) FROM {pivoted_table_name}").fetchone()[0]
        cols = len(conn.execute(f"SELECT * FROM {pivoted_table_name} LIMIT 0").df().columns) - 1

        if has_converted_meds:
            logger.info(f"Pivoted {table_name}: {count} combo_ids with {cols} medication_unit columns")
        else:
            logger.info(f"Pivoted {table_name}: {count} combo_ids with {cols} category columns")
        return pivoted_table_name

    except Exception as e:
        logger.error(f"Error pivoting {table_name}: {str(e)}")
        return None


def _create_wide_dataset(
    conn: duckdb.DuckDBPyConnection,
    base_cohort: pd.DataFrame,
    event_time_queries: List[str],
    pivoted_table_names: Dict[str, str],
    raw_table_names: Dict[str, str],
    tables_to_load: List[str],
    pivot_tables: List[str],
    category_filters: Dict[str, List[str]],
    cohort_df: Optional[pd.DataFrame] = None
) -> pd.DataFrame:
    """Create the final wide dataset by joining all tables."""
    
    # Create union of all event times
    union_query = " UNION ALL ".join(event_time_queries)
    
    # Build the main query
    query = f"""
    WITH all_events AS (
        SELECT DISTINCT hospitalization_id, event_time
        FROM ({union_query}) uni_time
    ),
    expanded_cohort AS (
        SELECT 
            a.*,
            b.event_time,
            a.hospitalization_id || '_' || strftime(b.event_time, '%Y%m%d%H%M') AS combo_id
        FROM base_cohort a
        INNER JOIN all_events b ON a.hospitalization_id = b.hospitalization_id
    )
    SELECT ec.*
    """
    
    # Add ADT columns
    if 'adt' in conn.execute("SHOW TABLES").df()['name'].values:
        adt_cols = [col for col in conn.execute("SELECT * FROM adt LIMIT 0").df().columns 
                   if col not in ['hospitalization_id']]
        if adt_cols:
            adt_col_list = ', '.join([f"adt_combo.{col}" for col in adt_cols])
            query = query.replace("SELECT ec.*", f"SELECT ec.*, {adt_col_list}")
    
    # Add pivoted table columns
    for table_name, pivoted_table_name in pivoted_table_names.items():
        pivot_cols = conn.execute(f"SELECT * FROM {pivoted_table_name} LIMIT 0").df().columns
        pivot_cols = [col for col in pivot_cols if col != 'combo_id']
        
        if pivot_cols:
            pivot_col_list = ', '.join([f"{pivoted_table_name}.{col}" for col in pivot_cols])
            query = query.replace("SELECT ec.*", f"SELECT ec.*, {pivot_col_list}")
    
    # Add non-pivoted table columns (respiratory_support)
    for table_name in tables_to_load:
        if table_name not in pivot_tables and table_name in raw_table_names:
            timestamp_col = _get_timestamp_column(table_name)
            if not timestamp_col:
                continue
                
            raw_cols = conn.execute(f"SELECT * FROM {raw_table_names[table_name]} LIMIT 0").df().columns
            table_cols = [col for col in raw_cols if col not in ['hospitalization_id', timestamp_col]]
            
            if table_cols:
                col_list = ', '.join([f"{table_name}_combo.{col}" for col in table_cols])
                query = query.replace("SELECT ec.*", f"SELECT ec.*, {col_list}")
    
    # Add FROM clause
    query += " FROM expanded_cohort ec"
    
    # Add ADT join
    if 'adt' in conn.execute("SHOW TABLES").df()['name'].values:
        query += """
        LEFT JOIN (
            SELECT 
                hospitalization_id || '_' || strftime(in_dttm, '%Y%m%d%H%M') AS combo_id,
                *
            FROM adt
            WHERE in_dttm IS NOT NULL
        ) adt_combo USING (combo_id)
        """
    
    # Add joins for pivoted tables
    for table_name, pivoted_table_name in pivoted_table_names.items():
        query += f" LEFT JOIN {pivoted_table_name} USING (combo_id)"
    
    # Add joins for non-pivoted tables
    for table_name in tables_to_load:
        if table_name not in pivot_tables and table_name in raw_table_names:
            timestamp_col = _get_timestamp_column(table_name)
            if timestamp_col:
                raw_cols = conn.execute(f"SELECT * FROM {raw_table_names[table_name]} LIMIT 0").df().columns
                if timestamp_col in raw_cols:
                    table_cols = [col for col in raw_cols if col not in ['hospitalization_id', timestamp_col]]
                    if table_cols:
                        col_list = ', '.join(table_cols)
                        query += f"""
                        LEFT JOIN (
                            SELECT 
                                hospitalization_id || '_' || strftime({timestamp_col}, '%Y%m%d%H%M') AS combo_id,
                                {col_list}
                            FROM {raw_table_names[table_name]}
                            WHERE {timestamp_col} IS NOT NULL
                        ) {table_name}_combo USING (combo_id)
                        """
    
    # Execute query
    logger.debug("Executing join query")
    result_df = conn.execute(query).df()

    # Apply final time filtering if cohort_df is provided
    if cohort_df is not None:
        pre_filter_count = len(result_df)
        logger.debug("Applying cohort time window filtering to final dataset")
        
        # Merge with cohort_df to get time windows
        result_df = pd.merge(
            result_df,
            cohort_df[['hospitalization_id', 'start_time', 'end_time']],
            on='hospitalization_id',
            how='inner'
        )
        
        # Ensure event_time column is datetime
        if not pd.api.types.is_datetime64_any_dtype(result_df['event_time']):
            result_df['event_time'] = pd.to_datetime(result_df['event_time'])
        
        # Filter to time window
        result_df = result_df[
            (result_df['event_time'] >= result_df['start_time']) &
            (result_df['event_time'] <= result_df['end_time'])
        ].copy()
        
        # Drop the time window columns
        result_df = result_df.drop(columns=['start_time', 'end_time'])

        logger.info(f"  Final time filtering: {pre_filter_count} -> {len(result_df)} records")
    
    # Remove duplicate columns
    result_df = result_df.loc[:, ~result_df.columns.duplicated()]
    
    # Add day-based columns
    result_df['date'] = pd.to_datetime(result_df['event_time']).dt.date
    result_df = result_df.sort_values(['hospitalization_id', 'event_time']).reset_index(drop=True)
    result_df['day_number'] = result_df.groupby('hospitalization_id')['date'].rank(method='dense').astype(int)
    result_df['hosp_id_day_key'] = (result_df['hospitalization_id'].astype(str) + '_day_' +
                                    result_df['day_number'].astype(str))

    logger.info("    === SPECIAL: MISSING COLUMNS ===")
    # Add missing columns for requested categories
    _add_missing_columns(result_df, category_filters, tables_to_load)

    logger.info("    4.S.6: Final cleanup")
    # Clean up
    columns_to_drop = ['combo_id', 'date']
    result_df = result_df.drop(columns=[col for col in columns_to_drop if col in result_df.columns])
    logger.debug("           - Removing duplicate columns")
    logger.debug("           - Dropping temporary columns (combo_id, date)")

    logger.info(f"           - Wide dataset created: {len(result_df)} records with {len(result_df.columns)} columns")
    
    return result_df


def _add_missing_columns(
    df: pd.DataFrame,
    category_filters: Dict[str, List[str]],
    tables_loaded: List[str]
):
    """Add missing columns for categories that were requested but not found in data."""

    if not category_filters:
        return

    # Get medication tables from config
    config = _load_wide_tables_config()
    medication_tables = []
    for table_name, table_config in config.get('tables', {}).items():
        if table_config.get('supports_unit_conversion', False):
            medication_tables.append(table_name)

    for table_name, categories in category_filters.items():
        if table_name in tables_loaded and categories:
            for category in categories:
                # For medication tables, check for unit-aware columns (e.g., norepinephrine_mcg_min)
                if table_name in medication_tables:
                    # Look for any column that starts with the category name followed by underscore
                    pattern_matches = [col for col in df.columns if col.startswith(f"{category}_")]

                    if not pattern_matches and category not in df.columns:
                        # No unit-aware column or exact match found, add empty column
                        df[category] = np.nan
                        logger.debug(f"           - Added missing column: {category}")
                    elif pattern_matches:
                        # Found unit-aware columns, don't add empty column
                        logger.debug(f"           - Found unit-aware columns for {category}: {pattern_matches}")
                else:
                    # For non-medication tables, use exact matching as before
                    if category not in df.columns:
                        df[category] = np.nan
                        logger.debug(f"           - Added missing column: {category}")


def _process_in_batches(
    conn: duckdb.DuckDBPyConnection,
    clif_instance,
    all_hosp_ids: List[str],
    patient_df: pd.DataFrame,
    hospitalization_df: pd.DataFrame,
    adt_df: pd.DataFrame,
    tables_to_load: List[str],
    category_filters: Dict[str, List[str]],
    pivot_tables: List[str],
    wide_tables: List[str],
    batch_size: int,
    show_progress: bool,
    save_to_data_location: bool,
    output_filename: Optional[str],
    output_format: str,
    return_dataframe: bool,
    cohort_df: Optional[pd.DataFrame] = None
) -> Optional[pd.DataFrame]:
    """Process hospitalizations in batches using the new approach."""
    
    # Split into batches
    batches = [all_hosp_ids[i:i + batch_size] for i in range(0, len(all_hosp_ids), batch_size)]
    batch_results = []
    
    iterator = tqdm(batches, desc="Processing batches") if show_progress else batches
    
    for batch_idx, batch_hosp_ids in enumerate(iterator):
        try:
            logger.info(f"    4.B.{batch_idx + 1}: Processing batch {batch_idx + 1}/{len(batches)}")
            logger.debug(f"             - {len(batch_hosp_ids)} hospitalizations in batch")
            
            # Filter base tables for this batch
            batch_hosp_df = hospitalization_df[hospitalization_df['hospitalization_id'].isin(batch_hosp_ids)]
            batch_adt_df = adt_df[adt_df['hospitalization_id'].isin(batch_hosp_ids)]
            
            # Filter cohort_df for this batch if provided
            batch_cohort_df = None
            if cohort_df is not None:
                batch_cohort_df = cohort_df[cohort_df['hospitalization_id'].isin(batch_hosp_ids)].copy()
            
            # Clean up tables from previous batch
            tables_df = conn.execute("SHOW TABLES").df()
            for idx, row in tables_df.iterrows():
                table_name = row['name']
                if table_name not in ['base_cohort', 'adt']:
                    try:
                        # Try to drop as table first
                        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                    except:
                        # If that fails, try to drop as view
                        try:
                            conn.execute(f"DROP VIEW IF EXISTS {table_name}")
                        except:
                            pass
            
            # Process this batch
            batch_result = _process_hospitalizations(
                conn, clif_instance, batch_hosp_ids, patient_df, batch_hosp_df, batch_adt_df,
                tables_to_load, category_filters, pivot_tables, wide_tables,
                show_progress=False, cohort_df=batch_cohort_df
            )
            
            if batch_result is not None and len(batch_result) > 0:
                batch_results.append(batch_result)
                logger.info(f"             - Batch {batch_idx + 1} completed: {len(batch_result)} records")
            
            # Clean up after batch
            import gc
            gc.collect()
            
        except Exception as e:
            logger.error(f"Failed to process batch {batch_idx + 1}: {str(e)}")
            continue

    # Combine results
    if batch_results:
        logger.info(f"             - Combining {len(batch_results)} batch results")
        final_df = pd.concat(batch_results, ignore_index=True)
        logger.info(f"             - Final dataset: {len(final_df)} records with {len(final_df.columns)} columns")
        
        if save_to_data_location:
            _save_dataset(final_df, clif_instance.data_directory, output_filename, output_format)
        
        return final_df if return_dataframe else None
    else:
        logger.error("No data processed successfully")
        return None