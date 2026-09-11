"""
Outlier handling utilities for pyCLIF tables.

This module provides functions to detect and handle outliers in clinical data
based on configurable range specifications. Values outside the specified ranges
are converted to NaN.
"""

import os
import yaml
import pandas as pd
import polars as pl
from typing import Optional, Dict, Any, Tuple
from pathlib import Path
from tqdm import tqdm


def apply_outlier_handling(table_obj, outlier_config_path: Optional[str] = None) -> None:
    """
    Apply outlier handling to a table object's dataframe.

    This function identifies numeric values that fall outside acceptable ranges
    and converts them to NaN. For category-dependent columns (vitals, labs,
    medications, assessments), ranges are applied based on the category value.

    Uses ultra-fast Polars implementation with progress tracking.

    Parameters
    ----------
    table_obj
        A pyCLIF table object with .df (DataFrame) and .table_name attributes
    outlier_config_path : str, optional
        Path to custom outlier configuration YAML.
        If None, uses internal CLIF standard config.

    Returns
    -------
    None
        modifies table_obj.df in-place
    """
    if table_obj.df is None or table_obj.df.empty:
        print("No data to process for outlier handling.")
        return

    # Load outlier configuration
    config = _load_outlier_config(outlier_config_path)
    if not config:
        print("Failed to load outlier configuration.")
        return

    # Print which configuration is being used
    if outlier_config_path is None:
        print("Using CLIF standard outlier ranges\n")
    else:
        print(f"Using custom outlier ranges from: {outlier_config_path}\n")

    # Get table-specific configuration
    table_config = config.get('tables', {}).get(table_obj.table_name, {})
    if not table_config:
        print(f"No outlier configuration found for table: {table_obj.table_name}")
        return

    # Filter columns that exist in the dataframe
    existing_columns = {col: conf for col, conf in table_config.items() if col in table_obj.df.columns}

    if not existing_columns:
        print("No configured columns found in dataframe.")
        return

    # Ultra-fast processing with single conversion
    _process_all_columns_ultra_fast(table_obj, existing_columns)


def _load_outlier_config(config_path: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Load outlier configuration from YAML file."""
    try:
        if config_path is None:
            # Use internal CLIF config
            config_path = os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                'schemas',
                'outlier_config.yaml'
            )

        if not os.path.exists(config_path):
            print(f"Outlier configuration file not found: {config_path}")
            return None

        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)

    except Exception as e:
        print(f"Error loading outlier configuration: {str(e)}")
        return None


def _process_all_columns_ultra_fast(table_obj, column_configs: Dict[str, Any]) -> None:
    """
    Ultra-fast processing of all columns using single Polars conversion.

    This function processes all columns in a single pass with minimal conversions
    and maximum parallelization.
    """
    # Get before statistics (in pandas for compatibility)
    before_stats = _compute_all_statistics_fast(table_obj, column_configs)

    # Convert to Polars LazyFrame ONCE for maximum optimization
    df_lazy = pl.from_pandas(table_obj.df).lazy()

    # Build all column expressions
    print("Building outlier expressions...")
    column_expressions = _build_all_expressions(table_obj, column_configs)

    if not column_expressions:
        print("No expressions to apply.")
        return

    # Apply all transformations in single operation with progress
    print("Applying outlier filtering...")
    with tqdm(total=1, desc="Processing", unit="operation") as pbar:
        df_result = df_lazy.with_columns(column_expressions).collect()
        pbar.update(1)

    # Convert back to pandas ONCE
    table_obj.df = df_result.to_pandas()

    # Get after statistics and print detailed results
    after_stats = _compute_all_statistics_fast(table_obj, column_configs)
    _print_detailed_statistics(table_obj, before_stats, after_stats, column_configs)


def _build_all_expressions(table_obj, column_configs: Dict[str, Any]) -> list:
    """
    Build all outlier filtering expressions for all columns at once.

    Returns a list of Polars expressions to apply.
    """
    expressions = []

    # Progress bar for expression building
    with tqdm(total=len(column_configs), desc="Building expressions", unit="column") as pbar:
        for column_name, column_config in column_configs.items():
            expr = _build_column_expression(table_obj, column_name, column_config)
            if expr is not None:
                expressions.append(expr)
            pbar.update(1)

    return expressions


def _build_column_expression(table_obj, column_name: str, column_config: Dict[str, Any]):
    """Build a Polars expression for a single column based on its configuration."""

    # Category-dependent columns (vitals, labs, assessments)
    if (table_obj.table_name in ['vitals', 'labs', 'patient_assessments'] and
        column_name in ['vital_value', 'lab_value_numeric', 'numerical_value']):
        return _build_category_dependent_expression(table_obj, column_name, column_config)

    # Medication dose column
    elif (table_obj.table_name in ['medication_admin_continuous', 'medication_admin_intermittent'] and column_name == 'med_dose'):
        return _build_medication_expression(table_obj, column_config)

    # Simple range columns
    else:
        return _build_simple_range_expression(column_name, column_config)


def _build_category_dependent_expression(table_obj, column_name: str, column_config: Dict[str, Any]):
    """Build expression for category-dependent columns."""
    # Determine category column
    if table_obj.table_name == 'vitals':
        category_col = 'vital_category'
    elif table_obj.table_name == 'labs':
        category_col = 'lab_category'
    elif table_obj.table_name == 'patient_assessments':
        category_col = 'assessment_category'
    else:
        return None

    # Check if category column exists in dataframe
    if category_col not in table_obj.df.columns:
        print(f"Warning: Category column '{category_col}' not found in dataframe. Skipping outlier handling for {column_name}.")
        return None

    # Start with the original column
    expr = pl.col(column_name)

    # Build chained when-then-otherwise for all categories
    for category, range_config in column_config.items():
        if isinstance(range_config, dict) and 'min' in range_config and 'max' in range_config:
            min_val = range_config['min']
            max_val = range_config['max']

            # Condition: category matches AND value is outlier
            condition = (
                (pl.col(category_col).str.to_lowercase() == category.lower()) &
                ((pl.col(column_name) < min_val) | (pl.col(column_name) > max_val))
            )

            # Chain the condition
            expr = pl.when(condition).then(None).otherwise(expr)

    return expr.alias(column_name)


def _build_medication_expression(table_obj, column_config: Dict[str, Any]):
    """Build expression for medication dose column."""
    # Start with the original column
    expr = pl.col('med_dose')

    # Build chained when-then-otherwise for all medication/unit combinations
    for med_category, unit_configs in column_config.items():
        if isinstance(unit_configs, dict):
            for unit, range_config in unit_configs.items():
                if isinstance(range_config, dict) and 'min' in range_config and 'max' in range_config:
                    min_val = range_config['min']
                    max_val = range_config['max']

                    # Condition: medication/unit matches AND value is outlier
                    condition = (
                        (pl.col('med_category').str.to_lowercase() == med_category.lower()) &
                        (pl.col('med_dose_unit').str.to_lowercase() == unit.lower()) &
                        ((pl.col('med_dose') < min_val) | (pl.col('med_dose') > max_val))
                    )

                    # Chain the condition
                    expr = pl.when(condition).then(None).otherwise(expr)

    return expr.alias('med_dose')


def _build_simple_range_expression(column_name: str, column_config: Dict[str, Any]):
    """Build expression for simple range columns."""
    if isinstance(column_config, dict) and 'min' in column_config and 'max' in column_config:
        min_val = column_config['min']
        max_val = column_config['max']

        # Simple outlier condition
        expr = pl.when(
            (pl.col(column_name) < min_val) | (pl.col(column_name) > max_val)
        ).then(None).otherwise(pl.col(column_name))

        return expr.alias(column_name)

    return None


def _compute_all_statistics_fast(table_obj, column_configs: Dict[str, Any]) -> Dict[str, Any]:
    """
    Compute statistics for all columns efficiently using Polars.

    Returns a dictionary with statistics for each column type.
    """
    stats = {}

    for column_name, column_config in column_configs.items():
        # Category-dependent columns
        if (table_obj.table_name in ['vitals', 'labs', 'patient_assessments'] and
            column_name in ['vital_value', 'lab_value_numeric', 'numerical_value']):

            # Determine category column
            if table_obj.table_name == 'vitals':
                category_col = 'vital_category'
            elif table_obj.table_name == 'labs':
                category_col = 'lab_category'
            elif table_obj.table_name == 'patient_assessments':
                category_col = 'assessment_category'
            else:
                continue

            # Check if category column exists
            if category_col in table_obj.df.columns:
                stats[column_name] = _get_category_statistics_pandas(table_obj.df, column_name, category_col)
            else:
                # Treat as simple range if category column is missing
                stats[column_name] = {
                    'simple_range': {
                        'non_null_count': table_obj.df[column_name].notna().sum(),
                        'total_count': len(table_obj.df)
                    }
                }

        # Medication dose column
        elif (table_obj.table_name in ['medication_admin_continuous', 'medication_admin_intermittent'] and column_name == 'med_dose'):
            stats[column_name] = _get_medication_statistics_pandas(table_obj.df)

        # Simple range columns
        else:
            stats[column_name] = {
                'simple_range': {
                    'non_null_count': table_obj.df[column_name].notna().sum(),
                    'total_count': len(table_obj.df)
                }
            }

    return stats


def _print_detailed_statistics(table_obj, before_stats: Dict[str, Any], after_stats: Dict[str, Any], column_configs: Dict[str, Any]) -> None:
    """
    Print detailed statistics in the same format as the original implementation.
    """
    for column_name, column_config in column_configs.items():
        # Category-dependent columns
        if (table_obj.table_name in ['vitals', 'labs', 'patient_assessments'] and
            column_name in ['vital_value', 'lab_value_numeric', 'numerical_value']):

            # Determine table display name
            if table_obj.table_name == 'vitals':
                table_display_name = "Vitals"
            elif table_obj.table_name == 'labs':
                table_display_name = "Labs"
            elif table_obj.table_name == 'patient_assessments':
                table_display_name = "Patient Assessments"
            else:
                continue

            before_cat_stats = before_stats.get(column_name, {})
            after_cat_stats = after_stats.get(column_name, {})

            print(f"\n{table_display_name} Table - Category Statistics:")
            for category in sorted(set(before_cat_stats.keys()) | set(after_cat_stats.keys())):
                before_count = before_cat_stats.get(category, {}).get('non_null_count', 0)
                after_count = after_cat_stats.get(category, {}).get('non_null_count', 0)
                nullified = before_count - after_count

                if before_count > 0:
                    percentage = (nullified / before_count) * 100
                    print(f"  {category:<20}: {before_count:>6} values → {nullified:>6} nullified ({percentage:>5.1f}%)")
                else:
                    print(f"  {category:<20}: {before_count:>6} values → {nullified:>6} nullified (  0.0%)")

        # Medication dose column
        elif (table_obj.table_name in ['medication_admin_continuous', 'medication_admin_intermittent'] and column_name == 'med_dose'):
            before_med_stats = before_stats.get(column_name, {})
            after_med_stats = after_stats.get(column_name, {})

            print(f"\nMedication Table - Category/Unit Statistics:")
            for med_unit in sorted(set(before_med_stats.keys()) | set(after_med_stats.keys())):
                before_count = before_med_stats.get(med_unit, {}).get('non_null_count', 0)
                after_count = after_med_stats.get(med_unit, {}).get('non_null_count', 0)
                nullified = before_count - after_count

                if before_count > 0:
                    percentage = (nullified / before_count) * 100
                    print(f"  {med_unit:<30}: {before_count:>6} values → {nullified:>6} nullified ({percentage:>5.1f}%)")
                else:
                    print(f"  {med_unit:<30}: {before_count:>6} values → {nullified:>6} nullified (  0.0%)")

        # Simple range columns
        else:
            before_simple = before_stats.get(column_name, {}).get('simple_range', {})
            after_simple = after_stats.get(column_name, {}).get('simple_range', {})

            before_count = before_simple.get('non_null_count', 0)
            after_count = after_simple.get('non_null_count', 0)
            nullified = before_count - after_count

            if before_count > 0:
                percentage = (nullified / before_count) * 100
                print(f"{column_name:<30}: {before_count:>6} values → {nullified:>6} nullified ({percentage:>5.1f}%)")
            else:
                print(f"{column_name:<30}: {before_count:>6} values → {nullified:>6} nullified (  0.0%)")


def _get_category_statistics_pandas(df: pd.DataFrame, column_name: str, category_col: str) -> Dict[str, Dict[str, int]]:
    """Get per-category statistics for non-null values using polars for optimization."""
    try:
        # Convert to Polars for faster processing
        df_pl = pl.from_pandas(df)

        # Single group_by operation to get statistics for all categories
        stats_df = (
            df_pl
            .filter(pl.col(category_col).is_not_null())
            .group_by(category_col)
            .agg([
                pl.col(column_name).is_not_null().sum().alias('non_null_count'),
                pl.len().alias('total_count')
            ])
        )

        # Convert back to the expected dictionary format
        stats = {}
        for row in stats_df.to_dicts():
            category = row[category_col]
            stats[category] = {
                'non_null_count': row['non_null_count'],
                'total_count': row['total_count']
            }

        return stats
    except Exception as e:
        print(f"Warning: Could not get category statistics: {str(e)}")
        return {}


def _get_medication_statistics_pandas(df: pd.DataFrame) -> Dict[str, Dict[str, int]]:
    """Get per-medication-unit statistics for non-null values using polars for optimization."""
    try:
        # Convert to Polars for faster processing
        df_pl = pl.from_pandas(df)

        # Single group_by operation to get statistics for all medication/unit combinations
        stats_df = (
            df_pl
            .filter(
                pl.col('med_category').is_not_null() &
                pl.col('med_dose_unit').is_not_null()
            )
            .group_by(['med_category', 'med_dose_unit'])
            .agg([
                pl.col('med_dose').is_not_null().sum().alias('non_null_count'),
                pl.len().alias('total_count')
            ])
        )

        # Convert back to the expected dictionary format
        stats = {}
        for row in stats_df.to_dicts():
            med_category = row['med_category']
            unit = row['med_dose_unit']
            key = f"{med_category} ({unit})"
            stats[key] = {
                'non_null_count': row['non_null_count'],
                'total_count': row['total_count']
            }

        return stats
    except Exception as e:
        print(f"Warning: Could not get medication statistics: {str(e)}")
        return {}


def _process_category_dependent_column_pandas(table_obj, column_name: str, column_config: Dict[str, Any]) -> None:
    """Process columns where ranges depend on category values using polars for optimization."""
    # Determine the category column name and table display name
    if table_obj.table_name == 'vitals':
        category_col = 'vital_category'
        table_display_name = "Vitals"
    elif table_obj.table_name == 'labs':
        category_col = 'lab_category'
        table_display_name = "Labs"
    elif table_obj.table_name == 'patient_assessments':
        category_col = 'assessment_category'
        table_display_name = "Patient Assessments"
    else:
        return

    # Get before statistics
    before_stats = _get_category_statistics_pandas(table_obj.df, column_name, category_col)

    # Convert to Polars for faster processing
    df_pl = pl.from_pandas(table_obj.df)

    # Build single expression chain for all categories
    expr = pl.col(column_name)
    for category, range_config in column_config.items():
        if isinstance(range_config, dict) and 'min' in range_config and 'max' in range_config:
            min_val = range_config['min']
            max_val = range_config['max']

            # Create condition for this category and outlier values
            condition = (
                (pl.col(category_col).str.to_lowercase() == category.lower()) &
                ((pl.col(column_name) < min_val) | (pl.col(column_name) > max_val))
            )

            # Chain the when-then-otherwise for outlier nullification
            expr = pl.when(condition).then(None).otherwise(expr)

    # Apply all transformations in single operation
    df_pl = df_pl.with_columns(expr.alias(column_name))

    # Convert back to pandas and update the original dataframe
    table_obj.df = df_pl.to_pandas()

    # Get after statistics
    after_stats = _get_category_statistics_pandas(table_obj.df, column_name, category_col)

    # Print detailed category statistics
    print(f"\n{table_display_name} Table - Category Statistics:")
    for category in sorted(set(before_stats.keys()) | set(after_stats.keys())):
        before_count = before_stats.get(category, {}).get('non_null_count', 0)
        after_count = after_stats.get(category, {}).get('non_null_count', 0)
        nullified = before_count - after_count

        if before_count > 0:
            percentage = (nullified / before_count) * 100
            print(f"  {category:<20}: {before_count:>6} values → {nullified:>6} nullified ({percentage:>5.1f}%)")
        else:
            print(f"  {category:<20}: {before_count:>6} values → {nullified:>6} nullified (  0.0%)")


def _process_medication_column_pandas(table_obj, column_config: Dict[str, Any]) -> None:
    """Process medication dose column with unit-dependent ranges using polars for optimization."""

    # Get before statistics
    before_stats = _get_medication_statistics_pandas(table_obj.df)

    # Convert to Polars for faster processing
    df_pl = pl.from_pandas(table_obj.df)

    # Build single expression chain for all medication/unit combinations
    expr = pl.col('med_dose')
    for med_category, unit_configs in column_config.items():
        if isinstance(unit_configs, dict):
            for unit, range_config in unit_configs.items():
                if isinstance(range_config, dict) and 'min' in range_config and 'max' in range_config:
                    min_val = range_config['min']
                    max_val = range_config['max']

                    # Create condition for this medication category/unit and outlier values
                    condition = (
                        (pl.col('med_category').str.to_lowercase() == med_category.lower()) &
                        (pl.col('med_dose_unit').str.to_lowercase() == unit.lower()) &
                        ((pl.col('med_dose') < min_val) | (pl.col('med_dose') > max_val))
                    )

                    # Chain the when-then-otherwise for outlier nullification
                    expr = pl.when(condition).then(None).otherwise(expr)

    # Apply all transformations in single operation
    df_pl = df_pl.with_columns(expr.alias('med_dose'))

    # Convert back to pandas and update the original dataframe
    table_obj.df = df_pl.to_pandas()

    # Get after statistics
    after_stats = _get_medication_statistics_pandas(table_obj.df)

    # Print detailed medication statistics
    print(f"\nMedication Table - Category/Unit Statistics:")
    for med_unit in sorted(set(before_stats.keys()) | set(after_stats.keys())):
        before_count = before_stats.get(med_unit, {}).get('non_null_count', 0)
        after_count = after_stats.get(med_unit, {}).get('non_null_count', 0)
        nullified = before_count - after_count

        if before_count > 0:
            percentage = (nullified / before_count) * 100
            print(f"  {med_unit:<30}: {before_count:>6} values → {nullified:>6} nullified ({percentage:>5.1f}%)")
        else:
            print(f"  {med_unit:<30}: {before_count:>6} values → {nullified:>6} nullified (  0.0%)")


def _process_simple_range_column_pandas(table_obj, column_name: str, column_config: Dict[str, Any]) -> None:
    """Process columns with simple min/max ranges using polars for optimization."""
    if isinstance(column_config, dict) and 'min' in column_config and 'max' in column_config:
        min_val = column_config['min']
        max_val = column_config['max']

        # Get before count using pandas before conversion
        before_count = table_obj.df[column_name].notna().sum()

        # Convert to Polars for faster processing
        df_pl = pl.from_pandas(table_obj.df)

        # Apply outlier filtering in single vectorized operation
        expr = pl.when(
            (pl.col(column_name) < min_val) | (pl.col(column_name) > max_val)
        ).then(None).otherwise(pl.col(column_name))

        df_pl = df_pl.with_columns(expr.alias(column_name))

        # Convert back to pandas and update the original dataframe
        table_obj.df = df_pl.to_pandas()

        # Get after count and print statistics
        after_count = table_obj.df[column_name].notna().sum()
        nullified = before_count - after_count

        if before_count > 0:
            percentage = (nullified / before_count) * 100
            print(f"{column_name:<30}: {before_count:>6} values → {nullified:>6} nullified ({percentage:>5.1f}%)")
        else:
            print(f"{column_name:<30}: {before_count:>6} values → {nullified:>6} nullified (  0.0%)")


def get_outlier_summary(table_obj, outlier_config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Get a summary of potential outliers without modifying the data.

    This reuses the same range expressions as apply_outlier_handling()
    for interactive use with table objects. It provides actual outlier counts
    and percentages without modifying the data.

    Parameters
    ----------
    table_obj
        A pyCLIF table object with .df, .table_name, and .schema attributes
    outlier_config_path : str, optional
        Path to custom outlier configuration. If None, uses CLIF standard config.

    Returns
    -------
    dict
        Summary of outliers with keys:
        - table_name: Name of the table
        - total_rows: Total number of rows
        - config_source: "CLIF standard" or "Custom"
        - outliers: List of outlier validation results with counts and percentages

    See Also
    --------
    apply_outlier_handling : Applies the same ranges, nulling the outliers

    Examples
    --------
    >>> from clifpy.tables.vitals import Vitals
    >>> from clifpy.utils.outlier_handler import get_outlier_summary
    >>>
    >>> vitals = Vitals.from_file()
    >>> summary = get_outlier_summary(vitals)
    >>> print(f"Found {len(summary['outliers'])} outlier patterns")
    """
    if table_obj.df is None or table_obj.df.empty:
        return {"status": "No data to analyze"}

    # Load outlier configuration
    config = _load_outlier_config(outlier_config_path)
    if not config:
        return {"status": "Failed to load configuration"}

    # Check if table has outlier configuration. The config keys off table_name,
    # not the CLIF schema, so a schema is not required to report ranges.
    table_config = config.get('tables', {}).get(table_obj.table_name, {})
    if not table_config:
        return {"status": f"No configuration for table: {table_obj.table_name}"}

    existing_columns = {
        col: conf for col, conf in table_config.items()
        if col in table_obj.df.columns
    }
    if not existing_columns:
        return {
            "status": (
                "No configured columns present in data for table: "
                f"{table_obj.table_name}"
            )
        }

    return {
        "table_name": table_obj.table_name,
        "total_rows": len(table_obj.df),
        "config_source": "CLIF standard" if outlier_config_path is None else "Custom",
        "columns_analyzed": sorted(existing_columns),
        "outliers": _count_outliers(table_obj, existing_columns),
    }


def _count_outliers(table_obj, column_configs: Dict[str, Any]) -> Dict[str, Any]:
    """Count out-of-range values per column without modifying the data.

    Reuses the same expressions ``apply_outlier_handling`` uses to null
    outliers, so the summary can never disagree with what handling would do.
    Applies them to a copy and counts the values that would be dropped.
    """
    df = table_obj.df
    frame = df if isinstance(df, pl.DataFrame) else pl.from_pandas(df)

    counts: Dict[str, Any] = {}
    for column_name, column_config in column_configs.items():
        expr = _build_column_expression(table_obj, column_name, column_config)
        if expr is None:
            continue
        try:
            before = frame.select(pl.col(column_name).is_not_null().sum()).item()
            after = (
                frame.with_columns(expr)
                .select(pl.col(column_name).is_not_null().sum())
                .item()
            )
        except Exception as e:
            # A non-numeric column configured with a range is a data problem,
            # not a reason to fail the whole summary.
            counts[column_name] = {"status": f"Could not evaluate range: {e}"}
            continue

        counts[column_name] = {
            "non_null_count": before,
            "outlier_count": before - after,
            "outlier_percent": round((before - after) / before * 100, 2) if before else 0.0,
        }

    return counts