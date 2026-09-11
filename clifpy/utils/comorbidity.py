"""
Comorbidity calculation utilities for CLIF data.

This module provides functionality to calculate comorbidity indices such as
the Charlson Comorbidity Index (CCI) from hospital diagnosis data.
"""

import polars as pl
import pandas as pd
import yaml
import os
from typing import Union, Dict, List, Any
from pathlib import Path
from tqdm import tqdm


def calculate_elix(
    hospital_diagnosis: Union['HospitalDiagnosis', pd.DataFrame, pl.DataFrame],
    hierarchy: bool = True
) -> pl.DataFrame:
    """
    Calculate Elixhauser Comorbidity Index for hospitalizations.

    This function processes hospital diagnosis data to calculate Elixhauser scores
    using the Quan (2011) adaptation with ICD-10-CM codes and van Walraven weights.

    Parameters
    ----------
    hospital_diagnosis : HospitalDiagnosis object, pandas DataFrame, or polars DataFrame
        containing diagnosis data with columns:
        - hospitalization_id
        - diagnosis_code
        - diagnosis_code_format
    hierarchy : bool, default=True
        Apply assign0 logic to prevent double counting
        of conditions when both mild and severe forms are present

    Returns
    -------
    pd.DataFrame
        DataFrame with columns:
        - hospitalization_id (index)
        - 31 binary condition columns (0/1)
        - elix_score (weighted sum)
    """

    # Load Elixhauser configuration
    elix_config = _load_elix_config()

    # Print configuration info as requested
    print(f"name: \"{elix_config['name']}\"")
    print(f"version: \"{elix_config['version']}\"")
    print(f"supported_formats:")
    for fmt in elix_config['supported_formats']:
        print(f"  - {fmt}")

    # Convert input to polars DataFrame
    if hasattr(hospital_diagnosis, 'df'):
        # HospitalDiagnosis object
        df = pl.from_pandas(hospital_diagnosis.df)
    elif isinstance(hospital_diagnosis, pd.DataFrame):
        df = pl.from_pandas(hospital_diagnosis)
    elif isinstance(hospital_diagnosis, pl.DataFrame):
        df = hospital_diagnosis
    else:
        raise ValueError("hospital_diagnosis must be HospitalDiagnosis object, pandas DataFrame, or polars DataFrame")

    # Filter to only ICD10CM codes (case-insensitive; discard other formats)
    df_filtered = df.filter(
        pl.col("diagnosis_code_format").str.to_lowercase() == "icd10cm"
    )

    # Preprocess diagnosis codes: lowercase and remove decimals (e.g., "I21.45" -> "i2145")
    df_processed = df_filtered.with_columns([
        pl.col("diagnosis_code").str.to_lowercase().str.replace_all(".", "", literal=True).alias("diagnosis_code_clean")
    ])

    # Map diagnosis codes to Elixhauser conditions
    condition_mappings = elix_config['diagnosis_code_mappings']['ICD10CM']
    weights = elix_config['weights']

    # Create condition presence indicators
    condition_columns = []

    for condition_name, condition_info in tqdm(condition_mappings.items(), desc="Mapping ICD codes to Elixhauser conditions"):
        condition_codes = condition_info['codes']

        # Create a boolean expression for this condition
        condition_expr = pl.lit(False)
        for code in condition_codes:
            condition_expr = condition_expr | pl.col("diagnosis_code_clean").str.starts_with(code.lower())

        condition_columns.append(condition_expr.alias(f"{condition_name}_present"))

    # Add condition indicators to dataframe
    df_with_conditions = df_processed.with_columns(condition_columns)

    # Group by hospitalization_id and aggregate condition presence
    condition_names = list(condition_mappings.keys())

    # Create aggregation expressions
    agg_exprs = []
    for condition_name in condition_names:
        agg_exprs.append(
            pl.col(f"{condition_name}_present").max().alias(condition_name)
        )

    # Group by hospitalization and get condition presence
    df_grouped = df_with_conditions.group_by("hospitalization_id").agg(agg_exprs)

    # Apply hierarchy logic if enabled (assign0)
    if hierarchy:
        df_grouped = _apply_hierarchy_logic(df_grouped, elix_config['hierarchies'])

    # Calculate Elixhauser score
    df_with_score = _calculate_elix_score(df_grouped, weights)

    # Convert boolean columns to integers for consistency
    condition_names = list(condition_mappings.keys())
    cast_exprs = []
    for col in df_with_score.columns:
        if col in condition_names:
            cast_exprs.append(pl.col(col).cast(pl.Int32).alias(col))
        else:
            cast_exprs.append(pl.col(col))

    df_with_score = df_with_score.select(cast_exprs)

    # Ensure hospitalization_id is string type
    df_with_score = df_with_score.with_columns([
        pl.col("hospitalization_id").cast(pl.Utf8).alias("hospitalization_id")
    ])

    # Convert to pandas DataFrame before returning
    return df_with_score.to_pandas()


def calculate_cci(
    hospital_diagnosis: Union['HospitalDiagnosis', pd.DataFrame, pl.DataFrame],
    hierarchy: bool = True
) -> pl.DataFrame:
    """
    Calculate Charlson Comorbidity Index (CCI) for hospitalizations.

    This function processes hospital diagnosis data to calculate CCI scores
    using the Quan (2011) adaptation with ICD-10-CM codes.

    Parameters
    ----------
    hospital_diagnosis : HospitalDiagnosis object, pandas DataFrame, or polars DataFrame
        containing diagnosis data with columns:
        - hospitalization_id
        - diagnosis_code
        - diagnosis_code_format
    hierarchy : bool, default=True
        Apply assign0 logic to prevent double counting
        of conditions when both mild and severe forms are present

    Returns
    -------
    pd.DataFrame
        DataFrame with columns:
        - hospitalization_id (index)
        - 17 binary condition columns (0/1)
        - cci_score (weighted sum)
    """

    # Load CCI configuration
    cci_config = _load_cci_config()

    # Print configuration info as requested
    print(f"name: \"{cci_config['name']}\"")
    print(f"version: \"{cci_config['version']}\"")
    print(f"supported_formats:")
    for fmt in cci_config['supported_formats']:
        print(f"  - {fmt}")

    # Convert input to polars DataFrame
    if hasattr(hospital_diagnosis, 'df'):
        # HospitalDiagnosis object
        df = pl.from_pandas(hospital_diagnosis.df)
    elif isinstance(hospital_diagnosis, pd.DataFrame):
        df = pl.from_pandas(hospital_diagnosis)
    elif isinstance(hospital_diagnosis, pl.DataFrame):
        df = hospital_diagnosis
    else:
        raise ValueError("hospital_diagnosis must be HospitalDiagnosis object, pandas DataFrame, or polars DataFrame")

    # Filter to only ICD10CM codes (case-insensitive; discard other formats)
    df_filtered = df.filter(
        pl.col("diagnosis_code_format").str.to_lowercase() == "icd10cm"
    )

    # Preprocess diagnosis codes: lowercase and remove decimals (e.g., "I21.45" -> "i2145")
    df_processed = df_filtered.with_columns([
        pl.col("diagnosis_code").str.to_lowercase().str.replace_all(".", "", literal=True).alias("diagnosis_code_clean")
    ])

    # Map diagnosis codes to CCI conditions
    condition_mappings = cci_config['diagnosis_code_mappings']['ICD10CM']
    weights = cci_config['weights']

    # Create condition presence indicators
    condition_columns = []

    for condition_name, condition_info in tqdm(condition_mappings.items(), desc="Mapping ICD codes to CCI conditions"):
        condition_codes = condition_info['codes']

        # Create a boolean expression for this condition
        condition_expr = pl.lit(False)
        for code in condition_codes:
            condition_expr = condition_expr | pl.col("diagnosis_code_clean").str.starts_with(code.lower())

        condition_columns.append(condition_expr.alias(f"{condition_name}_present"))

    # Add condition indicators to dataframe
    df_with_conditions = df_processed.with_columns(condition_columns)

    # Group by hospitalization_id and aggregate condition presence
    condition_names = list(condition_mappings.keys())

    # Create aggregation expressions
    agg_exprs = []
    for condition_name in condition_names:
        agg_exprs.append(
            pl.col(f"{condition_name}_present").max().alias(condition_name)
        )

    # Group by hospitalization and get condition presence
    df_grouped = df_with_conditions.group_by("hospitalization_id").agg(agg_exprs)

    # Apply hierarchy logic if enabled (assign0)
    if hierarchy:
        df_grouped = _apply_hierarchy_logic(df_grouped, cci_config['hierarchies'])

    # Calculate CCI score
    df_with_score = _calculate_cci_score(df_grouped, weights)

    # Convert boolean columns to integers for consistency
    condition_names = list(condition_mappings.keys())
    cast_exprs = []
    for col in df_with_score.columns:
        if col in condition_names:
            cast_exprs.append(pl.col(col).cast(pl.Int32).alias(col))
        else:
            cast_exprs.append(pl.col(col))

    df_with_score = df_with_score.select(cast_exprs)

    # Ensure hospitalization_id is string type
    df_with_score = df_with_score.with_columns([
        pl.col("hospitalization_id").cast(pl.Utf8).alias("hospitalization_id")
    ])

    # Convert to pandas DataFrame before returning
    return df_with_score.to_pandas()


def _load_elix_config() -> Dict[str, Any]:
    """Load Elixhauser configuration from YAML file."""
    config_path = Path(__file__).parent.parent / "data" / "comorbidity" / "elixhauser.yaml"

    if not config_path.exists():
        raise FileNotFoundError(f"Elixhauser configuration file not found: {config_path}")

    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    return config


def _load_cci_config() -> Dict[str, Any]:
    """Load CCI configuration from YAML file."""
    config_path = Path(__file__).parent.parent / "data" / "comorbidity" / "cci.yaml"

    if not config_path.exists():
        raise FileNotFoundError(f"CCI configuration file not found: {config_path}")

    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    return config


def _apply_hierarchy_logic(df: pl.DataFrame, hierarchies: Dict[str, List[str]]) -> pl.DataFrame:
    """
    Apply hierarchy logic (assign0) to prevent double counting of conditions.

    When both mild and severe forms of a condition are present, the mild form
    is set to 0 to avoid double counting.
    """
    df_result = df.clone()

    for _, condition_list in hierarchies.items():
        if len(condition_list) >= 2:
            # First condition is the severe form (takes precedence)
            # Subsequent conditions are mild forms (set to 0 if severe present)
            severe_condition = condition_list[0]
            mild_conditions = condition_list[1:]

            for mild_condition in mild_conditions:
                # If severe condition is present, set mild condition to 0
                df_result = df_result.with_columns([
                    pl.when(pl.col(severe_condition) == 1)
                    .then(0)
                    .otherwise(pl.col(mild_condition))
                    .alias(mild_condition)
                ])

    return df_result


def _calculate_elix_score(df: pl.DataFrame, weights: Dict[str, int]) -> pl.DataFrame:
    """Calculate the weighted Elixhauser score for each hospitalization."""

    # Create score calculation expression
    score_expr = pl.lit(0)

    for condition_name, weight in weights.items():
        if condition_name in df.columns:
            score_expr = score_expr + (pl.col(condition_name) * weight)

    # Add the score column
    df_with_score = df.with_columns([
        score_expr.alias("elix_score")
    ])

    return df_with_score


def _calculate_cci_score(df: pl.DataFrame, weights: Dict[str, int]) -> pl.DataFrame:
    """Calculate the weighted CCI score for each hospitalization."""

    # Create score calculation expression
    score_expr = pl.lit(0)

    for condition_name, weight in weights.items():
        if condition_name in df.columns:
            score_expr = score_expr + (pl.col(condition_name) * weight)

    # Add the score column
    df_with_score = df.with_columns([
        score_expr.alias("cci_score")
    ])

    return df_with_score