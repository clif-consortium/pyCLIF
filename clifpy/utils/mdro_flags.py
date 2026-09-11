"""
MDRO (Multi-Drug Resistant Organism) Flag Calculation Utility

This module provides functionality to calculate MDR, XDR, PDR, and DTR flags
for organisms based on antimicrobial susceptibility testing results.
"""

import pandas as pd
import numpy as np
import yaml
import os
from typing import Optional, List, Dict, Any
from pathlib import Path
import logging


def calculate_mdro_flags(
    culture,  # MicrobiologyCulture table object
    susceptibility,  # MicrobiologySusceptibility table object
    organism_name: str,
    cohort: Optional[pd.DataFrame] = None,
    hospitalization_ids: Optional[List[str]] = None,
    config_path: Optional[str] = None
) -> pd.DataFrame:
    """
    Calculate MDRO flags (MDR, XDR, PDR, DTR) for a specified organism.

    This function analyzes antimicrobial susceptibility data to determine if
    organisms meet criteria for multi-drug resistance classifications.

    Parameters
    ----------
    culture : MicrobiologyCulture
        Microbiology culture table containing organism information and
        hospitalization linkage. Must have columns: organism_id,
        hospitalization_id, organism_category
    susceptibility : MicrobiologySusceptibility
        Antimicrobial susceptibility table containing test results.
        Must have columns: organism_id, antimicrobial_category,
        susceptibility_category
    organism_name : str
        Organism to calculate flags for (must match organism_category value).
        Example: 'pseudomonas_aeruginosa'
    cohort : pd.DataFrame, optional
        DataFrame with columns: hospitalization_id, start_dttm, end_dttm
        If provided, filters culture results to those within the date range.
        Requires culture table to have a datetime column for comparison.
    hospitalization_ids : List[str], optional
        Specific hospitalization IDs to filter for. If provided, only these
        hospitalizations will be included in the analysis.
    config_path : str, optional
        Path to mdro.yaml configuration file. If not provided, uses the
        default location: clifpy/data/mdro.yaml

    Returns
    -------
    pd.DataFrame
        Wide format DataFrame with columns:
        - hospitalization_id: Hospital encounter identifier
        - organism_id: Organism culture identifier
        - [antimicrobial]_agent: Individual antimicrobial susceptibility results
          (e.g., amikacin_agent, ciprofloxacin_agent)
          Values: 'susceptible', 'intermediate', 'non_susceptible', 'NA', or None
        - [group]_group: Binary group resistance flags (0/1)
          (e.g., aminoglycosides_group, carbapenems_group)
        - mdro_[organism]_mdr: Multi-Drug Resistant flag (0/1)
        - mdro_[organism]_xdr: Extensively Drug Resistant flag (0/1)
        - mdro_[organism]_pdr: Pandrug Resistant flag (0/1)
        - mdro_[organism]_dtr: Difficult to Treat Resistance flag (0/1)
          (if applicable for the organism)

    Raises
    ------
    ValueError
        If organism_name is not found in the configuration
        If required columns are missing from input tables
    FileNotFoundError
        If the configuration file cannot be found

    Examples
    --------
    >>> from clifpy.tables import MicrobiologyCulture, MicrobiologySusceptibility
    >>> culture = MicrobiologyCulture(data_directory="./data", filetype="csv")
    >>> susceptibility = MicrobiologySusceptibility(data_directory="./data", filetype="csv")
    >>> mdro_flags = calculate_mdro_flags(
    ...     culture=culture,
    ...     susceptibility=susceptibility,
    ...     organism_name="pseudomonas_aeruginosa"
    ... )

    >>> # With cohort filtering
    >>> cohort_df = pd.DataFrame({
    ...     'hospitalization_id': ['H001', 'H002'],
    ...     'start_dttm': ['2023-01-01', '2023-01-15'],
    ...     'end_dttm': ['2023-01-10', '2023-01-20']
    ... })
    >>> mdro_flags = calculate_mdro_flags(
    ...     culture=culture,
    ...     susceptibility=susceptibility,
    ...     organism_name="pseudomonas_aeruginosa",
    ...     cohort=cohort_df
    ... )
    """
    logger = logging.getLogger(__name__)

    # -------------------------------------------------------------------------
    # 1. Load Configuration
    # -------------------------------------------------------------------------
    config = _load_mdro_config(config_path)

    if organism_name not in config['organisms']:
        available_organisms = ', '.join(config['organisms'].keys())
        raise ValueError(
            f"Organism '{organism_name}' not found in configuration. "
            f"Available organisms: {available_organisms}"
        )

    organism_config = config['organisms'][organism_name]

    # -------------------------------------------------------------------------
    # 2. Extract DataFrames and Validate Columns
    # -------------------------------------------------------------------------
    culture_df = culture.df.copy()
    susceptibility_df = susceptibility.df.copy()

    # Validate required columns
    required_culture_cols = ['organism_id', 'hospitalization_id', 'organism_category']
    missing_culture_cols = [col for col in required_culture_cols if col not in culture_df.columns]
    if missing_culture_cols:
        raise ValueError(
            f"Missing required columns in culture table: {missing_culture_cols}"
        )

    required_susc_cols = ['organism_id', 'antimicrobial_category', 'susceptibility_category']
    missing_susc_cols = [col for col in required_susc_cols if col not in susceptibility_df.columns]
    if missing_susc_cols:
        raise ValueError(
            f"Missing required columns in susceptibility table: {missing_susc_cols}"
        )

    # -------------------------------------------------------------------------
    # 3. Filter Culture Table First
    # -------------------------------------------------------------------------
    # Start by filtering culture for the specified organism
    culture_filtered = culture_df[culture_df['organism_category'] == organism_name].copy()
    logger.info(f"Filtered culture to {organism_name}: {len(culture_filtered)} rows")

    if len(culture_filtered) == 0:
        logger.warning(f"No data found for organism: {organism_name}")
        return pd.DataFrame(columns=['hospitalization_id', 'organism_id'])

    # Apply cohort filtering (if provided) - filters culture by date range
    if cohort is not None:
        culture_filtered = _apply_cohort_filter_to_culture(culture_filtered, cohort)
        logger.info(f"After cohort filtering: {len(culture_filtered)} rows")

    # Apply hospitalization ID filtering (if provided)
    if hospitalization_ids is not None:
        culture_filtered = culture_filtered[
            culture_filtered['hospitalization_id'].isin(hospitalization_ids)
        ].copy()
        logger.info(f"After hospitalization_id filtering: {len(culture_filtered)} rows")

    if len(culture_filtered) == 0:
        logger.warning("No data remaining after filtering")
        return pd.DataFrame(columns=['hospitalization_id', 'organism_id'])

    # -------------------------------------------------------------------------
    # 4. LEFT JOIN with Susceptibility Data
    # -------------------------------------------------------------------------
    # Use LEFT JOIN to preserve all culture rows even without susceptibility data
    merged_df = pd.merge(
        culture_filtered[['organism_id', 'hospitalization_id', 'organism_category']],
        susceptibility_df[['organism_id', 'antimicrobial_category', 'susceptibility_category']],
        on='organism_id',
        how='left'
    )

    logger.info(f"Merged culture and susceptibility data: {len(merged_df)} rows")

    # -------------------------------------------------------------------------
    # 5. Identify Organisms Without Susceptibility Data
    # -------------------------------------------------------------------------
    # Count organisms without any susceptibility testing
    organisms_without_susc = merged_df[merged_df['antimicrobial_category'].isna()]['organism_id'].nunique()
    if organisms_without_susc > 0:
        logger.info(f"{organisms_without_susc} organism(s) have no susceptibility testing data")

    # Filter to only rows with susceptibility data for MDRO calculation
    merged_df = merged_df[merged_df['antimicrobial_category'].notna()].copy()

    if len(merged_df) == 0:
        logger.warning("No organisms with susceptibility data found")
        return pd.DataFrame(columns=['hospitalization_id', 'organism_id'])

    # -------------------------------------------------------------------------
    # 6. Map Antimicrobial Categories to Groups
    # -------------------------------------------------------------------------
    antimicrobial_groups = organism_config['antimicrobial_groups']
    category_to_group = {}
    for group_name, categories in antimicrobial_groups.items():
        for category in categories:
            category_to_group[category] = group_name

    merged_df['antimicrobial_group'] = merged_df['antimicrobial_category'].map(category_to_group)

    # -------------------------------------------------------------------------
    # 6a. Filter to Only Include Antimicrobials Defined in Config
    # -------------------------------------------------------------------------
    # Filter to keep ONLY antimicrobials defined in config
    num_rows_before = len(merged_df)
    merged_df = merged_df[merged_df['antimicrobial_group'].notna()].copy()
    num_rows_after = len(merged_df)
    logger.info(f"Filtered antimicrobials: {num_rows_before} -> {num_rows_after} rows ({num_rows_before - num_rows_after} excluded)")

    # -------------------------------------------------------------------------
    # 7. Identify Resistant Results
    # -------------------------------------------------------------------------
    resistant_categories = organism_config.get('resistant_categories', ['non_susceptible', 'intermediate'])
    merged_df['is_resistant'] = merged_df['susceptibility_category'].isin(resistant_categories)

    # -------------------------------------------------------------------------
    # 7a. Check for Missing Antimicrobials (Data Quality)
    # -------------------------------------------------------------------------
    _check_missing_antimicrobials(merged_df, organism_config, organism_name)

    # -------------------------------------------------------------------------
    # 8. Calculate MDRO Flags for Each (hospitalization_id, organism_id)
    # -------------------------------------------------------------------------
    results = []
    resistance_defs = organism_config['resistance_definitions']

    for (hosp_id, org_id), group_data in merged_df.groupby(['hospitalization_id', 'organism_id']):
        flags = _calculate_flags_for_organism(group_data, resistance_defs, antimicrobial_groups)
        flags['hospitalization_id'] = hosp_id
        flags['organism_id'] = org_id
        results.append(flags)

    # -------------------------------------------------------------------------
    # 9. Create Results DataFrame with Wide Format
    # -------------------------------------------------------------------------
    if not results:
        return pd.DataFrame(columns=['hospitalization_id', 'organism_id'])

    # Create flags DataFrame
    flags_df = pd.DataFrame(results)

    # Create antimicrobial columns (wide format with individual susceptibility values)
    antimicrobial_df = _pivot_susceptibility_data(merged_df, organism_config)
    logger.info(f"Created {len(antimicrobial_df.columns) - 2} antimicrobial columns")

    # Create group columns (binary 0/1 for each antimicrobial group)
    group_df = _create_group_columns(merged_df, antimicrobial_groups, resistant_categories)
    logger.info(f"Created {len(group_df.columns) - 2} antimicrobial group columns")

    # Merge all components: flags + antimicrobial columns + group columns
    result_df = flags_df.merge(
        antimicrobial_df,
        on=['hospitalization_id', 'organism_id'],
        how='left'
    )
    result_df = result_df.merge(
        group_df,
        on=['hospitalization_id', 'organism_id'],
        how='left'
    )

    # Organize columns in logical order:
    # 1. IDs
    # 2. Antimicrobial columns (individual agents)
    # 3. Group columns
    # 4. MDRO flags
    id_cols = ['hospitalization_id', 'organism_id']
    flag_cols = [col for col in flags_df.columns if col not in id_cols]
    group_cols = [col for col in group_df.columns if col not in id_cols]
    antimicrobial_cols = [col for col in antimicrobial_df.columns if col not in id_cols]

    # Final column order
    column_order = id_cols + sorted(antimicrobial_cols) + sorted(group_cols) + sorted(flag_cols)
    result_df = result_df[column_order]

    logger.info(f"Calculated MDRO flags for {len(result_df)} organism cultures")

    return result_df


def _load_mdro_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Load the MDRO configuration from YAML file.

    Parameters
    ----------
    config_path : str, optional
        Path to configuration file. If None, uses default location.

    Returns
    -------
    dict
        Parsed YAML configuration

    Raises
    ------
    FileNotFoundError
        If configuration file not found
    """
    if config_path is None:
        # Default to clifpy/data/mdro.yaml
        current_file = Path(__file__)
        config_path = current_file.parent.parent / 'data' / 'mdro.yaml'

    config_path = Path(config_path)

    if not config_path.exists():
        raise FileNotFoundError(
            f"MDRO configuration file not found: {config_path}"
        )

    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    return config


def _apply_cohort_filter_to_culture(
    culture_df: pd.DataFrame,
    cohort: pd.DataFrame
) -> pd.DataFrame:
    """
    Filter culture data based on cohort date ranges.

    This filters the culture table to only include organism results that occurred
    within the specified time windows for each hospitalization in the cohort.

    Parameters
    ----------
    culture_df : pd.DataFrame
        Culture table data to filter. Must have columns: hospitalization_id
        and at least one datetime column (result_dttm, collect_dttm, or culture_dttm)
    cohort : pd.DataFrame
        Cohort DataFrame with columns: hospitalization_id, start_dttm, end_dttm

    Returns
    -------
    pd.DataFrame
        Filtered culture data containing only rows within cohort date ranges

    Raises
    ------
    ValueError
        If no suitable datetime column is found in culture_df
    """
    # Check for datetime column in culture table
    # Prefer result_dttm, then collect_dttm, then culture_dttm
    datetime_cols = ['result_dttm', 'collect_dttm', 'culture_dttm']
    culture_datetime_col = None

    for col in datetime_cols:
        if col in culture_df.columns:
            culture_datetime_col = col
            break

    if culture_datetime_col is None:
        raise ValueError(
            "Cannot apply cohort filtering: no datetime column found in culture table. "
            f"Expected one of: {datetime_cols}"
        )

    # Merge culture with cohort on hospitalization_id
    culture_with_cohort = pd.merge(
        culture_df,
        cohort[['hospitalization_id', 'start_dttm', 'end_dttm']],
        on='hospitalization_id',
        how='inner'
    )

    # Ensure datetime types
    culture_with_cohort[culture_datetime_col] = pd.to_datetime(culture_with_cohort[culture_datetime_col])
    culture_with_cohort['start_dttm'] = pd.to_datetime(culture_with_cohort['start_dttm'])
    culture_with_cohort['end_dttm'] = pd.to_datetime(culture_with_cohort['end_dttm'])

    # Filter to rows where culture datetime is within cohort date range
    filtered = culture_with_cohort[
        (culture_with_cohort[culture_datetime_col] >= culture_with_cohort['start_dttm']) &
        (culture_with_cohort[culture_datetime_col] <= culture_with_cohort['end_dttm'])
    ].copy()

    # Drop the temporary cohort columns
    filtered = filtered.drop(columns=['start_dttm', 'end_dttm'])

    return filtered


def _check_missing_antimicrobials(
    merged_df: pd.DataFrame,
    organism_config: Dict[str, Any],
    organism_name: str
) -> None:
    """
    Check for antimicrobials defined in config but missing from dataset.

    Prints warnings for missing antimicrobials, with special attention to
    critical agents required for specific resistance flags (e.g., DTR).

    Parameters
    ----------
    merged_df : pd.DataFrame
        Merged culture and susceptibility data with antimicrobial_category column
    organism_config : dict
        Organism configuration containing antimicrobial_groups and resistance_definitions
    organism_name : str
        Name of the organism being analyzed (for display in warnings)

    Returns
    -------
    None
        Prints warnings directly to console
    """
    # Get all antimicrobials defined in config
    antimicrobial_groups = organism_config['antimicrobial_groups']
    all_defined_antimicrobials = set()
    for group_name, agents in antimicrobial_groups.items():
        all_defined_antimicrobials.update(agents)

    # Get antimicrobials actually present in dataset
    tested_antimicrobials = set(merged_df['antimicrobial_category'].dropna().unique())

    # Find missing antimicrobials
    missing_antimicrobials = all_defined_antimicrobials - tested_antimicrobials

    if not missing_antimicrobials:
        return  # All defined antimicrobials are present

    # Identify critical missing antimicrobials (required for specific flags)
    critical_missing = set()
    resistance_defs = organism_config.get('resistance_definitions', {})

    for flag_name, flag_def in resistance_defs.items():
        criteria = flag_def.get('criteria', {})
        if criteria.get('type') == 'specific_agents_resistant':
            required_agents = set(criteria.get('required_agents', []))
            critical_for_this_flag = required_agents & missing_antimicrobials
            if critical_for_this_flag:
                critical_missing.update(critical_for_this_flag)

    # Print warnings for critical missing antimicrobials
    if critical_missing:
        print(f"\n⚠️  CRITICAL WARNING: Missing required antimicrobials for {organism_name}")
        print("=" * 70)
        for agent in sorted(critical_missing):
            # Find which flag(s) require this agent
            flags_requiring = []
            for flag_name, flag_def in resistance_defs.items():
                criteria = flag_def.get('criteria', {})
                if criteria.get('type') == 'specific_agents_resistant':
                    if agent in criteria.get('required_agents', []):
                        flags_requiring.append(flag_def.get('name', flag_name.upper()))

            print(f"  • {agent}")
            if flags_requiring:
                print(f"    Required for: {', '.join(flags_requiring)}")
        print("=" * 70)
        print("Note: Organisms missing these agents will NOT be flagged for the")
        print("      resistance categories listed above, even if otherwise resistant.")
        print()

    # Print non-critical missing antimicrobials
    non_critical_missing = missing_antimicrobials - critical_missing
    if non_critical_missing:
        print(f"\nℹ️  Missing antimicrobials from {organism_name} dataset:")
        for agent in sorted(non_critical_missing):
            # Find which group this belongs to
            group = None
            for group_name, agents in antimicrobial_groups.items():
                if agent in agents:
                    group = group_name
                    break
            print(f"  • {agent} (group: {group})")
        print()


def _calculate_flags_for_organism(
    group_data: pd.DataFrame,
    resistance_defs: Dict[str, Any],
    antimicrobial_groups: Dict[str, List[str]]
) -> Dict[str, int]:
    """
    Calculate MDRO flags for a single organism culture.

    Parameters
    ----------
    group_data : pd.DataFrame
        Susceptibility data for one organism (all antimicrobials tested)
    resistance_defs : dict
        Resistance definitions from config
    antimicrobial_groups : dict
        Antimicrobial group definitions

    Returns
    -------
    dict
        Dictionary with flag column names as keys and 0/1 values
    """
    flags = {}

    # Identify which groups have at least one resistant agent
    resistant_data = group_data[group_data['is_resistant']].copy()

    # Count resistant groups
    resistant_groups = resistant_data['antimicrobial_group'].dropna().unique()
    num_resistant_groups = len(resistant_groups)

    # Total number of groups tested (not all groups may be tested)
    tested_groups = group_data['antimicrobial_group'].dropna().unique()
    num_tested_groups = len(tested_groups)

    # Total number of agents tested
    tested_agents = group_data['antimicrobial_category'].unique()
    num_tested_agents = len(tested_agents)

    # Number of resistant agents
    resistant_agents = resistant_data['antimicrobial_category'].unique()
    num_resistant_agents = len(resistant_agents)

    # Calculate each flag
    for flag_name, flag_def in resistance_defs.items():
        criteria = flag_def['criteria']
        criteria_type = criteria['type']
        column_name = flag_def['column_name']

        if criteria_type == 'min_groups_resistant':
            # MDR: >= min_groups resistant
            min_groups = criteria['min_groups']
            flags[column_name] = 1 if num_resistant_groups >= min_groups else 0

        elif criteria_type == 'max_groups_susceptible':
            # XDR: resistant to all but <= max_groups_susceptible
            # Based on TOTAL defined groups, not just tested groups
            # For P. aeruginosa: 8 total groups, so XDR = resistant to ≥6 groups (8-2)
            max_groups_susceptible = criteria['max_groups_susceptible']
            total_defined_groups = len(antimicrobial_groups)
            min_resistant_for_xdr = total_defined_groups - max_groups_susceptible
            flags[column_name] = 1 if num_resistant_groups >= min_resistant_for_xdr else 0

        elif criteria_type == 'all_tested_resistant':
            # PDR: ALL defined agents must be tested AND all must be resistant
            # Get all defined antimicrobials from config
            all_defined_agents = set()
            for group_name, agents in antimicrobial_groups.items():
                all_defined_agents.update(agents)

            # Check if ALL defined agents were tested
            all_defined_tested = all_defined_agents.issubset(tested_agents)

            # Check if ALL defined agents are resistant
            all_defined_resistant = all_defined_agents.issubset(resistant_agents)

            # PDR = 1 only if ALL defined agents are tested AND resistant
            flags[column_name] = 1 if (all_defined_tested and all_defined_resistant) else 0

        elif criteria_type == 'specific_agents_resistant':
            # DTR: ALL required agents must be tested AND all must be resistant
            required_agents = set(criteria['required_agents'])

            # Check if ALL required agents were tested
            all_required_tested = required_agents.issubset(tested_agents)

            # Check if ALL required agents are resistant
            all_required_resistant = required_agents.issubset(resistant_agents)

            # DTR = 1 only if ALL required agents are tested AND resistant
            flags[column_name] = 1 if (all_required_tested and all_required_resistant) else 0

    return flags


def _prioritize_susceptibility(susceptibility_value: str) -> int:
    """
    Assign priority ranking to susceptibility values for duplicate handling.

    Lower numbers = higher priority (more resistant).
    Priority order: non_susceptible (1) > intermediate (2) > susceptible (3) > NA (4)

    Parameters
    ----------
    susceptibility_value : str
        Susceptibility category value

    Returns
    -------
    int
        Priority rank (1 = highest priority/most resistant)
    """
    priority_map = {
        'non_susceptible': 1,
        'intermediate': 2,
        'susceptible': 3,
        'NA': 4
    }

    # Default to lowest priority for unknown values
    return priority_map.get(susceptibility_value, 5)


def _pivot_susceptibility_data(
    merged_df: pd.DataFrame,
    organism_config: Dict[str, Any]
) -> pd.DataFrame:
    """
    Pivot susceptibility data to create wide format with antimicrobial columns.

    Creates one column per antimicrobial_category with susceptibility_category as values.
    Handles duplicate tests by keeping the most resistant result.
    Adds '_agent' suffix to all antimicrobial column names.

    Parameters
    ----------
    merged_df : pd.DataFrame
        Merged culture and susceptibility data with columns:
        hospitalization_id, organism_id, antimicrobial_category, susceptibility_category
    organism_config : dict
        Organism configuration with antimicrobial groups

    Returns
    -------
    pd.DataFrame
        Wide format DataFrame with columns:
        hospitalization_id, organism_id, [antimicrobial_agent columns...]
        Example: amikacin_agent, ciprofloxacin_agent, etc.
    """
    # Add priority column for handling duplicates
    merged_df = merged_df.copy()
    merged_df['_priority'] = merged_df['susceptibility_category'].apply(_prioritize_susceptibility)

    # Sort by priority (lower = more resistant) to handle duplicates
    merged_df = merged_df.sort_values('_priority')

    # Remove duplicates, keeping the most resistant (lowest priority value)
    deduplicated = merged_df.drop_duplicates(
        subset=['hospitalization_id', 'organism_id', 'antimicrobial_category'],
        keep='first'
    )

    # Pivot to wide format
    pivoted = deduplicated.pivot_table(
        index=['hospitalization_id', 'organism_id'],
        columns='antimicrobial_category',
        values='susceptibility_category',
        aggfunc='first'  # Should only be one value after deduplication
    )

    # Add _agent suffix to antimicrobial column names
    pivoted.columns = [f"{col}_agent" for col in pivoted.columns]

    # Reset index to make hospitalization_id and organism_id regular columns
    pivoted = pivoted.reset_index()

    # Fill NaN values with "not_tested" for antimicrobials not in susceptibility data
    agent_cols = [col for col in pivoted.columns if col.endswith('_agent')]
    pivoted[agent_cols] = pivoted[agent_cols].fillna("not_tested")

    return pivoted


def _create_group_columns(
    merged_df: pd.DataFrame,
    antimicrobial_groups: Dict[str, List[str]],
    resistant_categories: List[str]
) -> pd.DataFrame:
    """
    Create binary group columns indicating resistance in antimicrobial groups.

    For each antimicrobial group, creates a column with value:
    - 1 if ANY agent in that group is resistant (non_susceptible or intermediate)
    - 0 if all tested agents in group are susceptible OR group not tested
    Adds '_group' suffix to all group column names.

    Parameters
    ----------
    merged_df : pd.DataFrame
        Merged culture and susceptibility data with antimicrobial_group column
    antimicrobial_groups : dict
        Dictionary mapping group names to lists of antimicrobial categories
    resistant_categories : list
        List of susceptibility categories considered resistant

    Returns
    -------
    pd.DataFrame
        DataFrame with columns: hospitalization_id, organism_id, [group columns...]
        Example: aminoglycosides_group, carbapenems_group, etc.
    """
    # Identify resistant results
    merged_df = merged_df.copy()
    merged_df['is_resistant'] = merged_df['susceptibility_category'].isin(resistant_categories)

    # Group by hospitalization_id, organism_id, and antimicrobial_group
    # Check if ANY agent in each group is resistant
    group_resistance = merged_df.groupby(
        ['hospitalization_id', 'organism_id', 'antimicrobial_group']
    )['is_resistant'].any().reset_index()

    # Pivot to create one column per group
    group_pivoted = group_resistance.pivot_table(
        index=['hospitalization_id', 'organism_id'],
        columns='antimicrobial_group',
        values='is_resistant',
        aggfunc='any'
    )

    # Convert boolean to int (1/0)
    group_pivoted = group_pivoted.astype(object).fillna(False).astype(int)

    # Add _group suffix to group column names
    group_pivoted.columns = [f"{col}_group" for col in group_pivoted.columns]

    # Reset index
    group_pivoted = group_pivoted.reset_index()

    return group_pivoted
