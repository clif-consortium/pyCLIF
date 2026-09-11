"""
Configuration loading utilities for CLIF data processing.

This module provides functions to load configuration from JSON or YAML files
for consistent data loading across CLIF tables and orchestrator.
"""

import os
import json
import yaml
import logging
from typing import Dict, Any, Optional
from pathlib import Path

from ..schemas import DEFAULT_CLIF_VERSION

# Initialize logger for this module
logger = logging.getLogger('clifpy.utils.config')


def _load_config_file(config_path: str) -> Dict[str, Any]:
    """
    Load configuration from either JSON or YAML file with field mapping.

    Parameters
    ----------
    config_path : str
        Path to the configuration file

    Returns
    -------
    dict
        Configuration dictionary with normalized field names

    Raises
    ------
    json.JSONDecodeError
        If JSON file is invalid
    yaml.YAMLError
        If YAML file is invalid
    ValueError
        If file format is unsupported
    """
    file_ext = Path(config_path).suffix.lower()

    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            if file_ext == '.json':
                config = json.load(f)
            elif file_ext in ['.yaml', '.yml']:
                config = yaml.safe_load(f)
                # Map YAML field names to expected JSON field names
                if 'tables_path' in config:
                    config['data_directory'] = config.pop('tables_path')
            else:
                raise ValueError(
                    f"Unsupported config file format: {file_ext}\n"
                    "Supported formats are: .json, .yaml, .yml"
                )
    except json.JSONDecodeError as e:
        raise json.JSONDecodeError(
            f"Invalid JSON in configuration file {config_path}: {str(e)}",
            e.doc, e.pos
        )
    except yaml.YAMLError as e:
        raise yaml.YAMLError(
            f"Invalid YAML in configuration file {config_path}: {str(e)}"
        )

    return config


def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Load CLIF configuration from JSON or YAML file.

    Parameters
    ----------
    config_path : str, optional
        Path to the configuration file.
        If None, looks for 'config.json' or 'config.yaml' in current directory.

    Returns
    -------
    dict
        Configuration dictionary with required fields validated

    Raises
    ------
    FileNotFoundError
        If config file doesn't exist
    ValueError
        If required fields are missing or invalid
    json.JSONDecodeError
        If JSON config file is not valid
    yaml.YAMLError
        If YAML config file is not valid
    """
    # Determine config file path
    if config_path is None:
        # Look for config files in order of preference: JSON, YAML, YML
        cwd = os.getcwd()
        for filename in ['config.json', 'config.yaml', 'config.yml']:
            potential_path = os.path.join(cwd, filename)
            if os.path.exists(potential_path):
                config_path = potential_path
                break

        if config_path is None:
            raise FileNotFoundError(
                f"Configuration file not found in {cwd}\n"
                "Please either:\n"
                "  1. Create a config.json or config.yaml file in the current directory\n"
                "  2. Provide config_path parameter pointing to your config file\n"
                "  3. Provide data_directory, filetype, and timezone parameters directly"
            )

    # Check if config file exists
    if not os.path.exists(config_path):
        raise FileNotFoundError(
            f"Configuration file not found: {config_path}\n"
            "Please either:\n"
            "  1. Create a config.json or config.yaml file in the current directory\n"
            "  2. Provide config_path parameter pointing to your config file\n"
            "  3. Provide data_directory, filetype, and timezone parameters directly"
        )

    # Load configuration using helper function
    config = _load_config_file(config_path)
    
    # Validate required fields
    required_fields = ['data_directory', 'filetype', 'timezone']
    missing_fields = [field for field in required_fields if field not in config]
    
    if missing_fields:
        raise ValueError(
            f"Missing required fields in configuration file {config_path}: {missing_fields}\n"
            f"Required fields are: {required_fields}"
        )
    
    # Validate data_directory exists
    data_dir = config['data_directory']
    if not os.path.exists(data_dir):
        raise ValueError(
            f"Data directory specified in config does not exist: {data_dir}\n"
            f"Please check the 'data_directory' path in {config_path}"
        )
    
    # Validate filetype
    supported_filetypes = ['csv', 'parquet']
    if config['filetype'] not in supported_filetypes:
        raise ValueError(
            f"Unsupported filetype '{config['filetype']}' in {config_path}\n"
            f"Supported filetypes are: {supported_filetypes}"
        )
    
    logger.info(f"Configuration loaded from {config_path}")
    return config


def get_config_or_params(
    config_path: Optional[str] = None,
    data_directory: Optional[str] = None,
    filetype: Optional[str] = None,
    timezone: Optional[str] = None,
    output_directory: Optional[str] = None
) -> Dict[str, Any]:
    """
    Get configuration from either config file or direct parameters.
    
    Loading priority:
    
    1. If all required params provided directly → use them
    2. If config_path provided → load from that path, allow param overrides
    3. If no params and no config_path → auto-detect config.json/yaml/yml
    4. Parameters override config file values when both are provided
    
    Parameters
    ----------
    config_path : str, optional
        Path to configuration file
    data_directory : str, optional
        Direct parameter
    filetype : str, optional
        Direct parameter  
    timezone : str, optional
        Direct parameter
    output_directory : str, optional
        Direct parameter
        
    Returns
    -------
    dict
        Final configuration dictionary
        
    Raises
    ------
    ValueError
        If neither config nor required params are provided
    """
    # Check if all required params are provided directly
    required_params = [data_directory, filetype, timezone]
    if all(param is not None for param in required_params):
        # All required params provided - use them directly
        config = {
            'data_directory': data_directory,
            'filetype': filetype,
            'timezone': timezone
        }
        if output_directory is not None:
            config['output_directory'] = output_directory
        logger.debug("Using directly provided parameters")
        return config
    
    # Try to load from config file
    try:
        config = load_config(config_path)
    except FileNotFoundError:
        # If no config file and incomplete params, raise helpful error
        if any(param is not None for param in required_params):
            # Some params provided but not all
            missing = []
            if data_directory is None:
                missing.append('data_directory')
            if filetype is None:
                missing.append('filetype') 
            if timezone is None:
                missing.append('timezone')
            raise ValueError(
                f"Incomplete parameters provided. Missing: {missing}\n"
                "Please either:\n"
                "  1. Provide all required parameters (data_directory, filetype, timezone)\n"
                "  2. Create a config.json or config.yaml file\n"
                "  3. Provide a config_path parameter"
            )
        else:
            # No params and no config file - re-raise the original error
            raise
    
    # Override config values with any provided parameters
    if data_directory is not None:
        config['data_directory'] = data_directory
        logger.debug(f"Overriding data_directory: {data_directory}")

    if filetype is not None:
        config['filetype'] = filetype
        logger.debug(f"Overriding filetype: {filetype}")

    if timezone is not None:
        config['timezone'] = timezone
        logger.debug(f"Overriding timezone: {timezone}")

    if output_directory is not None:
        config['output_directory'] = output_directory
        logger.debug(f"Overriding output_directory: {output_directory}")
    
    return config


def create_example_config(
    data_directory: str = "./data",
    filetype: str = "parquet",
    timezone: str = "UTC",
    output_directory: str = "./output",
    config_path: str = "./config.json",
    format: str = "json"
) -> None:
    """
    Create an example configuration file in JSON or YAML format.

    Parameters
    ----------
    data_directory : str
        Path to data directory
    filetype : str
        File type (csv or parquet)
    timezone : str
        Timezone string
    output_directory : str
        Output directory path
    config_path : str
        Where to save the config file
    format : str
        Output format ("json" or "yaml")
    """
    # Determine format from file extension if not explicitly specified
    file_ext = Path(config_path).suffix.lower()
    if file_ext in ['.yaml', '.yml']:
        format = "yaml"
    elif file_ext == '.json':
        format = "json"

    if format.lower() == "yaml":
        # Use YAML field names for YAML format
        config = {
            "site": "EXAMPLE_SITE",
            "tables_path": data_directory,
            "filetype": filetype,
            "timezone": timezone,
            "output_directory": output_directory,
            "clif_version": DEFAULT_CLIF_VERSION
        }

        with open(config_path, 'w', encoding='utf-8') as f:
            yaml.dump(config, f, default_flow_style=False, indent=2)
    else:
        # Use JSON field names for JSON format
        config = {
            "data_directory": data_directory,
            "filetype": filetype,
            "timezone": timezone,
            "output_directory": output_directory,
            "clif_version": DEFAULT_CLIF_VERSION
        }

        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2)

    logger.info(f"Example {format.upper()} configuration file created at: {config_path}")