"""CLIF schema registry: version-aware loading of table schema YAML files.

Schemas are stored in per-version subdirectories of this package, e.g.
``clifpy/schemas/2.1/patient_schema.yaml`` and ``clifpy/schemas/3.0/...``.
Shared, version-agnostic configuration files (``validation_rules.yaml``,
``outlier_config.yaml``, ``wide_tables_config.yaml``) live at the schemas root.

This module is the single source of truth for which CLIF versions exist and
how a table name maps to a schema file within a version. Both
:class:`clifpy.tables.base_table.BaseTable` and the validator load schemas
through :func:`load_schema` rather than re-implementing path logic.
"""

import os
import logging
from typing import Any, Dict, Optional

import yaml

logger = logging.getLogger('clifpy.schemas')

# Directory containing this module (the schemas package root).
_SCHEMAS_ROOT = os.path.dirname(__file__)

#: Default CLIF version used when a caller does not specify one. Tracks the
#: package's own major version (clifpy 3.x ships CLIF 3.0 as the default);
#: pass ``clif_version="2.1"`` explicitly to validate against the older schemas.
DEFAULT_CLIF_VERSION = "3.0"

#: CLIF versions for which a schema subdirectory exists.
SUPPORTED_CLIF_VERSIONS = ("2.1", "3.0")

#: Per-version table-name -> schema-base-name overrides for tables that were
#: renamed between versions. The class-derived ``table_name`` stays stable;
#: only the on-disk schema file differs. Example: the 2.1 ``ecmo_mcs`` table
#: was renamed and redesigned as ``mcs`` in 3.0.
_SCHEMA_NAME_OVERRIDES: Dict[str, Dict[str, str]] = {
    "3.0": {
        "ecmo_mcs": "mcs",
        # CLIF 3.0 merged continuous and intermittent renal replacement into a
        # single renal_replacement_therapy table; the intermittent modes are
        # mode_category values (ihd, iuf), not a table of their own. There is
        # no crrt_therapy in the 3.0.0 data dictionary.
        "crrt_therapy": "renal_replacement_therapy",
    },
}


def _validate_version(clif_version: str) -> None:
    """Raise a clear ValueError if ``clif_version`` is not supported."""
    if clif_version not in SUPPORTED_CLIF_VERSIONS:
        raise ValueError(
            f"Unsupported CLIF version: {clif_version!r}. "
            f"Supported versions are: {list(SUPPORTED_CLIF_VERSIONS)}"
        )


def schema_dir(clif_version: str = DEFAULT_CLIF_VERSION) -> str:
    """Return the absolute path to the schema directory for ``clif_version``.

    Raises
    ------
    ValueError
        If ``clif_version`` is not in :data:`SUPPORTED_CLIF_VERSIONS`.
    """
    _validate_version(clif_version)
    return os.path.join(_SCHEMAS_ROOT, clif_version)


def resolve_schema_filename(table_name: str, clif_version: str = DEFAULT_CLIF_VERSION) -> str:
    """Return the schema file name for ``table_name`` under ``clif_version``.

    Applies any per-version rename overrides (see :data:`_SCHEMA_NAME_OVERRIDES`).
    """
    _validate_version(clif_version)
    base_name = _SCHEMA_NAME_OVERRIDES.get(clif_version, {}).get(table_name, table_name)
    return f"{base_name}_schema.yaml"


def schema_path(table_name: str, clif_version: str = DEFAULT_CLIF_VERSION) -> str:
    """Return the absolute path to a table's schema file for ``clif_version``."""
    return os.path.join(schema_dir(clif_version), resolve_schema_filename(table_name, clif_version))


def load_schema(
    table_name: str,
    clif_version: str = DEFAULT_CLIF_VERSION,
) -> Optional[Dict[str, Any]]:
    """Load and parse a table's YAML schema for the given CLIF version.

    Parameters
    ----------
    table_name : str
        snake_case table name (e.g. ``"patient"``, ``"respiratory_support"``).
    clif_version : str, optional
        CLIF version to load. Defaults to :data:`DEFAULT_CLIF_VERSION`.

    Returns
    -------
    dict or None
        Parsed schema, or ``None`` if the schema file does not exist.

    Raises
    ------
    ValueError
        If ``clif_version`` is not supported.
    """
    path = schema_path(table_name, clif_version)
    logger.debug("Loading schema for '%s' (CLIF %s) from %s", table_name, clif_version, path)
    if not os.path.exists(path):
        logger.warning("Schema file not found: %s", path)
        return None

    with open(path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)
