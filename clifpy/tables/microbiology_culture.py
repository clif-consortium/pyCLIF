from typing import Dict, List, Optional, Literal, Union
from datetime import timedelta
import pandas as pd
import numpy as np
import json
import os
from .base_table import BaseTable


class MicrobiologyCulture(BaseTable):
    """
    Microbiology Culture table wrapper inheriting from BaseTable.
    
    This class handles microbiology culture-specific data and validations including
    organism identification validation and culture method validation.
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
        Initialize the microbiology culture table.
        
        Parameters:
            data_directory (str): Path to the directory containing data files
            filetype (str): Type of data file (csv, parquet, etc.)
            timezone (str): Timezone for datetime columns
            output_directory (str, optional): Directory for saving output files and logs
            data (pd.DataFrame, optional): Pre-loaded data to use instead of loading from file
        """
        # Initialize time order validation errors list
        self.time_order_validation_errors: List[dict] = []
        
        super().__init__(
            data_directory=data_directory,
            filetype=filetype,
            timezone=timezone,
            output_directory=output_directory,
            data=data,
            clif_version=clif_version
        )

    def isvalid(self) -> bool:
        """Return ``True`` if the last validation finished without errors."""
        return not self.errors and not self.time_order_validation_errors

    def _run_table_specific_validations(self):
        """
        Run microbiology culture-specific validations.
        
        This overrides the base class method to add microbiology-specific validation
        for all columns that have permissible values defined in the schema.
        """
        self.validate_timestamp_order()
        
    # -------------------------------------------------------------------
    # Microbiology Culture Specific Methods
    # -------------------------------------------------------------------
    
    def validate_timestamp_order(self):
        """
        Check that order_dttm ≤ collect_dttm ≤ result_dttm.
        - Resets self.time_order_validation_errors
        - Adds one entry per violated rule
        - Extends self.errors and logs: 'Found {len(self.time_order_validation_errors)} time order validation errors'
        Returns a dataframe of all violating rows (union of both rules) or None if OK.
        """
        # Reset time order validation bucket
        self.time_order_validation_errors = []

        # The stored polars frame. Reading .df instead would convert the whole
        # table to pandas and cache that copy on the object for the rest of the
        # session -- on the validation path, over full-size data. Only the
        # violating rows are converted, at the return.
        df = self.data
        key_cols = ["patient_id", "hospitalization_id", "organism_id"]
        time_cols = ["order_dttm", "collect_dttm", "result_dttm"]

        # Check for missing columns
        missing = [col for col in time_cols if col not in df.columns]
        if missing:
            msg = (
                f"Missing required timestamp columns for time order validation: {', '.join(missing)}"
            )
            self.time_order_validation_errors.append({
                "type": "missing_time_order_columns",
                "columns": missing,
                "message": msg,
                "table": getattr(self, "table_name", "unknown"),
            })
            if hasattr(self, "errors"):
                self.errors.extend(self.time_order_validation_errors)
            self.logger.warning(msg)
            return None

        grace = timedelta(minutes=1)

        # Flag if order is ≥ 1 minute after collect (allow small jitter where collect ≥ order within 1 min)
        m_order_ge_collect = (df["order_dttm"] - df["collect_dttm"]) >= grace

        # Flag if collect is ≥ 1 minute after result (allow small jitter where result ≥ collect within 1 min)
        m_collect_ge_result = (df["collect_dttm"] - df["result_dttm"]) >= grace

        # Nulls compare as null in polars, not False; a row missing a timestamp is
        # not a violation, and sum()/any() would otherwise propagate the null.
        m_order_ge_collect = m_order_ge_collect.fill_null(False)
        m_collect_ge_result = m_collect_ge_result.fill_null(False)

        n1 = int(m_order_ge_collect.sum())
        n2 = int(m_collect_ge_result.sum())

        if n1 > 0:
            self.time_order_validation_errors.append({
                "type": "time_order_validation",
                "rule": "order_dttm <= collect_dttm, grace 1 min",
                "message": f"{n1} rows have order_dttm > collect_dttm",
                "rows": n1,
                "table": getattr(self, "table_name", "unknown"),
            })
        if n2 > 0:
            self.time_order_validation_errors.append({
                "type": "time_order_validation",
                "rule": "collect_dttm <= result_dttm, grace 1 min",
                "message": f"{n2} rows have collect_dttm > result_dttm",
                "rows": n2,
                "table": getattr(self, "table_name", "unknown"),
            })

        # Add range validation errors to main errors list (exact logging style)
        if self.time_order_validation_errors:
            if hasattr(self, "errors"):
                self.errors.extend(self.time_order_validation_errors)
            self.logger.warning(f"Found {len(self.time_order_validation_errors)} range validation errors")

        # Return violating rows (union), showing keys + timestamps
        any_bad = m_order_ge_collect | m_collect_ge_result
        if any_bad.any():
            show_cols = [*key_cols, "order_dttm", "collect_dttm", "result_dttm"]
            # Only the violating rows cross into pandas, to keep the documented
            # pandas return type without materializing the whole table.
            return df.filter(any_bad).select(
                [c for c in show_cols if c in df.columns]
            ).to_pandas()

        # Nothing to report
        self.logger.info("validate_timestamp_order: passed (no violations)")
        return None

    @staticmethod
    def cat_vs_name_map(
        df: pd.DataFrame,
        category_col: str,
        name_col: str,
        *,
        group_col: Optional[str] = None,                 # ← if provided, returns {group: {cat: [names...]}}
        dropna: bool = True,
        sort: Literal["freq_then_alpha", "alpha"] = "freq_then_alpha",
        max_names_per_cat: Optional[int] = None,
        include_counts: bool = False,                    # if True → lists of {"name":..., "n":...}
    ) -> Union[Dict[str, List[str]], Dict[str, Dict[str, List[str]]],
            Dict[str, Dict[str, List[Dict[str, int]]]], Dict[str, List[Dict[str, int]]]]:
        """
        Build mappings from category→names (2-level) or group→category→names (3-level).

        Returns:
        - if group_col is None:
                { category: [names...] }  or  { category: [{"name":..., "n":...}, ...] }
        - if group_col is provided:
                { group: { category: [names...] } }  or
                { group: { category: [{"name":..., "n":...}, ...] } }

        Notes
        - Names are unique per (category[, group]) and sorted by:
            freq desc, then alpha  (default), or alpha only if sort="alpha"
        - Set include_counts=True to return [{"name":..., "n":...}] instead of plain strings.
        - Set max_names_per_cat to truncate long lists per category.
        """
        if df is None:
            return {}

        required = [category_col, name_col] + ([group_col] if group_col else [])
        if any(col not in df.columns for col in required):
            return {}

        sub = df[required].copy()
        if dropna:
            sub = sub.dropna(subset=required)

        # frequency at the most granular level available
        group_by_cols = ([group_col] if group_col else []) + [category_col, name_col]
        counts = (
            sub.groupby(group_by_cols)
            .size()
            .reset_index(name="n")
        )

        def _sort_block(block: pd.DataFrame) -> pd.DataFrame:
            if sort == "alpha":
                return block.sort_values([name_col], ascending=[True], kind="mergesort")
            # default: freq desc then alpha
            return block.sort_values(["n", name_col], ascending=[False, True], kind="mergesort")

        def _emit_names(block: pd.DataFrame):
            if include_counts:
                out = [{"name": str(r[name_col]), "n": int(r["n"])} for _, r in block.iterrows()]
            else:
                out = block[name_col].astype(str).tolist()
            if max_names_per_cat is not None:
                out = out[:max_names_per_cat]
            return out

        if group_col:
            # 3-level: group → category → [names or {"name","n"}]
            result: Dict[str, Dict[str, List[Union[str, Dict[str, int]]]]] = {}
            for grp_val, grp_block in counts.groupby(group_col, sort=False):
                cat_map: Dict[str, List[Union[str, Dict[str, int]]]] = {}
                for cat_val, cat_block in grp_block.groupby(category_col, sort=False):
                    sorted_block = _sort_block(cat_block)
                    cat_map[str(cat_val)] = _emit_names(sorted_block)
                result[str(grp_val)] = cat_map
            return result
        else:
            # 2-level: category → [names or {"name","n"}]
            result2: Dict[str, List[Union[str, Dict[str, int]]]] = {}
            for cat_val, cat_block in counts.groupby(category_col, sort=False):
                sorted_block = _sort_block(cat_block)
                result2[str(cat_val)] = _emit_names(sorted_block)
            return result2

    # Wrapper methods for common category-vs-name maps
    def organism_group_cat_name_map(self, include_counts: bool = False, **kwargs):
        # {organism_group: {organism_category: [organism_name,...]}}
        return MicrobiologyCulture.cat_vs_name_map(
            self.data.to_pandas(),
            category_col="organism_category",
            name_col="organism_name",
            group_col="organism_group",
            include_counts=include_counts,
            **kwargs,
        )

    def organism_cat_name_map(self, include_counts: bool = False, **kwargs):
        # {organism_category: [organism_name,...]}
        return MicrobiologyCulture.cat_vs_name_map(
            self.data.to_pandas(),
            category_col="organism_category",
            name_col="organism_name",
            include_counts=include_counts,
            **kwargs,
        )

    def fluid_cat_name_map(self, include_counts: bool = False, **kwargs):
        # {fluid_category: [fluid_name,...]}
        return MicrobiologyCulture.cat_vs_name_map(
            self.data.to_pandas(),
            category_col="fluid_category",
            name_col="fluid_name",
            include_counts=include_counts,
            **kwargs,
        )

    def top_fluid_org_outliers(
        self,
        level: Literal["organism_group", "organism_category"] = "organism_group",
        min_count: int = 0,
        top_k: int = 10,
    ) -> Dict[str, pd.DataFrame]:
        """
        Identify top positive and negative outliers in fluid_category vs organism_group or organism_category.

        Parameters:
            level (str): "organism_group" or "organism_category" (non-standard)
            min_count (int): Minimum observed count to consider
            top_k (int): Number of top positive and negative outliers to return

        Returns:
            Dict with keys "top_positive" and "top_negative", each containing a DataFrame of outliers.
        """
        # pd.crosstab plus the numpy chi-square residual maths below is
        # pandas-native, so this path stays pandas -- but it is converted once,
        # from .data, instead of twice through the caching .df property.
        frame = self.data.to_pandas()
        tbl = pd.crosstab(frame["fluid_category"], frame[level])
        if tbl.empty:
            return {"top_positive": pd.DataFrame(), "top_negative": pd.DataFrame()}

        total = tbl.values.sum()
        exp = (tbl.sum(1).values.reshape(-1,1) @ tbl.sum(0).values.reshape(1,-1)) / total
        with np.errstate(divide="ignore", invalid="ignore"):
            z = (tbl.values - exp) / np.sqrt(exp)

        long = pd.DataFrame({
            "fluid_category": np.repeat(tbl.index.values, tbl.shape[1]),
            level: np.tile(tbl.columns.values, tbl.shape[0]),
            "observed": tbl.values.ravel().astype(float),
            "expected": exp.ravel().astype(float),
            "std_resid": z.ravel().astype(float),
        }).dropna()

        long = long[long["observed"] >= min_count]
        top_pos = long.sort_values("std_resid", ascending=False).head(top_k).reset_index(drop=True)
        top_neg = long.sort_values("std_resid", ascending=True).head(top_k).reset_index(drop=True)
        return {"top_positive": top_pos, "top_negative": top_neg}