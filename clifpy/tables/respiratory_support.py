from typing import Optional, Union
import pandas as pd
from .base_table import BaseTable
from ..utils.waterfall import process_resp_support_waterfall


class RespiratorySupport(BaseTable):
    """
    Respiratory support table wrapper inheriting from BaseTable.
    
    This class handles respiratory support data and validations while
    leveraging the common functionality provided by BaseTable.
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
        Initialize the respiratory_support table.
        
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
    
    def waterfall(
    self,
    *,
    id_col: str = "hospitalization_id",
    bfill: bool = False,
    verbose: bool = True,
    return_dataframe: bool = False
) -> Union['RespiratorySupport', pd.DataFrame]:
        """
        Clean + waterfall-fill the respiratory_support table.

        Parameters
        ----------
        id_col : str
            Encounter-level identifier column (default: hospitalization_id)
        bfill : bool
            If True, numeric setters are back-filled after forward-fill
        verbose : bool
            Print progress messages
        return_dataframe : bool
            If True, returns DataFrame instead of RespiratorySupport instance

        Returns
        -------
        RespiratorySupport
            New instance with processed data (or DataFrame if return_dataframe=True)

        Notes
        -----
        The waterfall function expects data in UTC timezone. If your data is in a
        different timezone, it will be converted to UTC for processing, then converted
        back to the original timezone on return. The original object is not modified.
        """
        if self.data is None or self.data.is_empty():
            raise ValueError("No data available to process. Load data first.")

        # process_resp_support_waterfall (utils/waterfall.py:8) is pandas-native,
        # so this path stays pandas. Converting from .data explicitly gives the
        # same frame without populating -- and then permanently holding -- the
        # cached pandas view on the table. to_pandas() already returns a fresh
        # frame, so the .copy() it replaces is not needed.
        df_copy = self.data.to_pandas()

        # --- Capture original tz (if any), convert to UTC for processing
        original_tz = None
        if 'recorded_dttm' in df_copy.columns:
            if pd.api.types.is_datetime64tz_dtype(df_copy['recorded_dttm']):
                original_tz = df_copy['recorded_dttm'].dt.tz
                if verbose and str(original_tz) != 'UTC':
                    print(f"Converting timezone from {original_tz} to UTC for waterfall processing")
                # Convert to UTC (no-op if already UTC)
                df_copy['recorded_dttm'] = df_copy['recorded_dttm'].dt.tz_convert('UTC')
            else:
                # tz-naive; leave as-is (function expects UTC semantics already)
                original_tz = None

        # --- Run the waterfall (expects UTC)
        processed_df = process_resp_support_waterfall(
            df_copy,
            id_col=id_col,
            bfill=bfill,
            verbose=verbose
        )

        # --- Convert back to original tz if we had one
        if original_tz is not None:
            # Guard: ensure tz-aware before tz_convert
            if pd.api.types.is_datetime64tz_dtype(processed_df['recorded_dttm']):
                if verbose and str(original_tz) != 'UTC':
                    print(f"Converting timezone from UTC back to {original_tz} after processing")
                processed_df = processed_df.copy()
                processed_df['recorded_dttm'] = processed_df['recorded_dttm'].dt.tz_convert(original_tz)
            else:
                # If something made it tz-naive, localize then convert (shouldn't happen, but safe)
                processed_df = processed_df.copy()
                processed_df['recorded_dttm'] = (
                    processed_df['recorded_dttm']
                    .dt.tz_localize('UTC')
                    .dt.tz_convert(original_tz)
                )

        # Return DataFrame if requested
        if return_dataframe:
            return processed_df

        # Otherwise, return a new wrapped instance (timezone metadata stays the same as the current object)
        return RespiratorySupport(
            data_directory=self.data_directory,
            filetype=self.filetype,
            timezone=self.timezone,  # this is your package-level field; we keep it unchanged
            output_directory=self.output_directory,
            data=processed_df
        )


    
