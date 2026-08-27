from datetime import datetime
from typing import Optional, List, Dict
import pandas as pd
import polars as pl
import os
from .base_table import BaseTable


class Adt(BaseTable):
    """
    ADT (Admission/Discharge/Transfer) table wrapper inheriting from BaseTable.
    
    This class handles ADT-specific data and validations while
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
        Initialize the ADT table.
        
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
            # Old signature: adt(data)
            # Use dummy values for required parameters
            data_directory = "."
            filetype = "parquet"
        
        super().__init__(
            data_directory=data_directory,
            filetype=filetype,
            timezone=timezone,
            output_directory=output_directory,
            data=data,
            clif_version=clif_version
        )

    # ------------------------------------------------------------------
    # ADT Specific Methods
    # ------------------------------------------------------------------
    def check_overlapping_admissions(self, save_overlaps: bool = False, overlaps_output_directory: Optional[str] = None) -> int:
        """
        Check for overlapping admissions within the same hospitalization.

        Identifies cases where a patient has overlapping stays in different locations
        within the same hospitalization (i.e., the out_dttm of one location is after
        the in_dttm of the next location).

        Parameters:
            save_overlaps (bool): If True, save detailed overlap information to CSV. Default is False.
            overlaps_output_directory (str, optional): Directory for saving the overlaps CSV file. 
                If None, uses the output_directory provided at initialization.

        Returns:
            int: Count of unique hospitalizations that have overlapping admissions

        Raises:
            RuntimeError: If an error occurs during processing
        """
        try:
            if self.data is None:
                return 0

            if 'hospitalization_id' not in self.data.columns:
                error = "hospitalization_id is missing."
                raise ValueError(error)

            # Each row is compared against the next row of the same
            # hospitalization. shift(-1).over(...) expresses that directly, so
            # the row-by-row .iloc loop over pandas groups is gone -- and with
            # it the full pandas copy of the table it used to require.
            pairs = (
                self.data
                .drop_nulls(subset=['hospitalization_id'])   # pandas groupby dropped these
                .sort(['hospitalization_id', 'in_dttm'])
                .with_columns(
                    pl.col('location_name').shift(-1).over('hospitalization_id').alias('_next_name'),
                    pl.col('location_category').shift(-1).over('hospitalization_id').alias('_next_category'),
                    pl.col('in_dttm').shift(-1).over('hospitalization_id').alias('_next_in_dttm'),
                )
                .filter(
                    # Last row of each hospitalization has no successor.
                    pl.col('_next_in_dttm').is_not_null()
                    # pandas compared object-dtype values with !=, where
                    # None != None is False and None != 'icu' is True. Plain !=
                    # yields null when either side is null, which would drop the
                    # second case; ne_missing treats null as a comparable value
                    # and reproduces pandas exactly.
                    & pl.col('location_name').ne_missing(pl.col('_next_name'))
                    # NaT > x was False in pandas; null > x is null here.
                    & (pl.col('out_dttm') > pl.col('_next_in_dttm')).fill_null(False)
                )
            )

            overlapping_hospitalizations = pairs['hospitalization_id'].unique()

            # Save overlaps to CSV if requested
            if save_overlaps and not pairs.is_empty():
                overlaps_df = pairs.select(
                    pl.col('hospitalization_id'),
                    pl.col('location_name').alias('Initial Location'),
                    pl.col('location_category').alias('Initial Location Category'),
                    pl.col('_next_name').alias('Overlapping Location'),
                    pl.col('_next_category').alias('Overlapping Location Category'),
                    pl.col('in_dttm').alias('Admission Start'),
                    pl.col('out_dttm').alias('Admission End'),
                    pl.col('_next_in_dttm').alias('Next Admission Start'),
                ).to_pandas()
                # Determine the directory to save the overlaps file
                save_dir = overlaps_output_directory if overlaps_output_directory is not None else self.output_directory
                if save_dir is not None:
                    os.makedirs(save_dir, exist_ok=True)
                    file_path = os.path.join(save_dir, 'overlapping_admissions.csv')
                    overlaps_df.to_csv(file_path, index=False)
                else:
                    # Fallback to original method if no directory is specified
                    self.save_dataframe(overlaps_df, 'overlapping_admissions')

            return len(overlapping_hospitalizations)

        except Exception as e:
            # Handle errors gracefully
            raise RuntimeError(f"Error checking time overlap: {str(e)}")
