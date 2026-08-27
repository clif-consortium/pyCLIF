from typing import Optional
import pandas as pd
import polars as pl
from .base_table import BaseTable


class Position(BaseTable):
    """
    Position table wrapper inheriting from BaseTable.
    
    This class handles patient position data and validations while
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
        Initialize the position table.
        
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
            # Old signature: position(data)
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
    
    def get_position_category_stats(self) -> pd.DataFrame:
        """
        Return summary statistics for each position category, including missingness and unique patient counts.
        Expects columns: 'position_category', 'position_name', and optionally 'hospitalization_id'.
        """
        if self.data is None or 'position_category' not in self.data.columns or 'hospitalization_id' not in self.data.columns:
            return {"status": "Missing columns"}

        # Computed on the stored polars frame; only the per-category result --
        # one row per category -- is converted, so the full table is never
        # copied into pandas.
        #
        # drop_nulls on the group key matches pandas' groupby(dropna=True), and
        # drop_nulls before n_unique matches pandas' nunique(), which does not
        # count NaN. polars does neither by default.
        stats = (
            self.data
            .drop_nulls(subset=['position_category'])
            .group_by('position_category')
            .agg(
                pl.col('position_category').count().alias('count'),
                pl.col('hospitalization_id').drop_nulls().n_unique().alias('unique'),
            )
            .sort('position_category')
            .to_pandas()
            .set_index('position_category')
        )

        return stats