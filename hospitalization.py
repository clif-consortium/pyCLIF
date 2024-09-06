import pandas as pd

class Hospitalization:

    def __init__(self, df: pd.DataFrame,data_dir):
        self.df = df
        self.data_dir = data_dir

    def get_columns(self):
        return self.df.columns