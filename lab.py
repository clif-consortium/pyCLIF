from dataclasses import dataclass
import pandas as pd

@dataclass
class Lab:
    encounter_id: int
    lab_order_dttm: str
    lab_collect_dttm: str
    lab_result_dttm: str
    lab_name: str
    lab_category: str
    lab_group: str
    lab_value: float
    reference_unit: str
    lab_type_name: str

    def __init__(self, df: pd.DataFrame,data_dir):
        self.df = df
        self.data_dir = data_dir

    def check_schema(self):
        required_columns = [
            'encounter_id', 'lab_order_dttm', 'lab_collect_dttm', 'lab_result_dttm',
            'lab_name', 'lab_category', 'lab_group', 'lab_value', 'reference_unit', 'lab_type_name'
        ]
        missing_columns = [col for col in required_columns if col not in self.df.columns]
        if missing_columns:
            raise ValueError(f"Missing columns in the lab data: {missing_columns}")

    def check_mapping(self, vocab_file):
        vocab = pd.read_csv(vocab_file)
        invalid_mappings = self.df[~self.df['lab_name'].isin(vocab['lab_name'])]
        if not invalid_mappings.empty:
            raise ValueError(f"Invalid lab names found: {invalid_mappings['lab_name'].unique()}")

    def get_summary(self):
        summary = self.df.groupby('lab_category')['lab_value'].describe()
        return summary

    def get_raw_strings(self):
        raw_strings = self.df.applymap(str)
        return raw_strings
