import pandas as pd
import os
from tqdm.auto import tqdm
from .lab import Lab
from .patient import Patient
from .hospitalization import Hospitalization
import duckdb
import json

class CLIF:
    def __init__(self, data_dir, filetype='csv'):
        """
        Initialize the CLIF object.

        Args:
            data_dir (str): Directory where data files are stored.
            filetype (str): Type of files to load ('csv' or 'parquet'). Default is 'csv'.
        """
        self.data_dir = data_dir
        self.filetype = filetype
        # self.all_table_dtype = self.load_all_table__dtype_json()
        self.conn = self.create_connection()
        self.loaded_tables = {}
        self.lab = None
        self.patient = None
        self.hospitalization = None
        print('CLIF Object Initialized !!! ⛑️')


    def load(self, table_list=None):
        """
        Load tables from the table_list into the CLIF object.

        Args:
            table_list (list): List of table names to load. If None, loads all tables by default.
        """

        # Load tables based on the provided list
        
        for table in tqdm(table_list,position = 0, desc='Loading '):
            file_path = os.path.join(self.data_dir, f"{table}.{self.filetype}")
            if os.path.exists(file_path):
                if self.filetype == 'csv':
                    df = duckdb.read_csv(file_path ).df()
                elif self.filetype == 'parquet':
                    df = duckdb.read_parquet(file_path).df()
                else:
                    raise ValueError("Unsupported filetype. Only 'csv' and 'parquet' are supported.")
        
                ## Init Table Obj

                if table == 'patient':
                    self.patient = Patient(df,self.data_dir)
                    self.loaded_tables[table]=True

                if table == 'hospitalization':
                    self.hospitalization = Hospitalization(df,self.data_dir)
                    self.loaded_tables[table]=True
                
                if table == 'labs':
                    self.lab = Lab(df,self.data_dir)
                    self.loaded_tables[table]=True

            else:
                print(f"File {file_path} not found. Skipping loading of {table} table.")

        return print(' ✅ Loaded Tables : ', self.get_loaded_tables())          

    def get_loaded_tables(self):
        """
        Get a list of all loaded tables.

        Returns:
            list: List of loaded table names.
        """
        return list(self.loaded_tables.keys())

    def create_connection(self):
        """Create a database connection."""

        return duckdb.connect(database = ":memory:")