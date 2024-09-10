from datetime import datetime
from tqdm.auto import tqdm
import pandas as pd
import duckdb
import json
import re
import os

class Patient:

    def __init__(self ,data_dir = None, df: pd.DataFrame = None, filetype='csv'):
        self.filetype = filetype
        self.data_dir = data_dir
        self.df = df if df is not None else self.load_data()
        self.val_json = self.load_json()
        self.missing_columns = None
        self.non_standard_columns = None
        self.site_mapping = None
        self.temporal_columns_to_fix = []
        self.duck = duckdb
        self.get_duckdb_register()

    
    def get_columns(self):
        return self.df.columns.tolist()
    
    def get_df(self,col=None):
        if col is None:
            col = self.val_json["base_columns"]
        return self.df[col]

    def get_duckdb_register(self):
        try:
            duckdb.unregister("patient")
        except:
            pass
        finally:
            duckdb.register("patient", self.df)
    
    def load_json(self):
        with open('pyCLIF/mCIDE/patient.json', 'r') as file:
            data = json.load(file)  
        return data

    def load_mapping(self,mappings_path):
        with open(mappings_path, 'r') as file:
            data = json.load(file)  
        return data
    
    def load_data(self):
        """
        Load the patient data from a file in the specified directory.

        Returns:
            pd.DataFrame: DataFrame containing patient data.
        """
        # Determine the file path based on the directory and filetype
        file_path = os.path.join(self.data_dir, f"patient.{self.filetype}")
        
        # Load the data based on filetype
        if os.path.exists(file_path):
            if self.filetype == 'csv':
                df = duckdb.read_csv(file_path).df()
            elif self.filetype == 'parquet':
                df = duckdb.read_parquet(file_path).df()
            else:
                raise ValueError("Unsupported filetype. Only 'csv' and 'parquet' are supported.")
            print(f"Data loaded successfully from {file_path}")
            return df
        else:
            raise FileNotFoundError(f"The file {file_path} does not exist in the specified directory.")

    
    def table_heath(self):
        for check, result in self.val_json["heath_check_up"].items():
            print( "✅ Pass" if result else "❌ Fail",check.upper())

    def validate(self):
        print('++' * 30)
        print(" " * 7,"CLIF Patient Table Checks")
        print('++' * 30)
        print(' '*5 + '⭐ CLIF Standard Columns:')
        print(self.val_json["base_columns"])

        for x in tqdm(list(self.val_json["heath_check_up"].keys()), desc='Processing Validation 🧪🧪🧪 '):
            if x=='check_id_duplicate':
                print('\n' + ' '*5,x.upper())
                self.check_id_duplicate()
            if x=='check_missing_columns':
                print('\n' + ' '*5,x.upper())
                self.check_missing_columns()
            if x=='check_category':
                print('\n' + ' '*5,x.upper())
                self.check_category()
            if x=='check_date_time_format':
                print('\n' + ' '*5,x.upper())
                self.check_date_time_format()
            

    def check_id_duplicate(self):

            results = duckdb.sql(f'''SELECT patient_id, COUNT(*) as count
                                    FROM patient
                                    GROUP BY patient_id
                                    HAVING COUNT(*) > 1
                                    ORDER BY count DESC''').df()
            
            if len(results)>0:
                print("❌ Fail : Duplicates found in patient_id with count:",results.shape[0])
            else:
                print("✅ Pass : No duplicates found in patient_id. ")
                self.val_json['heath_check_up']['check_id_duplicate']=True


    def check_missing_columns(self):

        current_columns = self.get_columns()
        self.missing_columns = [col for col in self.val_json["base_columns"] if col not in current_columns]
        self.non_standard_columns = [col for col in current_columns if col not in self.val_json["base_columns"]]

        print("🥔 WHat if not Name? : Columns Not Part of CLIF Standard : ",self.non_standard_columns)

        if len(self.missing_columns)>0:
            print("❌ Fail : Columns Missing from Patient Table: ",self.missing_columns)
        else:
            print("✅ Pass : No missing columns found")
            self.val_json['heath_check_up']['check_missing_columns']=True

    def check_category(self):
        to_check = list(self.val_json["category_columns"].keys())
        faults = len(to_check)

        # Iterate through category columns specified in val_json
        for col in to_check:
            # Check if the category column is in the DataFrame's columns
            if col not in self.get_columns():
                print(f"❌ Fail: Missing column: {col}")
                continue
            
            # Retrieve permissible values from mCIDE_mapping (keys are the permissible values)
            permissible_values = set(self.val_json["mCIDE_mapping"][col].keys())

            # Check if all values in the column are within the permissible values
            invalid_values = self.df[~self.df[col].isin(permissible_values)][col].unique()

            if len(invalid_values) > 0:
                print(f"❌ Fail: Invalid values found in '{col}': {invalid_values}")
            else:
                print(f"✔️ Pass : All values in '{col}' are within the permissible values.")
                faults -= 1
            
        if faults != 0:
            print('''❌ Fail : Overall check_category()''')
        else:
            self.val_json['heath_check_up']['check_category'] = True


    def check_date_time_format(self):
         
         date_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}$')
         faults = len(self.val_json["temporal_columns"])

         for col in self.val_json["temporal_columns"]:
             
            if col not in self.df.columns:
                print(f"❌ Fail : missing column : ",col)
                continue
            
            # Drop NaN or NULL values
            non_na_values =  self.df[col].dropna()

            # Check if the column is already in datetime format
            if pd.api.types.is_datetime64_any_dtype(non_na_values):
                # If in datetime format, check if all values match the format pattern using .strftime()
                formatted_dates = non_na_values.dt.strftime('%Y-%m-%d')
                if all(formatted_dates.str.match(date_pattern)):
                    print(f"✅ Pass : Column {col}: All values are in datetime format and match the pattern %Y-%m-%d.")
                    faults-=1
                else:
                    print(f"❌ Fail : Column {col}: Values are in datetime format but do not match the pattern %Y-%m-%d.")
                    self.temporal_columns_to_fix.append(col)
                continue
            
            non_na_values = non_na_values.astype(str).replace(['NULL', 'null', 'Null'], None).dropna()

            try:
                # Attempt to convert the column to datetime with the specified format
                pd.to_datetime(non_na_values, format='%Y-%m-%d', errors='raise')
                
                # After conversion, check if all non-null values strictly match the format YYYY-MM-DD using regex
                if all(non_na_values.astype(str).str.match(date_pattern)):
                    print(f"✅ Pass : Column {col}: All values are parsable and match the format %Y-%m-%d.")
                    faults-=1
                else:
                    print(f"❌ Fail : Values are parsable but do not strictly match the format %Y-%m-%d.")
                    self.temporal_columns_to_fix.append(col)

            except ValueError as e:
                print(f"Column {col}: Fail - Contains values that are not parsable in the format %Y-%m-%d: {e}")
                print('''❌ Fail : check_date_time_format()''')
            
         if faults!=0:
             print('''❌ Fail : Overall check_date_time_format()''')
         else:
             self.val_json['heath_check_up']['check_date_time_format']=True


    def map_to_category(self, value, mapping):
        for category, values in mapping.items():
            if value in values:
                return category
        return 'No Mapping Found'  # Default category if value not found in mappings
    
    def add_clif_category(self, mappings_path=None, export=True):
        
        # Use provided mappings or default to generic
        if mappings_path is None:
            print("No site-specific mappings provided. Using default generic mappings for category assignment.")
            mappings = self.val_json['mCIDE_mapping']
        else:
            print("Using site-specific mappings for category assignment.")
            self.site_mapping = self.load_mapping(mappings_path)
            mappings=self.site_mapping
      
        
        for category_col, name_col in tqdm(self.val_json['category_columns'].items()):
            self.df[category_col] = self.df[name_col].apply(lambda x: self.map_to_category(str(x), mappings[category_col]))
            print(f"✅ Completed mapping for '{name_col}' to '{category_col}'.")


        # Print the mappings applied for each category
        print("\n       🗺️ Category Mappings Applied:")
        for category_col, name_col in self.val_json['category_columns'].items():
            unique_values = self.df[[name_col, category_col]].drop_duplicates()
            print(f"\n📍 {name_col} to {category_col} Mapping:".upper())
            for _, row in unique_values.iterrows():
                print(f"  {row[name_col]} -> {row[category_col]}")

        # Create new mapping if export is True
        if export and mappings_path is None:
            new_mappings = {}
            for category_col, name_col in self.val_json['category_columns'].items():
                new_mappings[category_col] = {}
                unique_values = self.df[[name_col, category_col]].drop_duplicates()
                for category_name in unique_values[category_col].unique():
                    values = unique_values[unique_values[category_col] == category_name][name_col].tolist()
                    new_mappings[category_col][category_name] = values
            
            new_mappings['category_columns']=self.val_json['category_columns']
            
            # Generate export file name using current date and time
            export_filename = f"Hospitalization-SiteSpecific-{datetime.now().strftime('%Y%m%d')}.json"
            export_path = os.path.join(self.data_dir, export_filename)
            
            with open(export_path, 'w') as f:
                json.dump(new_mappings, f, indent=4)
            print(f"\n 📄 Site-specific mapping exported successfully to {export_path}.")
        
        self.get_duckdb_register()




                 