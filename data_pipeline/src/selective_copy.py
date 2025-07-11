import pandas as pd
import json
from sqlalchemy import inspect, text, MetaData, Table, Column
from sqlalchemy.exc import SQLAlchemyError

class SelectiveCopier:
    def __init__(self, source_engine, target_engine, config_path="config/table_config.json"):
        self.source_engine = source_engine
        self.target_engine = target_engine
        self.config = self.load_config(config_path)
    
    def load_config(self, config_path):
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            return {"selective_tables": {}, "exclude_tables": [], "batch_size": 1000}
    
    def get_table_columns(self, table_name):
        inspector = inspect(self.source_engine)
        return [col['name'] for col in inspector.get_columns(table_name)]
    
    def create_selective_table(self, table_name, selected_columns):
        # Get source table metadata
        metadata = MetaData()
        source_table = Table(table_name, metadata, autoload_with=self.source_engine)
        
        # Create new table with selected columns only
        target_columns = []
        for col in source_table.columns:
            if col.name in selected_columns:
                target_columns.append(col.copy())
        
        target_table = Table(f"{table_name}_selective", metadata, *target_columns)
        metadata.create_all(self.target_engine, tables=[target_table], checkfirst=True)
        
        return target_table.name
    
    def copy_selective_data(self, source_table, target_table, columns, batch_size=1000):
        try:
            columns_str = ", ".join(columns)
            query = f"SELECT {columns_str} FROM {source_table}"
            
            for chunk in pd.read_sql(query, self.source_engine, chunksize=batch_size):
                chunk.to_sql(target_table, self.target_engine, if_exists='append', index=False)
            
            return True
        except Exception as e:
            print(f"Error copying selective data: {e}")
            return False
    
    def process_selective_tables(self):
        results = {}
        selective_tables = self.config.get("selective_tables", {})
        batch_size = self.config.get("batch_size", 1000)
        
        for table_name, columns in selective_tables.items():
            print(f"Processing selective copy for table: {table_name}")
            
            # Verify columns exist
            available_columns = self.get_table_columns(table_name)
            valid_columns = [col for col in columns if col in available_columns]
            
            if not valid_columns:
                print(f"No valid columns found for table {table_name}")
                results[table_name] = False
                continue
            
            # Create target table
            target_table_name = self.create_selective_table(table_name, valid_columns)
            
            # Copy data
            success = self.copy_selective_data(table_name, target_table_name, valid_columns, batch_size)
            results[table_name] = success
            
            if success:
                print(f"Successfully copied {len(valid_columns)} columns from {table_name}")
        
        return results
    
    def copy_specific_table_columns(self, table_name, columns, target_table_name=None):
        if not target_table_name:
            target_table_name = f"{table_name}_selective"
        
        # Verify columns exist
        available_columns = self.get_table_columns(table_name)
        valid_columns = [col for col in columns if col in available_columns]
        
        if not valid_columns:
            return False
        
        # Create target table
        self.create_selective_table(table_name, valid_columns)
        
        # Copy data
        return self.copy_selective_data(table_name, target_table_name, valid_columns)
    
    def get_filtered_tables(self, exclude_patterns=None):
        if exclude_patterns is None:
            exclude_patterns = self.config.get("exclude_tables", [])
        
        inspector = inspect(self.source_engine)
        all_tables = inspector.get_table_names()
        
        filtered_tables = []
        for table in all_tables:
            if not any(pattern in table for pattern in exclude_patterns):
                filtered_tables.append(table)
        
        return filtered_tables