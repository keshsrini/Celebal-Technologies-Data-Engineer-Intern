import pandas as pd
from sqlalchemy import inspect, text, MetaData, Table
from sqlalchemy.exc import SQLAlchemyError

class DatabaseCloner:
    def __init__(self, source_engine, target_engine):
        self.source_engine = source_engine
        self.target_engine = target_engine
    
    def get_all_tables(self):
        inspector = inspect(self.source_engine)
        return inspector.get_table_names()
    
    def clone_table_structure(self, table_name):
        metadata = MetaData()
        source_table = Table(table_name, metadata, autoload_with=self.source_engine)
        
        # Create table in target database
        metadata.create_all(self.target_engine, tables=[source_table], checkfirst=True)
        return True
    
    def copy_table_data(self, table_name, batch_size=1000):
        try:
            # Read data in chunks
            query = f"SELECT * FROM {table_name}"
            
            for chunk in pd.read_sql(query, self.source_engine, chunksize=batch_size):
                chunk.to_sql(table_name, self.target_engine, if_exists='append', index=False)
            
            return True
        except Exception as e:
            print(f"Error copying data for table {table_name}: {e}")
            return False
    
    def clone_single_table(self, table_name, batch_size=1000):
        print(f"Cloning table: {table_name}")
        
        # Clone structure
        if self.clone_table_structure(table_name):
            print(f"Structure cloned for {table_name}")
        
        # Copy data
        if self.copy_table_data(table_name, batch_size):
            print(f"Data copied for {table_name}")
            return True
        return False
    
    def clone_all_tables(self, batch_size=1000, exclude_tables=None):
        if exclude_tables is None:
            exclude_tables = []
        
        tables = self.get_all_tables()
        results = {}
        
        for table in tables:
            if any(exclude in table for exclude in exclude_tables):
                print(f"Skipping table: {table}")
                continue
            
            results[table] = self.clone_single_table(table, batch_size)
        
        return results
    
    def get_table_count(self, table_name, engine):
        try:
            result = engine.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            return result.scalar()
        except:
            return 0
    
    def verify_clone(self):
        tables = self.get_all_tables()
        verification = {}
        
        for table in tables:
            source_count = self.get_table_count(table, self.source_engine)
            target_count = self.get_table_count(table, self.target_engine)
            
            verification[table] = {
                'source_count': source_count,
                'target_count': target_count,
                'match': source_count == target_count
            }
        
        return verification