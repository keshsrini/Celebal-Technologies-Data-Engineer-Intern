import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import fastavro
import json
import os
from sqlalchemy import text

class FileExporter:
    def __init__(self, db_engine, output_dir="output"):
        self.db_engine = db_engine
        self.output_dir = output_dir
    
    def export_table_to_csv(self, table_name, filename=None):
        if not filename:
            filename = f"{table_name}.csv"
        
        df = pd.read_sql(f"SELECT * FROM {table_name}", self.db_engine)
        csv_path = os.path.join(self.output_dir, "csv", filename)
        df.to_csv(csv_path, index=False)
        return csv_path
    
    def export_table_to_parquet(self, table_name, filename=None):
        if not filename:
            filename = f"{table_name}.parquet"
        
        df = pd.read_sql(f"SELECT * FROM {table_name}", self.db_engine)
        parquet_path = os.path.join(self.output_dir, "parquet", filename)
        df.to_parquet(parquet_path, index=False)
        return parquet_path
    
    def export_table_to_avro(self, table_name, filename=None):
        if not filename:
            filename = f"{table_name}.avro"
        
        df = pd.read_sql(f"SELECT * FROM {table_name}", self.db_engine)
        avro_path = os.path.join(self.output_dir, "avro", filename)
        
        # Convert DataFrame to records
        records = df.to_dict('records')
        
        # Create Avro schema from DataFrame
        schema = self._create_avro_schema(df, table_name)
        
        with open(avro_path, 'wb') as out:
            fastavro.writer(out, schema, records)
        
        return avro_path
    
    def _create_avro_schema(self, df, table_name):
        fields = []
        for col, dtype in df.dtypes.items():
            if pd.api.types.is_integer_dtype(dtype):
                avro_type = "long"
            elif pd.api.types.is_float_dtype(dtype):
                avro_type = "double"
            elif pd.api.types.is_bool_dtype(dtype):
                avro_type = "boolean"
            else:
                avro_type = ["null", "string"]
            
            fields.append({"name": col, "type": avro_type})
        
        return {
            "type": "record",
            "name": table_name,
            "fields": fields
        }
    
    def export_all_formats(self, table_name):
        results = {}
        results['csv'] = self.export_table_to_csv(table_name)
        results['parquet'] = self.export_table_to_parquet(table_name)
        results['avro'] = self.export_table_to_avro(table_name)
        return results