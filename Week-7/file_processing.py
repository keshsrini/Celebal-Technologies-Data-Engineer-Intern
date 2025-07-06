import re
import pandas as pd
from datetime import datetime

def process_files(data_lake_path):
    files = list_files_in_container(data_lake_path)
    
    for file in files:
        filename = file.name
        
        
        if filename.startswith('CUST_MSTR_'):
            date_str = filename.split('_')[-1].replace('.csv', '')
            process_cust_mstr(file, date_str)
        elif filename.startswith('master_child_export-'):
            date_str = filename.split('-')[-1].replace('.csv', '')
            process_master_child(file, date_str)
        elif filename == 'H_ECOM_ORDER.csv':
            process_ecom_order(file)

def list_files_in_container(data_lake_path):
    pass
