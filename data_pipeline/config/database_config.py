import os
from sqlalchemy import create_engine

class DatabaseConfig:
    def __init__(self):
        self.source_db = {
            'type': os.getenv('SOURCE_DB_TYPE', 'sqlite'),
            'host': os.getenv('SOURCE_DB_HOST', 'localhost'),
            'port': os.getenv('SOURCE_DB_PORT', '5432'),
            'database': os.getenv('SOURCE_DB_NAME', 'source.db'),
            'username': os.getenv('SOURCE_DB_USER', ''),
            'password': os.getenv('SOURCE_DB_PASS', '')
        }
        
        self.target_db = {
            'type': os.getenv('TARGET_DB_TYPE', 'sqlite'),
            'host': os.getenv('TARGET_DB_HOST', 'localhost'),
            'port': os.getenv('TARGET_DB_PORT', '5432'),
            'database': os.getenv('TARGET_DB_NAME', 'target.db'),
            'username': os.getenv('TARGET_DB_USER', ''),
            'password': os.getenv('TARGET_DB_PASS', '')
        }
    
    def get_connection_string(self, db_config):
        if db_config['type'] == 'sqlite':
            return f"sqlite:///{db_config['database']}"
        elif db_config['type'] == 'postgresql':
            return f"postgresql://{db_config['username']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        elif db_config['type'] == 'mysql':
            return f"mysql+pymysql://{db_config['username']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    
    def get_source_engine(self):
        return create_engine(self.get_connection_string(self.source_db))
    
    def get_target_engine(self):
        return create_engine(self.get_connection_string(self.target_db))