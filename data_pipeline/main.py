#!/usr/bin/env python3

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config.database_config import DatabaseConfig
from src.file_export import FileExporter
from src.triggers import TriggerManager
from src.db_clone import DatabaseCloner
from src.selective_copy import SelectiveCopier

class DataPipeline:
    def __init__(self):
        self.db_config = DatabaseConfig()
        self.source_engine = self.db_config.get_source_engine()
        self.target_engine = self.db_config.get_target_engine()
        
        self.file_exporter = FileExporter(self.source_engine)
        self.db_cloner = DatabaseCloner(self.source_engine, self.target_engine)
        self.selective_copier = SelectiveCopier(self.source_engine, self.target_engine)
        self.trigger_manager = TriggerManager(self.run_pipeline)
    
    def export_to_files(self, table_name):
        """Export single table to CSV, Parquet, and Avro formats"""
        print(f"Exporting table {table_name} to multiple formats...")
        results = self.file_exporter.export_all_formats(table_name)
        for format_type, path in results.items():
            print(f"{format_type.upper()}: {path}")
        return results
    
    def clone_database(self):
        """Clone all tables from source to target database"""
        print("Starting full database clone...")
        results = self.db_cloner.clone_all_tables()
        
        success_count = sum(1 for success in results.values() if success)
        print(f"Cloned {success_count}/{len(results)} tables successfully")
        
        # Verify clone
        verification = self.db_cloner.verify_clone()
        print("\nVerification Results:")
        for table, info in verification.items():
            status = "✓" if info['match'] else "✗"
            print(f"{status} {table}: {info['source_count']} -> {info['target_count']}")
        
        return results
    
    def selective_copy(self):
        """Copy selective tables and columns"""
        print("Starting selective table copy...")
        results = self.selective_copier.process_selective_tables()
        
        success_count = sum(1 for success in results.values() if success)
        print(f"Processed {success_count}/{len(results)} selective tables successfully")
        
        return results
    
    def run_pipeline(self):
        """Main pipeline execution"""
        print(f"\n{'='*50}")
        print("Data Pipeline Execution Started")
        print(f"{'='*50}")
        
        try:
            # Get available tables
            tables = self.db_cloner.get_all_tables()
            print(f"Found {len(tables)} tables in source database")
            
            # Export first table to files (demo)
            if tables:
                self.export_to_files(tables[0])
            
            # Run selective copy
            self.selective_copy()
            
            print("Pipeline execution completed successfully")
            
        except Exception as e:
            print(f"Pipeline execution failed: {e}")
    
    def setup_triggers(self, schedule_type="daily", **kwargs):
        """Setup automated triggers"""
        print(f"Setting up {schedule_type} trigger...")
        self.trigger_manager.setup_schedule(schedule_type, **kwargs)
        self.trigger_manager.start_all()
        print("Triggers activated. Pipeline will run automatically.")
        
        try:
            input("Press Enter to stop triggers...")
        except KeyboardInterrupt:
            pass
        finally:
            self.trigger_manager.stop_all()
            print("Triggers stopped.")

def main():
    pipeline = DataPipeline()
    
    if len(sys.argv) < 2:
        print("Usage: python main.py [export|clone|selective|trigger|run]")
        print("  export    - Export tables to CSV/Parquet/Avro")
        print("  clone     - Clone entire database")
        print("  selective - Copy selective tables/columns")
        print("  trigger   - Setup automated triggers")
        print("  run       - Run complete pipeline once")
        return
    
    command = sys.argv[1].lower()
    
    if command == "export":
        table_name = input("Enter table name to export: ")
        pipeline.export_to_files(table_name)
    
    elif command == "clone":
        pipeline.clone_database()
    
    elif command == "selective":
        pipeline.selective_copy()
    
    elif command == "trigger":
        schedule_type = input("Enter schedule type (daily/hourly/minutes): ").lower()
        if schedule_type == "daily":
            time_str = input("Enter time (HH:MM, default 09:00): ") or "09:00"
            pipeline.setup_triggers("daily", time=time_str)
        elif schedule_type == "minutes":
            minutes = int(input("Enter interval in minutes (default 30): ") or "30")
            pipeline.setup_triggers("minutes", minutes=minutes)
        else:
            pipeline.setup_triggers("hourly")
    
    elif command == "run":
        pipeline.run_pipeline()
    
    else:
        print(f"Unknown command: {command}")

if __name__ == "__main__":
    main()