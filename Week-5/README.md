# Data Pipeline Project

A comprehensive data pipeline solution for database-to-file exports and database replication with automated triggers.

## Features

1. **Multi-format Export**: Export data to CSV, Parquet, and Avro formats
2. **Automated Triggers**: Schedule-based and event-based pipeline execution
3. **Full Database Cloning**: Copy all tables from source to target database
4. **Selective Data Copy**: Copy specific tables and columns based on configuration

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Set up sample databases (optional):
```bash
python setup_sample_db.py
```

3. Configure database connections in environment variables or modify `config/database_config.py`

## Usage

### Command Line Interface

```bash
# Export tables to multiple formats
python main.py export

# Clone entire database
python main.py clone

# Copy selective tables/columns
python main.py selective

# Setup automated triggers
python main.py trigger

# Run complete pipeline once
python main.py run
```

### Configuration

Edit `config/table_config.json` to specify:
- Selective tables and columns to copy
- Tables to exclude from operations
- Batch size for data processing

### Environment Variables

Set these environment variables for database connections:

**Source Database:**
- `SOURCE_DB_TYPE` (sqlite/postgresql/mysql)
- `SOURCE_DB_HOST`
- `SOURCE_DB_PORT`
- `SOURCE_DB_NAME`
- `SOURCE_DB_USER`
- `SOURCE_DB_PASS`

**Target Database:**
- `TARGET_DB_TYPE`
- `TARGET_DB_HOST`
- `TARGET_DB_PORT`
- `TARGET_DB_NAME`
- `TARGET_DB_USER`
- `TARGET_DB_PASS`

## Project Structure

```
data_pipeline/
├── config/
│   ├── database_config.py      # Database connection settings
│   └── table_config.json       # Selective tables/columns configuration
├── src/
│   ├── file_export.py          # CSV/Parquet/Avro export functionality
│   ├── triggers.py             # Schedule and event triggers
│   ├── db_clone.py             # Full database cloning
│   └── selective_copy.py       # Selective table/column copying
├── output/                     # Generated output files
│   ├── csv/
│   ├── parquet/
│   └── avro/
├── triggers/                   # For event-based triggering
├── requirements.txt            # Python dependencies
├── main.py                     # Main entry point
└── setup_sample_db.py          # Sample database creation
```

## Supported Databases

- SQLite
- PostgreSQL
- MySQL

## Output Formats

- **CSV**: Broad compatibility for reporting and analysis
- **Parquet**: Efficient columnar format for analytics
- **Avro**: Schema evolution and compact serialization