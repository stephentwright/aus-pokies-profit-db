"""
Program to create database tables and ingest raw data.

This program:
1. Uses the metadata file to determine column names for each table
2. Creates compact table names in the format [TYPE]_GAMING_DATA_START_END_DATE
3. Loads data from each file into the corresponding table
"""
import pandas as pd
from pathlib import Path
from datetime import datetime
import re
import db_utils

# Paths
METADATA_FILE = Path(__file__).parent / "raw-data" / "download_metadata.csv"
RAW_DATA_DIR = Path(__file__).parent / "raw-data"

def shorten_column_name(col_name):
    """
    Shorten column names to make them more compact.
    
    Args:
        col_name: Original column name
        
    Returns:
        Shortened, database-friendly column name
    """
    # Common replacements
    replacements = {
        'local government area': 'lga',
        'government': 'gov',
        'area': 'area',
        'venue': 'ven',
        'trading': 'trd',
        'number': 'num',
        'electronic': 'elec',
        'gaming': 'gam',
        'machine': 'mach',
        'machines': 'mach',
        'monthly': 'mon',
        'profit': 'profit',
        'expenditure': 'expend',
        'average': 'avg',
        'statistics': 'stats',
        'statistical': 'stat',
        'division': 'div',
        'postcode': 'pcode',
        'liquor': 'liq',
        'licence': 'lic',
        'premises': 'prem',
        'count': 'cnt',
        'population': 'pop',
        'per 100k': 'per_100k'
    }
    
    # Convert to lowercase and remove special chars
    col_name = col_name.lower().strip()
    
    # Remove text like "as at [date]"
    col_name = re.sub(r'as at.*$', '', col_name).strip()
    
    # Replace common phrases first (before word-by-word replacement)
    for old, new in replacements.items():
        col_name = col_name.replace(old, new)
    
    # Remove parentheses and their contents
    col_name = re.sub(r'\([^)]*\)', '', col_name)
    
    # Replace spaces and special chars with underscore
    col_name = re.sub(r'[^\w\s]', '', col_name)
    col_name = re.sub(r'\s+', '_', col_name)
    
    # Remove consecutive underscores
    col_name = re.sub(r'_+', '_', col_name)
    
    # Remove leading/trailing underscores
    col_name = col_name.strip('_')
    
    return col_name

def create_table_name(data_type, start_date, end_date):
    """
    Create compact table name from metadata.
    
    Args:
        data_type: Type of data (club, hotel, etc.)
        start_date: Start date string (YYYY-MM-DD)
        end_date: End date string (YYYY-MM-DD)
        
    Returns:
        Compact table name
    """
    # Shorten data type
    type_map = {
        'club': 'CLB',
        'hotel': 'HTL',
        'venue': 'VEN',
        'lga': 'LGA',
        'postcode': 'PC',
        'monthly': 'MON',
        'annual': 'ANN'
    }
    
    short_type = type_map.get(data_type.lower(), data_type[:3].upper())
    
    # Format dates compactly (YYYYMM)
    start = datetime.strptime(str(start_date), '%Y%m%d').strftime('%Y%m')
    end = datetime.strptime(str(end_date), '%Y%m%d').strftime('%Y%m')
    
    return f"{short_type}_GAM_{start}_{end}".lower()

def get_postgres_type(dtype):
    """
    Map pandas dtype to PostgreSQL type.
    
    Args:
        dtype: Pandas dtype
        
    Returns:
        PostgreSQL type string
    """
    if pd.api.types.is_integer_dtype(dtype):
        return "INTEGER"
    elif pd.api.types.is_float_dtype(dtype):
        return "NUMERIC"
    else:
        return "TEXT"

def parse_header_names(header_str):
    """
    Parse the pipe-separated header names from metadata.
    
    Args:
        header_str: Pipe-separated header string
        
    Returns:
        List of header names
    """
    # Split by pipe and clean up newlines
    headers = header_str.split('|')
    headers = [h.replace('\n', ' ').strip() for h in headers]
    return headers

def create_and_load_table(conn, file_path, table_name, header_row, expected_headers, start_period, end_period):
    """
    Create table and load data from Excel/CSV file.
    
    Args:
        conn: Database connection
        file_path: Path to data file
        table_name: Name for the table
        header_row: Row number where headers are located (0-indexed)
        expected_headers: List of expected header names from metadata
        start_period: Start date of data period
        end_period: End date of data period
    """
    print(f"\nProcessing {file_path.name}...")
    print(f"  Header row: {header_row}")
    print(f"  Expected columns: {len(expected_headers)}")
    
    # Read the file
    if file_path.suffix.lower() in ['.xlsx', '.xls']:
        df = pd.read_excel(file_path, header=header_row, engine='openpyxl')
    else:
        df = pd.read_csv(file_path, header=header_row)
    
    print(f"  Actual columns: {len(df.columns)}")
    
    # Clean and shorten column names
    original_cols = df.columns.tolist()
    df.columns = [shorten_column_name(str(col)) for col in df.columns]
    
    print(f"  Column mapping:")
    for orig, new in zip(original_cols, df.columns):
        print(f"    '{orig}' -> '{new}'")
    
    # Remove rows with all NaN values
    df = df.dropna(how='all')
    
    # Create table schema
    columns_def = []
    for col in df.columns:
        sql_type = get_postgres_type(df[col].dtype)
        columns_def.append(f"{col} {sql_type}")
    
    # Drop table if exists and create new one
    cursor = conn.cursor()
    
    try:
        cursor.execute(f"DROP TABLE IF EXISTS land.{table_name}")
        
        create_table_sql = f"""
        CREATE TABLE land.{table_name} (
            id SERIAL PRIMARY KEY,
            start_period DATE,
            end_period DATE,
            {', '.join(columns_def)},
            loaded_at TIMESTAMPTZ DEFAULT now()
        )
        """
        
        cursor.execute(create_table_sql)
        conn.commit()
        print(f"  ✓ Created table: land.{table_name}")
        
        # Prepare data for bulk insert
        # Convert periods to date format (YYYYMMDD -> YYYY-MM-DD)
        start_date = datetime.strptime(str(start_period), '%Y%m%d').strftime('%Y-%m-%d')
        end_date = datetime.strptime(str(end_period), '%Y%m%d').strftime('%Y-%m-%d')
        
        data_columns = ['start_period', 'end_period'] + list(df.columns)
        data_rows = []
        
        for _, row in df.iterrows():
            row_data = [start_date, end_date]
            for col in df.columns:
                val = row[col]
                # Convert NaN to None
                if pd.isna(val):
                    row_data.append(None)
                else:
                    row_data.append(val)
            data_rows.append(tuple(row_data))
        
        # Bulk insert
        if data_rows:
            from psycopg2.extras import execute_values
            insert_query = f"""
                INSERT INTO land.{table_name} ({', '.join(data_columns)})
                VALUES %s
            """
            execute_values(cursor, insert_query, data_rows)
            conn.commit()
            print(f"  ✓ Loaded {len(data_rows)} rows")
        else:
            print(f"  ⚠ No data to load")
            
    except Exception as e:
        conn.rollback()
        print(f"  ✗ Error: {e}")
        raise
    finally:
        cursor.close()

def main():
    """Main execution function."""
    print("="*70)
    print("Starting database table creation and data ingestion")
    print("="*70)
    
    # Test database connection
    print("\nTesting database connection...")
    if not db_utils.test_connection():
        print("✗ Cannot connect to database. Please check:")
        print("  1. PostgreSQL is running (cd postgresDB && docker compose up -d)")
        print("  2. Database credentials are correct")
        return
    
    print("✓ Database connection successful")
    
    # Read metadata
    print(f"\nReading metadata from {METADATA_FILE}...")
    metadata_df = pd.read_csv(METADATA_FILE)
    print(f"✓ Found {len(metadata_df)} files to process")
    
    # Connect to database
    conn = db_utils.get_connection('db_load')
    
    try:
        # Process each file
        success_count = 0
        error_count = 0
        
        for idx, row in metadata_df.iterrows():
            try:
                # Extract metadata
                data_type = row['type']
                filename = row['filename']
                header_row = int(row['header_row'])
                header_names = row['header_names']
                start_period = row['start_period']
                end_period = row['end_period']
                
                # Parse expected headers
                expected_headers = parse_header_names(header_names)
                
                # Create table name
                table_name = create_table_name(data_type, start_period, end_period)
                
                # File path
                file_path = RAW_DATA_DIR / filename
                
                if not file_path.exists():
                    print(f"\n✗ File not found: {filename}")
                    error_count += 1
                    continue
                
                # Create and load table
                create_and_load_table(
                    conn=conn,
                    file_path=file_path,
                    table_name=table_name,
                    header_row=header_row,
                    expected_headers=expected_headers,
                    start_period=start_period,
                    end_period=end_period
                )
                
                success_count += 1
                
            except Exception as e:
                print(f"\n✗ Failed to process {row['filename']}: {e}")
                error_count += 1
                continue
        
        print("\n" + "="*70)
        print("Summary")
        print("="*70)
        print(f"Total files: {len(metadata_df)}")
        print(f"Successfully processed: {success_count}")
        print(f"Errors: {error_count}")
        print("="*70)
        
    finally:
        conn.close()

if __name__ == "__main__":
    main()
