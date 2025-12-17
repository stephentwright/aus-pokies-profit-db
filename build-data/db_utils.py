"""
Database utilities for connecting to PostgreSQL database.
"""
import psycopg2
from psycopg2.extras import execute_values
import os

# Database connection parameters
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'aus_pokies'),
    'user': os.getenv('DB_USER', 'db_load'),
    'password': os.getenv('DB_PASSWORD', 'load_pass')
}

def get_connection(role='db_load'):
    """
    Get a database connection for the specified role.
    
    Args:
        role: One of 'db_load', 'db_user', 'db_owner', 'db_external'
    
    Returns:
        psycopg2 connection object
    """
    config = DB_CONFIG.copy()
    
    # Override user and password based on role
    role_passwords = {
        'db_load': 'load_pass',
        'db_user': 'user_pass',
        'db_owner': 'owner_pass',
        'db_external': 'external_pass'
    }
    
    config['user'] = role
    config['password'] = os.getenv(f'DB_{role.upper()}_PASSWORD', role_passwords.get(role, 'load_pass'))
    
    return psycopg2.connect(**config)

def execute_query(conn, query, params=None):
    """
    Execute a query and return results.
    
    Args:
        conn: Database connection
        query: SQL query string
        params: Optional query parameters
    
    Returns:
        Query results if SELECT, None otherwise
    """
    with conn.cursor() as cur:
        cur.execute(query, params)
        if cur.description:
            return cur.fetchall()
        conn.commit()
        return None

def bulk_insert(conn, table_name, columns, data):
    """
    Bulk insert data into a table.
    
    Args:
        conn: Database connection
        table_name: Name of table (schema.table)
        columns: List of column names
        data: List of tuples containing row data
    """
    with conn.cursor() as cur:
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s"
        execute_values(cur, query, data)
        conn.commit()

def test_connection():
    """Test database connection."""
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT version();")
            version = cur.fetchone()
            print(f"Connected to: {version[0]}")
        conn.close()
        return True
    except Exception as e:
        print(f"Connection failed: {e}")
        return False

if __name__ == "__main__":
    test_connection()
