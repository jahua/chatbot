#!/usr/bin/env python
"""
Script to check the database structure, particularly focusing on the master table
"""

import os
import sys
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from tabulate import tabulate

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database connection info
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "336699")
DB_HOST = os.getenv("DB_HOST", "3.76.40.121")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "trip_dw")

def get_db_connection():
    """Get a connection to the database"""
    try:
        connection = psycopg2.connect(
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME
        )
        return connection
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        raise

def check_tables(connection):
    """Check what tables exist in the database"""
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("""
            SELECT 
                table_schema,
                table_name
            FROM 
                information_schema.tables
            WHERE 
                table_schema NOT IN ('pg_catalog', 'information_schema')
            ORDER BY 
                table_schema, table_name
            """)
            
            tables = cursor.fetchall()
            
            print("\n=== Database Tables ===")
            table_data = [[table['table_schema'], table['table_name']] for table in tables]
            print(tabulate(table_data, headers=["Schema", "Table"], tablefmt="grid"))
            
            return tables
    except Exception as e:
        logger.error(f"Error checking tables: {e}")
        return []

def check_table_columns(connection, schema, table_name):
    """Check the columns of a specific table"""
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("""
            SELECT 
                column_name, 
                data_type, 
                is_nullable
            FROM 
                information_schema.columns
            WHERE 
                table_schema = %s AND
                table_name = %s
            ORDER BY 
                ordinal_position
            """, (schema, table_name))
            
            columns = cursor.fetchall()
            
            print(f"\n=== Columns for {schema}.{table_name} ===")
            column_data = [[col['column_name'], col['data_type'], col['is_nullable']] for col in columns]
            print(tabulate(column_data, headers=["Column", "Type", "Nullable"], tablefmt="grid"))
            
            return columns
    except Exception as e:
        logger.error(f"Error checking columns for {schema}.{table_name}: {e}")
        return []

def check_sample_data(connection, schema, table_name, limit=5):
    """Check sample data from a table"""
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(f"""
            SELECT * FROM {schema}.{table_name} LIMIT {limit}
            """)
            
            rows = cursor.fetchall()
            
            if rows:
                print(f"\n=== Sample Data for {schema}.{table_name} (first {limit} rows) ===")
                headers = rows[0].keys()
                data = [[row[col] for col in headers] for row in rows]
                print(tabulate(data, headers=headers, tablefmt="grid"))
            else:
                print(f"\nNo data found in {schema}.{table_name}")
            
            return rows
    except Exception as e:
        logger.error(f"Error checking sample data for {schema}.{table_name}: {e}")
        return []

def check_postgis_extension(connection):
    """Check if PostGIS extension is available"""
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("""
            SELECT extname, extversion 
            FROM pg_extension 
            WHERE extname = 'postgis'
            """)
            
            result = cursor.fetchone()
            
            if result:
                print(f"\n=== PostGIS Info ===")
                print(f"PostGIS version: {result['extversion']}")
                return True
            else:
                print("\nPostGIS extension is not installed")
                return False
    except Exception as e:
        logger.error(f"Error checking PostGIS extension: {e}")
        return False

def check_spatial_columns(connection, schema, table_name):
    """Check for spatial columns in the specified table"""
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            # First check if the table exists in geometry_columns view
            cursor.execute("""
            SELECT 
                f_table_schema,
                f_table_name,
                f_geometry_column,
                coord_dimension,
                srid,
                type
            FROM 
                geometry_columns
            WHERE 
                f_table_schema = %s AND
                f_table_name = %s
            """, (schema, table_name))
            
            columns = cursor.fetchall()
            
            if columns:
                print(f"\n=== Spatial Columns for {schema}.{table_name} ===")
                column_data = [
                    [col['f_geometry_column'], col['type'], col['srid'], col['coord_dimension']] 
                    for col in columns
                ]
                print(tabulate(column_data, 
                      headers=["Column", "Type", "SRID", "Dimensions"], 
                      tablefmt="grid"))
            else:
                print(f"\nNo spatial columns found in geometry_columns for {schema}.{table_name}")
                
                # Check if there are columns with geometry type
                cursor.execute("""
                SELECT 
                    column_name, 
                    data_type, 
                    udt_name
                FROM 
                    information_schema.columns
                WHERE 
                    table_schema = %s AND
                    table_name = %s AND
                    (data_type LIKE '%geometry%' OR udt_name LIKE '%geometry%')
                """, (schema, table_name))
                
                geom_columns = cursor.fetchall()
                
                if geom_columns:
                    print(f"However, found columns with geometry-like types:")
                    geom_data = [[col['column_name'], col['data_type'], col['udt_name']] for col in geom_columns]
                    print(tabulate(geom_data, headers=["Column", "Type", "UDT Name"], tablefmt="grid"))
                
            return columns
    except Exception as e:
        logger.error(f"Error checking spatial columns for {schema}.{table_name}: {e}")
        return []

def main():
    """Main function to check database structure"""
    try:
        # Connect to the database
        connection = get_db_connection()
        
        # Check if PostGIS is available
        postgis_available = check_postgis_extension(connection)
        
        # Check what tables exist
        tables = check_tables(connection)
        
        # Focus on master_card table
        master_schema = "dw"  # using dw schema for all operations
        master_table = "master_card"  # based on the user query
        
        # Check if master_card table exists
        master_exists = any(t['table_name'] == master_table and t['table_schema'] == master_schema for t in tables)
        
        if master_exists:
            # Check columns
            check_table_columns(connection, master_schema, master_table)
            
            # Check for spatial columns
            if postgis_available:
                check_spatial_columns(connection, master_schema, master_table)
            
            # Check sample data
            check_sample_data(connection, master_schema, master_table)
        else:
            print(f"\nThe table {master_schema}.{master_table} does not exist.")
            
            # Try to find tables with "master" in their name
            master_tables = [t for t in tables if "master" in t['table_name'].lower()]
            if master_tables:
                print("\nFound tables with 'master' in their name:")
                for table in master_tables:
                    print(f"- {table['table_schema']}.{table['table_name']}")
                    check_table_columns(connection, table['table_schema'], table['table_name'])
        
        # Close the connection
        connection.close()
        
    except Exception as e:
        logger.error(f"Error checking database structure: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 