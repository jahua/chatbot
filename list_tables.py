#!/usr/bin/env python
"""
Script to list all tables in the trip_dw database
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor

# Database connection info - using the same credentials from your logs
DB_USER = "postgres" 
DB_PASSWORD = "336699"
DB_HOST = "3.76.40.121"
DB_PORT = "5432"
DB_NAME = "trip_dw"

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
        print(f"Error connecting to database: {e}")
        raise

def list_tables():
    """List all tables in the database"""
    try:
        connection = get_db_connection()
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            # Query to get all tables from all schemas
            cursor.execute("""
            SELECT 
                table_schema, 
                table_name 
            FROM 
                information_schema.tables 
            WHERE 
                table_type = 'BASE TABLE' 
                AND table_schema NOT IN ('pg_catalog', 'information_schema')
            ORDER BY 
                table_schema, 
                table_name
            """)
            
            tables = cursor.fetchall()
            
            print(f"Found {len(tables)} tables in the database:")
            print("-" * 50)
            
            current_schema = None
            for table in tables:
                schema = table['table_schema']
                if schema != current_schema:
                    current_schema = schema
                    print(f"\nSchema: {schema}")
                    print("-" * 20)
                
                print(f"- {table['table_name']}")
            
            # Also list the columns of the regions table specifically
            if any(t['table_schema'] == 'data_lake' and t['table_name'] == 'regions' for t in tables):
                print("\n")
                print("=" * 50)
                print("Details for data_lake.regions table:")
                print("=" * 50)
                
                cursor.execute("""
                SELECT 
                    column_name, 
                    data_type,
                    is_nullable
                FROM 
                    information_schema.columns 
                WHERE 
                    table_schema = 'data_lake' 
                    AND table_name = 'regions'
                ORDER BY 
                    ordinal_position
                """)
                
                columns = cursor.fetchall()
                for col in columns:
                    nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
                    print(f"- {col['column_name']}: {col['data_type']} {nullable}")
            
        connection.close()
        
    except Exception as e:
        print(f"Error listing tables: {e}")
        raise

if __name__ == "__main__":
    list_tables() 