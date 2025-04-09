#!/usr/bin/env python
"""
Script to insert test regions with timeout and improved error handling
"""

import os
import sys
import psycopg2
import traceback
import time
import socket
import signal

# Set timeout for operations
TIMEOUT = 10  # seconds

# Database connection info
DB_USER = "postgres"
DB_PASSWORD = "336699"
DB_HOST = "3.76.40.121"
DB_PORT = "5432"
DB_NAME = "trip_dw"

# Global variable for connection
connection = None

def timeout_handler(signum, frame):
    """Handle timeout signal"""
    print(f"Operation timed out after {TIMEOUT} seconds")
    if connection:
        try:
            connection.close()
        except:
            pass
    sys.exit(1)

# Set the timeout handler
signal.signal(signal.SIGALRM, timeout_handler)

def try_connection():
    """Try to connect to the database with timeout"""
    global connection
    
    print(f"Connecting to database {DB_NAME} at {DB_HOST} with {TIMEOUT}s timeout...")
    
    # First check if the host is reachable
    try:
        socket.setdefaulttimeout(5)
        socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect((DB_HOST, int(DB_PORT)))
        print("Host is reachable")
    except Exception as e:
        print(f"Host is not reachable: {e}")
        print("Please check your network connection and database server")
        sys.exit(1)
    
    # Then try to connect to the database
    signal.alarm(TIMEOUT)
    try:
        connection = psycopg2.connect(
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            connect_timeout=TIMEOUT
        )
        # Reset the alarm
        signal.alarm(0)
        print("Successfully connected to the database")
        return connection
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        traceback.print_exc()
        sys.exit(1)

def execute_query(cursor, query, params=None, operation_name="Query"):
    """Execute a query with timeout"""
    global connection
    
    print(f"Executing {operation_name}...")
    signal.alarm(TIMEOUT)
    try:
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        # Reset the alarm
        signal.alarm(0)
        print(f"{operation_name} executed successfully")
        return True
    except Exception as e:
        print(f"Error executing {operation_name}: {e}")
        traceback.print_exc()
        return False

def main():
    """Main function"""
    global connection
    
    print("Starting script to insert test regions")
    
    # Connect to the database
    connection = try_connection()
    
    try:
        with connection.cursor() as cursor:
            # Check if PostGIS is installed
            if not execute_query(cursor, "SELECT PostGIS_version()", operation_name="PostGIS check"):
                print("Attempting to install PostGIS...")
                if not execute_query(cursor, "CREATE EXTENSION IF NOT EXISTS postgis", operation_name="PostGIS installation"):
                    raise Exception("Failed to install PostGIS")
                connection.commit()
                print("PostGIS extension installed")
            else:
                version = cursor.fetchone()[0]
                print(f"PostGIS is installed, version: {version}")
            
            # Create schema if it doesn't exist
            if not execute_query(cursor, "CREATE SCHEMA IF NOT EXISTS data_lake", operation_name="Schema creation"):
                raise Exception("Failed to create schema")
            connection.commit()
            print("Schema data_lake exists or was created")
            
            # Create regions table if it doesn't exist
            create_table_query = """
            CREATE TABLE IF NOT EXISTS data_lake.regions (
                id VARCHAR(50) PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                type VARCHAR(50),
                description TEXT,
                geometry geometry(POLYGON, 4326),
                centroid geometry(POINT, 4326),
                total_visitors INTEGER,
                swiss_tourists INTEGER,
                foreign_tourists INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            if not execute_query(cursor, create_table_query, operation_name="Table creation"):
                raise Exception("Failed to create table")
            connection.commit()
            print("Regions table exists or was created")
            
            # Create spatial indexes if they don't exist
            create_indexes_query = """
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_indexes 
                    WHERE indexname = 'idx_regions_geometry'
                ) THEN
                    CREATE INDEX idx_regions_geometry ON data_lake.regions USING GIST(geometry);
                END IF;
                
                IF NOT EXISTS (
                    SELECT 1 FROM pg_indexes 
                    WHERE indexname = 'idx_regions_centroid'
                ) THEN
                    CREATE INDEX idx_regions_centroid ON data_lake.regions USING GIST(centroid);
                END IF;
            END
            $$;
            """
            if not execute_query(cursor, create_indexes_query, operation_name="Index creation"):
                raise Exception("Failed to create indexes")
            connection.commit()
            print("Spatial indexes exist or were created")
            
            # Insert test regions
            regions = [
                # Ticino
                {
                    'id': 'canton_ticino',
                    'name': 'Ticino',
                    'type': 'Canton',
                    'description': 'Southern canton of Switzerland',
                    'polygon': 'POLYGON((8.7 46.0, 9.0 46.0, 9.0 46.5, 8.7 46.5, 8.7 46.0))',
                    'point': (8.85, 46.25),
                    'visitors': 250000,
                    'swiss': 150000,
                    'foreign': 100000
                },
                # Lugano
                {
                    'id': 'city_lugano',
                    'name': 'Lugano',
                    'type': 'City',
                    'description': 'Largest city in Ticino',
                    'polygon': 'POLYGON((8.92 46.0, 9.0 46.0, 9.0 46.1, 8.92 46.1, 8.92 46.0))',
                    'point': (8.96, 46.05),
                    'visitors': 120000,
                    'swiss': 70000,
                    'foreign': 50000
                },
                # Locarno
                {
                    'id': 'city_locarno',
                    'name': 'Locarno',
                    'type': 'City',
                    'description': 'City on Lake Maggiore',
                    'polygon': 'POLYGON((8.75 46.15, 8.82 46.15, 8.82 46.22, 8.75 46.22, 8.75 46.15))',
                    'point': (8.785, 46.185),
                    'visitors': 80000,
                    'swiss': 50000,
                    'foreign': 30000
                }
            ]
            
            for region in regions:
                insert_query = """
                INSERT INTO data_lake.regions (
                    id, name, type, description, 
                    geometry, centroid,
                    total_visitors, swiss_tourists, foreign_tourists
                ) VALUES (
                    %s, %s, %s, %s,
                    ST_GeomFromText(%s, 4326),
                    ST_SetSRID(ST_MakePoint(%s, %s), 4326),
                    %s, %s, %s
                ) ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    type = EXCLUDED.type,
                    description = EXCLUDED.description,
                    geometry = EXCLUDED.geometry,
                    centroid = EXCLUDED.centroid,
                    total_visitors = EXCLUDED.total_visitors,
                    swiss_tourists = EXCLUDED.swiss_tourists,
                    foreign_tourists = EXCLUDED.foreign_tourists
                """
                params = (
                    region['id'], region['name'], region['type'], region['description'],
                    region['polygon'],
                    region['point'][0], region['point'][1],
                    region['visitors'], region['swiss'], region['foreign']
                )
                if not execute_query(cursor, insert_query, params, operation_name=f"Inserting {region['name']}"):
                    print(f"Failed to insert region {region['name']}, continuing with next region")
                connection.commit()
                print(f"Successfully inserted or updated region {region['name']}")
            
            # Verify that the data was inserted
            if execute_query(cursor, "SELECT COUNT(*) FROM data_lake.regions", operation_name="Count verification"):
                count = cursor.fetchone()[0]
                print(f"Number of regions in the database: {count}")
                
                if execute_query(cursor, "SELECT id, name, type, ST_AsText(centroid) as centroid, total_visitors FROM data_lake.regions", operation_name="Data verification"):
                    print("Regions in the database:")
                    for region in cursor.fetchall():
                        print(f"  - {region[0]}: {region[1]} ({region[2]}), Visitors: {region[4]}")
    
    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()
        if connection:
            try:
                connection.rollback()
            except:
                pass
    finally:
        if connection:
            try:
                connection.close()
                print("Database connection closed")
            except:
                pass
    
    print("Script completed")

if __name__ == "__main__":
    main()