#!/usr/bin/env python
"""
Simple script to insert a single test region with detailed error reporting
"""

import os
import sys
import psycopg2
import traceback

# Database connection info
DB_USER = "postgres"
DB_PASSWORD = "336699"
DB_HOST = "3.76.40.121"
DB_PORT = "5432"
DB_NAME = "trip_dw"

print("Starting script to insert a single test region")

# Try to connect to the database
try:
    print(f"Connecting to database {DB_NAME} at {DB_HOST}...")
    connection = psycopg2.connect(
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME
    )
    print("Successfully connected to the database")
except Exception as e:
    print(f"Error connecting to the database: {e}")
    traceback.print_exc()
    sys.exit(1)

# Check if PostGIS is installed
try:
    print("Checking if PostGIS is installed...")
    with connection.cursor() as cursor:
        cursor.execute("SELECT PostGIS_version();")
        version = cursor.fetchone()[0]
        print(f"PostGIS is installed, version: {version}")
except Exception as e:
    print(f"PostGIS is not installed or error checking: {e}")
    print("Attempting to install PostGIS...")
    try:
        with connection.cursor() as cursor:
            cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
            connection.commit()
            print("PostGIS extension installed")
    except Exception as e:
        print(f"Error installing PostGIS: {e}")
        traceback.print_exc()
        connection.close()
        sys.exit(1)

# Create schema if it doesn't exist
try:
    print("Creating schema data_lake if it doesn't exist...")
    with connection.cursor() as cursor:
        cursor.execute("CREATE SCHEMA IF NOT EXISTS data_lake;")
        connection.commit()
        print("Schema data_lake exists or was created")
except Exception as e:
    print(f"Error creating schema: {e}")
    traceback.print_exc()
    connection.close()
    sys.exit(1)

# Create regions table if it doesn't exist
try:
    print("Creating regions table if it doesn't exist...")
    with connection.cursor() as cursor:
        cursor.execute("""
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
        );
        """)
        connection.commit()
        print("Regions table exists or was created")
        
        # Create spatial indexes if they don't exist
        cursor.execute("""
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
        """)
        connection.commit()
        print("Spatial indexes exist or were created")
except Exception as e:
    print(f"Error creating regions table: {e}")
    traceback.print_exc()
    connection.close()
    sys.exit(1)

# Insert a single test region: Ticino
try:
    print("Inserting test region 'Ticino'...")
    with connection.cursor() as cursor:
        cursor.execute("""
        INSERT INTO data_lake.regions (
            id, name, type, description, 
            geometry, centroid,
            total_visitors, swiss_tourists, foreign_tourists
        ) VALUES (
            'canton_ticino', 'Ticino', 'Canton', 'Southern canton of Switzerland',
            ST_GeomFromText('POLYGON((8.7 46.0, 9.0 46.0, 9.0 46.5, 8.7 46.5, 8.7 46.0))', 4326),
            ST_SetSRID(ST_MakePoint(8.85, 46.25), 4326),
            250000, 150000, 100000
        ) ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            type = EXCLUDED.type,
            description = EXCLUDED.description,
            geometry = EXCLUDED.geometry,
            centroid = EXCLUDED.centroid,
            total_visitors = EXCLUDED.total_visitors,
            swiss_tourists = EXCLUDED.swiss_tourists,
            foreign_tourists = EXCLUDED.foreign_tourists
        """)
        connection.commit()
        print("Successfully inserted or updated test region 'Ticino'")
except Exception as e:
    print(f"Error inserting test region: {e}")
    traceback.print_exc()
    connection.rollback()
    connection.close()
    sys.exit(1)

# Verify that the data was inserted
try:
    print("Verifying data was inserted...")
    with connection.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM data_lake.regions")
        count = cursor.fetchone()[0]
        print(f"Number of regions in the database: {count}")
        
        cursor.execute("""
        SELECT id, name, type, ST_AsText(centroid) as centroid, total_visitors 
        FROM data_lake.regions
        """)
        
        print("Regions in the database:")
        for region in cursor.fetchall():
            print(f"  - {region[0]}: {region[1]} ({region[2]}), Visitors: {region[4]}")
except Exception as e:
    print(f"Error verifying data: {e}")
    traceback.print_exc()
    connection.close()
    sys.exit(1)

# Close the connection
connection.close()
print("Script completed successfully") 