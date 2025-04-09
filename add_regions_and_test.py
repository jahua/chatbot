#!/usr/bin/env python
"""
Script to add more regions and test application
"""

import os
import sys
import psycopg2
import subprocess
import traceback

# Database connection info
DB_USER = "postgres"
DB_PASSWORD = "336699"
DB_HOST = "3.76.40.121"
DB_PORT = "5432"
DB_NAME = "trip_dw"

print("Starting script to add more regions and test application")

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

# Insert additional test regions: Lugano and Locarno
try:
    print("Inserting test regions 'Lugano' and 'Locarno'...")
    with connection.cursor() as cursor:
        # Lugano
        cursor.execute("""
        INSERT INTO data_lake.regions (
            id, name, type, description, 
            geometry, centroid,
            total_visitors, swiss_tourists, foreign_tourists
        ) VALUES (
            'city_lugano', 'Lugano', 'City', 'Largest city in Ticino',
            ST_GeomFromText('POLYGON((8.92 46.0, 9.0 46.0, 9.0 46.1, 8.92 46.1, 8.92 46.0))', 4326),
            ST_SetSRID(ST_MakePoint(8.96, 46.05), 4326),
            120000, 70000, 50000
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
        
        # Locarno
        cursor.execute("""
        INSERT INTO data_lake.regions (
            id, name, type, description, 
            geometry, centroid,
            total_visitors, swiss_tourists, foreign_tourists
        ) VALUES (
            'city_locarno', 'Locarno', 'City', 'City on Lake Maggiore',
            ST_GeomFromText('POLYGON((8.75 46.15, 8.82 46.15, 8.82 46.22, 8.75 46.22, 8.75 46.15))', 4326),
            ST_SetSRID(ST_MakePoint(8.785, 46.185), 4326),
            80000, 50000, 30000
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
        
        # Bellinzona
        cursor.execute("""
        INSERT INTO data_lake.regions (
            id, name, type, description, 
            geometry, centroid,
            total_visitors, swiss_tourists, foreign_tourists
        ) VALUES (
            'city_bellinzona', 'Bellinzona', 'City', 'Capital of Ticino with historic castles',
            ST_GeomFromText('POLYGON((8.98 46.18, 9.05 46.18, 9.05 46.22, 8.98 46.22, 8.98 46.18))', 4326),
            ST_SetSRID(ST_MakePoint(9.015, 46.2), 4326),
            70000, 45000, 25000
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
        print("Successfully inserted or updated test regions")
except Exception as e:
    print(f"Error inserting test regions: {e}")
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

# Ask to run the application
print("\nWould you like to run the application to test the geospatial functionality? (y/n)")
response = input().strip().lower()
if response == 'y':
    print("Starting the application...")
    try:
        subprocess.run(["uvicorn", "app.main:app", "--reload", "--log-level", "debug"])
    except Exception as e:
        print(f"Error starting the application: {e}")
        traceback.print_exc()
else:
    print("Skipping application launch")

print("Test complete") 