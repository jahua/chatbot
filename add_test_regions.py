#!/usr/bin/env python
"""
Script to add test regions directly without relying on master_card data
"""

import os
import sys
import psycopg2
from psycopg2.extras import RealDictCursor
import traceback

# Database connection info
DB_USER = "postgres"
DB_PASSWORD = "336699"
DB_HOST = "3.76.40.121"
DB_PORT = "5432"
DB_NAME = "trip_dw"

# Set up simple console logging
def log(message):
    print(f"[INFO] {message}")

def get_db_connection():
    """Get a connection to the database"""
    try:
        log(f"Connecting to database {DB_NAME} at {DB_HOST}...")
        connection = psycopg2.connect(
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME
        )
        log("Successfully connected to the database")
        return connection
    except Exception as e:
        log(f"Error connecting to database: {e}")
        traceback.print_exc()
        sys.exit(1)

def ensure_regions_table(connection):
    """Ensure the regions table exists"""
    try:
        with connection.cursor() as cursor:
            # Check if table exists
            cursor.execute("""
            SELECT EXISTS (
               SELECT 1 FROM information_schema.tables 
               WHERE table_schema = 'data_lake'
               AND table_name = 'regions'
            );
            """)
            exists = cursor.fetchone()[0]
            
            if not exists:
                log("Creating regions table...")
                cursor.execute("""
                CREATE SCHEMA IF NOT EXISTS data_lake;
                
                CREATE TABLE data_lake.regions (
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
                
                -- Create spatial indexes for better query performance
                CREATE INDEX idx_regions_geometry ON data_lake.regions USING GIST(geometry);
                CREATE INDEX idx_regions_centroid ON data_lake.regions USING GIST(centroid);
                """)
                connection.commit()
                log("Created regions table successfully")
            else:
                log("Regions table already exists")
    except Exception as e:
        log(f"Error ensuring regions table: {e}")
        traceback.print_exc()
        connection.rollback()
        raise

def add_test_regions(connection):
    """Add test regions to the database"""
    try:
        with connection.cursor() as cursor:
            # Define test regions
            test_regions = [
                # Ticino Canton
                {
                    'id': 'canton_ticino',
                    'name': 'Ticino',
                    'type': 'Canton',
                    'description': 'Southern canton of Switzerland',
                    'polygon': 'POLYGON((8.7 46.0, 9.0 46.0, 9.0 46.5, 8.7 46.5, 8.7 46.0))',
                    'centroid': (8.85, 46.25),
                    'total_visitors': 250000,
                    'swiss_tourists': 150000,
                    'foreign_tourists': 100000
                },
                # Lugano
                {
                    'id': 'city_lugano',
                    'name': 'Lugano',
                    'type': 'City',
                    'description': 'Largest city in Ticino',
                    'polygon': 'POLYGON((8.92 46.0, 9.0 46.0, 9.0 46.1, 8.92 46.1, 8.92 46.0))',
                    'centroid': (8.96, 46.05),
                    'total_visitors': 120000,
                    'swiss_tourists': 70000,
                    'foreign_tourists': 50000
                },
                # Locarno
                {
                    'id': 'city_locarno',
                    'name': 'Locarno',
                    'type': 'City',
                    'description': 'City on Lake Maggiore',
                    'polygon': 'POLYGON((8.75 46.15, 8.82 46.15, 8.82 46.22, 8.75 46.22, 8.75 46.15))',
                    'centroid': (8.785, 46.185),
                    'total_visitors': 80000,
                    'swiss_tourists': 50000,
                    'foreign_tourists': 30000
                },
                # Bellinzona
                {
                    'id': 'city_bellinzona',
                    'name': 'Bellinzona',
                    'type': 'City',
                    'description': 'Capital of Ticino with historic castles',
                    'polygon': 'POLYGON((8.98 46.18, 9.05 46.18, 9.05 46.22, 8.98 46.22, 8.98 46.18))',
                    'centroid': (9.015, 46.2),
                    'total_visitors': 70000,
                    'swiss_tourists': 45000,
                    'foreign_tourists': 25000
                },
                # Ascona
                {
                    'id': 'city_ascona',
                    'name': 'Ascona',
                    'type': 'City',
                    'description': 'Resort town on Lake Maggiore',
                    'polygon': 'POLYGON((8.76 46.15, 8.78 46.15, 8.78 46.17, 8.76 46.17, 8.76 46.15))',
                    'centroid': (8.77, 46.16),
                    'total_visitors': 60000,
                    'swiss_tourists': 30000,
                    'foreign_tourists': 30000
                }
            ]
            
            # Insert each region
            for region in test_regions:
                log(f"Adding region: {region['name']}")
                cursor.execute("""
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
                    foreign_tourists = EXCLUDED.foreign_tourists,
                    updated_at = CURRENT_TIMESTAMP
                """, (
                    region['id'],
                    region['name'],
                    region['type'],
                    region['description'],
                    region['polygon'],
                    region['centroid'][0], region['centroid'][1],
                    region['total_visitors'],
                    region['swiss_tourists'],
                    region['foreign_tourists']
                ))
                connection.commit()
                log(f"Added region {region['name']} successfully")
            
            log(f"Added {len(test_regions)} test regions to the database")
    except Exception as e:
        log(f"Error adding test regions: {e}")
        traceback.print_exc()
        connection.rollback()
        raise

def verify_regions(connection):
    """Verify regions were added correctly"""
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            # Count regions
            cursor.execute("SELECT COUNT(*) as count FROM data_lake.regions")
            count = cursor.fetchone()['count']
            log(f"Number of regions in database: {count}")
            
            # Get sample regions
            cursor.execute("""
            SELECT 
                id, name, type, description, 
                ST_AsText(geometry) as geometry_wkt,
                ST_AsText(centroid) as centroid_wkt,
                total_visitors, swiss_tourists, foreign_tourists
            FROM data_lake.regions
            ORDER BY total_visitors DESC
            """)
            
            regions = cursor.fetchall()
            log("Regions in database:")
            for i, region in enumerate(regions, 1):
                log(f"{i}. {region['name']} ({region['type']})")
                log(f"   Description: {region['description']}")
                log(f"   Visitors: Total={region['total_visitors']}, Swiss={region['swiss_tourists']}, Foreign={region['foreign_tourists']}")
                log(f"   Centroid: {region['centroid_wkt']}")
                log("   -----------------")
    except Exception as e:
        log(f"Error verifying regions: {e}")
        traceback.print_exc()
        raise

def main():
    """Main function"""
    log("Starting script to add test regions to the database")
    
    # Connect to the database
    connection = get_db_connection()
    
    try:
        # Ensure regions table exists
        ensure_regions_table(connection)
        
        # Add test regions
        add_test_regions(connection)
        
        # Verify regions
        verify_regions(connection)
        
    except Exception as e:
        log(f"Error: {e}")
        traceback.print_exc()
    finally:
        # Close the connection
        connection.close()
        log("Database connection closed")
    
    log("Script completed successfully")

if __name__ == "__main__":
    main() 