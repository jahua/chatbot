#!/usr/bin/env python
"""
Script to insert a test region into the regions table
"""

import os
import sys
import logging
import psycopg2
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database connection info
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
        logger.error(f"Error connecting to database: {e}")
        raise

def insert_test_regions(connection):
    """Insert test regions into the regions table"""
    try:
        with connection.cursor() as cursor:
            # Test region 1: Ticino
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
            
            # Test region 2: Lugano
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
            
            # Test region 3: Locarno
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
            
            connection.commit()
            logger.info("Successfully inserted 3 test regions")
            
    except Exception as e:
        logger.error(f"Error inserting test regions: {e}")
        connection.rollback()
        raise

def verify_regions_data(connection):
    """Verify that regions data was properly inserted"""
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM data_lake.regions")
            count = cursor.fetchone()[0]
            logger.info(f"Number of regions in the database: {count}")
            
            if count > 0:
                cursor.execute("""
                SELECT id, name, type, ST_AsText(centroid) as centroid, total_visitors 
                FROM data_lake.regions
                """)
                
                regions = cursor.fetchall()
                logger.info("Regions in the database:")
                for region in regions:
                    logger.info(f"  - {region[0]}: {region[1]} ({region[2]}), Visitors: {region[4]}")
                    
    except Exception as e:
        logger.error(f"Error verifying regions data: {e}")
        raise

def main():
    """Main function to insert test regions"""
    try:
        logger.info("Starting insertion of test regions")
        
        # Connect to the database
        connection = get_db_connection()
        
        # Insert test regions
        insert_test_regions(connection)
        
        # Verify the data
        verify_regions_data(connection)
        
        connection.close()
        
        logger.info("Test regions insertion complete")
        
    except Exception as e:
        logger.error(f"Error inserting test regions: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 