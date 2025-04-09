#!/usr/bin/env python
"""
Script to check if the regions table exists and has the right structure
"""

import os
import sys
import psycopg2
from psycopg2.extras import RealDictCursor
import logging

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
        logger.info(f"Connecting to database {DB_NAME} at {DB_HOST}")
        connection = psycopg2.connect(
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME
        )
        logger.info("Successfully connected to database")
        return connection
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        raise

def check_postgis_extension(connection):
    """Check if PostGIS extension is installed"""
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT PostGIS_version();")
            version = cursor.fetchone()[0]
            logger.info(f"PostGIS is installed, version: {version}")
            return True
    except Exception as e:
        logger.error(f"PostGIS not installed or error checking: {e}")
        return False

def check_regions_table(connection):
    """Check if regions table exists and its structure"""
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            # Check if the table exists
            cursor.execute("""
            SELECT EXISTS (
               SELECT FROM information_schema.tables 
               WHERE table_schema = 'data_lake'
               AND table_name = 'regions'
            );
            """)
            
            exists = cursor.fetchone()['exists']
            
            if not exists:
                logger.error("The data_lake.regions table does not exist")
                logger.info("Creating the regions table...")
                create_regions_table(connection)
                return False
            
            logger.info("The data_lake.regions table exists")
            
            # Check table structure
            cursor.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'data_lake'
            AND table_name = 'regions'
            ORDER BY ordinal_position;
            """)
            
            columns = cursor.fetchall()
            
            logger.info("Table structure:")
            for col in columns:
                logger.info(f"  - {col['column_name']}: {col['data_type']} (nullable: {col['is_nullable']})")
            
            # Check if spatial columns use PostGIS
            try:
                cursor.execute("""
                SELECT f_geometry_column, type, srid, coord_dimension
                FROM geometry_columns
                WHERE f_table_schema = 'data_lake'
                AND f_table_name = 'regions';
                """)
                
                spatial_columns = cursor.fetchall()
                
                if spatial_columns:
                    logger.info("Spatial columns:")
                    for col in spatial_columns:
                        logger.info(f"  - {col['f_geometry_column']}: {col['type']} (SRID: {col['srid']}, dimensions: {col['coord_dimension']})")
                else:
                    logger.warning("No spatial columns found in geometry_columns table.")
            except Exception as e:
                logger.error(f"Error checking spatial columns: {e}")
            
            return True
            
    except Exception as e:
        logger.error(f"Error checking regions table: {e}")
        return False

def create_regions_table(connection):
    """Create the regions table"""
    try:
        with connection.cursor() as cursor:
            # First ensure the schema exists
            cursor.execute("CREATE SCHEMA IF NOT EXISTS data_lake;")
            
            # Create the regions table
            cursor.execute("""
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
            logger.info("Successfully created regions table")
            return True
    except Exception as e:
        logger.error(f"Error creating regions table: {e}")
        connection.rollback()
        return False

def main():
    """Main function"""
    try:
        connection = get_db_connection()
        
        # Check PostGIS extension
        postgis_installed = check_postgis_extension(connection)
        if not postgis_installed:
            logger.info("Installing PostGIS extension...")
            with connection.cursor() as cursor:
                cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
                connection.commit()
                logger.info("PostGIS extension installed")
        
        # Check regions table
        regions_table_ok = check_regions_table(connection)
        
        # Close connection
        connection.close()
        
        logger.info("Checks completed")
        
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 