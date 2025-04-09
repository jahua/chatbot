#!/usr/bin/env python
"""
Script to create and populate the regions table using data from the master_card table
with proper PostGIS spatial data types - FIXED VERSION with better error handling
"""

import os
import sys
import logging
import json
import psycopg2
import traceback
import re
from psycopg2.extras import RealDictCursor
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Add the parent directory to the path so we can import the app modules
script_dir = Path(os.path.dirname(os.path.abspath(__file__)))
parent_dir = script_dir.parent.parent
sys.path.append(str(parent_dir))

# Database connection info
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "336699")
DB_HOST = os.getenv("DB_HOST", "3.76.40.121")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "trip_dw")

def get_db_connection():
    """Get a connection to the database"""
    try:
        logger.info(f"Connecting to database {DB_NAME} at {DB_HOST}...")
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

def ensure_postgis_extension(connection):
    """Ensure the PostGIS extension is installed"""
    try:
        with connection.cursor() as cursor:
            cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
            connection.commit()
            logger.info("Ensured PostGIS extension is installed")
            
            # Verify PostGIS is working
            cursor.execute("SELECT PostGIS_version();")
            version = cursor.fetchone()[0]
            logger.info(f"PostGIS version: {version}")
    except Exception as e:
        logger.error(f"Error ensuring PostGIS extension: {e}")
        connection.rollback()
        raise

def create_regions_table(connection):
    """Create the regions table if it doesn't exist, using proper PostGIS geometry types"""
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS data_lake;
            
            DROP TABLE IF EXISTS data_lake.regions;
            
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
            logger.info("Created regions table with spatial data types")
    except Exception as e:
        logger.error(f"Error creating regions table: {e}")
        connection.rollback()
        raise

def check_wkt_format(wkt_string):
    """Check if string is a valid WKT polygon"""
    if not wkt_string:
        return False
    
    # Basic WKT polygon pattern
    pattern = r'POLYGON\s*\(\s*\(\s*[-+]?([0-9]*\.[0-9]+|[0-9]+)\s+[-+]?([0-9]*\.[0-9]+|[0-9]+)(,\s*[-+]?([0-9]*\.[0-9]+|[0-9]+)\s+[-+]?([0-9]*\.[0-9]+|[0-9]+))*\s*\)\s*\)'
    
    return bool(re.match(pattern, wkt_string, re.IGNORECASE))

def fix_wkt_format(wkt_string, lat, lon):
    """Attempt to fix WKT string format or create a new one from coordinates"""
    if not wkt_string or not lat or not lon:
        return None
    
    # Check if it's already a properly formatted WKT string
    if check_wkt_format(wkt_string):
        return wkt_string
    
    try:
        # Try to extract coordinates from the broken WKT string
        coord_pattern = r'[-+]?(?:\d*\.\d+|\d+)'
        coords = re.findall(coord_pattern, wkt_string)
        
        if len(coords) >= 8:  # Need at least 4 points for a polygon (x,y pairs)
            # Try to reconstruct a proper polygon
            pts = []
            for i in range(0, len(coords), 2):
                if i+1 < len(coords):
                    pts.append(f"{coords[i]} {coords[i+1]}")
            
            # Make sure the polygon is closed
            if pts[0] != pts[-1]:
                pts.append(pts[0])
                
            return f"POLYGON(({', '.join(pts)}))"
    except Exception as e:
        logger.warning(f"Failed to fix WKT string: {e}")
    
    # If all else fails, create a simple square around the centroid
    offset = 0.01  # ~1km at the equator
    return f"POLYGON(({lon-offset} {lat-offset}, {lon+offset} {lat-offset}, {lon+offset} {lat+offset}, {lon-offset} {lat+offset}, {lon-offset} {lat-offset}))"

def inspect_master_card_data(connection):
    """Inspect the master_card table structure and data"""
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            # Check table structure
            cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = 'data_lake' 
            AND table_name = 'master_card' 
            ORDER BY ordinal_position
            """)
            
            columns = cursor.fetchall()
            logger.info("master_card table structure:")
            for col in columns:
                logger.info(f"  - {col['column_name']}: {col['data_type']}")
            
            # Get a sample of the data
            cursor.execute("""
            SELECT 
                geo_type, geo_name, bounding_box, 
                central_latitude, central_longitude
            FROM data_lake.master_card
            WHERE 
                geo_name IS NOT NULL
                AND geo_type IS NOT NULL
                AND bounding_box IS NOT NULL
                AND central_latitude IS NOT NULL
                AND central_longitude IS NOT NULL
            LIMIT 3
            """)
            
            samples = cursor.fetchall()
            logger.info("Sample data from master_card:")
            for i, sample in enumerate(samples, 1):
                logger.info(f"Sample {i}:")
                logger.info(f"  - geo_type: {sample['geo_type']}")
                logger.info(f"  - geo_name: {sample['geo_name']}")
                logger.info(f"  - central_coordinates: ({sample['central_latitude']}, {sample['central_longitude']})")
                logger.info(f"  - bounding_box: {sample['bounding_box'][:100]}...")
                logger.info(f"  - WKT format valid: {check_wkt_format(sample['bounding_box'])}")
            
            return bool(samples)
    except Exception as e:
        logger.error(f"Error inspecting master_card data: {e}")
        logger.error(traceback.format_exc())
        return False

def populate_regions_from_master_card(connection):
    """Populate the regions table with data from master_card table using proper spatial data types"""
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            # Check if we have any qualifying data
            cursor.execute("""
            SELECT COUNT(*) as count
            FROM data_lake.master_card
            WHERE 
                geo_name IS NOT NULL
                AND geo_type IS NOT NULL
                AND bounding_box IS NOT NULL
                AND central_latitude IS NOT NULL
                AND central_longitude IS NOT NULL
            """)
            
            count = cursor.fetchone()['count']
            if count == 0:
                logger.warning("No qualifying data found in master_card table. Falling back to test regions.")
                populate_test_regions(connection)
                return
            
            logger.info(f"Found {count} rows in master_card with geographic data")
            
            # First query to get distinct regions from master_card
            cursor.execute("""
            SELECT DISTINCT
                geo_type,
                geo_name,
                bounding_box,
                central_latitude,
                central_longitude
            FROM 
                data_lake.master_card
            WHERE 
                geo_name IS NOT NULL
                AND geo_type IS NOT NULL
                AND bounding_box IS NOT NULL
                AND central_latitude IS NOT NULL
                AND central_longitude IS NOT NULL
            """)
            
            regions = cursor.fetchall()
            logger.info(f"Found {len(regions)} distinct regions in master_card table")
            
            # Insert regions into the regions table
            inserted_count = 0
            error_count = 0
            for region in regions:
                try:
                    region_id = f"{region['geo_type']}_{region['geo_name'].lower().replace(' ', '_')}"
                    
                    # Debug output for this region
                    logger.info(f"Processing region: {region['geo_name']} ({region['geo_type']})")
                    logger.debug(f"  - bounding_box: {region['bounding_box'][:50]}...")
                    
                    # Check and fix WKT format
                    wkt = region['bounding_box']
                    if not check_wkt_format(wkt):
                        logger.warning(f"Invalid WKT format for {region['geo_name']}: {wkt[:50]}...")
                        wkt = fix_wkt_format(
                            wkt, 
                            region['central_latitude'],
                            region['central_longitude']
                        )
                        if not wkt:
                            logger.error(f"Cannot fix WKT format for {region['geo_name']}, skipping")
                            error_count += 1
                            continue
                        logger.info(f"Fixed WKT format: {wkt}")
                    
                    # Get visitor counts for this region
                    cursor.execute("""
                    SELECT 
                        SUM(CASE WHEN segment = 'Domestic' THEN txn_cnt ELSE 0 END) as swiss_tourists,
                        SUM(CASE WHEN segment = 'International' THEN txn_cnt ELSE 0 END) as foreign_tourists,
                        SUM(txn_cnt) as total_visitors
                    FROM 
                        data_lake.master_card
                    WHERE 
                        geo_type = %s
                        AND geo_name = %s
                    """, (region['geo_type'], region['geo_name']))
                    
                    visitor_data = cursor.fetchone()
                    logger.debug(f"  - visitor_data: {visitor_data}")
                    
                    # Insert the region with proper geometry conversions
                    cursor.execute("""
                    INSERT INTO data_lake.regions (
                        id, name, type, 
                        geometry, centroid,
                        total_visitors, swiss_tourists, foreign_tourists
                    ) VALUES (
                        %s, %s, %s, 
                        ST_GeomFromText(%s, 4326), 
                        ST_SetSRID(ST_MakePoint(%s, %s), 4326),
                        %s, %s, %s
                    )
                    ON CONFLICT (id) DO UPDATE SET
                        name = EXCLUDED.name,
                        type = EXCLUDED.type,
                        geometry = EXCLUDED.geometry,
                        centroid = EXCLUDED.centroid,
                        total_visitors = EXCLUDED.total_visitors,
                        swiss_tourists = EXCLUDED.swiss_tourists,
                        foreign_tourists = EXCLUDED.foreign_tourists,
                        updated_at = CURRENT_TIMESTAMP
                    """, (
                        region_id,
                        region['geo_name'],
                        region['geo_type'],
                        wkt,  # Fixed WKT format
                        region['central_longitude'],  # Point coordinates are (longitude, latitude)
                        region['central_latitude'],
                        visitor_data['total_visitors'] if visitor_data and visitor_data['total_visitors'] else 0,
                        visitor_data['swiss_tourists'] if visitor_data and visitor_data['swiss_tourists'] else 0,
                        visitor_data['foreign_tourists'] if visitor_data and visitor_data['foreign_tourists'] else 0
                    ))
                    inserted_count += 1
                    
                    # Commit every record to isolate errors
                    connection.commit()
                    logger.info(f"Inserted region {region_id} successfully")
                    
                    # Log progress occasionally
                    if inserted_count % 10 == 0:
                        logger.info(f"Inserted {inserted_count} regions so far ({error_count} errors)")
                        
                except Exception as e:
                    error_count += 1
                    logger.error(f"Error inserting region {region.get('geo_name', 'unknown')}: {e}")
                    logger.error(traceback.format_exc())
                    connection.rollback()
            
            # Final stats
            logger.info(f"Finished processing. Inserted {inserted_count} regions successfully, {error_count} failures")
            
            if inserted_count == 0:
                logger.warning("No regions were inserted from master_card. Falling back to test regions.")
                populate_test_regions(connection)
                return
            
            # Create an index on the region name for faster searches
            cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_regions_name ON data_lake.regions (name);
            CREATE INDEX IF NOT EXISTS idx_regions_type ON data_lake.regions (type);
            """)
            connection.commit()
            logger.info("Created indexes on regions table")
            
    except Exception as e:
        logger.error(f"Error populating regions table: {e}")
        logger.error(traceback.format_exc())
        connection.rollback()
        raise

def populate_test_regions(connection):
    """Populate the regions table with test data when master_card data is not available"""
    logger.info("Adding test regions to the database")
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
                logger.info(f"Adding test region: {region['name']}")
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
                logger.info(f"Added test region {region['name']} successfully")
            
            logger.info(f"Added {len(test_regions)} test regions to the database")
            
    except Exception as e:
        logger.error(f"Error adding test regions: {e}")
        logger.error(traceback.format_exc())
        connection.rollback()
        raise

def verify_regions_data(connection):
    """Verify that regions data was properly inserted"""
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SELECT COUNT(*) as count FROM data_lake.regions")
            result = cursor.fetchone()
            if result and result['count'] > 0:
                logger.info(f"Successfully verified regions data. Found {result['count']} regions.")
                
                # Show some sample regions
                cursor.execute("""
                SELECT id, name, type, 
                       ST_AsText(centroid) as centroid_wkt, 
                       ST_AsGeoJSON(geometry) as geojson,
                       total_visitors 
                FROM data_lake.regions 
                ORDER BY total_visitors DESC 
                LIMIT 5
                """)
                
                samples = cursor.fetchall()
                logger.info("Sample regions (highest visitor counts):")
                for i, sample in enumerate(samples, 1):
                    logger.info(f"{i}. {sample['name']} ({sample['type']}): {sample['total_visitors']} visitors")
                    logger.info(f"   Centroid: {sample['centroid_wkt']}")
                
                return True
            else:
                logger.warning("No regions found in the database after initialization.")
                return False
    except Exception as e:
        logger.error(f"Error verifying regions data: {e}")
        logger.error(traceback.format_exc())
        return False

def main():
    """Main function to create and populate the regions table"""
    try:
        logger.info("Starting regions table initialization from master_card data")
        
        # Connect to the database
        connection = get_db_connection()
        
        # Ensure PostGIS extension is installed
        ensure_postgis_extension(connection)
        
        # Inspect master_card data structure
        has_data = inspect_master_card_data(connection)
        if not has_data:
            logger.warning("Could not find valid data in master_card table. Will use test data.")
        
        # Create the regions table
        create_regions_table(connection)
        
        # Populate regions from master_card or fallback to test data
        if has_data:
            populate_regions_from_master_card(connection)
        else:
            populate_test_regions(connection)
        
        # Verify the data
        success = verify_regions_data(connection)
        
        # Close the connection
        connection.close()
        
        if success:
            logger.info("Regions table initialization complete")
        else:
            logger.error("Regions table initialization may have failed - no regions found")
            sys.exit(1)
        
    except Exception as e:
        logger.error(f"Error initializing regions table: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main() 