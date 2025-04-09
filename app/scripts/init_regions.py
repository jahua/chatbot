"""
Script to initialize region data in the database
This script creates the regions table if it doesn't exist
and loads the region data from geojson files.
"""

import os
import json
import sys
import logging
from pathlib import Path
import psycopg2
from psycopg2.extras import RealDictCursor

# Setup logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
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

# Path to the Tourism regions directory
regions_dir = os.path.join(parent_dir, "note", "Tourism regions")
regions_json_path = os.path.join(regions_dir, "regions.json")
geojson_paths = [
    os.path.join(regions_dir, "ticinomap.geojson"),
    os.path.join(regions_dir, "Bellinzona e Alto Ticino.geojson")
]

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

def create_regions_table(connection):
    """Create the regions table if it doesn't exist"""
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS data_lake;
            
            CREATE TABLE IF NOT EXISTS data_lake.regions (
                id VARCHAR(50) PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                description TEXT,
                visitor_count INTEGER,
                geometry JSON NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            -- Create spatial index if PostGIS extension is available
            DO $$
            BEGIN
                IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'postgis') THEN
                    -- Add geometry column if not exists
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_schema = 'data_lake' 
                        AND table_name = 'regions' 
                        AND column_name = 'geom'
                    ) THEN
                        EXECUTE 'ALTER TABLE data_lake.regions ADD COLUMN geom geometry(GEOMETRY, 4326)';
                        EXECUTE 'CREATE INDEX idx_regions_geom ON data_lake.regions USING GIST(geom)';
                    END IF;
                END IF;
            END
            $$;
            """)
            connection.commit()
            logger.info("Created regions table if it didn't exist")
    except Exception as e:
        logger.error(f"Error creating regions table: {e}")
        connection.rollback()
        raise

def load_regions_from_json(connection):
    """Load regions from the regions.json file"""
    try:
        if not os.path.exists(regions_json_path):
            logger.warning(f"Regions JSON file not found: {regions_json_path}")
            return
            
        with open(regions_json_path, 'r') as f:
            regions = json.load(f)
            
        with connection.cursor() as cursor:
            # First clear existing data
            cursor.execute("DELETE FROM data_lake.regions")
            
            # Insert new data
            for region in regions:
                # Convert geometry to string if it's a dictionary
                geometry_json = json.dumps(region["geometry"]) if isinstance(region["geometry"], dict) else region["geometry"]
                
                cursor.execute("""
                INSERT INTO data_lake.regions (id, name, description, visitor_count, geometry)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    description = EXCLUDED.description,
                    visitor_count = EXCLUDED.visitor_count,
                    geometry = EXCLUDED.geometry,
                    updated_at = CURRENT_TIMESTAMP
                """, (
                    region["id"],
                    region["name"],
                    region.get("description", ""),
                    region.get("visitors", 0),
                    geometry_json
                ))
                
                # If PostGIS is available, also update the geometry column
                cursor.execute("""
                DO $$
                BEGIN
                    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'postgis') THEN
                        UPDATE data_lake.regions
                        SET geom = ST_GeomFromGeoJSON(%s)
                        WHERE id = %s;
                    END IF;
                END
                $$;
                """, (geometry_json, region["id"]))
                
            connection.commit()
            logger.info(f"Loaded {len(regions)} regions from JSON")
    except Exception as e:
        logger.error(f"Error loading regions from JSON: {e}")
        connection.rollback()
        raise

def load_geojson_files(connection):
    """Load regions from GeoJSON files"""
    try:
        for geojson_path in geojson_paths:
            if not os.path.exists(geojson_path):
                logger.warning(f"GeoJSON file not found: {geojson_path}")
                continue
                
            with open(geojson_path, 'r') as f:
                geojson = json.load(f)
                
            if "features" not in geojson:
                logger.warning(f"Invalid GeoJSON format in {geojson_path}")
                continue
                
            with connection.cursor() as cursor:
                for feature in geojson["features"]:
                    if "properties" not in feature or "geometry" not in feature:
                        continue
                        
                    properties = feature["properties"]
                    geometry = feature["geometry"]
                    
                    # Generate an ID if none exists
                    region_id = properties.get("id", f"region_{properties.get('name', 'unknown')}".lower().replace(' ', '_'))
                    region_name = properties.get("name", "Unknown Region")
                    region_desc = properties.get("description", "")
                    
                    # Convert geometry to string
                    geometry_json = json.dumps(geometry)
                    
                    cursor.execute("""
                    INSERT INTO data_lake.regions (id, name, description, geometry)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        name = EXCLUDED.name,
                        description = EXCLUDED.description,
                        geometry = EXCLUDED.geometry,
                        updated_at = CURRENT_TIMESTAMP
                    """, (
                        region_id,
                        region_name,
                        region_desc,
                        geometry_json
                    ))
                    
                    # If PostGIS is available, also update the geometry column
                    cursor.execute("""
                    DO $$
                    BEGIN
                        IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'postgis') THEN
                            UPDATE data_lake.regions
                            SET geom = ST_GeomFromGeoJSON(%s)
                            WHERE id = %s;
                        END IF;
                    END
                    $$;
                    """, (geometry_json, region_id))
                    
                connection.commit()
                logger.info(f"Loaded regions from {geojson_path}")
    except Exception as e:
        logger.error(f"Error loading regions from GeoJSON: {e}")
        connection.rollback()
        raise

def main():
    """Main function to initialize region data"""
    try:
        logger.info("Starting region data initialization")
        
        # Connect to the database
        connection = get_db_connection()
        
        # Create the regions table if it doesn't exist
        create_regions_table(connection)
        
        # Load regions from JSON and GeoJSON files
        load_regions_from_json(connection)
        load_geojson_files(connection)
        
        # Check if any regions exist
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SELECT COUNT(*) as count FROM data_lake.regions")
            result = cursor.fetchone()
            if result and result['count'] > 0:
                logger.info(f"Successfully initialized region data. Found {result['count']} regions.")
            else:
                logger.warning("No regions found in the database after initialization.")
        
        # Close the connection
        connection.close()
        
        logger.info("Region data initialization complete")
        
    except Exception as e:
        logger.error(f"Error initializing region data: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 