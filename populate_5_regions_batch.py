#!/usr/bin/env python
"""
Script to populate regions in small batches with detailed logging
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
    timestamp = "BATCH"
    print(f"{timestamp} - {message}")

def get_db_connection():
    """Get a connection to the database with longer timeout"""
    try:
        log(f"Connecting to database {DB_NAME} at {DB_HOST}...")
        connection = psycopg2.connect(
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            connect_timeout=30
        )
        log("Successfully connected to the database")
        return connection
    except Exception as e:
        log(f"Error connecting to database: {e}")
        traceback.print_exc()
        sys.exit(1)

def populate_regions_batch(connection, batch_size=5, start_offset=0):
    """Populate regions table in small batches for better tracking"""
    try:
        batch_count = 0
        total_inserted = 0
        
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            # First get the total count
            cursor.execute("""
            SELECT COUNT(DISTINCT geo_name) 
            FROM data_lake.master_card 
            WHERE 
                geo_name IS NOT NULL
                AND geo_type IS NOT NULL
                AND bounding_box IS NOT NULL
                AND central_latitude IS NOT NULL
                AND central_longitude IS NOT NULL
            """)
            total_count = cursor.fetchone()['count']
            log(f"Found {total_count} distinct regions in master_card table")
            
            # Process in batches
            while True:
                log(f"Starting batch {batch_count + 1} (offset {start_offset})")
                
                # Get a batch of distinct regions
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
                ORDER BY
                    geo_type, geo_name
                LIMIT %s OFFSET %s
                """, (batch_size, start_offset))
                
                regions_batch = cursor.fetchall()
                if not regions_batch:
                    log("No more regions to process")
                    break
                
                log(f"Processing {len(regions_batch)} regions in batch {batch_count + 1}")
                
                # Insert regions from this batch
                batch_inserted = 0
                for region in regions_batch:
                    # Debug output
                    log(f"Processing region: {region['geo_name']} ({region['geo_type']})")
                    log(f"  Coordinates: {region['central_latitude']}, {region['central_longitude']}")
                    log(f"  Bounding box: {region['bounding_box'][:50]}...")
                    
                    region_id = f"{region['geo_type']}_{region['geo_name'].lower().replace(' ', '_')}"
                    
                    # Get visitor counts for this region
                    try:
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
                        log(f"  Visitor data fetched: {visitor_data}")
                        
                        # Insert the region with proper geometry conversions
                        try:
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
                                region['bounding_box'],  # WKT format
                                region['central_longitude'],  # Point coordinates are (longitude, latitude)
                                region['central_latitude'],
                                visitor_data['total_visitors'] if visitor_data else 0,
                                visitor_data['swiss_tourists'] if visitor_data else 0,
                                visitor_data['foreign_tourists'] if visitor_data else 0
                            ))
                            connection.commit()
                            log(f"  Successfully inserted/updated region {region_id}")
                            batch_inserted += 1
                            total_inserted += 1
                        except Exception as e:
                            connection.rollback()
                            log(f"  Error inserting region {region_id}: {e}")
                            log(f"  Data: {region}")
                            traceback.print_exc()
                    except Exception as e:
                        log(f"  Error getting visitor data for {region_id}: {e}")
                        traceback.print_exc()
                
                log(f"Batch {batch_count + 1} complete. Inserted {batch_inserted} regions.")
                
                # Move to next batch
                start_offset += batch_size
                batch_count += 1
                
                # Verify progress
                cursor.execute("SELECT COUNT(*) as count FROM data_lake.regions")
                current_count = cursor.fetchone()['count']
                log(f"Current total in regions table: {current_count}")
                
                if batch_count >= 3:  # Just do 3 batches for testing
                    log("Reached batch limit for testing. Breaking.")
                    break
                
            # Final stats
            log(f"Batch processing complete. Inserted/updated {total_inserted} regions in {batch_count} batches.")
            
            # Create indexes if needed
            log("Creating indexes on regions table...")
            cursor.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_indexes 
                    WHERE indexname = 'idx_regions_name'
                ) THEN
                    CREATE INDEX idx_regions_name ON data_lake.regions (name);
                END IF;
                
                IF NOT EXISTS (
                    SELECT 1 FROM pg_indexes 
                    WHERE indexname = 'idx_regions_type'
                ) THEN
                    CREATE INDEX idx_regions_type ON data_lake.regions (type);
                END IF;
            END
            $$;
            """)
            connection.commit()
            log("Created indexes on regions table")
            
    except Exception as e:
        log(f"Error populating regions table: {e}")
        traceback.print_exc()
        connection.rollback()
        raise

def verify_regions_data(connection):
    """Verify that regions data was properly inserted"""
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SELECT COUNT(*) as count FROM data_lake.regions")
            result = cursor.fetchone()
            if result and result['count'] > 0:
                log(f"Successfully verified regions data. Found {result['count']} regions.")
                
                # Show some sample regions
                cursor.execute("""
                SELECT id, name, type, 
                       ST_AsText(centroid) as centroid_wkt, 
                       total_visitors 
                FROM data_lake.regions 
                ORDER BY total_visitors DESC 
                LIMIT 5
                """)
                
                samples = cursor.fetchall()
                log("Sample regions (highest visitor counts):")
                for i, sample in enumerate(samples, 1):
                    log(f"{i}. {sample['name']} ({sample['type']}): {sample['total_visitors']} visitors")
                    log(f"   Centroid: {sample['centroid_wkt']}")
                
            else:
                log("No regions found in the database after batch processing.")
    except Exception as e:
        log(f"Error verifying regions data: {e}")
        raise

def main():
    """Main function"""
    log("Starting batch population of regions table from master_card data")
    
    # Connect to the database
    connection = get_db_connection()
    
    try:
        # Ensure the regions table is ready
        with connection.cursor() as cursor:
            # Make sure the regions table exists
            log("Checking if regions table exists...")
            cursor.execute("""
            SELECT EXISTS (
               SELECT 1 FROM information_schema.tables 
               WHERE table_schema = 'data_lake'
               AND table_name = 'regions'
            );
            """)
            exists = cursor.fetchone()[0]
            
            if not exists:
                log("Regions table does not exist. Creating it first...")
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
                log("Created regions table with spatial data types")
            else:
                log("Regions table already exists")
        
        # Populate regions in batches
        populate_regions_batch(connection, batch_size=5, start_offset=0)
        
        # Verify the data
        verify_regions_data(connection)
        
    except Exception as e:
        log(f"Error during batch processing: {e}")
        traceback.print_exc()
    finally:
        # Close the connection
        connection.close()
        log("Database connection closed")
    
    log("Batch processing complete")

if __name__ == "__main__":
    main()