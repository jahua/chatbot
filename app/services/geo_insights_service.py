from typing import Dict, List, Any, Optional
from app.db.database import DatabaseService
import logging
import json
import random
import traceback
import time # Added for cache TTL

logger = logging.getLogger(__name__)

# Added Cache Class (assuming it's defined elsewhere or here)
class GeoInsightsCache:
    """Simple cache for geospatial data"""
    def __init__(self, ttl=3600):
        self._cache = {}
        self._ttl = ttl
        self._timestamps = {}
        
    def get(self, key):
        if key not in self._cache:
            return None
        if time.time() - self._timestamps[key] > self._ttl:
            del self._cache[key]
            del self._timestamps[key]
            return None
        return self._cache[key]
        
    def set(self, key, value):
        self._cache[key] = value
        self._timestamps[key] = time.time()

class GeoInsightsService:
    def __init__(self, db_service: DatabaseService):
        self.db_service = db_service
        self.cache = GeoInsightsCache() # Initialize the cache
        logger.info("GeoInsightsService initialized successfully")
    
    def search_regions(self, query: str, region_type: str = None) -> List[Dict[str, Any]]:
        """Search for regions with optimized query."""
        try:
            # Map common region type names to database values
            if region_type:
                region_type_map = {
                    'city': 'Msa',
                    'canton': 'State',
                    'country': 'Country',
                }
                region_type = region_type_map.get(region_type.lower(), region_type)
                logger.info(f"Mapped region type '{region_type}' for query: {query}")
            
            # First check if the materialized view exists
            check_view_sql = """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'geo_insights' 
                    AND table_name = 'region_metrics'
                )
            """
            view_exists_result = self.db_service.execute_query(check_view_sql)
            view_exists = view_exists_result[0]['exists'] if view_exists_result else False
            
            if view_exists:
                logger.info("Using geo_insights.region_metrics materialized view")
                # Use the materialized view and join with region_centers for coordinate data
                sql = """
                    WITH region_match AS (
                        SELECT 
                            rm.geo_type,
                            rm.geo_name,
                            SUM(rm.total_visitors) as total_visitors,
                            SUM(rm.swiss_tourists) as swiss_tourists,
                            SUM(rm.foreign_tourists) as foreign_tourists,
                            SUM(rm.total_spend) as total_spend
                        FROM geo_insights.region_metrics rm
                        WHERE LOWER(rm.geo_name) = LOWER(:query)
                        AND (:region_type IS NULL OR LOWER(rm.geo_type) = LOWER(:region_type))
                        AND rm.year = 2023
                        GROUP BY rm.geo_type, rm.geo_name
                    )
                    SELECT 
                        r.geo_type,
                        r.geo_name,
                        r.total_visitors,
                        r.swiss_tourists,
                        r.foreign_tourists,
                        r.total_spend,
                        COALESCE(rc.central_latitude, 0) as central_latitude,
                        COALESCE(rc.central_longitude, 0) as central_longitude,
                        rc.bounding_box
                    FROM region_match r
                    LEFT JOIN geo_insights.region_centers rc 
                    ON LOWER(r.geo_name) = LOWER(rc.geo_name) AND LOWER(r.geo_type) = LOWER(rc.geo_type)
                    ORDER BY r.total_visitors DESC
                    LIMIT 10
                """
            else:
                logger.info("Materialized view not found, using direct query on data_lake.master_card")
                # Improved direct query: use more flexible matching and optimize for better performance
                sql = """
                    WITH region_data AS (
                        SELECT 
                            geo_type,
                            geo_name,
                            segment,
                            txn_cnt,
                            txn_amt,
                            central_latitude,
                            central_longitude,
                            bounding_box
                        FROM data_lake.master_card
                        WHERE LOWER(geo_name) = LOWER(:query)
                        AND central_latitude IS NOT NULL
                        AND central_longitude IS NOT NULL
                        LIMIT 5000
                    ),
                    aggregated_data AS (
                        SELECT 
                            geo_type,
                            geo_name,
                            SUM(CASE WHEN segment = 'Domestic' THEN txn_cnt ELSE 0 END) as swiss_tourists,
                            SUM(CASE WHEN segment = 'International' THEN txn_cnt ELSE 0 END) as foreign_tourists,
                            SUM(txn_cnt) as total_visitors,
                            SUM(txn_amt) as total_spend,
                            AVG(central_latitude) as central_latitude,
                            AVG(central_longitude) as central_longitude,
                            MAX(bounding_box) as bounding_box
                        FROM region_data
                        GROUP BY geo_type, geo_name
                    )
                    SELECT * FROM aggregated_data
                    ORDER BY total_visitors DESC
                    LIMIT 10
                """
            
            logger.info(f"Executing search query for '{query}' with type {region_type}")
            params = {"query": query, "region_type": region_type}
            results = self.db_service.execute_query(sql, params)
            logger.info(f"Query returned {len(results)} results")
            
            # If no results found using exact match, try fuzzy search
            if not results:
                logger.info(f"No exact matches found for '{query}', trying fuzzy search")
                fuzzy_sql = """
                    WITH region_data AS (
                        SELECT 
                            geo_type,
                            geo_name,
                            segment,
                            txn_cnt,
                            txn_amt,
                            central_latitude,
                            central_longitude,
                            bounding_box
                        FROM data_lake.master_card
                        WHERE LOWER(geo_name) LIKE LOWER(:fuzzy_query)
                        AND central_latitude IS NOT NULL
                        AND central_longitude IS NOT NULL
                        LIMIT 5000
                    ),
                    aggregated_data AS (
                        SELECT 
                            geo_type,
                            geo_name,
                            SUM(CASE WHEN segment = 'Domestic' THEN txn_cnt ELSE 0 END) as swiss_tourists,
                            SUM(CASE WHEN segment = 'International' THEN txn_cnt ELSE 0 END) as foreign_tourists,
                            SUM(txn_cnt) as total_visitors,
                            SUM(txn_amt) as total_spend,
                            AVG(central_latitude) as central_latitude,
                            AVG(central_longitude) as central_longitude,
                            MAX(bounding_box) as bounding_box
                        FROM region_data
                        GROUP BY geo_type, geo_name
                    )
                    SELECT * FROM aggregated_data
                    ORDER BY total_visitors DESC
                    LIMIT 10
                """
                fuzzy_params = {"fuzzy_query": f"%{query}%"}
                logger.info(f"Trying fuzzy search with pattern: %{query}%")
                results = self.db_service.execute_query(fuzzy_sql, fuzzy_params)
                logger.info(f"Fuzzy search returned {len(results)} results")
                
            # If still no results, try simplified search (without conditions)
            if not results:
                logger.info("No results from fuzzy search, trying to get top regions")
                simplified_sql = """
                    SELECT DISTINCT
                        geo_type,
                        geo_name,
                        'Msa' as region_type,
                        central_latitude,
                        central_longitude,
                        COUNT(*) as total_visitors,
                        0 as swiss_tourists,
                        0 as foreign_tourists,
                        0 as total_spend
                    FROM data_lake.master_card
                    WHERE central_latitude IS NOT NULL
                    AND central_longitude IS NOT NULL
                    GROUP BY geo_type, geo_name, central_latitude, central_longitude
                    ORDER BY total_visitors DESC
                    LIMIT 5
                """
                results = self.db_service.execute_query(simplified_sql)
                logger.info(f"Simplified search returned {len(results)} results")
            
            # Log sample results for debugging
            if results:
                sample = results[0]
                logger.info(f"Sample result: {sample['geo_name']} ({sample['geo_type']}) at ({sample.get('central_latitude', 'N/A')}, {sample.get('central_longitude', 'N/A')})")
            
            return results
        except Exception as e:
            logger.error(f"Error searching regions: {str(e)}")
            logger.error(traceback.format_exc())
            return []
    
    def get_region_insights(self, region_id: str) -> Dict[str, Any]:
        """Get detailed insights about a specific region from master_card"""
        cache_key = f"insights_direct:{region_id}" # Using direct key
        cached_result = self.cache.get(cache_key)
        if cached_result is not None:
            logger.info(f"Using cached insight data for region: {region_id}")
            return cached_result
        
        try:
            parts = region_id.split('_', 1)
            if len(parts) != 2: raise ValueError("Invalid region_id format")
            region_type = parts[0]
            region_name_parts = parts[1].split('_')
            region_name = ' '.join(part.capitalize() for part in region_name_parts)
            logger.info(f"Getting insights for Type: {region_type}, Name: {region_name}")
        except Exception as e:
             logger.error(f"Error parsing region_id '{region_id}': {e}"); return {}

        sql = """
        WITH region_data AS (
            SELECT 
                bounding_box,
                central_latitude,
                central_longitude,
                segment,
                txn_cnt,
                txn_amt,
                txn_date
            FROM 
                data_lake.master_card
            WHERE 
                geo_type = :region_type
                AND geo_name = :region_name
                AND txn_date >= '2023-01-01'::date
                AND txn_date <= '2023-01-31'::date
            LIMIT 10000
        )
        SELECT 
            MAX(bounding_box) as bounding_box,
            AVG(central_latitude)::FLOAT AS central_latitude,
            AVG(central_longitude)::FLOAT AS central_longitude,
            SUM(CASE WHEN segment = 'Domestic' THEN txn_cnt ELSE 0 END)::FLOAT AS swiss_tourists,
            SUM(CASE WHEN segment = 'International' THEN txn_cnt ELSE 0 END)::FLOAT AS foreign_tourists,
            SUM(CASE WHEN segment = 'Domestic' OR segment = 'International' THEN txn_cnt ELSE 0 END)::FLOAT AS total_visitors,
            SUM(txn_amt)::FLOAT AS total_spending,
            SUM(CASE WHEN segment = 'Domestic' THEN txn_amt ELSE 0 END)::FLOAT AS domestic_spending,
            SUM(CASE WHEN segment = 'International' THEN txn_amt ELSE 0 END)::FLOAT AS international_spending,
            AVG(CASE WHEN segment = 'Domestic' THEN txn_cnt ELSE NULL END)::FLOAT as avg_daily_swiss,
            AVG(CASE WHEN segment = 'International' THEN txn_cnt ELSE NULL END)::FLOAT as avg_daily_foreign,
            MAX(CASE WHEN segment = 'Overall' THEN txn_cnt ELSE 0 END)::FLOAT as peak_daily_visitors_overall,
            MIN(txn_date) as first_date,
            MAX(txn_date) as last_date,
            COUNT(DISTINCT txn_date)::INTEGER AS days_with_data
        FROM 
            region_data
        """
        
        try:
            results = self.db_service.execute_query(sql, {"region_type": region_type, "region_name": region_name})
            
            if not results or results[0]['total_visitors'] is None:
                logger.warning(f"No direct insights found for Type: {region_type}, Name: {region_name}")
                return {}

            result = results[0]
            geom_json = self._get_geojson_from_wkt(result.get('bounding_box'))
            
            processed_result = {
                'region_id': region_id,
                'region_name': region_name,
                'region_type': region_type,
                'geometry': geom_json,
                'central_latitude': float(result.get('central_latitude', 0) or 0),
                'central_longitude': float(result.get('central_longitude', 0) or 0),
                'swiss_tourists': float(result.get('swiss_tourists', 0) or 0),
                'foreign_tourists': float(result.get('foreign_tourists', 0) or 0),
                'total_visitors': float(result.get('total_visitors', 0) or 0),
                'total_spending': float(result.get('total_spending', 0) or 0),
                'domestic_spending': float(result.get('domestic_spending', 0) or 0),
                'international_spending': float(result.get('international_spending', 0) or 0),
                'avg_daily_swiss': float(result.get('avg_daily_swiss', 0) or 0),
                'avg_daily_foreign': float(result.get('avg_daily_foreign', 0) or 0),
                'peak_daily_visitors_overall': float(result.get('peak_daily_visitors_overall', 0) or 0),
                'days_with_data': int(result.get('days_with_data', 0) or 0),
                'first_date': result.get('first_date'),
                'last_date': result.get('last_date')
            }
            
            self.cache.set(cache_key, processed_result)
            return processed_result
            
        except Exception as e:
            logger.error(f"Error getting direct region insights for {region_id}: {str(e)}")
            logger.error(traceback.format_exc())
            return {}
            
    def get_spatial_patterns(self, region_id: str) -> Dict[str, Any]:
        """Analyze spatial patterns, fetching both stats and points from master_card."""
        cache_key = f"patterns_direct:{region_id}" # Using direct key
        cached_result = self.cache.get(cache_key)
        if cached_result is not None:
            logger.info(f"Using cached spatial pattern data (with points) for region: {region_id}")
            return cached_result
            
        try:
            parts = region_id.split('_', 1)
            if len(parts) != 2: raise ValueError("Invalid region_id format")
            region_type = parts[0]
            region_name_parts = parts[1].split('_')
            region_name = ' '.join(part.capitalize() for part in region_name_parts)
        except Exception as e: 
            logger.error(f"Error parsing region_id '{region_id}' for spatial patterns: {e}"); return {}

        sql = """
        WITH region_base_data AS (
            SELECT
                industry,
                txn_cnt,
                txn_amt,
                central_latitude,
                central_longitude
            FROM
                data_lake.master_card
            WHERE
                geo_type = :region_type
                AND geo_name = :region_name
                AND txn_cnt > 0
                AND central_latitude IS NOT NULL
                AND central_longitude IS NOT NULL
            LIMIT 5000
        ),
        region_points AS (
            SELECT
                central_latitude AS latitude,
                central_longitude AS longitude,
                industry,
                txn_cnt,
                txn_amt
            FROM region_base_data
            LIMIT 1000
        ),
        region_stats AS (
             SELECT
                industry,
                txn_cnt,
                txn_amt,
                central_latitude,
                central_longitude,
                CASE
                   WHEN AVG(central_latitude) OVER () IS NOT NULL AND
                        AVG(central_longitude) OVER () IS NOT NULL AND
                        central_latitude IS NOT NULL AND central_longitude IS NOT NULL
                   THEN SQRT(
                       POWER(central_latitude - AVG(central_latitude) OVER (), 2) +
                       POWER(central_longitude - AVG(central_longitude) OVER (), 2)
                   ) * 111
                   ELSE NULL
                END AS distance_km
            FROM region_base_data
        )
        SELECT
            (SELECT AVG(txn_cnt)::FLOAT FROM region_stats WHERE distance_km IS NOT NULL) AS avg_activity,
            (SELECT STDDEV(txn_cnt)::FLOAT FROM region_stats WHERE distance_km IS NOT NULL) AS std_activity,
            (SELECT AVG(distance_km)::FLOAT FROM region_stats WHERE distance_km IS NOT NULL) AS avg_distance,
            (SELECT STDDEV(distance_km)::FLOAT FROM region_stats WHERE distance_km IS NOT NULL) AS std_distance,
            (SELECT MIN(distance_km)::FLOAT FROM region_stats WHERE distance_km IS NOT NULL) AS min_distance,
            (SELECT MAX(distance_km)::FLOAT FROM region_stats WHERE distance_km IS NOT NULL) AS max_distance,
            (SELECT array_agg(DISTINCT industry) FROM region_stats) AS industries,
            (SELECT COUNT(*)::INTEGER FROM region_stats WHERE distance_km IS NOT NULL) AS total_points_for_stats,
            (SELECT json_agg(json_build_object(
                'latitude', latitude,
                'longitude', longitude,
                'industry', industry,
                'txn_cnt', txn_cnt,
                'txn_amt', txn_amt
            )) FROM region_points) AS points_data
        """
        
        try:
            results = self.db_service.execute_query(sql, {"region_type": region_type, "region_name": region_name})
            
            if not results or results[0]['total_points_for_stats'] is None or results[0]['total_points_for_stats'] == 0:
                logger.warning(f"No direct spatial patterns data found for Type: {region_type}, Name: {region_name}")
                return {}
                
            result = results[0]
            
            points_list = []
            if result.get('points_data'):
                try:
                    if isinstance(result['points_data'], str):
                        points_list = json.loads(result['points_data'])
                    elif isinstance(result['points_data'], list):
                         points_list = result['points_data']
                    else:
                         logger.warning(f"Unexpected type for points_data: {type(result['points_data'])}")
                except json.JSONDecodeError as json_err:
                    logger.error(f"Failed to decode points_data JSON: {json_err}")
            
            processed_result = {
                'avg_activity': float(result.get('avg_activity', 0) or 0),
                'std_activity': float(result.get('std_activity', 0) or 0),
                'avg_distance': float(result.get('avg_distance', 0) or 0),
                'std_distance': float(result.get('std_distance', 0) or 0),
                'min_distance': float(result.get('min_distance', 0) or 0),
                'max_distance': float(result.get('max_distance', 0) or 0),
                'industries': result.get('industries', []) or [], # Ensure list
                'total_points': int(result.get('total_points_for_stats', 0) or 0),
                'points': points_list
            }
            
            self.cache.set(cache_key, processed_result)
            return processed_result
            
        except Exception as e:
            logger.error(f"Error getting direct spatial patterns for {region_id}: {str(e)}")
            logger.error(traceback.format_exc())
            return {}
    
    def get_hotspots(self, region_id: str) -> List[Dict[str, Any]]:
        """Identify hotspots within a region from master_card"""
        cache_key = f"hotspots_direct:{region_id}" # Using direct key
        cached_result = self.cache.get(cache_key)
        if cached_result is not None:
            logger.info(f"Using cached hotspot data for region: {region_id}")
            return cached_result
            
        try:
            parts = region_id.split('_', 1)
            if len(parts) != 2: raise ValueError("Invalid region_id format")
            region_type = parts[0]
            region_name_parts = parts[1].split('_')
            region_name = ' '.join(part.capitalize() for part in region_name_parts)
        except Exception as e: 
            logger.error(f"Error parsing region_id '{region_id}' for hotspots: {e}"); return []

        sql = """
        SELECT 
            central_latitude AS latitude,
            central_longitude AS longitude,
            industry,
            SUM(txn_cnt)::FLOAT AS density, 
            SUM(txn_amt)::FLOAT AS total_spend,
            COUNT(*)::INTEGER AS point_count 
        FROM 
            data_lake.master_card
        WHERE 
            geo_type = :region_type
            AND geo_name = :region_name
            AND central_latitude IS NOT NULL
            AND central_longitude IS NOT NULL
        GROUP BY 
            central_latitude, central_longitude, industry
        HAVING 
            SUM(txn_cnt) > 0
        ORDER BY 
            density DESC
        LIMIT 50
        """
        
        try:
            results = self.db_service.execute_query(sql, {"region_type": region_type, "region_name": region_name})
            processed_results = self._process_hotspot_results(results)
            self.cache.set(cache_key, processed_results)
            return processed_results
            
        except Exception as e:
            logger.error(f"Error identifying direct hotspots for {region_id}: {str(e)}")
            logger.error(traceback.format_exc())
            return []
    
    def _get_geojson_from_wkt(self, wkt_string: Optional[str]) -> Optional[str]:
        # ... (Implementation remains the same)
        pass

    def _process_geometry(self, geometry_str: Optional[str]) -> Optional[str]:
        # ... (Implementation remains the same)
        pass

    def _process_hotspot_results(self, results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        # ... (Implementation remains the same)
        pass

    def get_temporal_insights(self, region_name: str) -> List[Dict[str, Any]]:
        """Get temporal insights for a specific region."""
        try:
            query = """
                SELECT 
                    ti.region_id,
                    ti.month,
                    ti.year,
                    ti.total_visitors,
                    ti.total_spend,
                    ti.swiss_tourists,
                    ti.foreign_tourists
                FROM geo_insights.temporal_insights ti
                WHERE ti.region_id = :region_id
                AND ti.year >= 2023
                ORDER BY ti.year DESC, ti.month DESC
                LIMIT 6
            """
            
            results = self.db_service.execute_query(query, (region_name,))
            if not results:
                return []
            
            # Convert numeric strings to float
            processed_results = []
            for row in results:
                processed_row = {
                    'month': int(row['month']),
                    'year': int(row['year']),
                    'total_visitors': float(row['total_visitors']),
                    'swiss_tourists': float(row['swiss_tourists']),
                    'foreign_tourists': float(row['foreign_tourists']),
                    'total_spend': float(row['total_spend'])
                }
                processed_results.append(processed_row)
            
            return processed_results
            
        except Exception as e:
            logger.error(f"Error getting temporal insights for region {region_name}: {str(e)}")
            return []

# Ensure Bellinzonese specific methods are removed if they were present 