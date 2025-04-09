import json
import logging
import os
from typing import List, Dict, Any, Optional
from ..utils.geo_map_utils import GeoMapUtils

logger = logging.getLogger(__name__)

class TourismRegionService:
    """Service for working with tourism region data and visualizations"""
    
    def __init__(self):
        logger.info("TourismRegionService initialized")
        self.geo_map_utils = GeoMapUtils()
        self.regions = self._load_regions()
        self.geojson_data = self._load_geojson()
        
    def _load_regions(self) -> List[Dict[str, Any]]:
        """Load region data from JSON file"""
        try:
            # Find path relative to current file
            base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            regions_path = os.path.join(base_dir, "note", "Tourism regions", "regions.json")
            
            if os.path.exists(regions_path):
                with open(regions_path, 'r') as f:
                    regions = json.load(f)
                    
                # Process regions to add center coordinates if not present
                for region in regions:
                    if 'geometry' in region and 'coordinates' in region['geometry']:
                        # Calculate center from geometry if not specified
                        if not region.get('central_latitude') or not region.get('central_longitude'):
                            # For polygon types, calculate average of all points
                            if region['geometry']['type'] == 'Polygon':
                                coords = region['geometry']['coordinates'][0]
                                lats = [coord[1] for coord in coords]
                                lons = [coord[0] for coord in coords]
                                region['central_latitude'] = sum(lats) / len(lats)
                                region['central_longitude'] = sum(lons) / len(lons)
                
                logger.info(f"Loaded {len(regions)} tourism regions")
                return regions
            else:
                logger.warning(f"Regions file not found at {regions_path}")
                return []
        except Exception as e:
            logger.error(f"Error loading regions: {str(e)}")
            return []
    
    def _load_geojson(self) -> Dict[str, Any]:
        """Load GeoJSON data for Ticino regions"""
        try:
            # Find path relative to current file
            base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            geojson_path = os.path.join(base_dir, "note", "Tourism regions", "ticinomap.geojson")
            
            if os.path.exists(geojson_path):
                return self.geo_map_utils.load_geojson(geojson_path)
            else:
                logger.warning(f"GeoJSON file not found at {geojson_path}")
                return {}
        except Exception as e:
            logger.error(f"Error loading GeoJSON data: {str(e)}")
            return {}
    
    def get_regions(self) -> List[Dict[str, Any]]:
        """Get list of all tourism regions"""
        return self.regions
    
    def get_region_by_id(self, region_id: str) -> Optional[Dict[str, Any]]:
        """Get a region by its ID"""
        return next((r for r in self.regions if r['id'] == region_id), None)
    
    def get_region_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Get a region by its name (case-insensitive partial match)"""
        name_lower = name.lower()
        return next((r for r in self.regions if name_lower in r['name'].lower()), None)
    
    def create_region_map(self, title: str = "Ticino Tourism Regions") -> Dict[str, Any]:
        """Create a map showing all tourism regions in Ticino"""
        if not self.regions or not self.geojson_data:
            logger.warning("No region or GeoJSON data available for mapping")
            return {}
        
        return self.geo_map_utils.create_multi_region_map(
            self.geojson_data,
            self.regions,
            title
        )
    
    def create_region_industry_map(self, region_id: str, industry_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Create a map showing industry distribution in a specific region"""
        region = self.get_region_by_id(region_id) or self.get_region_by_name(region_id)
        
        if not region:
            logger.warning(f"Region '{region_id}' not found")
            return {}
            
        if not industry_data:
            logger.warning(f"No industry data provided for region '{region_id}'")
            return {}
            
        return self.geo_map_utils.create_industry_distribution_map(
            self.geojson_data,
            industry_data,
            region['name'],
            f"Industry Distribution in {region['name']}"
        )
        
    def get_aggregated_industry_data(self, industry_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Aggregate industry data for visualization"""
        if not industry_data:
            return {}
            
        # Group data by industry
        industry_groups = {}
        for point in industry_data:
            industry = point.get('industry', 'Unknown')
            if industry not in industry_groups:
                industry_groups[industry] = {
                    'count': 0,
                    'total_txn': 0,
                    'total_amount': 0
                }
            
            industry_groups[industry]['count'] += 1
            industry_groups[industry]['total_txn'] += float(point.get('txn_cnt', 0) or 0)
            industry_groups[industry]['total_amount'] += float(point.get('txn_amt', 0) or 0)
            
        # Convert to list for easier visualization
        result = []
        for industry, data in industry_groups.items():
            result.append({
                'industry': industry,
                'count': data['count'],
                'total_transactions': data['total_txn'],
                'total_amount': data['total_amount'],
                'avg_transaction_size': data['total_amount'] / data['total_txn'] if data['total_txn'] > 0 else 0
            })
            
        # Sort by count
        result.sort(key=lambda x: x['count'], reverse=True)
        
        return {
            'industries': result,
            'total_points': sum(item['count'] for item in result),
            'total_transactions': sum(item['total_transactions'] for item in result),
            'total_amount': sum(item['total_amount'] for item in result)
        } 