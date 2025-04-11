import requests
import json
import os
import geopandas as gpd
from shapely.geometry import shape
import pandas as pd

def download_swiss_boundaries():
    """
    Download Swiss administrative boundaries in GeoJSON format.
    This uses the publicly available simplified data from geoBoundaries.
    """
    print("Downloading Swiss canton boundaries...")
    
    # URL for Swiss cantons data (simplified)
    url = "https://raw.githubusercontent.com/deldersveld/topojson/master/countries/switzerland/switzerland-cantons.json"
    
    try:
        response = requests.get(url)
        response.raise_for_status()  # Check for HTTP errors
        
        # This is a TopoJSON file, we need to extract the GeoJSON
        topojson_data = response.json()
        
        # Extract GeoJSON features for cantons
        geojson_features = []
        
        # Navigate the TopoJSON structure
        for obj_name, obj in topojson_data['objects'].items():
            if 'geometries' in obj:
                for geometry in obj['geometries']:
                    # Convert TopoJSON reference to actual GeoJSON
                    # This is a simplified approach - for full conversion use a dedicated TopoJSON library
                    properties = geometry.get('properties', {})
                    
                    # Create a feature
                    feature = {
                        'type': 'Feature',
                        'properties': properties,
                        'geometry': geometry
                    }
                    
                    geojson_features.append(feature)
        
        print(f"Downloaded {len(geojson_features)} canton features")
        
        return {
            'type': 'FeatureCollection',
            'features': geojson_features
        }
        
    except Exception as e:
        print(f"Error downloading Swiss boundaries: {str(e)}")
        return None

def download_ticino_districts():
    """
    Download more detailed boundary data for districts in Ticino.
    """
    print("Downloading Ticino district boundaries...")
    
    # This would normally fetch from a Swiss government source
    # For now, we'll use an open data URL if available
    
    try:
        # Try to use OpenStreetMap data via Nominatim or Overpass API
        # This is a placeholder - in production, use a direct data source
        
        # Alternative: Use a pre-made GeoJSON if available online
        print("Note: Using simplified district boundaries. For production use, download from geo.admin.ch")
        
        # Return empty for now - this should be replaced with actual data
        return {
            'type': 'FeatureCollection',
            'features': []
        }
        
    except Exception as e:
        print(f"Error downloading Ticino districts: {str(e)}")
        return None

def filter_ticino_data(swiss_geojson):
    """
    Filter the Swiss GeoJSON to only include Ticino.
    """
    if not swiss_geojson:
        return None
    
    ticino_features = []
    
    for feature in swiss_geojson['features']:
        properties = feature.get('properties', {})
        
        # Look for Ticino by name or code
        # Different datasets use different property names
        canton_name = properties.get('name', properties.get('NAME', "")).lower()
        canton_code = properties.get('id', properties.get('KANTONSNUM', ""))
        
        if 'ticino' in canton_name or 'tessin' in canton_name or str(canton_code) == '21':
            ticino_features.append(feature)
            print(f"Found Ticino: {canton_name} (code: {canton_code})")
    
    if not ticino_features:
        print("Warning: Ticino canton not found in the data.")
        
    return {
        'type': 'FeatureCollection',
        'features': ticino_features
    }

def create_detailed_ticino_geojson():
    """
    Create a comprehensive GeoJSON file for Ticino with detailed boundaries.
    """
    # Get Swiss canton boundaries
    swiss_geojson = download_swiss_boundaries()
    
    # Filter to get Ticino only
    ticino_canton_geojson = filter_ticino_data(swiss_geojson)
    
    # Get Ticino district boundaries
    ticino_districts_geojson = download_ticino_districts()
    
    # Combine the data
    combined_features = []
    
    if ticino_canton_geojson:
        combined_features.extend(ticino_canton_geojson['features'])
        print(f"Added {len(ticino_canton_geojson['features'])} canton features")
    
    if ticino_districts_geojson:
        combined_features.extend(ticino_districts_geojson['features'])
        print(f"Added {len(ticino_districts_geojson['features'])} district features")
    
    # Create the final GeoJSON
    final_geojson = {
        'type': 'FeatureCollection',
        'features': combined_features
    }
    
    # Save to file
    with open('ticino_boundaries.geojson', 'w') as f:
        json.dump(final_geojson, f, indent=2)
    
    print(f"Saved GeoJSON with {len(combined_features)} features to ticino_boundaries.geojson")
    return final_geojson

def alternative_osm_boundaries():
    """
    Alternative method: Use OSMnx to get Ticino boundaries from OpenStreetMap.
    """
    try:
        import osmnx as ox
        print("Using OSMnx to download Ticino boundaries from OpenStreetMap...")
        
        # Get the boundary polygon for Ticino
        ticino = ox.geocode_to_gdf("Ticino, Switzerland")
        
        # Convert to GeoJSON
        ticino_geojson = json.loads(ticino.to_json())
        
        # Save to file
        with open('ticino_osm_boundary.geojson', 'w') as f:
            json.dump(ticino_geojson, f, indent=2)
        
        print("Saved OpenStreetMap boundaries to ticino_osm_boundary.geojson")
        return ticino_geojson
        
    except ImportError:
        print("OSMnx not installed. Install with: pip install osmnx")
        return None
    except Exception as e:
        print(f"Error getting OpenStreetMap data: {str(e)}")
        return None

def alternative_swiss_geoportal_instructions():
    """
    Instructions for downloading official boundaries from Swiss geoportal.
    """
    instructions = """
INSTRUCTIONS FOR DOWNLOADING OFFICIAL TICINO BOUNDARIES:

For the most accurate and official boundaries, follow these steps:

1. Visit the Swiss Federal Geoportal: https://map.geo.admin.ch/

2. Search for "Ticino" in the search bar

3. In the left sidebar, click on "Catalog" and search for "administrative boundaries"

4. Select the layer "Administrative boundaries of Switzerland" or "Swiss boundaries"

5. Use the "Draw & Measure" tool to draw a rectangle around Ticino

6. Click on "Advanced tools" and select "Export"

7. Choose GeoJSON format and download the file

8. Use this downloaded file in your Streamlit application for accurate boundaries

This will give you official and detailed boundaries from the Swiss Federal Office of Topography.
"""
    print(instructions)
    
    # Save instructions to file
    with open('swiss_geoportal_instructions.txt', 'w') as f:
        f.write(instructions)
    
    print("Saved instructions to swiss_geoportal_instructions.txt")

# Main execution
if __name__ == "__main__":
    print("Generating Ticino GeoJSON with real boundaries...")
    
    # Try the primary method
    ticino_geojson = create_detailed_ticino_geojson()
    
    # Try the alternative method if needed
    if not ticino_geojson or len(ticino_geojson['features']) == 0:
        print("\nTrying alternative method...")
        alternative_osm_boundaries()
    
    # Always show instructions for the official method
    print("\nFor best results:")
    alternative_swiss_geoportal_instructions()
    
    print("\nDone! Check the current directory for output files.")