import json
import logging
import os
import random
from typing import Dict, List, Any, Optional, Tuple

import plotly.graph_objects as go
from plotly.subplots import make_subplots

logger = logging.getLogger(__name__)

class GeoMapUtils:
    """Utility class for working with geospatial data and creating visualizations"""

    # Define a list of colors for industry categories
    INDUSTRY_COLORS = [
        "#1f77b4",  # Blue
        "#ff7f0e",  # Orange
        "#2ca02c",  # Green
        "#d62728",  # Red
        "#9467bd",  # Purple
        "#8c564b",  # Brown
        "#e377c2",  # Pink
        "#7f7f7f",  # Gray
        "#bcbd22",  # Olive
        "#17becf",  # Cyan
    ]

    def __init__(self):
        logger.info("GeoMapUtils initialized")

    def load_geojson(self, file_path: str) -> Dict[str, Any]:
        """Load geojson data from file"""
        try:
            # First try direct path
            with open(file_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            # If file not found, try some backup/standard locations
            fallback_paths = [
                "app/static/geojson/ticinomap.geojson",  # Try Ticino-specific map
                "app/static/geojson/switzerland_cantons.geojson",  # Fallback to Switzerland map
                "app/static/geojson/Bellinzona e Alto Ticino.geojson"  # Try specific region
            ]
            
            for fallback_path in fallback_paths:
                try:
                    with open(fallback_path, 'r') as f:
                        return json.load(f)
                except FileNotFoundError:
                    continue
                    
            # If we get here, try one more approach - load from regions.json
            try:
                with open("app/static/geojson/regions.json", 'r') as f:
                    regions_data = json.load(f)
                    # Convert to GeoJSON format
                    features = []
                    for region in regions_data:
                        features.append({
                            "type": "Feature",
                            "properties": {
                                "name": region["name"],
                                "id": region["id"]
                            },
                            "geometry": region["geometry"]
                        })
                    return {"type": "FeatureCollection", "features": features}
            except FileNotFoundError:
                pass
                
            # If all else fails, return empty GeoJSON
            logger.warning(f"Could not find any GeoJSON files. Tried {file_path} and fallbacks.")
            return {"type": "FeatureCollection", "features": []}

    def _rgb_to_rgba(self, rgb_color: str, alpha: float = 0.6) -> str:
        """Convert RGB color to RGBA with transparency"""
        if not rgb_color:
            return "rgba(0, 0, 255, 0.6)"  # Default blue with transparency
            
        try:
            # Handle hex colors
            if rgb_color.startswith("#"):
                if len(rgb_color) == 7:  # #RRGGBB format
                    r = int(rgb_color[1:3], 16)
                    g = int(rgb_color[3:5], 16)
                    b = int(rgb_color[5:7], 16)
                    return f"rgba({r}, {g}, {b}, {alpha})"
                elif len(rgb_color) == 4:  # #RGB format
                    r = int(rgb_color[1], 16) * 16 + int(rgb_color[1], 16)
                    g = int(rgb_color[2], 16) * 16 + int(rgb_color[2], 16)
                    b = int(rgb_color[3], 16) * 16 + int(rgb_color[3], 16)
                    return f"rgba({r}, {g}, {b}, {alpha})"
            
            # Handle rgb format
            if rgb_color.startswith("rgb("):
                rgb_values = rgb_color[4:-1].split(",")
                r, g, b = [int(val.strip()) for val in rgb_values]
                return f"rgba({r}, {g}, {b}, {alpha})"
                
            # Default fallback
            return f"rgba(0, 0, 255, {alpha})"
        except Exception as e:
            logger.warning(f"Error converting color {rgb_color} to RGBA: {str(e)}")
            return f"rgba(0, 0, 255, {alpha})"

    def _get_industry_color(self, industry: str, index: int = 0) -> str:
        """Get a consistent color for a specific industry"""
        # Make the color assignment deterministic based on industry name
        if not industry:
            industry = "Unknown"
            
        # Use hash of industry name to pick a color
        hash_val = sum(ord(c) for c in industry)
        color_index = hash_val % len(self.INDUSTRY_COLORS)
        
        return self.INDUSTRY_COLORS[color_index]

    def create_multi_region_map(
        self, 
        geojson_data: Dict[str, Any], 
        regions: List[Dict[str, Any]], 
        title: str = "Tourism Regions"
    ) -> Dict[str, Any]:
        """Create a map with multiple regions highlighted"""
        if not geojson_data or not regions:
            logger.warning("No geojson data or regions provided for map creation")
            return {}
            
        # Create the base map
        fig = go.Figure()
        
        # Add the base GeoJSON layer for all regions
        fig.add_trace(go.Choroplethmapbox(
            geojson=geojson_data,
            locations=[feature['properties']['name'] for feature in geojson_data['features']],
            z=[1] * len(geojson_data['features']),  # Uniform color for all regions
            colorscale=[[0, 'rgba(200, 200, 200, 0.3)'], [1, 'rgba(200, 200, 200, 0.3)']],
            marker_opacity=0.3,
            marker_line_width=1,
            marker_line_color='gray',
            showscale=False,
            hoverinfo='skip',
            name="Base Regions"
        ))
        
        # Get center coordinates for the map view
        if regions and len(regions) > 0:
            # Collect all coordinates from regions with valid data
            lats = []
            lons = []
            for region in regions:
                if region.get('central_latitude') and region.get('central_longitude'):
                    lats.append(region['central_latitude'])
                    lons.append(region['central_longitude'])
            
            # Set center and zoom based on region data
            if lats and lons:
                center_lat = sum(lats) / len(lats)
                center_lon = sum(lons) / len(lons)
                # Default to Ticino if no coordinates available
                center = {"lat": center_lat, "lon": center_lon}
                zoom = 9  # Adjusted for Ticino region
            else:
                # Default to center of Ticino
                center = {"lat": 46.3327, "lon": 8.8014}
                zoom = 8
        else:
            # Default to center of Ticino
            center = {"lat": 46.3327, "lon": 8.8014}
            zoom = 8
        
        # Add markers for each region
        for i, region in enumerate(regions):
            if region.get('central_latitude') and region.get('central_longitude'):
                # Create marker for the region
                fig.add_trace(go.Scattermapbox(
                    lat=[region['central_latitude']],
                    lon=[region['central_longitude']],
                    mode='markers',
                    marker=dict(
                        size=10,
                        color=self._get_industry_color(region.get('name', ''), i),
                        opacity=0.8
                    ),
                    text=[region.get('name', 'Unknown')],
                    hoverinfo='text',
                    name=region.get('name', f"Region {i+1}")
                ))
                
                # Create popup content for the marker
                hover_text = f"<b>{region.get('name', 'Unknown Region')}</b><br>"
                
                if region.get('visitor_count'):
                    hover_text += f"Visitors: {region.get('visitor_count', 0):,}<br>"
                    
                if region.get('swiss_visitors') and region.get('foreign_visitors'):
                    hover_text += f"Swiss: {region.get('swiss_visitors', 0):,}<br>"
                    hover_text += f"Foreign: {region.get('foreign_visitors', 0):,}<br>"
                
                if region.get('industry_counts'):
                    hover_text += "<br><b>Industries:</b><br>"
                    for industry, count in region.get('industry_counts', {}).items():
                        hover_text += f"{industry}: {count}<br>"
                
                fig.data[-1].text = [hover_text]
                fig.data[-1].hoverinfo = 'text'
        
        # Update the layout
        fig.update_layout(
            title=title,
            mapbox=dict(
                style="carto-positron",
                center=center,
                zoom=zoom
            ),
            margin={"r": 0, "t": 30, "l": 0, "b": 0},
            height=600,
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01,
                bgcolor="rgba(255, 255, 255, 0.8)",
                bordercolor="rgba(0, 0, 0, 0.3)",
                borderwidth=1
            )
        )
        
        # Convert to JSON for API response
        return {
            "map_type": "multi_region",
            "title": title,
            "plotly_json": fig.to_json(),
            "regions": [region.get('name', f"Region {i+1}") for i, region in enumerate(regions)],
            "center": center,
            "zoom": zoom
        }

    def create_industry_distribution_map(
        self, 
        geojson_data: Dict[str, Any], 
        industry_data: List[Dict[str, Any]], 
        region_name: str,
        title: str = "Industry Distribution"
    ) -> Dict[str, Any]:
        """Create a map showing industry distribution in a specific region"""
        if not geojson_data or not industry_data:
            logger.warning("No geojson data or industry points provided for map creation")
            return {}
            
        # Group industry points by industry type
        industry_groups = {}
        for point in industry_data:
            industry = point.get('industry', 'Unknown')
            if industry not in industry_groups:
                industry_groups[industry] = []
            industry_groups[industry].append(point)
        
        # Create figure with subplots - map and pie chart
        fig = make_subplots(
            rows=1, cols=2,
            specs=[[{"type": "mapbox"}, {"type": "pie"}]],
            column_widths=[0.7, 0.3]
        )
        
        # Add the base GeoJSON layer for the selected region
        region_feature = None
        for feature in geojson_data['features']:
            if feature['properties']['name'].lower() == region_name.lower():
                region_feature = feature
                break
        
        if region_feature:
            # Create a modified geojson with just the selected region
            single_region_geojson = {
                "type": "FeatureCollection",
                "features": [region_feature]
            }
            
            fig.add_trace(
                go.Choroplethmapbox(
                    geojson=single_region_geojson,
                    locations=[region_feature['properties']['name']],
                    z=[1],  # Uniform color for the region
                    colorscale=[[0, 'rgba(200, 200, 200, 0.3)'], [1, 'rgba(200, 200, 200, 0.3)']],
                    marker_opacity=0.3,
                    marker_line_width=1,
                    marker_line_color='gray',
                    showscale=False,
                    hoverinfo='skip',
                    name=region_name
                ),
                row=1, col=1
            )
        
        # Add scatter markers for each industry group
        industry_counts = {}
        for i, (industry, points) in enumerate(industry_groups.items()):
            industry_counts[industry] = len(points)
            
            # Extract coordinates
            lats = [float(p.get('latitude', 0)) for p in points if p.get('latitude')]
            lons = [float(p.get('longitude', 0)) for p in points if p.get('longitude')]
            
            if not lats or not lons:
                continue
                
            # Get a consistent color for this industry
            color = self._get_industry_color(industry, i)
            
            # Create marker trace for this industry
            fig.add_trace(
                go.Scattermapbox(
                    lat=lats,
                    lon=lons,
                    mode='markers',
                    marker=dict(
                        size=8,
                        color=color,
                        opacity=0.7
                    ),
                    text=[f"<b>{p.get('name', 'Business')}</b><br>Industry: {industry}<br>Transactions: {p.get('txn_cnt', 'N/A')}<br>Amount: ${float(p.get('txn_amt', 0)):,.2f}" for p in points],
                    hoverinfo='text',
                    name=f"{industry} ({len(points)})"
                ),
                row=1, col=1
            )
        
        # Add pie chart showing industry distribution
        labels = list(industry_counts.keys())
        values = list(industry_counts.values())
        
        if labels and values:
            colors = [self._get_industry_color(industry, i) for i, industry in enumerate(labels)]
            
            fig.add_trace(
                go.Pie(
                    labels=labels,
                    values=values,
                    textinfo='label+percent',
                    marker=dict(colors=colors),
                    hole=0.3,
                    showlegend=False
                ),
                row=1, col=2
            )
        
        # Calculate center coordinates based on all points
        all_lats = []
        all_lons = []
        for points in industry_groups.values():
            for point in points:
                if point.get('latitude') and point.get('longitude'):
                    all_lats.append(float(point['latitude']))
                    all_lons.append(float(point['longitude']))
        
        # Set center and zoom based on data points
        if all_lats and all_lons:
            center_lat = sum(all_lats) / len(all_lats)
            center_lon = sum(all_lons) / len(all_lons)
            center = {"lat": center_lat, "lon": center_lon}
            zoom = 10
        else:
            # Default to center of Ticino
            center = {"lat": 46.3327, "lon": 8.8014}
            zoom = 8
        
        # Update the layout
        fig.update_layout(
            title=title,
            mapbox=dict(
                style="carto-positron",
                center=center,
                zoom=zoom
            ),
            margin={"r": 0, "t": 30, "l": 0, "b": 0},
            height=600,
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01,
                bgcolor="rgba(255, 255, 255, 0.8)",
                bordercolor="rgba(0, 0, 0, 0.3)",
                borderwidth=1
            )
        )
        
        # Convert to JSON for API response
        return {
            "map_type": "industry_distribution",
            "title": title,
            "plotly_json": fig.to_json(),
            "region": region_name,
            "industries": [{"name": name, "count": count} for name, count in industry_counts.items()],
            "total_points": sum(industry_counts.values()),
            "center": center,
            "zoom": zoom
        }

    def create_visitor_comparison_map(
        self, 
        geojson_data: Dict[str, Any], 
        regions: List[Dict[str, Any]], 
        metric: str = "total_visitors",
        title: str = "Visitor Comparison by Region"
    ) -> Dict[str, Any]:
        """Create a choropleth map comparing visitor metrics across regions"""
        if not geojson_data or not regions:
            logger.warning("No geojson data or regions provided for map creation")
            return {}
            
        # Map region names to metrics for choropleth
        region_metrics = {}
        
        # Determine which metric to use for comparison
        metric_key = "total_visitors"
        if metric.lower() == "swiss":
            metric_key = "swiss_tourists"
            title = "Swiss Visitors in Ticino"
            colorscale = "Blues"
        elif metric.lower() == "foreign":
            metric_key = "foreign_tourists"
            title = "Foreign Visitors in Ticino"
            colorscale = "Reds"
        elif metric.lower() == "ratio":
            # For ratio, we'll calculate Swiss to Foreign ratio
            colorscale = "RdBu"
            title = "Swiss to Foreign Visitor Ratio in Ticino"
            
            for region in regions:
                # Try different name keys that might be in the data
                name = None
                for key in ['region_name', 'name', 'geo_name']:
                    if key in region and region[key]:
                        name = region[key]
                        break
                
                if not name:
                    continue
                    
                swiss = float(region.get('swiss_tourists', 0) or 0)
                foreign = float(region.get('foreign_tourists', 0) or 0)
                
                # Calculate ratio (avoid division by zero)
                if foreign > 0:
                    ratio = swiss / foreign
                elif swiss > 0:
                    ratio = float('inf')  # All Swiss, no foreign
                else:
                    ratio = 0  # No visitors
                    
                region_metrics[name] = ratio
        else:
            # Default to total visitors
            colorscale = "Viridis"
            
            for region in regions:
                # Try different name keys that might be in the data
                name = None
                for key in ['region_name', 'name', 'geo_name']:
                    if key in region and region[key]:
                        name = region[key]
                        break
                
                if not name:
                    continue
                    
                count = float(region.get('total_visitors', 0) or 0)
                region_metrics[name] = count
        
        # If we didn't already populate region_metrics (for ratio case)
        if not region_metrics and metric_key:
            for region in regions:
                # Try different name keys that might be in the data
                name = None
                for key in ['region_name', 'name', 'geo_name']:
                    if key in region and region[key]:
                        name = region[key]
                        break
                
                if not name:
                    continue
                    
                count = float(region.get(metric_key, 0) or 0)
                region_metrics[name] = count
        
        # Create the choropleth map
        fig = go.Figure()
        
        # Create lists for the choropleth
        locations = []
        z_values = []
        
        # Extract valid region names from geojson properties
        valid_region_names = {}
        for i, feature in enumerate(geojson_data.get('features', [])):
            props = feature.get('properties', {})
            if 'name' in props:
                valid_region_names[props['name']] = i
                logger.info(f"Found region in geojson: {props['name']}")
            elif 'Name' in props:
                valid_region_names[props['Name']] = i
                logger.info(f"Found region in geojson: {props['Name']}")
                
        logger.info(f"Valid region names from geojson: {list(valid_region_names.keys())}")
        logger.info(f"Region metrics: {list(region_metrics.keys())}")
        
        # Add a base map layer with all regions
        fig.add_trace(go.Choroplethmapbox(
            geojson=geojson_data,
            locations=[feature['properties'].get('name', feature['properties'].get('Name', f'Region_{i}')) 
                      for i, feature in enumerate(geojson_data.get('features', []))],
            z=[1] * len(geojson_data.get('features', [])),
            colorscale=[[0, 'rgba(220, 220, 220, 0.3)'], [1, 'rgba(220, 220, 220, 0.3)']],
            marker_opacity=0.3,
            marker_line_width=1,
            marker_line_color='gray',
            showscale=False,
            hoverinfo='skip',
            name='Base Regions',
            featureidkey="properties.name"
        ))
        
        # Match data with geojson features
        for name, value in region_metrics.items():
            if name in valid_region_names:
                locations.append(name)
                z_values.append(value)
                logger.info(f"Matched region name: {name} with value: {value}")
            else:
                # Try fuzzy matching for Ticino regions
                match_found = False
                for valid_name in valid_region_names:
                    # Try more flexible matching
                    if (name.lower() in valid_name.lower() or 
                        valid_name.lower() in name.lower() or 
                        name.lower().replace(' ', '_') == valid_name.lower().replace(' ', '_')):
                        logger.info(f"Fuzzy matched region '{name}' to '{valid_name}'")
                        locations.append(valid_name)
                        z_values.append(value)
                        match_found = True
                        break
                
                if not match_found:
                    logger.warning(f"Region name '{name}' not found in geojson properties")
        
        logger.info(f"Final locations: {locations}")
        logger.info(f"Final z-values: {z_values}")
        
        # Add markers for regions with coordinates that weren't matched to geojson
        unmatched_regions = []
        for region in regions:
            # Get region name
            name = None
            for key in ['region_name', 'name', 'geo_name']:
                if key in region and region[key]:
                    name = region[key]
                    break
            
            if not name:
                continue
                
            # Check if this region wasn't matched
            if name not in locations and name not in valid_region_names:
                for valid_name in valid_region_names:
                    if (name.lower() in valid_name.lower() or 
                        valid_name.lower() in name.lower() or 
                        name.lower().replace(' ', '_') == valid_name.lower().replace(' ', '_')):
                        # Was already matched with fuzzy matching
                        break
                else:
                    # Region wasn't matched, add marker if we have coordinates
                    if region.get('central_latitude') and region.get('central_longitude'):
                        unmatched_regions.append(region)
                        logger.info(f"Adding marker for unmatched region: {name}")
        
        # Add the choropleth layer
        if locations and z_values:
            # Determine hover template based on metric
            if metric.lower() == 'ratio':
                hovertemplate = '<b>%{location}</b><br>Swiss/Foreign Ratio: %{z:.2f}<extra></extra>'
            else:
                hovertemplate = f'<b>%{{location}}</b><br>{metric.title()} Visitors: %{{z:,.0f}}<extra></extra>'
            
            # Create list of colors for each region
            color_values = z_values.copy()
            custom_colorscale = [[0, 'rgb(220,220,220)'], [0.5, colorscale], [1, colorscale]]
            
            # Add the choropleth to the figure
            fig.add_trace(go.Choroplethmapbox(
                geojson=geojson_data,
                locations=locations,
                z=z_values,
                featureidkey="properties.name",  # For properties with 'name'
                colorscale=colorscale,
                marker_opacity=0.7,
                marker_line_width=1,
                marker_line_color='white',
                showscale=True,
                colorbar=dict(
                    title=metric.title() + " Visitors" if metric.lower() != 'ratio' else "Swiss/Foreign Ratio",
                    thickness=20,
                    len=0.7,
                    bgcolor='rgba(255,255,255,0.8)',
                    borderwidth=1,
                    x=0.95
                ),
                hovertemplate=hovertemplate
            ))
        
        # Add markers for unmatched regions
        if unmatched_regions:
            for region in unmatched_regions:
                name = None
                for key in ['region_name', 'name', 'geo_name']:
                    if key in region and region[key]:
                        name = region[key]
                        break
                
                if not name:
                    continue
                    
                lat = float(region['central_latitude'])
                lon = float(region['central_longitude'])
                
                # Get metric value
                value = 0
                if metric.lower() == "swiss":
                    value = float(region.get('swiss_tourists', 0) or 0)
                elif metric.lower() == "foreign":
                    value = float(region.get('foreign_tourists', 0) or 0)
                else:
                    value = float(region.get('total_visitors', 0) or 0)
                
                # Add a marker for this region
                marker_size = min(50, max(20, value / 1000))  # Scale marker size
                
                fig.add_trace(go.Scattermapbox(
                    lat=[lat],
                    lon=[lon],
                    mode='markers',
                    marker=dict(
                        size=marker_size,
                        color='red',
                        opacity=0.7
                    ),
                    text=[f"<b>{name}</b><br>{metric.title()} Visitors: {value:,.0f}"],
                    hoverinfo='text',
                    name=name
                ))
        
        # Get center coordinates for the map view - focus on Ticino
        center = {"lat": 46.3327, "lon": 8.8014}  # Ticino center
        zoom = 8  # Zoom level for Ticino
        
        # If we have specific region coordinates, use them
        if regions and len(regions) > 0:
            lats = []
            lons = []
            for region in regions:
                if region.get('central_latitude') and region.get('central_longitude'):
                    lats.append(float(region['central_latitude']))
                    lons.append(float(region['central_longitude']))
            
            # Only update center if we found coordinates
            if lats and lons:
                center = {"lat": sum(lats) / len(lats), "lon": sum(lons) / len(lons)}
                # Adjust zoom based on number of regions
                zoom = 9 if len(regions) == 1 else 8
        
        # Update the layout
        fig.update_layout(
            title=title,
            mapbox=dict(
                style="carto-positron",
                center=center,
                zoom=zoom
            ),
            margin={"r": 30, "t": 30, "l": 0, "b": 0},
            height=600
        )
        
        # Return the figure as a dict directly
        return fig.to_dict() 