from typing import Dict, List, Any, Optional
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import json
import logging
import traceback
import matplotlib.pyplot as plt
import base64
from io import BytesIO

logger = logging.getLogger(__name__)

class GeoVisualizationService:
    def __init__(self):
        logger.info("GeoVisualizationService initialized successfully")
    
    def _rgb_to_rgba(self, rgb_color, alpha=0.2):
        """Helper method to convert RGB color to RGBA with transparency"""
        try:
            # Handle "rgb(r, g, b)" format
            if rgb_color.startswith('rgb'):
                # Extract r, g, b values from the string
                rgb_values = rgb_color.replace('rgb(', '').replace(')', '').split(',')
                r = int(rgb_values[0].strip())
                g = int(rgb_values[1].strip())
                b = int(rgb_values[2].strip())
                return f'rgba({r}, {g}, {b}, {alpha})'
            else:
                # For any other format, return with default transparency
                return f'{rgb_color.split(")")[0]}, {alpha})'
        except Exception as e:
            logger.warning(f"Error converting RGB to RGBA: {str(e)}")
            return 'rgba(100, 100, 100, 0.2)'  # Fallback to a gray color
    
    def create_region_map(self, region_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Create a map visualization for regions"""
        try:
            if not region_data:
                logger.warning("No region data provided for visualization")
                return self._create_empty_visualization("No region data provided")
                
            # Log what data we're working with
            logger.info(f"Creating map for {len(region_data)} regions")
            for i, region in enumerate(region_data):
                logger.info(f"Region {i+1}: {region.get('geo_name', 'Unknown')} - Lat: {region.get('central_latitude')}, Lon: {region.get('central_longitude')}")
                
            # Extract region data
            center_lat = 46.8182  # Default Switzerland's center
            center_lon = 8.2275   # Default Switzerland's center
            
            # First attempt to create a map visualization
            try:
                # Create a map figure
                fig = go.Figure()
                
                # Add region data points
                region_markers = []
                region_names = []
                region_values = []
                
                for region in region_data:
                    region_name = region.get('region_name', region.get('geo_name', 'Unknown'))
                    region_names.append(region_name)
                    region_values.append(float(region.get('total_visitors', 0) or 0))
                    
                    # Try to get center coordinates if available - use zero check to handle None or 0
                    lat = region.get('central_latitude')
                    lon = region.get('central_longitude')
                    
                    if lat and lon and float(lat) != 0 and float(lon) != 0:
                        lat = float(lat)
                        lon = float(lon)
                        
                        # Use these for centering the map
                        center_lat = lat
                        center_lon = lon
                        
                        # Add marker for this region
                        region_markers.append({
                            'lat': lat,
                            'lon': lon,
                            'name': region_name,
                            'visitors': float(region.get('total_visitors', 0) or 0),
                            'swiss': float(region.get('swiss_tourists', 0) or 0),
                            'foreign': float(region.get('foreign_tourists', 0) or 0)
                        })
                
                # Add the markers to the map
                if region_markers:
                    lats = [m['lat'] for m in region_markers]
                    lons = [m['lon'] for m in region_markers]
                    
                    # Calculate marker sizes based on visitor counts
                    max_visitors = max([m['visitors'] for m in region_markers]) if region_markers else 1
                    # Ensure a reasonable size even for small values
                    sizes = [max(20, min(50, (m['visitors']/max_visitors)*100 + 15)) for m in region_markers]
                    
                    # Create hover text with rich information
                    texts = [f"<b>{m['name']}</b><br>Total Visitors: {m['visitors']:,.0f}<br>Swiss: {m['swiss']:,.0f}<br>Foreign: {m['foreign']:,.0f}" for m in region_markers]
                    
                    # Create the scatter mapbox trace
                    fig.add_trace(go.Scattermapbox(
                        lat=lats,
                        lon=lons,
                        mode='markers',
                        marker=dict(
                            size=sizes,
                            color='red',
                            opacity=0.7
                        ),
                        text=texts,
                        hoverinfo='text'
                    ))
                    
                    # Configure the mapbox layout
                    fig.update_layout(
                        mapbox=dict(
                            style='open-street-map',  # Use open street map style which doesn't require a token
                            center=dict(lat=center_lat, lon=center_lon),
                            zoom=7
                        ),
                        margin=dict(l=0, r=0, t=30, b=0),
                        title=f"Tourism in {region_names[0] if len(region_names) == 1 else 'Selected Regions'}"
                    )
                    
                    # Convert to dict and return
                    map_dict = fig.to_dict()
                    logger.info("Successfully created map visualization")
                    return map_dict
                else:
                    logger.warning("No valid map markers created")
                    raise ValueError("No valid map markers created")
                
            except Exception as map_error:
                logger.error(f"Error creating map visualization: {str(map_error)}")
                logger.error(traceback.format_exc())
                logger.info("Falling back to bar chart visualization")
                # Continue to bar chart creation below
                
            # Create a simple bar chart as fallback
            fig = go.Figure()
            
            regions_list = []
            visitors_values = []
            swiss_values = []
            foreign_values = []
            
            for region in region_data:
                region_name = region.get('region_name', region.get('geo_name', 'Unknown'))
                regions_list.append(region_name)
                visitors_values.append(float(region.get('total_visitors', 0) or 0))
                swiss_values.append(float(region.get('swiss_tourists', 0) or 0))
                foreign_values.append(float(region.get('foreign_tourists', 0) or 0))
            
            # Create a simple bar chart if we have regions
            if regions_list:
                # Add Swiss tourists
                fig.add_trace(go.Bar(
                    x=regions_list,
                    y=swiss_values,
                    name='Swiss Tourists',
                    marker_color='blue'
                ))
                
                # Add Foreign tourists
                fig.add_trace(go.Bar(
                    x=regions_list,
                    y=foreign_values,
                    name='Foreign Tourists',
                    marker_color='red'
                ))
                
                # Update layout
                fig.update_layout(
                    title=f"Tourist Distribution in {regions_list[0] if len(regions_list) == 1 else 'Selected Regions'}",
                    xaxis_title="Region",
                    yaxis_title="Number of Tourists",
                    barmode='group',
                    legend_title="Tourist Type",
                    template="plotly_white"
                )
                
                return fig.to_dict()
            else:
                # Create a simple message if no valid regions
                return self._create_empty_visualization("No valid region data available for visualization")
            
        except Exception as e:
            logger.error(f"Error creating region map: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            # Return a simple error visualization instead of None
            return self._create_empty_visualization(f"Error creating visualization: {str(e)}")
            
    def _create_empty_visualization(self, message: str) -> Dict[str, Any]:
        """Create a visualization with an error message when no data is available"""
        fig = go.Figure()
        
        # Add a text annotation explaining the issue
        fig.add_annotation(
            text=message,
            x=0.5,
            y=0.5,
            xref="paper",
            yref="paper",
            showarrow=False,
            font=dict(
                size=14,
                color="red"
            )
        )
        
        # Empty axes but with fixed range to show something
        fig.update_layout(
            xaxis=dict(range=[0, 1], showticklabels=False),
            yaxis=dict(range=[0, 1], showticklabels=False),
            template="plotly_white",
            title="No Data Available"
        )
        
        logger.warning(f"Created empty visualization with message: {message}")
        return fig.to_dict()
    
    def create_hotspot_map(self, hotspot_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Create a hotspot visualization"""
        try:
            if not hotspot_data:
                logger.warning("No hotspot data provided for visualization")
                return {}
                
            # Convert hotspot data to DataFrame
            df = pd.DataFrame(hotspot_data)
            
            # Ensure required columns exist
            required_cols = ['latitude', 'longitude', 'density']
            if not all(col in df.columns for col in required_cols):
                logger.warning(f"Hotspot data missing required columns. Available: {df.columns.tolist()}")
                missing_cols = [col for col in required_cols if col not in df.columns]
                logger.warning(f"Missing columns: {missing_cols}")
                return {}
            
            # Create map
            fig = go.Figure()
            
            # Add hotspots
            fig.add_trace(go.Scattermapbox(
                lon=df['longitude'],
                lat=df['latitude'],
                mode='markers',
                marker=dict(
                    size=df['density'] / max(df['density']) * 20 + 5,  # Scale to reasonable size
                    color=df['density'],
                    colorscale='Viridis',
                    showscale=True,
                    colorbar=dict(title="Activity")
                ),
                text=[
                    f"Industry: {row.get('industry', 'Unknown')}<br>" +
                    f"Activity: {row.get('density', 0):.0f}<br>" +
                    f"Spending: ${row.get('total_spend', 0):.2f}"
                    for _, row in df.iterrows()
                ],
                hoverinfo='text',
                name='Hotspots'
            ))
            
            # Update layout
            fig.update_layout(
                mapbox=dict(
                    style='carto-positron',
                    zoom=9,
                    center=dict(
                        lat=df['latitude'].mean(),
                        lon=df['longitude'].mean()
                    )
                ),
                margin=dict(l=0, r=0, t=30, b=0),
                title="Tourism Activity Hotspots",
                showlegend=True
            )
            
            return fig.to_dict()
            
        except Exception as e:
            logger.error(f"Error creating hotspot map: {str(e)}")
            return {}
    
    def create_spatial_pattern_chart(self, pattern_data: Dict[str, Any], region_id: str) -> Dict[str, Any]:
        """Create a map showing spatial patterns of individual data points."""
        try:
            # The pattern_data might contain summary stats, but we need point data
            # Let's assume point_data is fetched separately or passed in
            # For now, we'll expect it within pattern_data under a 'points' key
            
            if not pattern_data or 'points' not in pattern_data or not pattern_data['points']:
                logger.warning(f"No spatial point data provided for visualization for region {region_id}")
                # Try to get center from summary stats if points are missing
                center_lat = pattern_data.get('central_latitude', 46.8)
                center_lon = pattern_data.get('central_longitude', 8.2)
                zoom = 7
                title = f"Spatial Patterns for {region_id} (No points data)"
                fig = go.Figure(go.Scattermapbox(lat=[center_lat], lon=[center_lon], mode='markers', marker=dict(size=5), text="Region Center (No Point Data)"))
            else:
                points_df = pd.DataFrame(pattern_data['points'])
                
                # Ensure required columns exist
                required_cols = ['latitude', 'longitude', 'txn_cnt', 'industry']
                if not all(col in points_df.columns for col in required_cols):
                    logger.warning(f"Spatial points data missing required columns for {region_id}. Have: {points_df.columns.tolist()}")
                    return {}
                
                # Clean data - ensure numeric types
                points_df['latitude'] = pd.to_numeric(points_df['latitude'], errors='coerce')
                points_df['longitude'] = pd.to_numeric(points_df['longitude'], errors='coerce')
                points_df['txn_cnt'] = pd.to_numeric(points_df['txn_cnt'], errors='coerce').fillna(0)
                points_df = points_df.dropna(subset=['latitude', 'longitude'])
                
                if points_df.empty:
                     logger.warning(f"No valid spatial points after cleaning for region {region_id}")
                     return {}

                center_lat = points_df['latitude'].mean()
                center_lon = points_df['longitude'].mean()
                zoom = 10 # Zoom in closer for point data
                title = f"Spatial Activity Patterns for {region_id}"

                # Create map using Scattermapbox
                fig = px.scatter_mapbox(
                    points_df,
                    lat='latitude',
                    lon='longitude',
                    color='industry',
                    size='txn_cnt', 
                    size_max=15, # Adjust max marker size
                    hover_name='industry',
                    hover_data={'latitude':':.4f', 'longitude':':.4f', 'txn_cnt': ':.0f'},
                    # Consider adding 'txn_amt' to hover_data if available
                    opacity=0.7
                )

            # Update layout
            fig.update_layout(
                mapbox=dict(
                    style='carto-positron',
                    zoom=zoom,
                    center=dict(lat=center_lat, lon=center_lon)
                ),
                margin=dict(l=0, r=0, t=30, b=0),
                title=title,
                legend_title_text='Industry'
            )
            
            return fig.to_dict()
            
        except Exception as e:
            logger.error(f"Error creating spatial pattern map for {region_id}: {str(e)}")
            logger.error(traceback.format_exc())
            return {}
    
    def create_region_comparison(self, regions_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Create a comparison visualization for multiple regions"""
        try:
            if not regions_data:
                logger.warning("No region data provided for comparison")
                return {}
                
            # Create comparison chart
            fig = go.Figure()
            
            # Extract relevant data for comparison
            region_names = [region.get('region_name', 'Unknown') for region in regions_data]
            swiss_tourists = [float(region.get('swiss_tourists', 0) or 0) for region in regions_data]
            foreign_tourists = [float(region.get('foreign_tourists', 0) or 0) for region in regions_data]
            total_spending = [float(region.get('total_spending', 0) or 0) for region in regions_data]
            
            # Add bars for tourists
            fig.add_trace(go.Bar(
                x=region_names,
                y=swiss_tourists,
                name='Domestic Tourists'
            ))
            
            fig.add_trace(go.Bar(
                x=region_names,
                y=foreign_tourists,
                name='International Tourists'
            ))
            
            # Add line for spending if available
            if any(total_spending):
                fig.add_trace(go.Scatter(
                    x=region_names,
                    y=total_spending,
                    mode='lines+markers',
                    name='Total Spending',
                    yaxis='y2'
                ))
            
            # Update layout
            fig.update_layout(
                title='Region Comparison',
                xaxis_title='Region',
                yaxis_title='Visitor Count',
                yaxis2=dict(
                    title='Total Spending',
                    overlaying='y',
                    side='right'
                ),
                barmode='group',
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="right",
                    x=1
                )
            )
            
            return fig.to_dict()
            
        except Exception as e:
            logger.error(f"Error creating region comparison: {str(e)}")
            return {}

    def create_visitor_distribution_chart(self, region_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Create a chart showing visitor distribution across regions"""
        try:
            if not region_data:
                logger.warning("No region data provided for visitor distribution")
                return None
                
            # Extract visitor data
            chart_data = {
                'type': 'bar_chart',
                'data': {
                    'labels': [region.get('region_name', 'Unknown') for region in region_data],
                    'datasets': [
                        {
                            'label': 'Domestic Tourists',
                            'data': [float(region.get('swiss_tourists', 0) or 0) for region in region_data],
                            'backgroundColor': 'rgba(54, 162, 235, 0.6)'
                        },
                        {
                            'label': 'International Tourists',
                            'data': [float(region.get('foreign_tourists', 0) or 0) for region in region_data],
                            'backgroundColor': 'rgba(255, 99, 132, 0.6)'
                        }
                    ]
                },
                'options': {
                    'title': {
                        'display': True,
                        'text': 'Visitor Distribution by Region'
                    },
                    'scales': {
                        'yAxes': [{
                            'ticks': {
                                'beginAtZero': True
                            },
                            'scaleLabel': {
                                'display': True,
                                'labelString': 'Number of Visitors'
                            }
                        }]
                    }
                }
            }
            
            return chart_data
            
        except Exception as e:
            logger.error(f"Error creating visitor distribution chart: {str(e)}")
            return None

    def create_industry_bounding_box_map(self, pattern_data: Dict[str, Any], region_id: str) -> Dict[str, Any]:
        """Create a map showing aggregated industry data with merged bounding boxes and color coding."""
        try:
            if not pattern_data or 'points' not in pattern_data or not pattern_data['points']:
                logger.warning(f"No spatial point data provided for industry bounding box visualization for region {region_id}")
                return {}
                
            points_df = pd.DataFrame(pattern_data['points'])
            
            # Ensure required columns exist
            required_cols = ['latitude', 'longitude', 'txn_cnt', 'industry']
            if not all(col in points_df.columns for col in required_cols):
                logger.warning(f"Spatial points data missing required columns for {region_id}. Have: {points_df.columns.tolist()}")
                return {}
            
            # Clean data - ensure numeric types
            points_df['latitude'] = pd.to_numeric(points_df['latitude'], errors='coerce')
            points_df['longitude'] = pd.to_numeric(points_df['longitude'], errors='coerce')
            points_df['txn_cnt'] = pd.to_numeric(points_df['txn_cnt'], errors='coerce').fillna(0)
            if 'txn_amt' in points_df.columns:
                points_df['txn_amt'] = pd.to_numeric(points_df['txn_amt'], errors='coerce').fillna(0)
            points_df = points_df.dropna(subset=['latitude', 'longitude'])
            
            if points_df.empty:
                logger.warning(f"No valid spatial points after cleaning for region {region_id}")
                return {}
            
            # Get center coordinates for the map
            center_lat = points_df['latitude'].mean()
            center_lon = points_df['longitude'].mean()
            
            # Create figure
            fig = go.Figure()
            
            # Group data by industry
            industries = points_df['industry'].unique()
            
            # Define a color scale for different industries
            colorscale = px.colors.qualitative.Plotly
            
            # For each industry, create a merged bounding box
            for i, industry in enumerate(industries):
                industry_color = colorscale[i % len(colorscale)]
                industry_points = points_df[points_df['industry'] == industry]
                
                if len(industry_points) < 3:  # Need at least 3 points for a polygon
                    # Add individual points if not enough for polygon
                    fig.add_trace(go.Scattermapbox(
                        lat=industry_points['latitude'].tolist(),
                        lon=industry_points['longitude'].tolist(),
                        mode='markers',
                        marker=dict(
                            size=10,
                            color=industry_color,
                            opacity=0.7
                        ),
                        name=industry,
                        hoverinfo='text',
                        hovertext=[f"Industry: {industry}<br>Transactions: {cnt:,.0f}" 
                                 for cnt in industry_points['txn_cnt']]
                    ))
                    continue
                
                # Create a convex hull (bounding polygon) for each industry
                try:
                    from scipy.spatial import ConvexHull
                    points = industry_points[['longitude', 'latitude']].values
                    hull = ConvexHull(points)
                    
                    # Get the points forming the hull
                    hull_points = points[hull.vertices]
                    
                    # Add a line trace for the hull boundary
                    fig.add_trace(go.Scattermapbox(
                        lat=hull_points[:, 1].tolist() + [hull_points[0, 1]],  # Close the polygon
                        lon=hull_points[:, 0].tolist() + [hull_points[0, 0]],  # Close the polygon
                        mode='lines',
                        line=dict(
                            width=2,
                            color=industry_color
                        ),
                        fill='toself',
                        fillcolor=self._rgb_to_rgba(industry_color),
                        name=industry,
                        hoverinfo='text',
                        hovertext=[f"Industry: {industry}" for _ in hull_points]
                    ))
                    
                    # Add the industry points inside the hull
                    fig.add_trace(go.Scattermapbox(
                        lat=industry_points['latitude'].tolist(),
                        lon=industry_points['longitude'].tolist(),
                        mode='markers',
                        marker=dict(
                            size=industry_points['txn_cnt'] / industry_points['txn_cnt'].max() * 15 + 5,
                            color=industry_color,
                            opacity=0.7
                        ),
                        name=industry + " Locations",
                        hoverinfo='text',
                        hovertext=[f"Industry: {industry}<br>Transactions: {cnt:,.0f}" 
                                 for cnt in industry_points['txn_cnt']],
                        showlegend=False
                    ))
                    
                except Exception as hull_error:
                    logger.warning(f"Could not create convex hull for industry {industry}: {str(hull_error)}")
                    # Fallback to simple points
                    fig.add_trace(go.Scattermapbox(
                        lat=industry_points['latitude'].tolist(),
                        lon=industry_points['longitude'].tolist(),
                        mode='markers',
                        marker=dict(
                            size=10,
                            color=industry_color,
                            opacity=0.7
                        ),
                        name=industry,
                        hoverinfo='text',
                        hovertext=[f"Industry: {industry}<br>Transactions: {cnt:,.0f}" 
                                 for cnt in industry_points['txn_cnt']]
                    ))
            
            # Add a center marker
            fig.add_trace(go.Scattermapbox(
                lat=[center_lat],
                lon=[center_lon],
                mode='markers',
                marker=dict(
                    size=15,
                    color='black',
                    symbol='circle'
                ),
                name='Region Center',
                hoverinfo='text',
                hovertext=[f"Region Center: {region_id}"]
            ))
            
            # Update layout
            fig.update_layout(
                mapbox=dict(
                    style='carto-positron',
                    zoom=10,
                    center=dict(lat=center_lat, lon=center_lon)
                ),
                margin=dict(l=0, r=0, t=30, b=0),
                title=f"Industry Distribution in {region_id}",
                legend_title_text='Industries',
                legend=dict(
                    orientation='h',
                    yanchor='bottom',
                    y=1.02,
                    xanchor='right',
                    x=1
                )
            )
            
            return fig.to_dict()
            
        except Exception as e:
            logger.error(f"Error creating industry bounding box map for {region_id}: {str(e)}")
            logger.error(traceback.format_exc())
            return {}

    def generate_temporal_visualization(self, insights: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate temporal visualization from insights."""
        try:
            if not insights:
                return None

            # Extract data for plotting
            months = [row['month'] for row in insights]
            total_visitors = [row['total_visitors'] for row in insights]
            swiss_tourists = [row['swiss_tourists'] for row in insights]
            foreign_tourists = [row['foreign_tourists'] for row in insights]
            total_spend = [row['total_spend'] for row in insights]

            # Create figure with two subplots
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8))

            # Plot visitors
            ax1.plot(months, total_visitors, 'b-', label='Total Visitors')
            ax1.plot(months, swiss_tourists, 'g--', label='Swiss Tourists')
            ax1.plot(months, foreign_tourists, 'r--', label='Foreign Tourists')
            ax1.set_title('Visitor Trends')
            ax1.set_xlabel('Month')
            ax1.set_ylabel('Number of Visitors')
            ax1.legend()
            ax1.grid(True)

            # Plot spending
            ax2.plot(months, total_spend, 'b-', label='Total Spending')
            ax2.set_title('Spending Trends')
            ax2.set_xlabel('Month')
            ax2.set_ylabel('Total Spend (CHF)')
            ax2.legend()
            ax2.grid(True)

            # Adjust layout
            plt.tight_layout()

            # Convert plot to base64 string
            buffer = BytesIO()
            plt.savefig(buffer, format='png')
            buffer.seek(0)
            image_png = buffer.getvalue()
            buffer.close()
            plt.close()

            # Encode the image
            graphic = base64.b64encode(image_png).decode('utf-8')

            return {
                "type": "image",
                "format": "png",
                "data": graphic
            }

        except Exception as e:
            logger.error(f"Error generating temporal visualization: {str(e)}")
            return None

    def create_visitor_comparison_map(self, regions_data: List[Dict[str, Any]], metric: str = "total_visitors") -> Dict[str, Any]:
        """Create a choropleth map comparing visitor metrics across regions in Ticino
        
        Args:
            regions_data: List of region dictionaries with visitor data
            metric: Which metric to visualize - "total_visitors", "swiss", "foreign", or "ratio"
            
        Returns:
            Plotly figure dictionary for visualization
        """
        try:
            if not regions_data:
                logger.warning("No region data provided for visitor comparison map")
                return self._create_empty_visualization("No region data provided")
            
            # Import GeoMapUtils for geojson handling
            from app.utils.geo_map_utils import GeoMapUtils
            geo_utils = GeoMapUtils()
            
            # Try to load Ticino-specific geojson first - prioritize our combined file
            ticino_paths = [
                "app/static/geojson/ticino_regions.geojson",
                "app/static/geojson/switzerland_cities.geojson",
                "app/static/geojson/ticinomap.geojson",
                "app/static/geojson/Bellinzona e Alto Ticino.geojson",
                "app/static/geojson/regions.json"
            ]
            
            geojson_data = None
            for path in ticino_paths:
                try:
                    geojson_data = geo_utils.load_geojson(path)
                    if geojson_data and geojson_data.get('features'):
                        logger.info(f"Successfully loaded Ticino geojson from {path}")
                        break
                except Exception as e:
                    logger.warning(f"Failed to load Ticino geojson from {path}: {str(e)}")
            
            # If we couldn't load Ticino-specific data, fall back to standard geojson loading
            if not geojson_data or not geojson_data.get('features'):
                # Extract region information for geojson
                region_type = regions_data[0].get('geo_type', '').lower()
                if not region_type:
                    region_type = regions_data[0].get('region_type', '').lower()
                    
                if not region_type:
                    region_type = 'canton'  # Default to canton if no region type specified
                
                # Transform region type for filename
                region_type_map = {
                    'state': 'cantons',
                    'canton': 'cantons',
                    'msa': 'cities', 
                    'city': 'cities',
                    'county': 'districts',
                    'district': 'districts'
                }
                
                file_region_type = region_type_map.get(region_type, 'cantons')
                
                # Load the appropriate geojson based on region type
                geojson_path = f"app/static/geojson/switzerland_{file_region_type}.geojson"
                
                # Try to load geojson
                try:
                    geojson_data = geo_utils.load_geojson(geojson_path)
                    logger.info(f"Successfully loaded geojson from {geojson_path}")
                except Exception as e:
                    logger.warning(f"Failed to load geojson from {geojson_path}: {str(e)}")
                    # Try a fallback geojson
                    fallback_path = "app/static/geojson/switzerland_cantons.geojson"
                    try:
                        geojson_data = geo_utils.load_geojson(fallback_path)
                        logger.info(f"Successfully loaded fallback geojson from {fallback_path}")
                    except Exception as e2:
                        logger.error(f"Failed to load fallback geojson: {str(e2)}")
                        return self._create_empty_visualization("Could not load geographic boundaries")
            
            # Use the geo_map_utils to create the choropleth map
            map_data = geo_utils.create_visitor_comparison_map(geojson_data, regions_data, metric)
            
            if not map_data:
                logger.warning("Failed to create visitor comparison map")
                return self._create_empty_visualization("Failed to create choropleth map")
                
            return map_data
            
        except Exception as e:
            logger.error(f"Error creating visitor comparison map: {str(e)}")
            logger.error(traceback.format_exc())
            return self._create_empty_visualization(f"Error creating comparison map: {str(e)}") 