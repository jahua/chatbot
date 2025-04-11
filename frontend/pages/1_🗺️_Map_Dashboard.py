import streamlit as st
import geopandas as gpd
import folium
import os
import psycopg2
from psycopg2 import sql
from streamlit_folium import st_folium
import pandas as pd
import folium.plugins as plugins
from dotenv import load_dotenv
import plotly.graph_objects as go
import plotly.express as px
import json
import warnings
import numpy as np
from shapely.geometry import box, MultiPolygon
import re

# Load environment variables
load_dotenv()

# Set page configuration
st.set_page_config(
    page_title="Ticino Geo Insights Dashboard",
    page_icon="🗺️",
    layout="wide"
)

# Database connection parameters
DB_HOST = os.getenv("DB_HOST", "3.76.40.121")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "trip_dw")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "336699")

# Add custom CSS for better styling
st.markdown("""
<style>
    /* Modern UI styles */
    h1 {
        margin-bottom: 0.5rem;
        color: #1E3D59;
    }
    h2 {
        margin-top: 1.5rem;
        color: #2E5984;
    }
    .stButton>button {
        background-color: #2E5984;
        color: white;
        border-radius: 4px;
    }
    .stButton>button:hover {
        background-color: #1E3D59;
        color: white;
    }
    .custom-info-box {
        padding: 15px;
        background-color: #f8f9fa;
        border-left: 4px solid #2E5984;
        border-radius: 0 5px 5px 0;
        margin-bottom: 20px;
    }
    .stat-card {
        background-color: white;
        border-radius: 5px;
        padding: 15px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        margin-bottom: 15px;
    }
    [data-testid="stMetricValue"] {
        font-weight: bold !important;
        color: #1E3D59 !important;
    }
    .view-selector {
        background-color: #f8f9fa;
        padding: 15px;
        border-radius: 5px;
        margin-bottom: 20px;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    .stTabs [data-baseweb="tab"] {
        /* background-color: #f1f3f6; */ /* Removed custom background */
        border-radius: 4px 4px 0 0;
    }
</style>
""", unsafe_allow_html=True)

def connect_to_db():
    """Establish connection to PostgreSQL database"""
    try:
        # Create the connection
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        
        # Set up cursor and schema permissions
        cursor = conn.cursor()
        cursor.execute('SET search_path TO geo_insights, public;')
        return conn, cursor
    except psycopg2.Error as e:
        st.error(f"Database connection error: {str(e)}")
        return None, None

def load_shapefile_data():
    """Load data from Ticino shapefile or create fallback data"""
    try:
        # Get the script's directory
        script_dir = os.path.dirname(os.path.abspath(__file__))
        # Construct path relative to the script location (up two levels, then into app/static...)
        # Assumes structure: project_root/frontend/pages/script.py and project_root/app/static/...
        project_root_guess = os.path.join(script_dir, '..', '..')
        shapefile_path = os.path.join(project_root_guess, 'app', 'static', 'geojson', 'shapes', 'g1b23.shp')
        
        # Normalize the path (optional, but good practice)
        shapefile_path = os.path.normpath(shapefile_path)
        
        st.sidebar.info(f"Attempting to load shapefile from: {shapefile_path}")
        
        # Check if file exists
        if os.path.exists(shapefile_path):
            try:
                # Load with explicit fiona engine
                gdf = gpd.read_file(shapefile_path, engine="fiona")
                
                # Ensure the CRS is WGS84 (EPSG:4326) for Folium
                if gdf.crs and gdf.crs != "EPSG:4326":
                    gdf = gdf.to_crs(epsg=4326)
                
                # --- Filter for Ticino (KTNR = 21) ---
                if 'KTNR' in gdf.columns:
                    original_count = len(gdf)
                    gdf = gdf[gdf['KTNR'] == 21]
                    filtered_count = len(gdf)
                    st.sidebar.info(f"Filtered shapefile for Ticino (KTNR=21). Kept {filtered_count}/{original_count} regions.")
                    if filtered_count == 0:
                        st.sidebar.error("Filtering for KTNR=21 resulted in 0 features. Check shapefile KTNR column.")
                else:
                    st.sidebar.warning("'KTNR' column not found in shapefile. Cannot filter for Ticino.")
                # --- End Filter ---

                return gdf
            except Exception as e:
                st.sidebar.error(f"Error loading shapefile: {str(e)}")
                return create_fallback_data()
        else:
            st.sidebar.error(f"Shapefile not found at the calculated path.")
            return create_fallback_data()
    except Exception as e: # Catch errors during path calculation etc.
        st.error(f"An unexpected error occurred while locating shapefile data: {e}")
        return create_fallback_data()

def create_fallback_data():
    """Create fallback simplified Ticino boundary"""
    st.sidebar.warning("Using simplified Ticino boundaries")
    from shapely.geometry import Polygon
    
    # Approximate simplified Ticino boundary with more detailed polygon
    ticino_polygon = Polygon([
        (8.4, 46.4), (8.6, 46.5), (8.8, 46.5), (9.0, 46.4),
        (9.1, 46.3), (9.0, 46.1), (8.9, 46.0), (8.8, 45.9),
        (8.6, 45.8), (8.5, 45.8), (8.4, 45.9), (8.3, 46.0),
        (8.3, 46.2), (8.4, 46.4)
    ])
    
    # Create 5-7 smaller regions within Ticino for more detailed visualization
    regions = [
        {
            "name": "Bellinzona",
            "geometry": Polygon([
                (8.9, 46.2), (9.0, 46.3), (9.1, 46.2), (9.0, 46.1), (8.9, 46.2)
            ])
        },
        {
            "name": "Locarno",
            "geometry": Polygon([
                (8.7, 46.2), (8.8, 46.3), (8.9, 46.2), (8.8, 46.1), (8.7, 46.2)
            ])
        },
        {
            "name": "Lugano",
            "geometry": Polygon([
                (8.9, 46.0), (9.0, 46.1), (9.0, 45.9), (8.9, 45.9), (8.9, 46.0)
            ])
        },
        {
            "name": "Mendrisio",
            "geometry": Polygon([
                (8.8, 45.9), (8.9, 45.9), (8.9, 45.8), (8.8, 45.8), (8.8, 45.9)
            ])
        },
        {
            "name": "Blenio",
            "geometry": Polygon([
                (8.8, 46.4), (8.9, 46.5), (9.0, 46.4), (8.9, 46.3), (8.8, 46.4)
            ])
        },
        {
            "name": "Leventina",
            "geometry": Polygon([
                (8.6, 46.3), (8.7, 46.4), (8.8, 46.3), (8.7, 46.2), (8.6, 46.3)
            ])
        }
    ]
    
    # Create GeoDataFrame with the regions
    geometries = [region["geometry"] for region in regions]
    names = [region["name"] for region in regions]
    
    gdf = gpd.GeoDataFrame(
        {"BZNAME": names, "AREA_HA": [50000] * len(regions)}, 
        geometry=geometries,
        crs="EPSG:4326"
    )
    
    return gdf

def load_view_data(view_name):
    """Load data from a specific view in the database"""
    conn, cursor = connect_to_db()
    if conn and cursor:
        try:
            query = sql.SQL("""
                SELECT * FROM geo_insights.{}
            """).format(sql.Identifier(view_name))
            
            # Use pandas read_sql for better type handling
            df = pd.read_sql(query.as_string(conn), conn)
            
            return df
        except Exception as e:
            st.error(f"Error loading data from {view_name}: {str(e)}")
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    return pd.DataFrame()

def create_choropleth_map(gdf, data_df, region_col, value_col, title, color_scale='YlOrRd'):
    """Create a choropleth map by adding database values to the GeoJSON"""
    if gdf.empty or data_df.empty:
        st.warning("No data available to display for the map.")
        return None
    
    # Calculate the bounding box for map centering
    bounds = gdf.total_bounds
    center_lat = (bounds[1] + bounds[3]) / 2
    center_lon = (bounds[0] + bounds[2]) / 2
    
    # Create base map
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=9,
        tiles="CartoDB positron"
    )
    
    # Enhanced name normalization for better matching
    def normalize_name(name):
        """Normalize municipality/district names for better matching"""
        if not isinstance(name, str):
            return ""
        # Convert to lowercase and strip whitespace
        name = name.lower().strip()

        # 1. Handle accents/umlauts
        accent_map = {
            'ü': 'u', 'é': 'e', 'è': 'e', 'ê': 'e', 'à': 'a', 
            'á': 'a', 'â': 'a', 'ô': 'o', 'ö': 'o', 'ä': 'a', 
            'ç': 'c', 'í': 'i', 'î': 'i', 'ï': 'i', 'ù': 'u', 
            'ú': 'u', 'û': 'u'
        }
        for char, replacement in accent_map.items():
            name = name.replace(char, replacement)

        # 2. Remove specific administrative prefixes
        prefixes_to_remove = [
            "bezirk ",
            "arrondissement administratif ",
            "verwaltungskreis ",
            "distretto di ",
            "comune di ",
            "municipio di "
        ]
        for prefix in prefixes_to_remove:
            if name.startswith(prefix):
                name = name[len(prefix):]
                break # Assume only one prefix needs removing

        # 3. Remove content in parentheses
        name = re.sub(r'\s*\(.*\)\s*', '', name).strip()

        # 4. Remove general special characters (hyphen, apostrophe, period)
        name = name.replace("-", " ").replace("'", "").replace(".", "")
        
        # 5. Replace multiple spaces with single space
        name = " ".join(name.split())
        
        # 6. Common specific name variations (apply after prefix removal)
        replacements = {
            "saint": "st",
            "santo": "st",
            "santa": "st",
            "san": "st",
            "monte": "mt",
            "valle": "val"
        }
        name_parts = name.split()
        for i, part in enumerate(name_parts):
            if part in replacements:
                name_parts[i] = replacements[part]
        name = " ".join(name_parts)
        
        # 7. Handle specific cases like Biel/Bienne if necessary
        if name == "biel bienne":
            name = "biel/bienne"
        
        return name.strip()
    
    # Apply name normalization to both datasets
    gdf['name_normalized'] = gdf['BZNAME'].apply(normalize_name)
    data_df['name_normalized'] = data_df[region_col].apply(normalize_name)
    
    # Create a dictionary mapping from normalized name to value
    value_map = {row['name_normalized']: row[value_col] for _, row in data_df.iterrows()}
    
    # Add the value column directly to the GeoDataFrame
    gdf[value_col] = gdf['name_normalized'].map(value_map)
    
    # Count how many regions were mapped
    mapped_count = gdf[value_col].notna().sum()
    st.write(f"Successfully mapped {mapped_count} out of {len(gdf)} regions")
    
    # Calculate statistics for better color scaling
    valid_values = gdf[value_col].dropna()
    
    # --- Check for edge case: all valid values are identical ---
    bins_for_folium = None
    if not valid_values.empty:
        min_val = valid_values.min()
        max_val = valid_values.max()
        if abs(min_val - max_val) < 1e-9: # Check if min and max are effectively the same
            # Create minimal bins just to satisfy Folium
            range_size = abs(min_val) * 0.01 if min_val != 0 else 0.01 # Ensure non-zero range
            bins_for_folium = sorted([min_val - range_size, min_val, min_val + range_size])

    # Define custom color scales based on the metric type
    color_scales = {
        'total_spend': 'Greens',
        'spend_per_hectare': 'RdYlGn',
        'spend_per_visitor': 'YlOrRd',
        'swiss_tourists': 'Blues',
        'foreign_tourists': 'Purples',
        'total_visitors': 'YlOrRd',
        'foreign_tourist_percentage': 'RdYlBu',
        'industry_count': 'Spectral',
        'avg_transaction_value': 'viridis'
    }

    # Use appropriate color scale based on the metric
    selected_scale = color_scales.get(value_col, color_scale)

    # Create the choropleth arguments dictionary
    choropleth_args = {
        'geo_data': gdf.__geo_interface__,
        'name': title,
        'data': gdf,
        'columns': ['BZNAME', value_col],
        'key_on': 'feature.properties.BZNAME',
        'fill_color': selected_scale,
        'fill_opacity': 0.7,
        'line_opacity': 0.2,
        'line_weight': 2,
        'legend_name': value_col.replace('_', ' ').title(),
        'highlight': True,
        'smooth_factor': 0.5,
        'nan_fill_color': 'lightgray',
        'nan_fill_opacity': 0.4
    }

    # Only add 'bins' argument if we created artificial bins for the edge case
    if bins_for_folium:
        choropleth_args['bins'] = bins_for_folium

    # Create the choropleth with enhanced styling
    choropleth = folium.Choropleth(**choropleth_args).add_to(m)

    # Add tooltips with enhanced information
    # Ensure tooltips handle potential NaN values gracefully
    gdf['tooltip_value'] = gdf[value_col].apply(lambda x: f'{x:,.2f}' if pd.notna(x) else 'No data')
    folium.GeoJson(
        data=gdf.__geo_interface__, # Use geo_interface for potential compatibility
        name="Labels",
        style_function=lambda x: {
            'fillColor': 'transparent',
            'color': 'transparent',
            'weight': 1,
            'fillOpacity': 0.0,
            'dashArray': '3, 5'
        },
        highlight_function=lambda x: {
            'weight': 3,
            'fillOpacity': 0.0,
            'color': '#666666'
        },
        tooltip=folium.GeoJsonTooltip(
            fields=['BZNAME', 'tooltip_value'], # Use the formatted value
            aliases=['Region:', f'{value_col.replace("_", " ").title()}:'],
            style="""
                background-color: white;
                color: #333333;
                font-family: arial;
                font-size: 12px;
                padding: 10px;
                border: 1px solid #cccccc;
                border-radius: 3px;
                box-shadow: 3px 3px 3px rgba(0,0,0,0.1);
            """,
            sticky=True
        )
    ).add_to(m)

    # Add a legend with more information
    # Fetch stats again for legend display
    min_display = valid_values.min() if not valid_values.empty else 'N/A'
    mean_display = valid_values.mean() if not valid_values.empty else 'N/A'
    max_display = valid_values.max() if not valid_values.empty else 'N/A'
    std_display = valid_values.std() if not valid_values.empty else 'N/A'

    min_text = f'{min_display:,.2f}' if isinstance(min_display, (int, float)) else min_display
    mean_text = f'{mean_display:,.2f}' if isinstance(mean_display, (int, float)) else mean_display
    max_text = f'{max_display:,.2f}' if isinstance(max_display, (int, float)) else max_display
    std_text = f'{std_display:,.2f}' if isinstance(std_display, (int, float)) else std_display

    legend_html = f'''
    <div style="position: fixed; 
                bottom: 50px; right: 50px; width: 250px;
                border: 2px solid grey; z-index: 9999;
                background-color: white; padding: 10px;
                font-size: 14px;">
        <p><b>{title}</b></p>
        <p>Regions mapped: {mapped_count}/{len(gdf)}</p>
        <p>Min value: {min_text}</p>
        <p>Mean value: {mean_text}</p>
        <p>Max value: {max_text}</p>
        <p>Std Dev: {std_text}</p>
        <hr>
        <p style="color: #666666;"><small>Light gray areas indicate no data</small></p>
        <p style="color: #666666;"><small>Colors based on data distribution (auto)</small></p>
    </div>
    '''
    m.get_root().html.add_child(folium.Element(legend_html))
    
    # Add layer control and fullscreen button
    folium.LayerControl().add_to(m)
    plugins.Fullscreen().add_to(m)
    
    # Add measure tool for distance calculations
    plugins.MeasureControl(
        position='topleft',
        primary_length_unit='meters',
        secondary_length_unit='kilometers',
        primary_area_unit='sqmeters',
        secondary_area_unit='hectares'
    ).add_to(m)
    
    return m

def create_heatmap(df, lat_col, lon_col, value_col=None, radius=15):
    """Create a heatmap from point data"""
    # Calculate the bounding box for map centering
    center_lat = df[lat_col].mean()
    center_lon = df[lon_col].mean()
    
    # Create base map
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=9,
        tiles="CartoDB positron"
    )
    
    # Prepare data for heatmap
    heat_data = [[row[lat_col], row[lon_col], row.get(value_col, 1)] 
                for _, row in df.iterrows() if pd.notna(row[lat_col]) and pd.notna(row[lon_col])]
    
    # Add heatmap layer
    plugins.HeatMap(
        heat_data,
        radius=radius,
        blur=15,
        gradient={'0.4': 'blue', '0.65': 'lime', '1.0': 'red'} # Use string keys
    ).add_to(m)
    
    # Add fullscreen control
    plugins.Fullscreen().add_to(m)
    
    return m

def create_cluster_map(df, lat_col, lon_col, popup_cols=None):
    """Create a map with clustered markers"""
    # Calculate the bounding box for map centering
    center_lat = df[lat_col].mean()
    center_lon = df[lon_col].mean()
    
    # Create base map
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=9,
        tiles="CartoDB positron"
    )
    
    # Create marker cluster
    marker_cluster = plugins.MarkerCluster().add_to(m)
    
    # Add markers to the cluster
    for idx, row in df.iterrows():
        if pd.notna(row[lat_col]) and pd.notna(row[lon_col]):
            # Create popup content
            popup_content = ""
            if popup_cols:
                popup_content = "<div style='width: 200px; font-family: Arial, sans-serif; font-size: 12px;'>"
                for col in popup_cols:
                    if col in row and pd.notna(row[col]):
                        value = row[col]
                        # Format numeric values
                        if isinstance(value, (int, float)):
                            if col.lower().endswith(('count', 'tourists', 'visitors', 'points')):
                                value = f"{int(value):,}"
                            elif col.lower().endswith(('spend', 'value')):
                                value = f"CHF {value:,.2f}"
                            elif col.lower() == 'density':
                                value = f"{value:.2f}"
                            else:
                                value = f"{value:,.2f}" # Default numeric format
                        popup_content += f"<b>{col.replace('_', ' ').title()}:</b> {value}<br>"
                popup_content += "</div>"
            
            # Add marker with popup
            folium.Marker(
                location=[row[lat_col], row[lon_col]],
                popup=folium.Popup(popup_content, max_width=300) if popup_content else None,
                icon=folium.Icon(icon="info-sign", color="cadetblue")
            ).add_to(marker_cluster)
    
    # Add fullscreen control
    plugins.Fullscreen().add_to(m)
    
    return m

def create_industry_hotspot_map(df, region_gdf):
    """Create industry hotspot map with multiple layers"""
    # Calculate the bounding box for map centering
    center_lat = df['latitude'].mean()
    center_lon = df['longitude'].mean()
    
    # Create base map
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=9,
        tiles="CartoDB positron"
    )
    
    # Add Ticino region boundaries
    folium.GeoJson(
        region_gdf,
        name="Region Boundaries",
        style_function=lambda x: {
            'fillColor': 'transparent',
            'color': '#3388ff',
            'weight': 2,
            'fillOpacity': 0.1
        },
        tooltip=folium.GeoJsonTooltip(
            fields=['BZNAME'],
            aliases=['Region:'],
            style="background-color: white; color: #333333; font-family: arial; font-size: 12px;"
        )
    ).add_to(m)
    
    # Get unique industries
    industries = df['industry'].unique()
    
    # Create a color map for industries
    import random
    colors = px.colors.qualitative.Plotly # Use Plotly colors
    color_map = {ind: colors[i % len(colors)] for i, ind in enumerate(industries)}
    
    # Create feature groups for each industry
    for industry in industries:
        industry_data = df[df['industry'] == industry]
        
        # Create feature group
        feature_group = folium.FeatureGroup(name=f"{industry}")
        
        # Add circle markers for each point
        for idx, row in industry_data.iterrows():
            # Create popup content
            popup_content = f"""
            <div style="font-family: Arial; font-size: 12px; width: 200px;">
                <b>{row['geo_name']}</b><br>
                <b>Industry:</b> {row['industry']}<br>
                <b>Density:</b> {row['density']:.2f}<br>
                <b>Total Spend:</b> CHF {row['total_spend']:,.2f}<br>
                <b>Points:</b> {int(row['point_count']):,}
            </div>
            """
            
            # Add circle marker
            folium.CircleMarker(
                location=[row['latitude'], row['longitude']],
                radius=min(15, max(3, int(row['density'] * 5))),  # Adjusted scaling
                color=color_map[industry],
                fill=True,
                fill_color=color_map[industry],
                fill_opacity=0.7,
                popup=folium.Popup(popup_content, max_width=300)
            ).add_to(feature_group)
        
        # Add feature group to map
        feature_group.add_to(m)
    
    # Add layer control and fullscreen button
    folium.LayerControl().add_to(m)
    plugins.Fullscreen().add_to(m)
    
    return m

def create_time_series_map(df, region_gdf, time_col, value_col, region_col):
    """Create a time series map with slider"""
    # Calculate the bounding box for map centering
    bounds = region_gdf.total_bounds
    center_lat = (bounds[1] + bounds[3]) / 2
    center_lon = (bounds[0] + bounds[2]) / 2
    
    # Create base map
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=9,
        tiles="CartoDB positron"
    )
    
    # Get unique time periods
    time_periods = df[time_col].unique()
    time_periods.sort()
    
    # Create a feature group for each time period
    for period in time_periods:
        period_data = df[df[time_col] == period]
        
        # Create a feature group
        feature_group = folium.FeatureGroup(name=f"{period}", show=False)
        
        # Create a mapping between regions in the shapefile and the database
        region_gdf['name_lower'] = region_gdf['BZNAME'].str.lower().str.strip()
        period_data['name_lower'] = period_data[region_col].str.lower().str.strip()
        
        # Create a dictionary mapping from name to value
        value_map = {row['name_lower']: row[value_col] for _, row in period_data.iterrows()}
        
        # Add the value column directly to the GeoDataFrame
        temp_gdf = region_gdf.copy()
        temp_gdf[value_col] = temp_gdf['name_lower'].map(value_map)
        
        # Create choropleth for this time period
        choropleth = folium.Choropleth(
            geo_data=temp_gdf,
            name=f"{period}",
            data=temp_gdf,
            columns=['BZNAME', value_col],
            key_on='feature.properties.BZNAME',
            fill_color='YlOrRd',
            fill_opacity=0.7,
            line_opacity=0.2,
            legend_name=f"{value_col.replace('_', ' ').title()} ({period})",
            highlight=True,
            smooth_factor=0.5
        ).add_to(feature_group)
        
        # Add tooltips
        folium.GeoJson(
            data=temp_gdf,
            name=f"Labels {period}",
            style_function=lambda x: {'fillColor': 'transparent', 'color': 'transparent'},
            tooltip=folium.GeoJsonTooltip(
                fields=['BZNAME', value_col],
                aliases=['Region:', f'{value_col.replace("_", " ").title()}:'],
                style="""
                    background-color: white;
                    color: #333333;
                    font-family: arial;
                    font-size: 12px;
                    padding: 10px;
                """
            )
        ).add_to(feature_group)
        
        # Add feature group to map
        feature_group.add_to(m)
    
    # Set the first time period to be visible by default
    # Accessing features dictionary is brittle, maybe find another way or handle error
    try:
        first_period_key = list(m.get_root().children['features'].children.keys())[0]
        m.get_root().children['features'].children[first_period_key].options.update({'show': True})
    except Exception as e:
        st.warning(f"Could not set default visible layer for time slider: {e}")
    
    # Add time slider control - Placeholder as TimestampedGeoJson often needs specific data format
    st.warning("Time slider map is experimental and may require specific data formatting.")
    # plugins.TimestampedGeoJson(...).add_to(m) # Requires correctly formatted GeoJSON
    
    # Add layer control and fullscreen button
    folium.LayerControl().add_to(m)
    plugins.Fullscreen().add_to(m)
    
    return m

def create_plotly_map(df, lat_col, lon_col, color_col, size_col=None, hover_cols=None):
    """Create an interactive plotly scatter map"""
    # Convert to numeric if needed
    df[lat_col] = pd.to_numeric(df[lat_col], errors='coerce')
    df[lon_col] = pd.to_numeric(df[lon_col], errors='coerce')
    
    # Drop rows with missing coordinates
    df = df.dropna(subset=[lat_col, lon_col])
    
    # Set up hover data
    hover_data = {}
    if hover_cols:
        for col in hover_cols:
            if col in df.columns:
                # Use the original column name as the key
                hover_data[col] = ':,.2f' if df[col].dtype in ['float64', 'int64'] else True 
    
    # Create the map
    fig = px.scatter_mapbox(
        df, 
        lat=lat_col, 
        lon=lon_col, 
        color=color_col,
        size=size_col if size_col else None,
        hover_name=df.columns[1] if len(df.columns) > 1 else None,  # Use second column as label
        hover_data=hover_data,
        zoom=8,
        height=600,
        # width=1000, # Use container width
        color_continuous_scale=px.colors.sequential.Viridis,
        size_max=15 # Control max bubble size
    )
    
    # Update map layout
    fig.update_layout(
        mapbox_style="carto-positron",
        margin={"r":0,"t":30,"l":0,"b":0}, # Add top margin for title
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01
        ),
        title=f"{color_col.replace('_', ' ').title()} by Location" # Add a title
    )
    
    return fig

def create_analytics_choropleth(metric='total_spend', title=None):
    """
    Create a choropleth map using data from the choropleth_analytics view.
    This function now handles data loading and calls create_choropleth_map.
    """
    # Load shapefile data (should be filtered for Ticino)
    gdf = load_shapefile_data()
    if gdf is None or gdf.empty:
        st.error("Could not load Ticino shapefile data for the map.")
        return None
        
    # Load data from choropleth_analytics view
    df = load_view_data("choropleth_analytics")
    if df.empty:
        st.error("No data available from choropleth_analytics view.")
        return None
            
    # Set default color scales for different metrics
    color_scales = {
        'total_spend': 'Greens',
        'swiss_tourists': 'Blues',
        'foreign_tourists': 'Purples',
        'total_visitors': 'YlOrRd',
        'foreign_tourist_percentage': 'RdYlBu',
        'avg_transaction_value': 'Greens',
        'daily_spend': 'YlOrRd',
        'spend_per_visitor': 'RdYlGn',
        'industry_count': 'Spectral'
    }
    
    # Set default titles for different metrics
    metric_titles = {
        'total_spend': 'Total Spending (CHF)',
        'swiss_tourists': 'Swiss Tourist Count',
        'foreign_tourists': 'Foreign Tourist Count',
        'total_visitors': 'Total Visitors',
        'foreign_tourist_percentage': 'Foreign Tourist Percentage (%)',
                'avg_transaction_value': 'Average Transaction Value (CHF)',
        'daily_spend': 'Daily Spending (CHF)',
        'spend_per_visitor': 'Spending per Visitor (CHF)',
        'industry_count': 'Number of Industries'
    }
    
    # Create the choropleth map
    map_title = title or metric_titles.get(metric, metric)
    color_scale = color_scales.get(metric, 'YlOrRd')
    
    # Ensure the metric column exists in the dataframe
    if metric not in df.columns:
        st.error(f"Metric '{metric}' not found in choropleth_analytics data. Available: {df.columns.tolist()}")
        return None

    # Call the main map creation function
    choropleth_map = create_choropleth_map(
        gdf,
        df,
        'region_name', # Column in df that should match BZNAME in gdf
        metric,
        map_title,
        color_scale=color_scale
    )
    
    return choropleth_map
            
# Renamed function for clarity
def display_choropleth_map_view(): 
    """Display the main choropleth map visualization based on choropleth_analytics"""
    st.header("Regional Metrics Map")
    st.markdown("Visualize various metrics across Ticino regions.")
    
    # Use columns for better layout
    col1, col2 = st.columns([1, 3]) # Ratio for sidebar-like controls

    with col1:
        st.subheader("Map Options")
        # Define available metrics for this specific view
        metrics = {
            'Total Spend (CHF)': 'total_spend',
            'Swiss Tourists': 'swiss_tourists',
            'Foreign Tourists': 'foreign_tourists',
            'Total Visitors': 'total_visitors',
            'Foreign Tourist %': 'foreign_tourist_percentage',
            'Avg Transaction (CHF)': 'avg_transaction_value',
            'Daily Spend (CHF)': 'daily_spend',
            'Spend per Visitor (CHF)': 'spend_per_visitor',
            'Number of Industries': 'industry_count'
        }
        selected_metric_display = st.selectbox("Select Metric for Map", list(metrics.keys()), key='choropleth_metric')
        metric_col = metrics[selected_metric_display]

    # Load the data needed for metrics display (even if map fails)
    data_df = load_view_data("choropleth_analytics")
    
    with col1:
        if not data_df.empty:
            st.subheader("Summary Statistics")
            if metric_col in data_df.columns:
                st.metric("Average", f"{data_df[metric_col].mean():,.2f}")
                st.metric("Median", f"{data_df[metric_col].median():,.2f}")
                st.metric("Min", f"{data_df[metric_col].min():,.2f}")
                st.metric("Max", f"{data_df[metric_col].max():,.2f}")
            else:
                st.warning(f"Metric '{metric_col}' not found for stats.")
        else:
            st.warning("No data for statistics.")

    with col2:
        # Create and display the choropleth map
        with st.spinner("Generating map..."):
            choropleth_map = create_analytics_choropleth(
                metric=metric_col,
                title=f"{selected_metric_display} by Region"
            )
        
        if choropleth_map:
            # Use st_folium for better interactivity
            st_folium(choropleth_map, width=800, height=600, returned_objects=[])
        else:
            st.error("Map could not be generated.")

        # Show top regions table below the map
        if not data_df.empty and metric_col in data_df.columns:
            st.subheader("Top Regions by Selected Metric")
            # Ensure region_name exists before using it
            region_name_col = 'region_name' if 'region_name' in data_df.columns else data_df.columns[0] # Fallback if needed
            top_regions = data_df.nlargest(5, metric_col)[[region_name_col, metric_col]]
            top_regions.columns = ['Region', selected_metric_display]
            st.dataframe(top_regions, use_container_width=True)
        
            # Show distribution plot
            st.subheader(f"Distribution of {selected_metric_display}")
            fig = px.histogram(data_df, x=metric_col, nbins=10, # Reduced bins for clarity
                             title=f"Distribution of {selected_metric_display}")
            fig.update_layout(bargap=0.1)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No data available for table and distribution plot.")

def display_region_centers():
    """Display region centers visualizations"""
    st.header("Region Centers Analysis")
    st.markdown("Explore the central points calculated for different geographic regions.")

    # Load data
    df = load_view_data("region_centers")
    # region_gdf = load_shapefile_data() # Not needed for this view
    
    if df.empty:
        st.error("Could not load region_centers data.")
        return
    
    # Use columns for layout
    col1, col2 = st.columns([1, 3])
    
    # --- Column 1: Filters and Stats --- 
    with col1:
        st.subheader("Options & Stats")
        # Add filters
        geo_types = ["All"] + sorted(df['geo_type'].unique().tolist())
        selected_geo_type = st.selectbox(
            "Filter by Geography Type", 
            options=geo_types,
            key="centers_geo_type"
        )
        
        # Filter data based on selection
        filtered_df = df.copy()
        if selected_geo_type != "All":
            filtered_df = filtered_df[filtered_df['geo_type'] == selected_geo_type]
        
        # Display stats for the filtered data
        if not filtered_df.empty:
            # Dashboard metrics for filtered data
            st.metric("Total Centers Displayed", len(filtered_df))
            st.metric("Average Points per Center", f"{int(filtered_df['point_count'].mean()):,}")
            st.metric("Total Points in Selection", f"{int(filtered_df['point_count'].sum()):,}")
        else:
            st.warning("No centers match the current filter.")
    
    # --- Column 2: Maps and Plots --- 
    with col2:
        if not filtered_df.empty:
            # Tabs for different visualizations
            tab1, tab2 = st.tabs(["Center Map (Clusters)", "Point Distribution Plot"])
            
            with tab1:
                # Create map for region centers
                st.subheader("Region Centers Map")
                
                # Create cluster map
                with st.spinner("Generating cluster map..."):
                    centers_map = create_cluster_map(
                        filtered_df,
                        'avg_lat',
                        'avg_lon',
                        popup_cols=['geo_name', 'geo_type', 'point_count']
                    )
                
                if centers_map:
                    st_folium(centers_map, width=800, height=500, returned_objects=[])
                else:
                    st.warning("Could not create map with the selected data.")
                
                # Add heatmap option
                if st.checkbox("Show Heatmap (Based on Point Count)", key="center_heatmap"):
                    with st.spinner("Generating heatmap..."):
                        heatmap = create_heatmap(
                            filtered_df,
                            'avg_lat',
                            'avg_lon',
                            'point_count',
                            radius=20 # Slightly larger radius
                        )
                    
                    if heatmap:
                        st_folium(heatmap, width=800, height=500, returned_objects=[])
            
            with tab2:
                # Point distribution visualization
                st.subheader("Points per Center Distribution")
                
                # Sort by point count for bar chart
                sorted_df = filtered_df.sort_values('point_count', ascending=False).head(20)
                
                # Create bar chart
                fig_bar = px.bar(
                    sorted_df,
                    x='geo_name',
                    y='point_count',
                    color='geo_type',
                    labels={
                        'geo_name': 'Region Name',
                        'point_count': 'Number of Associated Points',
                        'geo_type': 'Geography Type'
                    },
                    title="Top 20 Centers by Associated Point Count",
                    height=500
                )
                fig_bar.update_layout(xaxis_title="", yaxis_title="Point Count")
                st.plotly_chart(fig_bar, use_container_width=True)
                
                # Create bubble map using plotly
                st.subheader("Geographic Distribution (Bubble Map)")
                with st.spinner("Generating bubble map..."):
                    fig_bubble = create_plotly_map(
                        filtered_df,
                        'avg_lat',
                        'avg_lon',
                        'geo_type',
                        'point_count',
                        hover_cols=['geo_name', 'point_count']
                    )
                st.plotly_chart(fig_bubble, use_container_width=True)
        else:
            st.info("No data available to display based on the current filter.")

# --- REMOVED UNUSED DISPLAY FUNCTIONS ---
# display_region_summary, display_spatial_patterns, display_region_monthly_stats,
# display_region_hotspots, display_temporal_insights, display_region_comparison,
# display_region_metrics functions have been removed.

def main():
    """Main function to run the simplified Streamlit app"""
    st.sidebar.title("🗺️ Ticino Geo Insights")
    
    # Simplified View Selection
    st.sidebar.header("Select Visualization")
    view_options = {
        "Regional Metrics Map": display_choropleth_map_view,
        "Region Centers": display_region_centers
    }
    
    # Use radio buttons for single selection
    selected_view_name = st.sidebar.radio(
        "Choose a view:",
        list(view_options.keys()),
        key="main_view_selector"
    )
    
    # Display selected view
    if selected_view_name:
        view_options[selected_view_name]()
    else:
        # Default view if needed (optional) - let's default to the map
        display_choropleth_map_view()

if __name__ == "__main__":
    main()