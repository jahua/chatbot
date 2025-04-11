import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import json
import io
import geopandas as gpd
import os
import folium
import folium.plugins as plugins
from streamlit_folium import st_folium
import warnings
import numpy as np
from shapely.geometry import box, MultiPolygon

# Set page configuration
st.set_page_config(
    page_title="Ticino Choropleth Map",
    page_icon="🗺️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    /* Simplified styles with better contrast */
    h1 {
        margin-bottom: 0.5rem;
    }
    h2 {
        margin-top: 1.5rem;
    }
    /* Keep the button styling for visibility */
    .stButton>button {
        background-color: #2E5984;
        color: white;
    }
    .stButton>button:hover {
        background-color: #1E3D59;
        color: white;
    }
    /* Remove custom tab styling */
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        white-space: pre-wrap;
        gap: 1px;
        padding-top: 10px;
        padding-bottom: 10px;
    }
    /* Use a simple styled info box */
    .custom-info-box {
        padding: 10px;
        border-left: 3px solid #2E5984;
        border-radius: 0 5px 5px 0;
        margin-bottom: 10px;
    }
    /* Ensure all text has good contrast */
    .stRadio label, .stCheckbox label, .stSelectbox label {
        color: inherit !important;
    }
    /* Fix for metric values */
    [data-testid="stMetricValue"] {
        font-weight: bold !important;
    }
    /* Fix for tooltips */
    .folium-tooltip {
        background-color: white !important;
        color: #333333 !important;
        border: 1px solid #cccccc !important;
    }
</style>
""", unsafe_allow_html=True)

# Get the current directory of the script
current_dir = os.path.dirname(os.path.abspath(__file__))

# Page title and description
st.title("Ticino Regions Map")
st.markdown("**Interactive Map of Ticino, Switzerland**")

# Sidebar for controls
st.sidebar.header("Map Controls")
data_source = st.sidebar.radio(
    "Data Source",
    ["Shapefile (Full Dataset)", "Quick Load (Municipalities Only)"],
    index=0
)

visualization_approach = st.sidebar.selectbox(
    "Visualization Approach",
    ["Direct Matplotlib", "Mapbox Style", "Standard Choropleth", "SVG Image"],
    index=0  # Default to Direct Matplotlib for guaranteed visibility
)

# Add region filtering
st.sidebar.header("Region Filtering")
with st.sidebar.expander("Filter Options", expanded=True):
    st.write("Filter regions to display:")
    
    # Get region types from data (after it's loaded)
    filter_all = st.checkbox("Show All Regions", value=True)
    
    # Add a search box for specific municipalities
    municipality_search = st.text_input("Search for a municipality:", "")
    
    # We'll use this container for region-specific filters
    filter_container = st.container()

# Add data export options
st.sidebar.header("Data Export")
with st.sidebar.expander("Export Options"):
    export_format = st.selectbox("Export Format", ["GeoJSON", "Shapefile", "CSV"])
    export_button = st.button("Export Data")

# Add a new option in the sidebar
st.sidebar.header("Map Views")
map_view = st.sidebar.radio(
    "Select View",
    ["Complete Map", "Only Ticino Boundary"],
    index=0
)

# Create a choropleth map from shapefile data
def create_choropleth_map(geojson_data, gdf):
    # Check if we have valid data
    if gdf.empty:
        st.warning("No valid shapefile data to display")
        return go.Figure()
    
    # Create figure
    fig = go.Figure()
    
    # Define a bright color palette with better contrast
    colors = [
        '#FF5733',  # Bright orange-red
        '#FFC300',  # Bright yellow
        '#DAF7A6',  # Light green
        '#9FE2BF',  # Mint green
        '#40E0D0',  # Turquoise
        '#6495ED',  # Cornflower blue
        '#CCCCFF',  # Periwinkle
        '#B19CD9',  # Light purple
        '#FF99FF',  # Pink
        '#FFBF00',  # Amber
        '#FF7F50'   # Coral
    ]
    
    # Use a different approach with simpler geometries and clearer borders
    for idx, row in gdf.iterrows():
        # Get geometry
        if row.geometry is None:
            continue
        
        # Get geometry type
        geometry_type = row.geometry.geom_type
        
        # Determine color based on BZNR or index
        if 'BZNR' in row:
            color_idx = int(row['BZNR']) % len(colors)
        else:
            color_idx = idx % len(colors)
            
        # Pick a color from our colors list
        color = colors[color_idx]
        
        # Get the name for the hover text
        if 'BZNAME' in row:
            name = row['BZNAME']
        else:
            name = f"Region {idx}"
        
        # Process different geometry types
        if geometry_type == 'Polygon':
            coords = row.geometry.exterior.coords
            lons, lats = zip(*coords)
            
            # Add polygon with explicit styling for visibility
            fig.add_trace(go.Scattermapbox(
                lat=list(lats),
                lon=list(lons),
                mode="lines",
                fill="toself",
                fillcolor=color,
                line=dict(width=2, color="black"),  # Thicker black border
                opacity=0.9,  # Higher opacity
                hoverinfo="text",
                hovertext=f"{name}<br>ID: {idx}",
                name=name
            ))
        elif geometry_type == 'MultiPolygon':
            for polygon in row.geometry.geoms:
                coords = polygon.exterior.coords
                lons, lats = zip(*coords)
                
                fig.add_trace(go.Scattermapbox(
                    lat=list(lats),
                    lon=list(lons),
                    mode="lines",
                    fill="toself",
                    fillcolor=color,
                    line=dict(width=2, color="black"),  # Thicker black border
                    opacity=0.9,  # Higher opacity
                    hoverinfo="text",
                    hovertext=f"{name}<br>ID: {idx}",
                    name=name
                ))
    
    # Calculate the bounding box of the data to center the map
    bounds = gdf.total_bounds
    center_lon = (bounds[0] + bounds[2]) / 2
    center_lat = (bounds[1] + bounds[3]) / 2
    
    # Configure the map with a light basemap and improved settings
    fig.update_layout(
        mapbox=dict(
            style="carto-positron",  # Light basemap
            zoom=9,  # Zoom in more
            center=dict(lat=center_lat, lon=center_lon)
        ),
        margin=dict(r=0, t=0, l=0, b=0),
        height=700,  # Taller map
        width=1000,  # Wider map
        showlegend=False,  # Hide legend to simplify view
        paper_bgcolor='white',  # White background
        plot_bgcolor='white'    # White plot area
    )
    
    # Add a mapbox access token if needed
    # fig.update_layout(mapbox_accesstoken="YOUR_MAPBOX_TOKEN")
    
    return fig

# Create a standard Plotly choropleth map (better visibility)
def create_standard_choropleth(gdf):
    # Check if we have valid data
    if gdf.empty:
        st.warning("No valid shapefile data to display")
        return go.Figure()
    
    # Create a new figure
    fig = go.Figure()
    
    # Define vibrant colors - high contrast for visibility
    colors = [
        '#FF0000',  # Bright red
        '#00FF00',  # Bright green  
        '#0000FF',  # Bright blue
        '#FFFF00',  # Yellow
        '#FF00FF',  # Magenta
        '#00FFFF',  # Cyan
        '#FFA500',  # Orange
        '#800080',  # Purple
        '#008000',  # Dark green
        '#FF69B4',  # Hot pink
        '#1E90FF'   # Dodger blue
    ]
    
    # Process each region
    for idx, row in gdf.iterrows():
        if row.geometry is None:
            continue
            
        # Determine color 
        if 'BZNR' in row:
            color_idx = int(row['BZNR']) % len(colors)
        else:
            color_idx = idx % len(colors)
        
        # Get shape name for display
        if 'BZNAME' in row:
            name = row['BZNAME']
        else:
            name = f"Region {idx}"
            
        # Convert geometry to GeoJSON-like format for Plotly
        geometry_json = json.loads(gpd.GeoSeries([row.geometry]).to_json())
        
        # Add shape to the figure
        fig.add_trace(go.Choropleth(
            geojson=geometry_json,
            locations=[0],  # Index in the GeoJSON
            z=[color_idx],  # Value for color
            colorscale=[[0, colors[color_idx]], [1, colors[color_idx]]],
            showscale=False,
            marker=dict(
                line=dict(color='black', width=2)
            ),
            hoverinfo="text",
            text=name,
            name=name
        ))
    
    # Calculate the center of the map for consistent display
    bounds = gdf.total_bounds
    center_lon = (bounds[0] + bounds[2]) / 2
    center_lat = (bounds[1] + bounds[3]) / 2
    
    # Set layout
    fig.update_geos(
        projection_type="mercator",
        center=dict(lon=center_lon, lat=center_lat),
        scope="world",
        showcoastlines=True, coastlinecolor="Black",
        showland=True, landcolor="white",
        showocean=True, oceancolor="lightblue",
        showlakes=True, lakecolor="lightblue",
        showrivers=True, rivercolor="lightblue",
        visible=True
    )
    
    fig.update_layout(
        height=700,
        margin={"r":0,"t":0,"l":0,"b":0},
        paper_bgcolor='white',
        geo=dict(
            projection=dict(scale=25),  # Zoom level
            center=dict(lat=center_lat, lon=center_lon)
        )
    )
    
    return fig

# Create a simple SVG display for guaranteed visibility
def create_svg_visualization(gdf):
    from matplotlib import pyplot as plt
    import io
    from PIL import Image
    import base64
    
    # Create a figure and axis
    fig, ax = plt.subplots(figsize=(12, 10), dpi=100)
    
    # Use a simple but effective coloring approach
    colors = [
        '#FF0000',  # Red
        '#00FF00',  # Green  
        '#0000FF',  # Blue
        '#FFFF00',  # Yellow
        '#FF00FF',  # Magenta
        '#00FFFF',  # Cyan
        '#FFA500',  # Orange
        '#800080',  # Purple
        '#FF69B4',  # Hot pink
    ]
    
    # Plot all geometries at once for basic background
    gdf.plot(
        ax=ax,
        edgecolor='black',
        linewidth=1.0,
        facecolor='lightgray',
    )
    
    # Then plot each geometry individually with custom colors
    for idx, row in gdf.iterrows():
        color = colors[idx % len(colors)]
        
        # Plot with thick black outline
        gdf.iloc[[idx]].plot(
            ax=ax,
            edgecolor='black',
            linewidth=1.5,
            facecolor=color,
            alpha=0.7,
        )
        
        # Add text label for important regions if space allows
        if idx % 10 == 0 and hasattr(row.geometry, 'centroid'):
            centroid = row.geometry.centroid
            if 'BZNAME' in row:
                name = row['BZNAME']
            else:
                name = f"Region {idx}"
            ax.text(
                centroid.x, 
                centroid.y, 
                name,
                fontsize=8,
                ha='center', 
                bbox=dict(facecolor='white', alpha=0.7, edgecolor='none', pad=1)
            )
    
    # Set plot limits based on data extent with a small buffer
    bounds = gdf.total_bounds
    x_buffer = (bounds[2] - bounds[0]) * 0.05
    y_buffer = (bounds[3] - bounds[1]) * 0.05
    ax.set_xlim(bounds[0] - x_buffer, bounds[2] + x_buffer)
    ax.set_ylim(bounds[1] - y_buffer, bounds[3] + y_buffer)
    
    # Add title and clean up the display
    ax.set_title('Ticino Regions Map', fontsize=16, weight='bold')
    ax.set_xlabel('Longitude', fontsize=12)
    ax.set_ylabel('Latitude', fontsize=12)
    ax.grid(True, linestyle='--', alpha=0.5)
    
    # Remove the default legend as it causes errors
    # Instead, add a textual annotation
    ax.annotate(
        f"Map contains {len(gdf)} regions",
        xy=(0.02, 0.98),
        xycoords='axes fraction',
        fontsize=12,
        backgroundcolor='white',
        va='top'
    )
    
    # Save the figure to a buffer with higher DPI for clarity
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=150, bbox_inches='tight')
    buf.seek(0)
    
    # Convert to base64 for display
    img = Image.open(buf)
    img_byte_arr = io.BytesIO()
    img.save(img_byte_arr, format='PNG')
    img_byte_arr = img_byte_arr.getvalue()
    
    plt.close(fig)  # Close the figure to free memory
    
    # Return the image as base64
    return base64.b64encode(img_byte_arr).decode()

# Create a direct matplotlib plot for immediate display
def create_direct_matplotlib(gdf):
    from matplotlib import pyplot as plt
    import matplotlib.colors as mcolors
    from matplotlib.colors import LinearSegmentedColormap
    
    # Create a figure and axis
    fig, ax = plt.subplots(figsize=(12, 10))
    
    # Create a better colormap for clear visibility of different regions
    colors = [
        '#FF0000',  # Red
        '#00CC00',  # Green
        '#0066FF',  # Blue
        '#FFCC00',  # Yellow
        '#CC00CC',  # Magenta
        '#00CCCC',  # Cyan
        '#FF6600',  # Orange
        '#6600CC',  # Purple
        '#FF0099',  # Pink
    ]
    
    # If a region type column exists, use it for coloring
    if 'region_type' in gdf.columns:
        # Create a categorical colormap based on region types
        region_types = gdf['region_type'].unique()
        if len(region_types) > 0:
            # Create a color column based on region type
            gdf['color'] = gdf['region_type'].map({
                region_types[i]: colors[i % len(colors)] 
                for i in range(len(region_types))
            })
            
            # Plot by region type with a legend
            for region_type in region_types:
                subset = gdf[gdf['region_type'] == region_type]
                color = subset['color'].iloc[0] if not subset.empty else 'gray'
                subset.plot(
                    ax=ax,
                    color=color,
                    edgecolor='black',
                    linewidth=0.8,
                    label=region_type
                )
            
            # Add a legend
            ax.legend(title="Region Types", loc='best')
    else:
        # Default plotting with BZNR if available
        column = 'BZNR' if 'BZNR' in gdf.columns else None
        
        # Plot with bright colors
        gdf.plot(
            ax=ax,
            column=column,
            cmap='tab20',
            edgecolor='black',
            linewidth=0.8,
            legend=False
        )
    
    # Improve labeling - add important municipality labels
    if 'BZNAME' in gdf.columns:
        # Find major municipalities to label
        major_municipalities = [
            'Lugano', 'Bellinzona', 'Locarno', 'Mendrisio', 
            'Chiasso', 'Biasca', 'Ascona'
        ]
        
        for idx, row in gdf.iterrows():
            name = row['BZNAME']
            if any(major in name for major in major_municipalities):
                # Add text label at the centroid
                if hasattr(row.geometry, 'centroid'):
                    centroid = row.geometry.centroid
                    ax.text(
                        centroid.x, centroid.y, 
                        name,
                        fontsize=9,
                        ha='center',
                        bbox=dict(facecolor='white', alpha=0.7, edgecolor='none', pad=1)
                    )
    
    # Set title and labels
    ax.set_title('Ticino Regions Map', fontsize=16, fontweight='bold')
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')
    
    # Add grid
    ax.grid(True, linestyle='--', alpha=0.5)
    
    return fig

# Update the Data tab function - moved before use
def show_data_tab(gdf):
    """Display data information in the data tab"""
    if gdf.empty:
        st.warning("No data available to display")
        return
    
    # Create options for exploring the data
    data_view = st.radio(
        "Select data view:",
        ["All Data", "Municipalities Only", "Summary Statistics"]
    )
    
    if data_view == "All Data":
        # Drop geometry column for clearer display
        display_df = gdf.drop(columns=['geometry'])
        
        # Display column information
        st.subheader("Available columns:")
        st.json(list(display_df.columns))
        
        # Display the dataframe
        st.dataframe(display_df)
        
    elif data_view == "Municipalities Only":
        # Filter to only show municipalities
        if 'region_type' in gdf.columns:
            municipalities = gdf[gdf['region_type'] == 'Municipality']
            if not municipalities.empty:
                # Display municipalities
                muni_df = municipalities.drop(columns=['geometry'])
                
                # Allow searching/filtering
                search = st.text_input("Search municipalities:", "")
                if search:
                    muni_df = muni_df[muni_df['BZNAME'].str.lower().str.contains(search.lower())]
                
                # Display with sorting option
                st.write(f"Showing {len(muni_df)} municipalities:")
                st.dataframe(muni_df)
            else:
                st.warning("No municipalities found in data")
        else:
            st.warning("Region type information not available")
    
    else:  # Summary Statistics
        # Show summary statistics for numeric columns
        numeric_cols = gdf.select_dtypes(include=['number']).columns
        if len(numeric_cols) > 0:
            st.subheader("Summary Statistics for Numeric Fields")
            st.dataframe(gdf[numeric_cols].describe())
        
        # Show region type counts if available
        if 'region_type' in gdf.columns:
            st.subheader("Region Type Distribution")
            type_counts = gdf['region_type'].value_counts().reset_index()
            type_counts.columns = ['Region Type', 'Count']
            st.dataframe(type_counts)
            
            # Plot the distribution
            import matplotlib.pyplot as plt
            fig, ax = plt.subplots(figsize=(8, 4))
            type_counts.plot.bar(x='Region Type', y='Count', ax=ax)
            st.pyplot(fig)

# Create tabs for different map views
tab1, tab2, tab3 = st.tabs(["Map View", "Municipalities Map", "Data View"])

# Create a lightweight municipalities map
@st.cache_data
def create_municipalities_map(_gdf):
    from matplotlib import pyplot as plt
    
    # Filter to only show municipalities
    if 'region_type' in _gdf.columns:
        municipalities = _gdf[_gdf['region_type'] == 'Municipality']
    else:
        # If no region_type, assume all are municipalities (this is likely accurate for Ticino)
        municipalities = _gdf
        
    if municipalities.empty:
        st.warning("No municipalities found in the data")
        return None
        
    # Create a figure and axis for the municipalities map
    fig, ax = plt.subplots(figsize=(12, 10))
    
    # Use a colorful scheme for visibility
    municipalities.plot(
        ax=ax,
        column='BZNR' if 'BZNR' in municipalities.columns else None,
        cmap='viridis',  # A nice perceptual colormap
        edgecolor='black',
        linewidth=0.6,
        legend=False,  # Avoid legend errors
        alpha=0.8
    )
    
    # Add municipality labels for major ones
    if 'BZNAME' in municipalities.columns:
        # Only label a select few to avoid overcrowding
        important_municipalities = [
            'Lugano', 'Bellinzona', 'Locarno', 'Mendrisio', 
            'Chiasso', 'Biasca', 'Ascona', 'Giubiasco', 'Minusio'
        ]
        
        for idx, row in municipalities.iterrows():
            name = row['BZNAME'] if isinstance(row['BZNAME'], str) else ""
            if any(muni in name for muni in important_municipalities):
                centroid = row.geometry.centroid
                ax.text(
                    centroid.x, centroid.y, 
                    name,
                    fontsize=9,
                    ha='center',
                    bbox=dict(facecolor='white', alpha=0.7, edgecolor='none', pad=1)
                )
    
    # Set title and labels
    ax.set_title('Ticino Municipalities Map', fontsize=16, fontweight='bold')
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')
    
    # Add grid
    ax.grid(True, linestyle='--', alpha=0.3)
    
    return fig

# Function to identify region types with specific municipality focus
def identify_region_types(gdf):
    """Identify region types from the GeoDataFrame attributes with focus on municipalities"""
    if 'BZNAME' in gdf.columns:
        # Add a region_type column if it doesn't exist
        if 'region_type' not in gdf.columns:
            # Generally all features in this shapefile are municipalities
            gdf['region_type'] = 'Municipality'
            
            # Try to identify district centers or special regions
            if 'BZNR' in gdf.columns:
                for idx, row in gdf.iterrows():
                    name = row['BZNAME'] if isinstance(row['BZNAME'], str) else ""
                    if 'distretto' in name.lower() or 'district' in name.lower():
                        gdf.at[idx, 'region_type'] = 'District'
                    elif 'cantone' in name.lower() or 'canton' in name.lower() or 'ticino' in name.lower():
                        gdf.at[idx, 'region_type'] = 'Canton'
    
    return gdf

# Filter regions based on selection and search
def filter_regions(gdf, region_types=None, show_all=True, search_term=""):
    """Filter the GeoDataFrame based on selected region types and search term"""
    filtered_df = gdf
    
    # Apply region type filter if needed
    if not show_all and region_types:
        filtered_df = filtered_df[filtered_df['region_type'].isin(region_types)]
    
    # Apply search filter if provided
    if search_term and 'BZNAME' in filtered_df.columns:
        search_term = search_term.lower()
        filtered_df = filtered_df[filtered_df['BZNAME'].str.lower().str.contains(search_term)]
    
    return filtered_df

# Export data function
def export_data(gdf, format_type):
    """Export the GeoDataFrame to the selected format"""
    if gdf.empty:
        st.warning("No data to export")
        return None
    
    filename = f"ticino_regions.{format_type.lower()}"
    
    if format_type == "GeoJSON":
        # Convert to GeoJSON
        geo_json = json.loads(gdf.to_json())
        return geo_json, filename, "application/json"
    
    elif format_type == "CSV":
        # Convert to CSV (without geometry column)
        csv_df = gdf.drop(columns=['geometry'])
        csv_data = csv_df.to_csv(index=False)
        return csv_data, filename, "text/csv"
    
    elif format_type == "Shapefile":
        # Shapefiles need to be zipped
        import zipfile
        
        # Create a temporary directory
        import tempfile
        temp_dir = tempfile.mkdtemp()
        temp_shapefile = os.path.join(temp_dir, "ticino_export.shp")
        
        # Save shapefile to temp directory
        gdf.to_file(temp_shapefile)
        
        # Zip the shapefile components
        zip_filename = os.path.join(temp_dir, "ticino_regions.zip")
        with zipfile.ZipFile(zip_filename, 'w') as zipf:
            for f in os.listdir(temp_dir):
                if f.startswith("ticino_export."):
                    file_path = os.path.join(temp_dir, f)
                    zipf.write(file_path, arcname=f)
        
        # Read the zip file
        with open(zip_filename, "rb") as f:
            return f.read(), "ticino_regions.zip", "application/zip"
    
    return None, "", ""

# Load data from shapefile
@st.cache_data
def load_shapefile_data():
    """Load data from Ticino shapefile."""
    try:
        # Get the current directory
        current_dir = os.path.dirname(os.path.abspath(__file__))
        shapefile_path = os.path.join(current_dir, 'g1b23.shp')
        
        # Check if file exists
        if os.path.exists(shapefile_path):
            st.sidebar.success(f"Found shapefile at: {shapefile_path}")
            
            # Try loading with different engines
            try:
                # Load with explicit fiona engine
                gdf = gpd.read_file(shapefile_path, engine="fiona")
                st.sidebar.success(f"Successfully loaded shapefile with {len(gdf)} features")
                
                # Ensure the CRS is WGS84 (EPSG:4326) for Folium
                if gdf.crs and gdf.crs != "EPSG:4326":
                    st.sidebar.info(f"Converting CRS from {gdf.crs} to WGS84")
                    gdf = gdf.to_crs(epsg=4326)
                
                return gdf
            except Exception as e:
                st.sidebar.error(f"Error loading with fiona: {str(e)}")
                # Try with pyogrio as fallback
                try:
                    gdf = gpd.read_file(shapefile_path, engine="pyogrio")
                    st.sidebar.success(f"Successfully loaded with pyogrio: {len(gdf)} features")
                    
                    # Ensure the CRS is WGS84 (EPSG:4326) for Folium
                    if gdf.crs and gdf.crs != "EPSG:4326":
                        st.sidebar.info(f"Converting CRS from {gdf.crs} to WGS84")
                        gdf = gdf.to_crs(epsg=4326)
                    
                    return gdf
                except Exception as e2:
                    st.sidebar.error(f"Also failed with pyogrio: {str(e2)}")
        else:
            st.sidebar.error(f"Shapefile not found at: {shapefile_path}")
            # List directory contents for debugging
            st.sidebar.info("Directory contents:")
            for file in os.listdir(current_dir):
                if file.endswith('.shp'):
                    st.sidebar.info(f"Found shapefile: {file}")
        
        # If shapefile loading failed, try loading from GeoJSON
        geojson_path = os.path.join(current_dir, 'municipalities.geojson')
        if os.path.exists(geojson_path):
            st.sidebar.info(f"Trying to load GeoJSON from: {geojson_path}")
            try:
                gdf = gpd.read_file(geojson_path)
                st.sidebar.success(f"Successfully loaded GeoJSON with {len(gdf)} features")
                return gdf
            except Exception as e:
                st.sidebar.error(f"Error loading GeoJSON: {str(e)}")
        
        # Fallback to built-in simplified data
        st.sidebar.warning("Using built-in simplified data as fallback")
        # Create a simple polygon for Ticino
        from shapely.geometry import Polygon
        # Approximate simplified Ticino boundary
        ticino_polygon = Polygon([
            (8.4, 46.4), (9.1, 46.4), (9.1, 45.8), (8.4, 45.8)
        ])
        gdf = gpd.GeoDataFrame(
            {'BZNAME': ['Ticino'], 'AREA_HA': [281215]}, 
            geometry=[ticino_polygon],
            crs="EPSG:4326"
        )
        return gdf
    except Exception as e:
        st.sidebar.error(f"Error in load_shapefile_data: {str(e)}")
        return None

# Create a lightweight function to prepare municipalities data (cached for speed)
@st.cache_data
def prepare_municipalities_data(_gdf):
    """Extract only municipalities from the data and prepare for display/export"""
    # If region_type exists, filter to municipalities
    if 'region_type' in _gdf.columns:
        municipalities = _gdf[_gdf['region_type'] == 'Municipality'].copy()
    else:
        # Otherwise assume all are municipalities
        municipalities = _gdf.copy()
        municipalities['region_type'] = 'Municipality'
        
    return municipalities

# Create a fast simplified municipalities map with fewer features for speed
@st.cache_data
def create_fast_municipalities_map(_municipalities_gdf):
    """Creates a simplified map focused only on municipalities with minimal styling for speed"""
    from matplotlib import pyplot as plt
    import matplotlib.colors as mcolors
    
    # Create a simplified figure
    fig, ax = plt.subplots(figsize=(10, 8))
    
    if _municipalities_gdf.empty:
        ax.text(0.5, 0.5, "No municipality data available", 
                ha='center', va='center', transform=ax.transAxes)
        return fig
    
    # Use a bright colormap with higher saturation for better visibility
    _municipalities_gdf.plot(
        ax=ax,
        column='BZNR' if 'BZNR' in _municipalities_gdf.columns else None,
        cmap='Spectral',  # Very distinct colors across spectrum
        edgecolor='black',
        linewidth=0.5,
        legend=False,
    )
    
    # Only add labels to major municipalities for speed
    if 'BZNAME' in _municipalities_gdf.columns:
        major_cities = [
            'Lugano', 'Bellinzona', 'Locarno', 'Mendrisio', 'Chiasso'
        ]
        
        for idx, row in _municipalities_gdf.iterrows():
            name = row['BZNAME'] if isinstance(row['BZNAME'], str) else ""
            if any(city in name for city in major_cities):
                centroid = row.geometry.centroid
                ax.text(
                    centroid.x, centroid.y, 
                    name,
                    fontsize=10,
                    ha='center', 
                    bbox=dict(facecolor='white', alpha=0.8, edgecolor='gray', boxstyle='round,pad=0.3')
                )
    
    # Improve visual presentation
    ax.set_title('Ticino Municipalities', fontsize=14, weight='bold')
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')
    ax.grid(False)  # Remove grid for cleaner look
    
    # Use tight layout for better use of space
    plt.tight_layout()
    
    return fig

# Load data from a saved GeoJSON file (municipalities only for speed)
@st.cache_data
def load_municipalities_geojson():
    """Load just the municipalities from a saved GeoJSON file for faster loading"""
    # Get the current file's directory
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    try:
        # Look for a municipalities geojson file
        geojson_path = os.path.join(current_dir, 'ticino_municipalities.geojson')
        
        if not os.path.exists(geojson_path):
            # Try the minimal version if available
            geojson_path = os.path.join(current_dir, 'ticino_municipalities_minimal.geojson')
            
        if not os.path.exists(geojson_path):
            st.sidebar.warning("No municipalities GeoJSON file found. Please create one first.")
            return gpd.GeoDataFrame()
            
        # Load the municipalities GeoJSON
        municipalities_gdf = gpd.read_file(geojson_path)
        
        # Add region_type if it doesn't exist
        if 'region_type' not in municipalities_gdf.columns:
            municipalities_gdf['region_type'] = 'Municipality'
            
        st.sidebar.success(f"Loaded {len(municipalities_gdf)} municipalities from GeoJSON")
        return municipalities_gdf
        
    except Exception as e:
        st.sidebar.error(f"Error loading municipalities GeoJSON: {str(e)}")
        return gpd.GeoDataFrame()

# Function to extract the boundary of Ticino canton from the data
def extract_ticino_boundary(gdf):
    """Extract the boundary of all municipalities to create a Ticino boundary"""
    if gdf is None or gdf.empty:
        st.sidebar.warning("No data available to extract boundary")
        return None
    
    try:
        # Dissolve all municipalities to get a single polygon for the Canton
        ticino_boundary = gdf.unary_union
        # Convert to GeoDataFrame for compatibility with folium
        ticino_gdf = gpd.GeoDataFrame(geometry=[ticino_boundary], crs=gdf.crs)
        return ticino_gdf
    except Exception as e:
        st.sidebar.error(f"Error extracting Ticino boundary: {str(e)}")
        return None

# Create a simple, clean map showing just the Ticino boundary
def create_ticino_boundary_map(boundary_gdf):
    """Create a simple, clean map showing just the Ticino boundary"""
    from matplotlib import pyplot as plt
    import matplotlib.patches as mpatches
    
    # Create figure and axis
    fig, ax = plt.subplots(figsize=(10, 12))
    
    if boundary_gdf.empty:
        ax.text(0.5, 0.5, "No Ticino boundary data available", 
                ha='center', va='center', transform=ax.transAxes)
        return fig
    
    # Plot the boundary with a bold, distinct style
    boundary_gdf.plot(
        ax=ax,
        color='royalblue',
        edgecolor='navy',
        linewidth=2.5,
        alpha=0.6
    )
    
    # Add a title and information about the boundary source
    if 'BZNAME' in boundary_gdf.columns:
        title_text = boundary_gdf['BZNAME'].iloc[0] if not boundary_gdf.empty else 'Canton Ticino Boundary'
    else:
        title_text = 'Canton Ticino Boundary'
    
    ax.set_title(title_text, fontsize=18, fontweight='bold')
    ax.set_xlabel('Longitude', fontsize=12)
    ax.set_ylabel('Latitude', fontsize=12)
    
    # Add a legend
    ticino_patch = mpatches.Patch(
        color='royalblue', 
        label='Canton Ticino', 
        alpha=0.6, 
        edgecolor='navy', 
        linewidth=2.5
    )
    ax.legend(handles=[ticino_patch], loc='upper right')
    
    # Clean up the plot
    ax.grid(True, linestyle='--', alpha=0.5)
    
    # Use tight layout
    plt.tight_layout()
    
    return fig

# Function to create an interactive Plotly map for the Ticino boundary
def create_interactive_boundary_map(boundary_gdf):
    """Create an interactive Plotly map showing just the Ticino boundary"""
    import plotly.graph_objects as go
    
    if boundary_gdf.empty:
        # Return an empty figure with a message
        fig = go.Figure()
        fig.add_annotation(
            text="No Ticino boundary data available",
            showarrow=False,
            font=dict(size=20)
        )
        return fig
    
    # Create figure
    fig = go.Figure()
    
    # Process each boundary geometry (usually just one)
    for idx, row in boundary_gdf.iterrows():
        if row.geometry is None:
            continue
        
        # Get geometry type
        geometry_type = row.geometry.geom_type
        
        # Get name (if available)
        name = row['BZNAME'] if 'BZNAME' in row else "Canton Ticino"
        
        # Process geometry based on type
        if geometry_type == 'Polygon':
            coords = row.geometry.exterior.coords
            lons, lats = zip(*coords)
            
            # Add polygon with styling for visibility
            fig.add_trace(go.Scattermapbox(
                lat=list(lats),
                lon=list(lons),
                mode="lines",
                fill="toself",
                fillcolor="rgba(0, 100, 255, 0.5)",  # More vibrant blue with transparency
                line=dict(width=4, color="darkblue"),  # Thicker blue border
                hoverinfo="text",
                hovertext=f"<b>{name}</b><br>Canton Ticino",
                name=name
            ))
        elif geometry_type == 'MultiPolygon':
            for polygon in row.geometry.geoms:
                coords = polygon.exterior.coords
                lons, lats = zip(*coords)
                
                fig.add_trace(go.Scattermapbox(
                    lat=list(lats),
                    lon=list(lons),
                    mode="lines",
                    fill="toself",
                    fillcolor="rgba(0, 100, 255, 0.5)",  # More vibrant blue with transparency
                    line=dict(width=4, color="darkblue"),  # Thicker blue border
                    hoverinfo="text",
                    hovertext=f"<b>{name}</b><br>Canton Ticino",
                    name=name
                ))
    
    # Calculate the bounding box of the data to center the map
    bounds = boundary_gdf.total_bounds
    center_lon = (bounds[0] + bounds[2]) / 2
    center_lat = (bounds[1] + bounds[3]) / 2
    
    # Configure the map with a light basemap
    fig.update_layout(
        mapbox=dict(
            style="carto-positron",  # Light basemap
            zoom=8.5,
            center=dict(lat=center_lat, lon=center_lon)
        ),
        margin=dict(r=0, t=50, l=0, b=0),
        height=700,
        title="Canton Ticino Boundary",
        title_font_size=20,
        title_font_family="Arial",
        title_x=0.5, # Center the title
        paper_bgcolor='white',
        showlegend=False
    )
    
    return fig

# Additional function for a pure Choropleth fallback if Mapbox has issues
def create_choropleth_boundary_map(boundary_gdf):
    """Create a standard Plotly choropleth map for the boundary (fallback option)"""
    import plotly.graph_objects as go
    
    if boundary_gdf.empty:
        # Return an empty figure with a message
        fig = go.Figure()
        fig.add_annotation(
            text="No Ticino boundary data available",
            showarrow=False,
            font=dict(size=20)
        )
        return fig
        
    # Create a GeoJSON-like representation
    gjson = json.loads(boundary_gdf.to_json())
    
    # Create choropleth map
    fig = go.Figure(go.Choroplethmapbox(
        geojson=gjson,
        locations=[0],  # Single feature
        z=[1],         # Single value
        colorscale=[[0, 'rgb(0,100,255)'], [1, 'rgb(0,100,255)']],
        marker_opacity=0.7,
        marker_line_width=2,
        showscale=False,
        hoverinfo="text",
        text="Canton Ticino"
    ))
    
    # Calculate the bounding box of the data to center the map
    bounds = boundary_gdf.total_bounds
    center_lon = (bounds[0] + bounds[2]) / 2
    center_lat = (bounds[1] + bounds[3]) / 2
    
    # Configure the map
    fig.update_layout(
        mapbox_style="carto-positron",
        mapbox_zoom=8,
        mapbox_center={"lat": center_lat, "lon": center_lon},
        margin={"r":0,"t":50,"l":0,"b":0},
        height=700,
        title="Canton Ticino Boundary",
        title_font_size=20
    )
    
    return fig

# Load a saved Ticino boundary file if available
@st.cache_data
def load_ticino_boundary():
    """Load a pre-saved Ticino boundary file if available"""
    # Get the current file's directory
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Check for boundary file
    boundary_path = os.path.join(current_dir, 'ticino_boundary.geojson')
    
    if os.path.exists(boundary_path):
        try:
            # Load the boundary GeoJSON
            boundary_gdf = gpd.read_file(boundary_path)
            st.sidebar.success("Loaded pre-saved Ticino boundary")
            return boundary_gdf
        except Exception as e:
            st.sidebar.warning(f"Error loading boundary file: {str(e)}")
    
    # No boundary file found or error loading
    return None

# Add a new function to generate and save a Ticino boundary
def generate_ticino_boundary(gdf, save=False):
    """Generate the Ticino boundary and optionally save it as a GeoJSON file"""
    if gdf is None or gdf.empty:
        st.sidebar.warning("No data available to generate boundary")
        return None
    
    try:
        ticino_boundary = extract_ticino_boundary(gdf)
        
        if save and ticino_boundary is not None:
            # Define the output path
            output_path = os.path.join(current_dir, 'ticino_boundary.geojson')
            # Save the boundary as GeoJSON
            ticino_boundary.to_file(output_path, driver='GeoJSON')
            st.sidebar.success(f"Ticino boundary saved to {output_path}")
        
        return ticino_boundary
    except Exception as e:
        st.sidebar.error(f"Error generating Ticino boundary: {str(e)}")
        return None

def get_shapefile_path():
    """Get the path to the shapefile"""
    return os.path.join(current_dir, 'g1b23.shp')

def main():
    """Main function to display choropleth map with Folium"""
    # Simplified header with clear contrast
    st.title("Ticino Choropleth Map")
    st.markdown("Interactive map of municipalities in Ticino, Switzerland")
    
    # Add a simple divider
    st.markdown("<hr>", unsafe_allow_html=True)
    
    # Setup the sidebar with simpler structure
    st.sidebar.title("Map Controls")
    
    # Display app info in a styled box
    st.sidebar.markdown("""
    <div class="custom-info-box">
        <strong>About This Map</strong><br>
        Interactive choropleth map of Ticino municipalities showing area sizes.
    </div>
    """, unsafe_allow_html=True)
    
    # Load the data
    with st.spinner("Loading map data..."):
        gdf = load_shapefile_data()
    
    # Add map display options with simplified UI
    st.sidebar.subheader("Display Options")
    map_style = st.sidebar.selectbox(
        "Map Style",
        ["CartoDB positron", "CartoDB dark_matter", "OpenStreetMap", "Stamen Terrain", "Stamen Toner"]
    )
    
    color_scheme = st.sidebar.selectbox(
        "Color Scheme",
        ["YlGnBu", "YlOrRd", "Greens", "Blues", "Purples", "Spectral", "RdYlGn"]
    )
    
    col1, col2 = st.sidebar.columns(2)
    show_legend = col1.checkbox("Legend", value=True)
    show_tooltips = col2.checkbox("Tooltips", value=True)
    
    # Add a button to generate boundary
    if st.sidebar.button("Generate Ticino Boundary"):
        with st.sidebar.spinner("Creating..."):
            boundary_result = generate_ticino_boundary(gdf, save=True)
            if boundary_result is not None:
                st.sidebar.success("Boundary generated")
    
    # Debug info in a collapsible section (simplified)
    with st.sidebar.expander("Technical Info", expanded=False):
        if gdf is not None and not gdf.empty:
            st.write(f"• {len(gdf)} municipalities loaded")
            st.write(f"• CRS: {gdf.crs}")
            
            # Show bounds in a more compact form
            bounds = gdf.total_bounds
            st.write(f"• Bounds: [{bounds[0]:.4f}, {bounds[1]:.4f}, {bounds[2]:.4f}, {bounds[3]:.4f}]")
    
    # Display the map
    if gdf is not None and not gdf.empty:
        try:
            # Extract Ticino boundary for centering the map
            ticino_boundary = extract_ticino_boundary(gdf)
            
            if ticino_boundary is not None:
                # Get the centroid of the boundary for map centering
                centroid = ticino_boundary.centroid.iloc[0]
                center = [centroid.y, centroid.x]
                
                # Check for potential CRS issues
                if abs(center[0]) > 90 or abs(center[1]) > 180:
                    st.warning(f"Map center coordinates may be invalid: {center}.")
                    # Attempt to fix by converting to WGS 84 if not already
                    if gdf.crs and gdf.crs != "EPSG:4326":
                        st.info(f"Converting from {gdf.crs} to WGS 84")
                        gdf = gdf.to_crs(epsg=4326)
                        ticino_boundary = ticino_boundary.to_crs(epsg=4326)
                        centroid = ticino_boundary.centroid.iloc[0]
                        center = [centroid.y, centroid.x]
                
                # Get bounds for map
                bounds = gdf.total_bounds  # minx, miny, maxx, maxy
                
                # Create a folium map with explicit bounds
                m = folium.Map(
                    location=center,
                    zoom_start=9,
                    tiles=map_style
                )
                
                # Add municipality boundaries with a choropleth style
                if 'AREA_HA' in gdf.columns:
                    # Create a simple outline layer first
                    folium.GeoJson(
                        data=gdf,
                        name="Municipalities Outline",
                        style_function=lambda x: {
                            'fillColor': 'transparent',
                            'color': 'black',
                            'weight': 0.5,
                            'fillOpacity': 0
                        }
                    ).add_to(m)
                    
                    # Create data dictionary mapping municipality names to areas
                    area_dict = dict(zip(gdf['BZNAME'], gdf['AREA_HA']))
                    
                    # Add as a separate choropleth
                    choropleth = folium.Choropleth(
                        geo_data=gdf,
                        name="Municipalities by Area",
                        data=area_dict,
                        columns=['BZNAME', 'AREA_HA'],
                        key_on='feature.properties.BZNAME',
                        fill_color=color_scheme,
                        fill_opacity=0.7,
                        line_opacity=0.2,
                        legend_name='Area (Hectares)',
                        highlight=True,
                        show_legend=show_legend
                    ).add_to(m)
                    
                    # Add tooltips to each feature if enabled
                    if show_tooltips:
                        for feature in choropleth.geojson.data['features']:
                            if 'properties' in feature and 'BZNAME' in feature['properties']:
                                municipality = feature['properties']['BZNAME']
                                if municipality in area_dict:
                                    feature['properties']['area'] = area_dict[municipality]
                        
                        # Add tooltips
                        folium.features.GeoJsonTooltip(
                            fields=['BZNAME', 'area'],
                            aliases=['Municipality:', 'Area (ha):'],
                            style=("background-color: white; color: #333333; "
                                  "font-family: arial; font-size: 12px; padding: 10px;")
                        ).add_to(choropleth.geojson)
                else:
                    # Fallback if AREA_HA is not available
                    folium.GeoJson(
                        data=gdf.__geo_interface__,
                        style_function=lambda x: {
                            'fillColor': '#3186cc',
                            'color': '#000000',
                            'weight': 1,
                            'fillOpacity': 0.5
                        },
                        tooltip=folium.GeoJsonTooltip(
                            fields=['BZNAME'] if 'BZNAME' in gdf.columns else [],
                            aliases=['Municipality:'] if 'BZNAME' in gdf.columns else [],
                            style="background-color: white; color: #333333; font-family: arial; font-size: 12px; padding: 10px;"
                        )
                    ).add_to(m)
                
                # Add Ticino boundary as an outline
                folium.GeoJson(
                    data=ticino_boundary.__geo_interface__,
                    style_function=lambda x: {
                        'fillColor': 'transparent',
                        'color': '#FF0000',
                        'weight': 2,
                        'dashArray': '5, 5'
                    },
                    name="Ticino Boundary"
                ).add_to(m)
                
                # Set map bounds to Ticino area
                m.fit_bounds([[bounds[1], bounds[0]], [bounds[3], bounds[2]]])
                
                # Add layer control
                folium.LayerControl().add_to(m)
                
                # Add fullscreen button
                plugins.Fullscreen().add_to(m)
                
                # Add minimal map controls for better compatibility
                try:
                    # Add simple measure tool
                    plugins.MeasureControl(position='topleft').add_to(m)
                except Exception:
                    pass
                
                # Display the map in Streamlit
                st_folium(m, width=1200, height=700)
                
                # Add simple stats below the map
                st.subheader("Ticino Overview")
                
                # Create columns for stats
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("Municipalities", f"{len(gdf)}", "")
                
                with col2:
                    # Calculate total area
                    total_area = gdf['AREA_HA'].sum() if 'AREA_HA' in gdf.columns else 0
                    st.metric("Total Area", f"{total_area:,.0f} ha", "")
                
                with col3:
                    # Find largest municipality
                    if 'AREA_HA' in gdf.columns and 'BZNAME' in gdf.columns:
                        largest_muni = gdf.loc[gdf['AREA_HA'].idxmax()]
                        st.metric("Largest Municipality", f"{largest_muni['BZNAME']}", f"{largest_muni['AREA_HA']:,.0f} ha")
            else:
                st.error("Could not determine Ticino boundary for map centering.")
        except Exception as e:
            st.error(f"Error creating map: {str(e)}")
    else:
        st.error("No data available to display.")
        
    # Add simple footer
    st.markdown("""
    <div style="margin-top: 30px; text-align: center; font-size: 12px;">
        Data: Swiss Federal Office of Topography | Viz: Streamlit + Folium
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()