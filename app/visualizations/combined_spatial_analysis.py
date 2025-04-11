import streamlit as st
import pandas as pd
import geopandas as gpd
import plotly.express as px
import plotly.graph_objects as go
from shapely import wkt
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection
DB_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(DB_URL)

def load_spatial_data():
    """Load the combined spatial analysis data"""
    query = """
    SELECT 
        geo_type,
        geo_name,
        bounding_box,
        total_points,
        total_transactions,
        total_spend,
        industry_count,
        area_sq_km,
        transaction_density,
        month,
        swiss_tourists,
        foreign_tourists,
        total_visitors,
        top_industries,
        industry_volumes,
        ST_AsText(centroid) as centroid,
        ST_AsText(envelope) as envelope
    FROM geo_insights.combined_spatial_analysis
    """
    df = pd.read_sql(query, engine)
    
    # Convert WKT to geometry
    df['geometry'] = df['bounding_box'].apply(wkt.loads)
    gdf = gpd.GeoDataFrame(df, geometry='geometry')
    
    return gdf

def create_combined_visualization(gdf):
    """Create a combined visualization with multiple layers"""
    st.title("Combined Spatial Analysis Dashboard")
    
    # Sidebar filters
    st.sidebar.header("Filters")
    geo_type = st.sidebar.selectbox("Select Geographic Type", gdf['geo_type'].unique())
    month = st.sidebar.selectbox("Select Month", sorted(gdf['month'].unique()))
    
    # Filter data
    filtered_gdf = gdf[(gdf['geo_type'] == geo_type) & (gdf['month'] == month)]
    
    # Create tabs for different visualizations
    tab1, tab2, tab3, tab4 = st.tabs([
        "Transaction Density Map",
        "Tourist Flow Analysis",
        "Industry Distribution",
        "Economic Clusters"
    ])
    
    with tab1:
        st.subheader("Transaction Density Heatmap")
        fig1 = px.choropleth_mapbox(
            filtered_gdf,
            geojson=filtered_gdf.geometry.__geo_interface__,
            locations=filtered_gdf.index,
            color='transaction_density',
            hover_name='geo_name',
            hover_data=['total_transactions', 'total_spend', 'area_sq_km'],
            color_continuous_scale="Viridis",
            mapbox_style="carto-positron",
            zoom=7,
            center={"lat": 46.8, "lon": 8.2},
            opacity=0.5
        )
        st.plotly_chart(fig1, use_container_width=True)
    
    with tab2:
        st.subheader("Tourist Flow Analysis")
        fig2 = go.Figure()
        
        # Add Swiss tourists
        fig2.add_trace(go.Bar(
            x=filtered_gdf['geo_name'],
            y=filtered_gdf['swiss_tourists'],
            name='Swiss Tourists',
            marker_color='blue'
        ))
        
        # Add foreign tourists
        fig2.add_trace(go.Bar(
            x=filtered_gdf['geo_name'],
            y=filtered_gdf['foreign_tourists'],
            name='Foreign Tourists',
            marker_color='red'
        ))
        
        fig2.update_layout(
            barmode='stack',
            title='Tourist Distribution by Region',
            xaxis_title='Region',
            yaxis_title='Number of Tourists'
        )
        st.plotly_chart(fig2, use_container_width=True)
    
    with tab3:
        st.subheader("Industry Distribution")
        # Create a sunburst chart for industry distribution
        industry_data = []
        for idx, row in filtered_gdf.iterrows():
            for industry, volume in zip(row['top_industries'], row['industry_volumes']):
                industry_data.append({
                    'region': row['geo_name'],
                    'industry': industry,
                    'volume': volume
                })
        
        industry_df = pd.DataFrame(industry_data)
        fig3 = px.sunburst(
            industry_df,
            path=['region', 'industry'],
            values='volume',
            title='Industry Distribution by Region'
        )
        st.plotly_chart(fig3, use_container_width=True)
    
    with tab4:
        st.subheader("Economic Clusters")
        # Create a scatter plot for economic clusters
        fig4 = px.scatter(
            filtered_gdf,
            x='total_transactions',
            y='total_spend',
            size='transaction_density',
            color='industry_count',
            hover_name='geo_name',
            title='Economic Clusters by Transaction Volume and Spend'
        )
        st.plotly_chart(fig4, use_container_width=True)
    
    # Add summary statistics
    st.sidebar.header("Summary Statistics")
    st.sidebar.metric("Total Regions", len(filtered_gdf))
    st.sidebar.metric("Average Transaction Density", 
                     f"{filtered_gdf['transaction_density'].mean():.2f}")
    st.sidebar.metric("Total Transactions", 
                     f"{filtered_gdf['total_transactions'].sum():,.0f}")
    st.sidebar.metric("Total Spend", 
                     f"CHF {filtered_gdf['total_spend'].sum():,.0f}")

def main():
    st.set_page_config(layout="wide")
    gdf = load_spatial_data()
    create_combined_visualization(gdf)

if __name__ == "__main__":
    main() 