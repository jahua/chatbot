#!/usr/bin/env python3
"""
Standalone Swisscom Insights Processing Script

This script processes data from the aoi_days_raw table and generates insights
without requiring the Streamlit application. It can be scheduled to run periodically
via cron job or another scheduler.

Usage:
    python swisscom_insights_processor.py --region bellinzonese --month 3 --year 2024 --output insights_report.html
"""

import argparse
import os
import json
import pandas as pd
import numpy as np
import psycopg2
import psycopg2.extras
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import seaborn as sns
from datetime import datetime
import logging
from pathlib import Path
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("swisscom_insights.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("swisscom_insights")

# Database connection parameters
DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "3.76.40.121"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "dbname": os.getenv("POSTGRES_DB", "trip_dw"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "336699"),
    "options": f"-c search_path={os.getenv('DB_SCHEMA', 'data_lake')}"
}

# Define region mapping
REGIONS = {
    "bellinzonese": "Bellinzonese",
    "ascona-locarno": "Ascona-Locarno",
    "luganese": "Luganese",
    "mendrisiotto": "Mendrisiotto"
}

def connect_to_db():
    """Establish connection to PostgreSQL database"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        logger.info(f"Successfully connected to database: {DB_CONFIG['dbname']}@{DB_CONFIG['host']}")
        logger.info(f"Using schema: {os.getenv('DB_SCHEMA', 'data_lake')}")
        return conn, cursor
    except psycopg2.Error as e:
        logger.error(f"Database connection error: {str(e)}")
        return None, None

def fetch_raw_data(cursor, region, month, year):
    """Fetch raw data from aoi_days_raw table"""
    try:
        schema = os.getenv("DB_SCHEMA", "data_lake")
        query = f"""
            SELECT 
                aoi_id, aoi_date, 
                visitors, dwelltimes, demographics, 
                top_swiss_municipalities
            FROM 
                {schema}.aoi_days_raw
            WHERE 
                aoi_id = %s AND 
                EXTRACT(MONTH FROM aoi_date) = %s AND 
                EXTRACT(YEAR FROM aoi_date) = %s
        """
        cursor.execute(query, (region, month, year))
        results = cursor.fetchall()
        
        if not results:
            logger.warning(f"No data found for region {region}, month {month}, year {year}")
            return None
        
        logger.info(f"Successfully fetched {len(results)} days of data")
        return results
    except Exception as e:
        logger.error(f"Error fetching data: {str(e)}")
        return None

def process_visitor_categories(raw_data):
    """Process visitor categories from raw data"""
    tourist_categories_data = []
    
    for row in raw_data:
        aoi_id, aoi_date, visitors, dwelltimes, demographics, top_municipalities = row
        
        if visitors:
            visitors_dict = visitors if isinstance(visitors, dict) else json.loads(visitors)
            categories = {
                "Swiss Commuters": "swissCommuter",
                "Swiss Locals": "swissLocal",
                "Swiss Tourists": "swissTourist",
                "Foreign Workers": "foreignWorker",
                "Foreign Tourists": "foreignTourist"
            }
            
            for display_name, key in categories.items():
                if key in visitors_dict:
                    tourist_categories_data.append({
                        "date": aoi_date,
                        "name": display_name,
                        "value": visitors_dict[key]
                    })
    
    df = pd.DataFrame(tourist_categories_data)
    if not df.empty:
        # Convert date to datetime if it's not already
        df['date'] = pd.to_datetime(df['date'])
        
        # Create aggregated dataframe by category
        agg_df = df.groupby("name")["value"].sum().reset_index()
        agg_df = agg_df.sort_values("value", ascending=False)
        
        # Create time series dataframe
        time_df = df.pivot_table(index='date', columns='name', values='value', aggfunc='sum').reset_index()
        time_df = time_df.fillna(0)
        
        return {
            "aggregated": agg_df,
            "time_series": time_df
        }
    else:
        logger.warning("No visitor category data found")
        return None

def process_dwell_time(raw_data):
    """Process dwell time data from raw data"""
    dwell_time_data = []
    
    for row in raw_data:
        aoi_id, aoi_date, visitors, dwelltimes, demographics, top_municipalities = row
        
        if dwelltimes:
            dwelltimes_dict = dwelltimes if isinstance(dwelltimes, dict) else json.loads(dwelltimes)
            dwell_ranges = [
                {"range": "0.5-1h", "key": "0.5-1", "sort": 1},
                {"range": "1-2h", "key": "1-2", "sort": 2},
                {"range": "2-3h", "key": "2-3", "sort": 3},
                {"range": "3-4h", "key": "3-4", "sort": 4},
                {"range": "4-5h", "key": "4-5", "sort": 5},
                {"range": "5-6h", "key": "5-6", "sort": 6},
                {"range": "6-7h", "key": "6-7", "sort": 7},
                {"range": "7-8h", "key": "7-8", "sort": 8},
                {"range": "8-24h", "key": "8-24", "sort": 9}
            ]
            
            for range_info in dwell_ranges:
                key = range_info["key"]
                if key in dwelltimes_dict and "total" in dwelltimes_dict[key]:
                    dwell_time_data.append({
                        "date": aoi_date,
                        "range": range_info["range"],
                        "value": dwelltimes_dict[key]["total"],
                        "sort_order": range_info["sort"]
                    })
    
    df = pd.DataFrame(dwell_time_data)
    if not df.empty:
        # Convert date to datetime if it's not already
        df['date'] = pd.to_datetime(df['date'])
        
        # Create aggregated dataframe
        agg_df = df.groupby(["range", "sort_order"])["value"].sum().reset_index()
        agg_df = agg_df.sort_values("sort_order")
        
        # Create time series dataframe
        time_df = df.pivot_table(index='date', columns='range', values='value', aggfunc='sum').reset_index()
        time_df = time_df.fillna(0)
        
        return {
            "aggregated": agg_df,
            "time_series": time_df
        }
    else:
        logger.warning("No dwell time data found")
        return None

def process_demographics(raw_data):
    """Process demographics data from raw data"""
    age_gender_data = []
    
    for row in raw_data:
        aoi_id, aoi_date, visitors, dwelltimes, demographics, top_municipalities = row
        
        if demographics:
            demographics_dict = demographics if isinstance(demographics, dict) else json.loads(demographics)
            age_groups = ["0-19", "20-39", "40-64", "65+"]
            
            for i, age_group in enumerate(age_groups):
                if age_group in demographics_dict:
                    male_count = demographics_dict[age_group].get("male", 0)
                    female_count = demographics_dict[age_group].get("female", 0)
                    age_gender_data.append({
                        "date": aoi_date,
                        "age": age_group,
                        "male": male_count,
                        "female": female_count,
                        "total": male_count + female_count,
                        "sort_order": i + 1
                    })
    
    df = pd.DataFrame(age_gender_data)
    if not df.empty:
        # Convert date to datetime if it's not already
        df['date'] = pd.to_datetime(df['date'])
        
        # Create aggregated dataframe
        agg_df = df.groupby(["age", "sort_order"]).agg({
            "male": "sum",
            "female": "sum",
            "total": "sum"
        }).reset_index()
        agg_df = agg_df.sort_values("sort_order")
        
        # Create time series dataframe
        time_df = df.pivot_table(index='date', columns='age', values=['male', 'female'], aggfunc='sum').reset_index()
        time_df.columns = [f"{col[0]}_{col[1]}" if col[1] else col[0] for col in time_df.columns]
        time_df = time_df.fillna(0)
        
        return {
            "aggregated": agg_df,
            "time_series": time_df
        }
    else:
        logger.warning("No demographics data found")
        return None

def process_municipalities(raw_data):
    """Process municipalities data from raw data"""
    municipalities_data = []
    
    for row in raw_data:
        aoi_id, aoi_date, visitors, dwelltimes, demographics, top_municipalities = row
        
        if top_municipalities:
            muni_list = top_municipalities if isinstance(top_municipalities, list) else json.loads(top_municipalities)
            for muni in muni_list:
                if isinstance(muni, dict) and "name" in muni and "value" in muni:
                    municipalities_data.append({
                        "date": aoi_date,
                        "name": muni["name"],
                        "value": muni["value"]
                    })
    
    df = pd.DataFrame(municipalities_data)
    if not df.empty:
        # Convert date to datetime if it's not already
        df['date'] = pd.to_datetime(df['date'])
        
        # Create aggregated dataframe
        agg_df = df.groupby("name")["value"].sum().reset_index()
        agg_df = agg_df.sort_values("value", ascending=False).head(10)
        
        # Create time series for top 5 municipalities
        top_5_municipalities = agg_df.head(5)["name"].tolist()
        time_df = df[df["name"].isin(top_5_municipalities)].pivot_table(
            index='date', 
            columns='name', 
            values='value', 
            aggfunc='sum'
        ).reset_index()
        time_df = time_df.fillna(0)
        
        return {
            "aggregated": agg_df,
            "time_series": time_df
        }
    else:
        logger.warning("No municipalities data found")
        return None

def generate_insights(data_dict):
    """Generate insights from the processed data"""
    insights = []
    
    try:
        # Insight 1: Visitor Mix
        if data_dict.get("visitor_categories") and not data_dict["visitor_categories"]["aggregated"].empty:
            df = data_dict["visitor_categories"]["aggregated"]
            total_visitors = df["value"].sum()
            
            # Calculate percentages
            categories_sorted = df.sort_values("value", ascending=False)
            top_category = categories_sorted.iloc[0]["name"]
            top_percentage = (categories_sorted.iloc[0]["value"] / total_visitors) * 100
            
            # Foreign vs Local split
            foreign_categories = ["Foreign Workers", "Foreign Tourists"]
            foreign_visitors = df[df["name"].isin(foreign_categories)]["value"].sum()
            foreign_percentage = (foreign_visitors / total_visitors) * 100
            
            insights.append({
                "title": "Visitor Mix",
                "points": [
                    f"Total visitors: {total_visitors:,}",
                    f"Most common visitor type: {top_category} ({top_percentage:.1f}%)",
                    f"Foreign visitors: {foreign_percentage:.1f}% of total"
                ]
            })
        
        # Insight 2: Dwell Time
        if data_dict.get("dwell_time") and not data_dict["dwell_time"]["aggregated"].empty:
            df = data_dict["dwell_time"]["aggregated"]
            
            # Find most common dwell time range
            most_common_range = df.loc[df["value"].idxmax()]["range"]
            
            # Calculate percentage of visitors staying more than 4 hours
            long_stay_ranges = ["4-5h", "5-6h", "6-7h", "7-8h", "8-24h"]
            long_stay_visitors = df[df["range"].isin(long_stay_ranges)]["value"].sum()
            total_dwell_visitors = df["value"].sum()
            long_stay_percentage = (long_stay_visitors / total_dwell_visitors) * 100
            
            insights.append({
                "title": "Visit Duration",
                "points": [
                    f"Most common visit duration: {most_common_range}",
                    f"{long_stay_percentage:.1f}% of visitors stay longer than 4 hours",
                    f"Average visitor turnover: {24/((df['sort_order'] * df['value']).sum() / total_dwell_visitors):.1f} times per day"
                ]
            })
        
        # Insight 3: Demographics
        if data_dict.get("demographics") and not data_dict["demographics"]["aggregated"].empty:
            df = data_dict["demographics"]["aggregated"]
            
            # Gender distribution
            total_male = df["male"].sum()
            total_female = df["female"].sum()
            total_visitors = total_male + total_female
            male_percentage = (total_male / total_visitors) * 100
            female_percentage = (total_female / total_visitors) * 100
            
            # Age distribution
            most_common_age = df.loc[df["total"].idxmax()]["age"]
            youth_percentage = df[df["age"] == "0-19"]["total"].sum() / total_visitors * 100
            senior_percentage = df[df["age"] == "65+"]["total"].sum() / total_visitors * 100
            
            insights.append({
                "title": "Visitor Demographics",
                "points": [
                    f"Gender split: {male_percentage:.1f}% male, {female_percentage:.1f}% female",
                    f"Most common age group: {most_common_age}",
                    f"Youth visitors (0-19): {youth_percentage:.1f}%",
                    f"Senior visitors (65+): {senior_percentage:.1f}%"
                ]
            })
        
        # Insight 4: Geographic Distribution
        if data_dict.get("municipalities") and not data_dict["municipalities"]["aggregated"].empty:
            df = data_dict["municipalities"]["aggregated"]
            
            top_municipality = df.iloc[0]["name"]
            top_5_concentration = df.head(5)["value"].sum() / df["value"].sum() * 100
            
            insights.append({
                "title": "Geographic Distribution",
                "points": [
                    f"Top visitor municipality: {top_municipality}",
                    f"Top 5 municipalities account for {top_5_concentration:.1f}% of visitors",
                    f"Total municipalities with visitors: {len(df)}"
                ]
            })
        
    except Exception as e:
        logger.error(f"Error generating insights: {str(e)}")
    
    return insights

def generate_plots(data_dict, output_dir):
    """Generate plots from the processed data and save to files"""
    plots = {}
    
    try:
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        # 1. Visitor Categories Pie Chart
        if data_dict.get("visitor_categories") and not data_dict["visitor_categories"]["aggregated"].empty:
            df = data_dict["visitor_categories"]["aggregated"]
            fig = px.pie(df, values='value', names='name', title="Visitor Categories")
            fig.update_traces(textposition='inside', textinfo='percent+label')
            fig.write_html(os.path.join(output_dir, "visitor_categories_pie.html"))
            plots["visitor_categories_pie"] = "visitor_categories_pie.html"
            
            # Visitor Categories Time Series
            time_df = data_dict["visitor_categories"]["time_series"]
            fig = px.line(time_df, x='date', y=time_df.columns[1:], title="Visitor Categories Over Time")
            fig.update_layout(xaxis_title="Date", yaxis_title="Number of Visitors")
            fig.write_html(os.path.join(output_dir, "visitor_categories_time.html"))
            plots["visitor_categories_time"] = "visitor_categories_time.html"
        
        # 2. Dwell Time Bar Chart
        if data_dict.get("dwell_time") and not data_dict["dwell_time"]["aggregated"].empty:
            df = data_dict["dwell_time"]["aggregated"]
            fig = px.bar(df, x='range', y='value', title="Dwell Time Distribution")
            fig.update_layout(xaxis_title="Time Range", yaxis_title="Number of Visitors")
            fig.write_html(os.path.join(output_dir, "dwell_time_bar.html"))
            plots["dwell_time_bar"] = "dwell_time_bar.html"
            
            # Dwell Time Time Series
            time_df = data_dict["dwell_time"]["time_series"]
            fig = px.line(time_df, x='date', y=time_df.columns[1:], title="Dwell Time Over Time")
            fig.update_layout(xaxis_title="Date", yaxis_title="Number of Visitors")
            fig.write_html(os.path.join(output_dir, "dwell_time_time.html"))
            plots["dwell_time_time"] = "dwell_time_time.html"
        
        # 3. Demographics Bar Chart
        if data_dict.get("demographics") and not data_dict["demographics"]["aggregated"].empty:
            df = data_dict["demographics"]["aggregated"]
            fig = go.Figure()
            fig.add_trace(go.Bar(x=df['age'], y=df['male'], name='Male', marker_color='#1F77B4'))
            fig.add_trace(go.Bar(x=df['age'], y=df['female'], name='Female', marker_color='#FF7F0E'))
            fig.update_layout(
                title="Visitor Demographics",
                xaxis_title="Age Group",
                yaxis_title="Number of Visitors",
                barmode='group'
            )
            fig.write_html(os.path.join(output_dir, "demographics_bar.html"))
            plots["demographics_bar"] = "demographics_bar.html"
        
        # 4. Top Municipalities Bar Chart
        if data_dict.get("municipalities") and not data_dict["municipalities"]["aggregated"].empty:
            df = data_dict["municipalities"]["aggregated"].head(10)
            fig = px.bar(df, x='name', y='value', title="Top 10 Municipalities by Visitor Count")
            fig.update_layout(xaxis_title="Municipality", yaxis_title="Number of Visitors")
            fig.write_html(os.path.join(output_dir, "municipalities_bar.html"))
            plots["municipalities_bar"] = "municipalities_bar.html"
            
            # Municipalities Time Series for Top 5
            time_df = data_dict["municipalities"]["time_series"]
            if not time_df.empty and len(time_df.columns) > 1:
                fig = px.line(time_df, x='date', y=time_df.columns[1:], title="Top 5 Municipalities Over Time")
                fig.update_layout(xaxis_title="Date", yaxis_title="Number of Visitors")
                fig.write_html(os.path.join(output_dir, "municipalities_time.html"))
                plots["municipalities_time"] = "municipalities_time.html"
        
        # 5. Combined Dashboard
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=(
                "Visitor Categories", 
                "Dwell Time Distribution", 
                "Visitor Demographics",
                "Top Municipalities"
            ),
            specs=[
                [{"type": "pie"}, {"type": "bar"}],
                [{"type": "bar"}, {"type": "bar"}]
            ]
        )
        
        # Add visitor categories pie chart
        if data_dict.get("visitor_categories") and not data_dict["visitor_categories"]["aggregated"].empty:
            df = data_dict["visitor_categories"]["aggregated"]
            fig.add_trace(
                go.Pie(
                    labels=df['name'], 
                    values=df['value'],
                    textinfo='percent',
                    showlegend=True
                ),
                row=1, col=1
            )
        
        # Add dwell time bar chart
        if data_dict.get("dwell_time") and not data_dict["dwell_time"]["aggregated"].empty:
            df = data_dict["dwell_time"]["aggregated"]
            fig.add_trace(
                go.Bar(x=df['range'], y=df['value']),
                row=1, col=2
            )
        
        # Add demographics stacked bar chart
        if data_dict.get("demographics") and not data_dict["demographics"]["aggregated"].empty:
            df = data_dict["demographics"]["aggregated"]
            fig.add_trace(
                go.Bar(x=df['age'], y=df['male'], name='Male', marker_color='#1F77B4'),
                row=2, col=1
            )
            fig.add_trace(
                go.Bar(x=df['age'], y=df['female'], name='Female', marker_color='#FF7F0E'),
                row=2, col=1
            )
        
        # Add top municipalities bar chart
        if data_dict.get("municipalities") and not data_dict["municipalities"]["aggregated"].empty:
            df = data_dict["municipalities"]["aggregated"].head(5)
            fig.add_trace(
                go.Bar(x=df['name'], y=df['value']),
                row=2, col=2
            )
        
        fig.update_layout(
            title_text="Swisscom Insights Dashboard",
            height=800
        )
        fig.write_html(os.path.join(output_dir, "dashboard.html"))
        plots["dashboard"] = "dashboard.html"
        
    except Exception as e:
        logger.error(f"Error generating plots: {str(e)}")
    
    return plots

def generate_report(region_name, month_name, year, data_dict, insights, plots, output_file):
    """Generate an HTML report with insights and plots"""
    try:
        # Create HTML template
        html_content = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Swisscom Insights Report - {region_name} - {month_name} {year}</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; line-height: 1.6; }}
                .header {{ background-color: #003C84; color: white; padding: 20px; margin-bottom: 20px; }}
                .container {{ max-width: 1200px; margin: 0 auto; }}
                .section {{ margin-bottom: 30px; }}
                h1, h2, h3 {{ margin-top: 0; }}
                .insight-box {{ background-color: #f5f5f5; border-left: 4px solid #003C84; padding: 15px; margin-bottom: 15px; }}
                .insight-title {{ font-weight: bold; margin-bottom: 10px; }}
                .insight-points {{ list-style-type: none; padding-left: 0; margin: 0; }}
                .insight-points li {{ margin-bottom: 5px; }}
                .plot-container {{ margin-top: 20px; }}
                iframe {{ width: 100%; height: 500px; border: none; }}
                .dashboard-container {{ margin-top: 30px; }}
                .footer {{ text-align: center; margin-top: 30px; padding-top: 20px; border-top: 1px solid #ccc; color: #666; }}
            </style>
        </head>
        <body>
            <div class="header">
                <div class="container">
                    <h1>Swisscom Insights Report</h1>
                    <p>{region_name} - {month_name} {year}</p>
                </div>
            </div>
            
            <div class="container">
                <div class="section">
                    <h2>Key Insights</h2>
        """
        
        # Add insights
        for insight in insights:
            html_content += f"""
                    <div class="insight-box">
                        <div class="insight-title">{insight['title']}</div>
                        <ul class="insight-points">
            """
            
            for point in insight['points']:
                html_content += f"""
                            <li>{point}</li>
                """
            
            html_content += """
                        </ul>
                    </div>
            """
        
        # Add dashboard
        if "dashboard" in plots:
            output_path = os.path.dirname(output_file)
            dashboard_path = os.path.join(output_path, plots["dashboard"])
            html_content += f"""
                <div class="section">
                    <h2>Dashboard Overview</h2>
                    <div class="dashboard-container">
                        <iframe src="{plots['dashboard']}" width="100%" height="800px"></iframe>
                    </div>
                </div>
            """
        
        # Add individual plot sections
        if "visitor_categories_pie" in plots or "visitor_categories_time" in plots:
            html_content += """
                <div class="section">
                    <h2>Visitor Categories</h2>
            """
            
            if "visitor_categories_pie" in plots:
                html_content += f"""
                    <div class="plot-container">
                        <h3>Visitor Mix</h3>
                        <iframe src="{plots['visitor_categories_pie']}"></iframe>
                    </div>
                """
            
            if "visitor_categories_time" in plots:
                html_content += f"""
                    <div class="plot-container">
                        <h3>Visitor Categories Over Time</h3>
                        <iframe src="{plots['visitor_categories_time']}"></iframe>
                    </div>
                """
            
            html_content += """
                </div>
            """
        
        if "dwell_time_bar" in plots or "dwell_time_time" in plots:
            html_content += """
                <div class="section">
                    <h2>Dwell Time Analysis</h2>
            """
            
            if "dwell_time_bar" in plots:
                html_content += f"""
                    <div class="plot-container">
                        <h3>Dwell Time Distribution</h3>
                        <iframe src="{plots['dwell_time_bar']}"></iframe>
                    </div>
                """
            
            if "dwell_time_time" in plots:
                html_content += f"""
                    <div class="plot-container">
                        <h3>Dwell Time Trends</h3>
                        <iframe src="{plots['dwell_time_time']}"></iframe>
                    </div>
                """
            
            html_content += """
                </div>
            """
        
        if "demographics_bar" in plots:
            html_content += f"""
                <div class="section">
                    <h2>Visitor Demographics</h2>
                    <div class="plot-container">
                        <iframe src="{plots['demographics_bar']}"></iframe>
                    </div>
                </div>
            """
        
        if "municipalities_bar" in plots or "municipalities_time" in plots:
            html_content += """
                <div class="section">
                    <h2>Geographic Distribution</h2>
            """
            
            if "municipalities_bar" in plots:
                html_content += f"""
                    <div class="plot-container">
                        <h3>Top Municipalities</h3>
                        <iframe src="{plots['municipalities_bar']}"></iframe>
                    </div>
                """
            
            if "municipalities_time" in plots:
                html_content += f"""
                    <div class="plot-container">
                        <h3>Municipality Trends</h3>
                        <iframe src="{plots['municipalities_time']}"></iframe>
                    </div>
                """
            
            html_content += """
                </div>
            """
        
        # Close HTML
        html_content += f"""
                <div class="footer">
                    <p>Report generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                    <p>Swisscom Insights Processor</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        # Write to file
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.info(f"Report successfully generated: {output_file}")
        return True
    
    except Exception as e:
        logger.error(f"Error generating report: {str(e)}")
        return False

def export_to_csv(data_dict, output_dir):
    """Export processed data to CSV files"""
    try:
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        # Export visitor categories
        if data_dict.get("visitor_categories"):
            if not data_dict["visitor_categories"]["aggregated"].empty:
                data_dict["visitor_categories"]["aggregated"].to_csv(
                    os.path.join(output_dir, "visitor_categories.csv"), index=False)
            if not data_dict["visitor_categories"]["time_series"].empty:
                data_dict["visitor_categories"]["time_series"].to_csv(
                    os.path.join(output_dir, "visitor_categories_time_series.csv"), index=False)
        
        # Export dwell time data
        if data_dict.get("dwell_time"):
            if not data_dict["dwell_time"]["aggregated"].empty:
                data_dict["dwell_time"]["aggregated"].to_csv(
                    os.path.join(output_dir, "dwell_time.csv"), index=False)
            if not data_dict["dwell_time"]["time_series"].empty:
                data_dict["dwell_time"]["time_series"].to_csv(
                    os.path.join(output_dir, "dwell_time_time_series.csv"), index=False)
        
        # Export demographics data
        if data_dict.get("demographics"):
            if not data_dict["demographics"]["aggregated"].empty:
                data_dict["demographics"]["aggregated"].to_csv(
                    os.path.join(output_dir, "demographics.csv"), index=False)
            if not data_dict["demographics"]["time_series"].empty:
                data_dict["demographics"]["time_series"].to_csv(
                    os.path.join(output_dir, "demographics_time_series.csv"), index=False)
        
        # Export municipalities data
        if data_dict.get("municipalities"):
            if not data_dict["municipalities"]["aggregated"].empty:
                data_dict["municipalities"]["aggregated"].to_csv(
                    os.path.join(output_dir, "municipalities.csv"), index=False)
            if not data_dict["municipalities"]["time_series"].empty:
                data_dict["municipalities"]["time_series"].to_csv(
                    os.path.join(output_dir, "municipalities_time_series.csv"), index=False)
        
        logger.info(f"Data exported to CSV files in directory: {output_dir}")
        return True
    except Exception as e:
        logger.error(f"Error exporting data to CSV: {str(e)}")
        return False

def main():
    """Main execution function"""
    # Setup argument parser
    parser = argparse.ArgumentParser(description="Swisscom Insights Processor")
    parser.add_argument("--region", type=str, choices=list(REGIONS.keys()), required=True,
                      help="Region to analyze (bellinzonese, ascona-locarno, luganese, mendrisiotto)")
    parser.add_argument("--month", type=int, choices=range(1, 13), required=True,
                      help="Month to analyze (1-12)")
    parser.add_argument("--year", type=int, choices=[2023, 2024, 2025], required=True,
                      help="Year to analyze (2023, 2024, 2025)")
    parser.add_argument("--output", type=str, default="swisscom_insights_report.html",
                      help="Output HTML file path")
    parser.add_argument("--output-dir", type=str, default="output",
                      help="Directory for output files and plots")
    parser.add_argument("--export-csv", action="store_true",
                      help="Export data to CSV files")
    parser.add_argument("--db-host", type=str, help="Database host")
    parser.add_argument("--db-port", type=str, help="Database port")
    parser.add_argument("--db-name", type=str, help="Database name")
    parser.add_argument("--db-user", type=str, help="Database username")
    parser.add_argument("--db-password", type=str, help="Database password")
    parser.add_argument("--db-schema", type=str, help="Database schema")
    
    # Parse arguments
    args = parser.parse_args()
    
    # Update DB_CONFIG if provided via arguments
    if args.db_host: DB_CONFIG["host"] = args.db_host
    if args.db_port: DB_CONFIG["port"] = args.db_port
    if args.db_name: DB_CONFIG["dbname"] = args.db_name
    if args.db_user: DB_CONFIG["user"] = args.db_user
    if args.db_password: DB_CONFIG["password"] = args.db_password
    if args.db_schema: 
        os.environ["DB_SCHEMA"] = args.db_schema
        DB_CONFIG["options"] = f"-c search_path={args.db_schema}"
    
    # Get region name for display
    region_name = REGIONS.get(args.region, args.region.capitalize())
    # Get month name for display
    month_name = datetime.date(2000, args.month, 1).strftime('%B')
    
    # Log script start
    logger.info(f"Starting Swisscom Insights Processor for {region_name}, {month_name} {args.year}")
    
    # Connect to database
    conn, cursor = connect_to_db()
    if not conn or not cursor:
        logger.error("Failed to connect to database. Exiting.")
        return 1
    
    try:
        # Fetch raw data
        raw_data = fetch_raw_data(cursor, args.region, args.month, args.year)
        if not raw_data:
            logger.error("No data found for the specified criteria. Exiting.")
            return 1
        
        # Process data
        logger.info("Processing visitor categories...")
        visitor_categories = process_visitor_categories(raw_data)
        
        logger.info("Processing dwell time data...")
        dwell_time = process_dwell_time(raw_data)
        
        logger.info("Processing demographics data...")
        demographics = process_demographics(raw_data)
        
        logger.info("Processing municipalities data...")
        municipalities = process_municipalities(raw_data)
        
        # Combine processed data
        data_dict = {
            "visitor_categories": visitor_categories,
            "dwell_time": dwell_time,
            "demographics": demographics,
            "municipalities": municipalities
        }
        
        # Create output directory
        os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
        os.makedirs(args.output_dir, exist_ok=True)
        
        # Generate insights
        logger.info("Generating insights...")
        insights = generate_insights(data_dict)
        
        # Generate plots
        logger.info("Generating plots...")
        plots = generate_plots(data_dict, args.output_dir)
        
        # Generate report
        logger.info("Generating HTML report...")
        generate_report(region_name, month_name, args.year, data_dict, insights, plots, args.output)
        
        # Export to CSV if requested
        if args.export_csv:
            logger.info("Exporting data to CSV...")
            export_to_csv(data_dict, args.output_dir)
        
        logger.info("Processing completed successfully.")
        return 0
        
    except Exception as e:
        logger.error(f"An error occurred during processing: {str(e)}")
        return 1
    
    finally:
        # Clean up database connection
        if cursor: cursor.close()
        if conn: conn.close()

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)