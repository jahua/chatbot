import streamlit as st
import pandas as pd
import psycopg2
import psycopg2.extras
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import datetime
import json

# Set page config for wide layout and page title/icon
st.set_page_config(page_title="Swisscom Insights", page_icon="📊", layout="wide")

# Set the page config - Streamlit uses filename, but we can set title
# st.set_option('deprecation.showPyplotGlobalUse', False) # This is likely redundant/handled by set_page_config

# Initialize db_config in session state if it doesn't exist (for standalone page usage)
if 'db_config' not in st.session_state:
    st.session_state.db_config = {
        "host": "3.76.40.121",
        "port": "5432",
        "dbname": "trip_dw",
        "user": "postgres",
        "password": "336699"
    }
    st.info("Using default database configuration. This can be customized in the main app.")

# --- Early Exit if DB Config is Missing --- 
if 'db_config' not in st.session_state or not all(st.session_state.db_config.values()):
    st.error("Database configuration is missing or incomplete...") # Abridged error
    st.stop() 

# --- Database Connection ---
def connect_to_db():
    """Establish connection to PostgreSQL database using config from session state"""
    db_config = st.session_state.db_config
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        # Set the search path to include the data_lake schema
        cursor.execute('SET search_path TO data_lake, public;')
        # schema_info = "" # Removed schema info display from sidebar
        # if "schema" in db_config and db_config["schema"]:
        #     schema_info = f" (Schema: {db_config['schema']})"
        # st.sidebar.success(f"DB Connected: {db_config['dbname']}@{db_config['host']}{schema_info}") # REMOVED THIS LINE
        return conn, cursor
    except psycopg2.Error as e:
        st.error(f"Database connection error: {str(e)}")
        st.sidebar.error("Connection Failed")
        return None, None

# --- Fetch Bellinzonese Municipalities ---
def fetch_bellinzonese_municipalities(cursor):
    """Fetch list of municipalities in the Bellinzonese region"""
    try:
        # Bellinzonese region ID
        bellinzonese_id = 'f7883818-99e1-4d20-b09a-5171bf16133a'
        
        query = """
            WITH municipality_data AS (
                SELECT 
                    jsonb_array_elements(top_swiss_municipalities)->>'name' AS municipality_name,
                    (jsonb_array_elements(top_swiss_municipalities)->>'visitors')::INTEGER AS visitor_count
                FROM 
                    swisscom_dashboard_view
                WHERE 
                    region_id = %s
                    AND top_swiss_municipalities IS NOT NULL
            )
            SELECT 
                municipality_name,
                SUM(visitor_count) AS total_visitors
            FROM 
                municipality_data
            GROUP BY 
                municipality_name
            ORDER BY 
                total_visitors DESC
            LIMIT 20;
        """
        cursor.execute(query, (bellinzonese_id,))
        results = cursor.fetchall()
        
        # Convert to list of dictionaries
        municipalities = [dict(row) for row in results]
        return municipalities
    except Exception as e:
        st.error(f"Error fetching municipalities: {str(e)}")
        return []

# --- Fetch Dashboard Data for Municipality ---
def fetch_municipality_data(cursor, municipality, month):
    """Fetch data for a specific municipality and month"""
    try:
        # Bellinzonese region ID
        bellinzonese_id = 'f7883818-99e1-4d20-b09a-5171bf16133a'
        
        # First get the region-level data
        region_query = """
            SELECT 
                region_id, region_name, month, year,
                swiss_commuters, swiss_locals, swiss_tourists, 
                foreign_workers, foreign_tourists, total_visitors,
                dwelltimes, demographics, top_swiss_municipalities
            FROM 
                data_lake.swisscom_dashboard_view
            WHERE 
                region_id = %s AND month = %s AND year = 2023
        """
        params = (bellinzonese_id, month)
        
        cursor.execute(region_query, params)
        region_results = cursor.fetchall()
        
        if not region_results:
            return pd.DataFrame()
        
        # Process data to extract municipality information
        municipality_data = []
        for row in region_results:
            row_dict = dict(row)
            
            if row_dict['top_swiss_municipalities'] is not None:
                try:
                    # Parse JSON if needed
                    if isinstance(row_dict['top_swiss_municipalities'], str):
                        municipalities_list = json.loads(row_dict['top_swiss_municipalities'])
                    else:
                        municipalities_list = row_dict['top_swiss_municipalities']
                    
                    # Find the specified municipality in the list
                    muni_entry = next((item for item in municipalities_list if item.get('name') == municipality), None)
                    
                    if muni_entry:
                        # Create a copy of the row for this municipality
                        muni_row = row_dict.copy()
                        
                        # Set visitor count for this municipality
                        muni_visitors = int(muni_entry.get('visitors', 0))
                        
                        # Preserve other data but scale down based on municipality proportion
                        total_visitors = row_dict['total_visitors'] if pd.notna(row_dict['total_visitors']) else 0
                        
                        if total_visitors > 0:
                            # Scale factor to adjust all visitor categories
                            scale_factor = muni_visitors / total_visitors
                            muni_row['swiss_commuters'] = int(row_dict['swiss_commuters'] * scale_factor) if pd.notna(row_dict['swiss_commuters']) else 0
                            muni_row['swiss_locals'] = int(row_dict['swiss_locals'] * scale_factor) if pd.notna(row_dict['swiss_locals']) else 0
                            muni_row['swiss_tourists'] = int(row_dict['swiss_tourists'] * scale_factor) if pd.notna(row_dict['swiss_tourists']) else 0
                            muni_row['foreign_workers'] = int(row_dict['foreign_workers'] * scale_factor) if pd.notna(row_dict['foreign_workers']) else 0
                            muni_row['foreign_tourists'] = int(row_dict['foreign_tourists'] * scale_factor) if pd.notna(row_dict['foreign_tourists']) else 0
                            muni_row['total_visitors'] = muni_visitors
                        else:
                            # If total_visitors is 0, just use the municipality visitors
                            muni_row['total_visitors'] = muni_visitors
                            # Simple distribution of visitor types
                            muni_row['swiss_commuters'] = int(muni_visitors * 0.2)
                            muni_row['swiss_locals'] = int(muni_visitors * 0.3)
                            muni_row['swiss_tourists'] = int(muni_visitors * 0.3)
                            muni_row['foreign_workers'] = int(muni_visitors * 0.05)
                            muni_row['foreign_tourists'] = int(muni_visitors * 0.15)
                        
                        municipality_data.append(muni_row)
                except (json.JSONDecodeError, TypeError) as e:
                    st.warning(f"Error parsing municipality data: {str(e)}")
                    continue
        
        return pd.DataFrame(municipality_data)
    except Exception as e:
        st.error(f"Error fetching municipality data: {str(e)}")
        return pd.DataFrame()

# --- Mock Data Generation ---
def get_sample_data(municipality, month, year=2023):
    """Generate sample data for testing when actual data is not available"""
    # Define multipliers for different municipalities
    # Note: Removed "All Bellinzonese" specific multiplier logic, handled by default
    municipality_multiplier = {
        'Bellinzona': 1.5,
        'Arbedo-Castione': 0.8,
        'Cadenazzo': 0.7,
        'Riviera': 0.9,
        'Isone': 0.5,
        # Add other specific municipalities if needed, or use default 1.0
    }.get(municipality, 1.0) # Default multiplier for unlisted or "All Bellinzonese"
    
    month_str = str(month)
    seasonal_multiplier = {'1': 0.7, '2': 0.8, '3': 0.9, '4': 1.0, '5': 1.2, '6': 1.3, '7': 1.5, '8': 1.5, '9': 1.2, '10': 1.0, '11': 0.8, '12': 0.7}.get(month_str, 1)
    total_multiplier = municipality_multiplier * seasonal_multiplier
    
    tourist_categories = pd.DataFrame([
        {"name": "Swiss Commuters", "value": int(1200 * total_multiplier)},
        {"name": "Swiss Locals", "value": int(3500 * total_multiplier)},
        {"name": "Swiss Tourists", "value": int(2800 * total_multiplier)},
        {"name": "Foreign Workers", "value": int(800 * total_multiplier)},
        {"name": "Foreign Tourists", "value": int(1500 * total_multiplier)}
    ])
    
    dwell_time = pd.DataFrame([
        {"range": "0.5-1h", "value": int(500 * total_multiplier), "sort_order": 1}, 
        {"range": "1-2h", "value": int(800 * total_multiplier), "sort_order": 2},
        {"range": "2-3h", "value": int(1200 * total_multiplier), "sort_order": 3}, 
        {"range": "3-4h", "value": int(900 * total_multiplier), "sort_order": 4},
        {"range": "4-5h", "value": int(600 * total_multiplier), "sort_order": 5}, 
        {"range": "5-6h", "value": int(400 * total_multiplier), "sort_order": 6},
        {"range": "6-7h", "value": int(300 * total_multiplier), "sort_order": 7}, 
        {"range": "7-8h", "value": int(200 * total_multiplier), "sort_order": 8},
        {"range": "8-24h", "value": int(1000 * total_multiplier), "sort_order": 9}
    ])
    
    age_gender = pd.DataFrame([
        {"age": "0-19", "male": int(300 * total_multiplier), "female": int(280 * total_multiplier), "sort_order": 1},
        {"age": "20-39", "male": int(800 * total_multiplier), "female": int(750 * total_multiplier), "sort_order": 2},
        {"age": "40-64", "male": int(600 * total_multiplier), "female": int(650 * total_multiplier), "sort_order": 3},
        {"age": "65+", "male": int(200 * total_multiplier), "female": int(250 * total_multiplier), "sort_order": 4}
    ])
    
    # Customize municipality/area names based on selected municipality for the Geographic chart
    # Removed "All Bellinzonese" case, now always generates area names
    if municipality == 'Bellinzona':
        muni_names = ["Centro", "Stazione", "Pedemonte", "Carasso", "Daro"]
    elif municipality == 'Arbedo-Castione':
        muni_names = ["Arbedo", "Castione", "Molinazzo", "Arbedo-Sud", "Castione-Nord"]
    elif municipality == 'Cadenazzo':
        muni_names = ["Cadenazzo", "Robasacco", "Stazione", "Cadenazzo-Sud", "Industriale"]
    elif municipality == 'Riviera':
        muni_names = ["Biasca", "Cresciano", "Lodrino", "Osogna", "Iragna"]
    elif municipality == 'Isone':
        muni_names = ["Isone", "Medeglia", "Centro", "Cima", "Isone-Est"]
    else: # Fallback for any unexpected input including "All Bellinzonese"
        muni_names = [f"Area {i+1}" for i in range(5)] # Use generic Area names as fallback
        
    top_municipalities = pd.DataFrame([
        {"name": muni_names[0], "value": int(1200 * total_multiplier)}, 
        {"name": muni_names[1], "value": int(900 * total_multiplier)},
        {"name": muni_names[2], "value": int(800 * total_multiplier)}, 
        {"name": muni_names[3], "value": int(600 * total_multiplier)},
        {"name": muni_names[4], "value": int(500 * total_multiplier)}
    ])
    
    return {
        "tourist_categories": tourist_categories, 
        "dwell_time": dwell_time,
        "age_gender": age_gender, 
        "top_municipalities": top_municipalities
    }

# --- Dashboard UI Components --- 
def render_header():
    """Render the header section of the dashboard"""
    st.title("📊 Bellinzonese Municipalities Insights")
    st.markdown("#### Swisscom Mobility Data Analysis by Municipality")
    st.markdown("---")

def render_filters():
    """Render the filter controls for municipalities and months (fetches from DB)."""
    
    # Initialize default lists
    default_municipalities = [
        {"name": "Bellinzona", "total_visitors": 2881954},
        {"name": "Arbedo-Castione", "total_visitors": 421577},
        {"name": "Cadenazzo", "total_visitors": 215753},
        {"name": "Riviera", "total_visitors": 272444},
        {"name": "Isone", "total_visitors": 45789}
    ]
    
    # Connect to DB and try to get actual municipalities
    municipalities_list = default_municipalities
    conn, cursor = None, None
    try:
        conn, cursor = connect_to_db()
        if conn and cursor:
            # Using the function that fetches municipalities, assuming it exists
            # NOTE: Ensure 'fetch_bellinzonese_municipalities' is defined correctly elsewhere
            municipalities_from_db = fetch_bellinzonese_municipalities(cursor)
            if municipalities_from_db and len(municipalities_from_db) > 0:
                # Filter to focus on specific Bellinzonese municipalities if needed
                # Or just use the list directly if the fetch function returns the desired ones
                bellinzonese_municipalities = []
                target_munis = {"Bellinzona", "Arbedo-Castione", "Cadenazzo", "Riviera", "Isone"}
                for muni in municipalities_from_db:
                    # Assuming fetch_bellinzonese_municipalities returns dicts with 'municipality_name'
                    if muni.get('municipality_name') in target_munis:
                        bellinzonese_municipalities.append({
                            "name": muni['municipality_name'],
                            # We might not need total_visitors here, just the names
                            # "total_visitors": muni.get('total_visitors', 0) 
                        })
                
                # If we found our target municipalities, use them
                if bellinzonese_municipalities:
                    municipalities_list = bellinzonese_municipalities
                else:
                    st.warning("Target municipalities not found in DB query result. Using defaults.") 
            else:
                 st.warning("No municipalities found via DB query. Using defaults.")
    except NameError:
        st.warning("`fetch_bellinzonese_municipalities` function not found. Using default list.")        
    except Exception as e:
        st.warning(f"Could not fetch municipality list from database, using defaults: {str(e)}")
    finally:
        if cursor: cursor.close()
        if conn: conn.close()
    
    # Set up municipality options from the determined list
    municipality_names = sorted([m["name"] for m in municipalities_list])

    # Set up month options
    months = [{"id": i, "name": datetime.date(2000, i, 1).strftime('%B')} for i in range(1, 13)]
    month_options = {m["name"]: m["id"] for m in months}
    month_names = list(month_options.keys())

    # Initialize session state for filters
    if 'municipality' not in st.session_state or st.session_state.municipality not in municipality_names:
        st.session_state.municipality = municipality_names[0] if municipality_names else None
    if 'month' not in st.session_state:
        st.session_state.month = month_names[datetime.datetime.now().month - 1]

    # --- RENDER FILTERS (within the calling column) --- 
    if municipality_names:
        current_municipality_index = municipality_names.index(st.session_state.municipality) 
        selected_municipality = st.selectbox(
            "Municipality", 
            options=municipality_names, 
            index=current_municipality_index, 
            key="sb_municipality"
        )
        st.session_state.municipality = selected_municipality
    else:
        st.error("No municipalities available for selection.")
        st.session_state.municipality = None 
    
    current_month_index = month_names.index(st.session_state.month)
    selected_month_name = st.selectbox(
        "Month", 
        options=month_names, 
        index=current_month_index, 
        key="sb_month"
    )
    st.session_state.month = selected_month_name 
    # This function will no longer return values, run_page reads from session state

def render_tabs(data):
    """Render the tabbed interface for different charts"""
    # Ensure data dictionary has all required keys with non-None values
    required_keys = ["tourist_categories", "dwell_time", "age_gender", "top_municipalities"]
    if not all(key in data for key in required_keys):
        st.warning("Data is missing some required components.")
        return
        
    tab_titles = ["Overview", "Tourist Categories", "Dwell Time Analysis", "Demographics", "Geographic Distribution"]
    tabs = st.tabs(tab_titles)

    def plot_or_info(df, plot_func, chart_title, info_message="No data available for this chart."):
        if df is not None and len(df) > 0:  # Using len() instead of .empty
            try:
                fig = plot_func(df)
                st.plotly_chart(fig, use_container_width=True)
            except Exception as e:
                st.error(f"Error plotting {chart_title}: {str(e)}")
        else:
            st.info(info_message)

    # Tab 0: Overview
    with tabs[0]:
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Tourist Categories")
            def plot_pie(df): 
                fig = px.pie(df, values='value', names='name', title="Visitor Mix", color_discrete_sequence=px.colors.qualitative.Set2)
                fig.update_traces(textposition='inside', textinfo='percent+label', hoverinfo='label+percent+value') 
                return fig
            plot_or_info(data.get("tourist_categories"), plot_pie, "Tourist Categories Pie")
        with col2:
            st.subheader("Dwell Time Distribution")
            def plot_dwell_bar(df): 
                fig = px.bar(df, x='range', y='value', title="Time Spent by Visitors", 
                             labels={'range': 'Dwell Time Range', 'value': 'Number of Visitors'}, 
                             color_discrete_sequence=['#8884d8'])
                return fig
            plot_or_info(data.get("dwell_time"), plot_dwell_bar, "Dwell Time Bar")
            
    # Tab 1: Tourist Categories
    with tabs[1]: 
        st.subheader("Tourist Categories Distribution")
        def plot_tourist_bar(df): 
            fig = px.bar(df, x='name', y='value', color='name', title="Visitor Count by Category", 
                         labels={'name': 'Category', 'value': 'Number of Visitors'}, 
                         color_discrete_sequence=px.colors.qualitative.Set2)
            return fig
        plot_or_info(data.get("tourist_categories"), plot_tourist_bar, "Tourist Categories Bar")
        
    # Tab 2: Dwell Time Analysis
    with tabs[2]: 
        st.subheader("Dwell Time Analysis")
        def plot_dwell_line(df):
            if 'sort_order' in df.columns:
                df = df.sort_values('sort_order')
            fig = px.line(df, x='range', y='value', title="Visitors by Dwell Time Range", 
                          labels={'range': 'Dwell Time Range', 'value': 'Number of Visitors'}, 
                          markers=True, line_shape='spline')
            return fig
        plot_or_info(data.get("dwell_time"), plot_dwell_line, "Dwell Time Line")
            
    # Tab 3: Demographics
    with tabs[3]:
        st.subheader("Age and Gender Distribution")
        def plot_age_gender(df):
            df_age = df.copy()
            # Ensure numeric types for male and female columns
            df_age['male'] = pd.to_numeric(df_age['male'], errors='coerce').fillna(0)
            df_age['female'] = pd.to_numeric(df_age['female'], errors='coerce').fillna(0)
            
            # Sort by sort_order if available
            if 'sort_order' in df_age.columns:
                df_age = df_age.sort_values('sort_order')
            
            # Create figure
            fig = go.Figure()
            fig.add_trace(go.Bar(x=df_age['age'], y=df_age['male'], name='Male', 
                                marker_color='#8884d8', hoverinfo='x+y+name'))
            fig.add_trace(go.Bar(x=df_age['age'], y=df_age['female'], name='Female', 
                                marker_color='#82ca9d', hoverinfo='x+y+name'))
            fig.update_layout(barmode='group', title="Visitors by Age Group and Gender", 
                             xaxis_title="Age Group", yaxis_title="Number of Visitors")
            return fig
        plot_or_info(data.get("age_gender"), plot_age_gender, "Age/Gender Bar")
        
    # Tab 4: Geographic Distribution
    with tabs[4]:
        st.subheader("Areas within the Municipality")
        def plot_muni_bar(df): 
            df_sorted = df.sort_values('value', ascending=False)
            fig = px.bar(df_sorted, x='name', y='value', title="Top Areas by Visitor Count", 
                         labels={'name': 'Area', 'value': 'Number of Visitors'}, 
                         color_discrete_sequence=['#ff7f0e'])
            return fig
        plot_or_info(data.get("top_municipalities"), plot_muni_bar, "Areas Bar")

# --- Data Processing Helper Functions ---
def process_fetched_data(df, selected_municipality):
    """Process the DataFrame (containing municipality-specific data)."""
    # The input df should now be the result from fetch_municipality_data,
    # which already attempted to filter/scale for the specific municipality.
    if df.empty:
        st.warning("Processing function received empty DataFrame.")
        return None

    # Aggregate visitor categories (from potentially scaled data)
    tourist_categories_data = [
        {"name": "Swiss Commuters", "value": df['swiss_commuters'].sum()},
        {"name": "Swiss Locals", "value": df['swiss_locals'].sum()},
        {"name": "Swiss Tourists", "value": df['swiss_tourists'].sum()},
        {"name": "Foreign Workers", "value": df['foreign_workers'].sum()},
        {"name": "Foreign Tourists", "value": df['foreign_tourists'].sum()}
    ]
    tourist_categories_df = pd.DataFrame(tourist_categories_data)
    tourist_categories_df = tourist_categories_df[tourist_categories_df['value'] > 0] 

    # Aggregate dwell times (from potentially scaled data)
    dwell_time_data = []
    dwell_ranges = [
        {"range": "0.5-1h", "idx": 0, "sort": 1}, {"range": "1-2h", "idx": 1, "sort": 2},
        {"range": "2-3h", "idx": 2, "sort": 3}, {"range": "3-4h", "idx": 3, "sort": 4},
        {"range": "4-5h", "idx": 4, "sort": 5}, {"range": "5-6h", "idx": 5, "sort": 6},
        {"range": "6-7h", "idx": 6, "sort": 7}, {"range": "7-8h", "idx": 7, "sort": 8},
        {"range": "8-24h", "idx": 8, "sort": 9}
    ]
    dwell_totals = {r['range']: 0 for r in dwell_ranges}
    for dwell_json_list in df['dwelltimes'].dropna():
        if isinstance(dwell_json_list, str):
            try: dwell_json_list = json.loads(dwell_json_list)
            except json.JSONDecodeError: continue # Skip malformed JSON
        if isinstance(dwell_json_list, list):
            for idx, count in enumerate(dwell_json_list):
                 if idx < len(dwell_ranges):
                     range_name = dwell_ranges[idx]['range']
                     dwell_totals[range_name] += int(count) if isinstance(count, (int, float)) else 0
                     
    for r in dwell_ranges:
        dwell_time_data.append({"range": r['range'], "value": dwell_totals[r['range']], "sort_order": r['sort']})
    dwell_time_df = pd.DataFrame(dwell_time_data)

    # Aggregate demographics (from potentially scaled data)
    age_gender_data = []
    age_groups = ["0-19", "20-39", "40-64", "65+"]
    daily_demos = []
    total_pop_for_demo = 0
    for index, row in df[['demographics', 'total_visitors']].dropna().iterrows():
        demo_json = row['demographics']
        total_visitors_day = row['total_visitors']
        if isinstance(demo_json, str):
             try: demo_json = json.loads(demo_json)
             except json.JSONDecodeError: continue
        if isinstance(demo_json, dict) and 'maleProportion' in demo_json and 'ageDistribution' in demo_json: # Simplified check
             male_prop = float(demo_json['maleProportion'])
             age_dist = [float(p) for p in demo_json['ageDistribution']]
             daily_demos.append({'male_prop': male_prop, 'age_dist': age_dist, 'total': total_visitors_day})
             total_pop_for_demo += total_visitors_day
             
    # Use the helper, but the fallback total is just the sum of the input df
    avg_male_prop, avg_age_dist = calculate_avg_demographics(daily_demos, total_pop_for_demo, df['total_visitors'].sum())
    
    total_monthly_visitors = df['total_visitors'].sum()
    for i, age_group in enumerate(age_groups):
        total_age_group = avg_age_dist[i] * total_monthly_visitors
        male_count = int(round(total_age_group * avg_male_prop))
        female_count = int(round(total_age_group * (1 - avg_male_prop)))
        age_gender_data.append({"age": age_group, "male": male_count, "female": female_count, "sort_order": i + 1})
    age_gender_df = pd.DataFrame(age_gender_data)

    # --- Prepare Geographic Distribution Data --- 
    # This now *always* generates sample areas based on the selected municipality
    top_municipalities_data = []
    if selected_municipality == 'Bellinzona':
        areas = ["Centro", "Stazione", "Pedemonte", "Carasso", "Daro"]
    elif selected_municipality == 'Arbedo-Castione':
        areas = ["Arbedo", "Castione", "Molinazzo", "Arbedo-Sud", "Castione-Nord"]
    elif selected_municipality == 'Cadenazzo':
        areas = ["Cadenazzo", "Robasacco", "Stazione", "Cadenazzo-Sud", "Industriale"]
    elif selected_municipality == 'Riviera':
        areas = ["Biasca", "Cresciano", "Lodrino", "Osogna", "Iragna"]
    elif selected_municipality == 'Isone':
        areas = ["Isone", "Medeglia", "Centro", "Cima", "Isone-Est"]
    else:
        areas = [f"Area {i+1}" for i in range(5)] # Default areas
        
    area_distribution = [0.35, 0.25, 0.20, 0.12, 0.08] # Sample distribution
    total_visitors_final = df['total_visitors'].sum() # Total from input df
    for i, area in enumerate(areas):
        visitor_count = int(round(total_visitors_final * area_distribution[i]))
        top_municipalities_data.append({"name": area, "value": visitor_count})
            
    top_municipalities_df = pd.DataFrame(top_municipalities_data)
    if not top_municipalities_df.empty:
        top_municipalities_df = top_municipalities_df.sort_values("value", ascending=False) # Already limited to 5 areas

    return {
        "tourist_categories": tourist_categories_df,
        "dwell_time": dwell_time_df,
        "age_gender": age_gender_df,
        "top_municipalities": top_municipalities_df # This now contains area data
    }
    
# --- (Helper function for demographics needed) ---
def calculate_avg_demographics(daily_demos, total_pop_for_demo, total_visitors_fallback):
    """Helper to calculate average demographic proportions."""
    age_groups = ["0-19", "20-39", "40-64", "65+"] # Define age groups
    if total_pop_for_demo > 0:
        avg_male_prop = sum(d['male_prop'] * d['total'] for d in daily_demos) / total_pop_for_demo
        avg_age_dist = [sum(d['age_dist'][i] * d['total'] for d in daily_demos) / total_pop_for_demo for i in range(len(age_groups))]
    elif daily_demos: # Fallback to simple average
        avg_male_prop = sum(d['male_prop'] for d in daily_demos) / len(daily_demos)
        avg_age_dist = [sum(d['age_dist'][i] for d in daily_demos) / len(daily_demos) for i in range(len(age_groups))]
    else: # No demographic data
        avg_male_prop = 0.5 # Default guess
        avg_age_dist = [0.25] * len(age_groups) # Default guess
    return avg_male_prop, avg_age_dist

# --- Main App Logic --- 
def run_page():
    render_header()
    col1, col2 = st.columns([1, 2])

    with col1:
        render_filters() # Renders filters, selection read from session state
        selected_municipality = st.session_state.get('municipality')
        selected_month_name = st.session_state.get('month')
        months = [{"id": i, "name": datetime.date(2000, i, 1).strftime('%B')} for i in range(1, 13)]
        month_options = {m["name"]: m["id"] for m in months}
        selected_month_id = month_options.get(selected_month_name) if selected_month_name else None

        if not selected_municipality or not selected_month_id:
            col1.warning("Please select a municipality and month.")
            st.stop()

    processed_data = None
    use_mock_data = False
    db_connected_status = False

    conn, cursor = connect_to_db()
    
    if conn and cursor:
        db_connected_status = True
        try:
            # Fetch data specifically for the selected municipality
            col1.info(f"Attempting data retrieval for {selected_municipality} / {selected_month_name}...")
            fetched_df = fetch_municipality_data(cursor, selected_municipality, selected_month_id)
            
            if fetched_df.empty:
                col1.warning(f"No data found in the database for {selected_municipality} in {selected_month_name}. Using sample data.")
                use_mock_data = True
            else:
                col1.success("Data found. Processing...")
                # Process the municipality-specific data
                processed_data = process_fetched_data(fetched_df, selected_municipality)
                
                if processed_data:
                     col1.success(f"Displaying data for {selected_municipality}.")
                else:
                     col1.error(f"Error processing fetched data for {selected_municipality}. Using sample data.")
                     use_mock_data = True

        except Exception as e:
            col1.error(f"Error during database query or processing: {str(e)}")
            use_mock_data = True
        
        finally:
            if cursor: cursor.close()
            if conn: conn.close()
    else:
        db_connected_status = False
        col1.warning("Database connection failed. Using sample data.")
        use_mock_data = True
    
    st.session_state.db_connected = db_connected_status

    if use_mock_data:
        # Generate sample data based on the selected municipality
        processed_data = get_sample_data(selected_municipality, selected_month_id)
        col1.info(f"Using sample data for {selected_municipality} visualization.")
        
    with col2:
        if processed_data:
            render_tabs(processed_data)
        else:
            col2.info("Data could not be loaded or generated. Check status messages on the left.") 

# --- Sidebar Content --- 
def render_sidebar():
    st.sidebar.title("Dashboard Options") # CHANGED TITLE
    st.sidebar.info(
        """
        This dashboard provides insights about visitor mobility within the 
        Bellinzonese region municipalities, based on Swisscom mobility data
        for the year 2023.
        
        Select a specific municipality and month to see detailed analytics.
        """
    )
    
    # Show database connection status
    status = "🟢 Connected" if st.session_state.get('db_connected', False) else "⚪ Not Connected"
    st.sidebar.markdown(f"**DB Status**: {status}")
    
    # Add municipality information
    with st.sidebar.expander("Municipality Information"):
        st.markdown("""
        ### Bellinzonese Municipalities
        
        **Bellinzona** - The capital city of Ticino canton and the largest municipality in the region. 
        It includes the historic center with three UNESCO-listed castles.
        
        **Arbedo-Castione** - Located north of Bellinzona, consists of the villages of Arbedo and Castione.
        
        **Cadenazzo** - Located south of Bellinzona at an important transport junction.
        
        **Riviera** - A municipality created from the merger of Cresciano, Iragna, Lodrino and Osogna.
        
        **Isone** - A small municipality in the hills west of Bellinzona, known for its military training center.
        """)
    
    # REMOVED DEBUG SECTION
    # # Add debug section in sidebar
    # with st.sidebar.expander("Debug Information"):
    #     # Show sanitized DB config
    #     if 'db_config' in st.session_state:
    #         sanitized_config = st.session_state.db_config.copy()
    #         if 'password' in sanitized_config:
    #             sanitized_config['password'] = '****'
    #         st.write("**Database Config:**")
    #         st.json(sanitized_config)
    #     else:
    #          st.write("**Database Config:** Not Set")
    #     
    #     # Show selected filters
    #     st.write("**Selected Filters:**")
    #     st.write({
    #         "Municipality": st.session_state.get('municipality', 'Not set'),
    #         "Month": st.session_state.get('month', 'Not set'),
    #         "Year": "2023 (Fixed)"
    #     })

# --- Page Execution Logic --- 
render_sidebar() # Sidebar renders independently of main page layout
run_page()