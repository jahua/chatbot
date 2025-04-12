import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import datetime
import json

# Set the page config - Streamlit uses filename, but we can set title
st.set_option('deprecation.showPyplotGlobalUse', False)

# --- Database Connection (Reads from st.session_state provided by app.py) ---
def connect_to_db():
    """Establish connection to PostgreSQL database using config from session state"""
    if 'db_config' not in st.session_state:
        st.error("Database configuration not found in session state. Ensure main app initializes st.session_state.db_config.")
        return None, None
    
    db_config = st.session_state.db_config
    required_keys = ["host", "port", "dbname", "user", "password"]
    
    # Check if all required keys exist and have values
    if not all(db_config.get(key) for key in required_keys):
        st.error("Database connection details are incomplete in session state.")
        st.sidebar.warning("Current DB Config (Incomplete):")
        st.sidebar.json(db_config) # Show config for debugging
        return None, None
        
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        st.sidebar.success(f"DB Connected: {db_config['dbname']}@{db_config['host']}") # Moved to sidebar
        return conn, cursor
    except psycopg2.Error as e:
        st.error(f"Database connection error: {str(e)}")
        st.sidebar.error("DB Config Used (Failed):")
        st.sidebar.json(db_config) # Show failing config
        return None, None

# --- Direct Access to aoi_days_raw ---
def fetch_directly_from_raw(cursor, region, month, year):
    """
    Alternative approach to directly fetch and process data from aoi_days_raw
    instead of using views - this is useful if you can't create views in your database
    """
    try:
        # Query to get the raw data for the selected filters
        query = """
            SELECT 
                aoi_id, aoi_date, 
                visitors, dwelltimes, demographics, 
                top_swiss_municipalities
            FROM 
                aoi_days_raw
            WHERE 
                aoi_id = %s AND 
                EXTRACT(MONTH FROM aoi_date) = %s AND 
                EXTRACT(YEAR FROM aoi_date) = %s
        """
        cursor.execute(query, (region, month, year))
        results = cursor.fetchall()
        
        if not results:
            return None
        
        # Process the results
        tourist_categories_data = []
        dwell_time_data = []
        age_gender_data = []
        municipalities_data = []
        
        for row in results:
            aoi_id, aoi_date, visitors, dwelltimes, demographics, top_municipalities = row
            
            # Process visitors data (tourist categories)
            if visitors:
                visitors_dict = visitors if isinstance(visitors, dict) else json.loads(visitors)
                if "SwissCommuters" in visitors_dict:
                    tourist_categories_data.append({"name": "Swiss Commuters", "value": visitors_dict["SwissCommuters"]})
                if "SwissLocals" in visitors_dict:
                    tourist_categories_data.append({"name": "Swiss Locals", "value": visitors_dict["SwissLocals"]})
                if "SwissTourists" in visitors_dict:
                    tourist_categories_data.append({"name": "Swiss Tourists", "value": visitors_dict["SwissTourists"]})
                if "ForeignWorkers" in visitors_dict:
                    tourist_categories_data.append({"name": "Foreign Workers", "value": visitors_dict["ForeignWorkers"]})
                if "ForeignTourists" in visitors_dict:
                    tourist_categories_data.append({"name": "Foreign Tourists", "value": visitors_dict["ForeignTourists"]})
            
            # Process dwell time data
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
                            "range": range_info["range"],
                            "value": dwelltimes_dict[key]["total"],
                            "sort_order": range_info["sort"]
                        })
            
            # Process demographics data (age and gender)
            if demographics:
                demographics_dict = demographics if isinstance(demographics, dict) else json.loads(demographics)
                age_groups = ["0-19", "20-39", "40-64", "65+"]
                for i, age_group in enumerate(age_groups):
                    if age_group in demographics_dict:
                        male_count = demographics_dict[age_group].get("male", 0)
                        female_count = demographics_dict[age_group].get("female", 0)
                        age_gender_data.append({
                            "age": age_group,
                            "male": male_count,
                            "female": female_count,
                            "sort_order": i + 1
                        })
            
            # Process municipalities data
            if top_municipalities:
                muni_list = top_municipalities if isinstance(top_municipalities, list) else json.loads(top_municipalities)
                for muni in muni_list:
                    if isinstance(muni, dict) and "name" in muni and "value" in muni:
                        municipalities_data.append({
                            "name": muni["name"],
                            "value": muni["value"]
                        })
        
        # Create DataFrames from the processed data
        tourist_categories_df = pd.DataFrame(tourist_categories_data)
        if not tourist_categories_df.empty:
            # Aggregate data if multiple rows for the same day/month/year
            tourist_categories_df = tourist_categories_df.groupby("name")["value"].sum().reset_index()
        
        dwell_time_df = pd.DataFrame(dwell_time_data)
        if not dwell_time_df.empty:
            # Ensure data is sorted by the correct order
            dwell_time_df = dwell_time_df.sort_values("sort_order")
        
        age_gender_df = pd.DataFrame(age_gender_data)
        if not age_gender_df.empty:
            # Aggregate and sort
            age_gender_df = age_gender_df.groupby(["age", "sort_order"]).agg({
                "male": "sum",
                "female": "sum"
            }).reset_index().sort_values("sort_order")
        
        municipalities_df = pd.DataFrame(municipalities_data)
        if not municipalities_df.empty:
            # Aggregate and get top 5
            municipalities_df = municipalities_df.groupby("name")["value"].sum().reset_index()
            municipalities_df = municipalities_df.sort_values("value", ascending=False).head(5)
        
        return {
            "tourist_categories": tourist_categories_df if not tourist_categories_df.empty else pd.DataFrame(columns=["name", "value"]),
            "dwell_time": dwell_time_df if not dwell_time_df.empty else pd.DataFrame(columns=["range", "value"]),
            "age_gender": age_gender_df if not age_gender_df.empty else pd.DataFrame(columns=["age", "male", "female"]),
            "top_municipalities": municipalities_df if not municipalities_df.empty else pd.DataFrame(columns=["name", "value"])
        }
    except Exception as e:
        st.error(f"Error fetching data directly from aoi_days_raw: {str(e)}")
        return None

# --- Database Query Functions Using Views ---
def fetch_tourist_categories(cursor, region, month, year):
    """Fetch tourist categories data from database"""
    try:
        query = """
            SELECT category_name AS name, visitor_count AS value
            FROM visitor_categories
            WHERE region_id = %s AND month = %s AND year = %s
            ORDER BY visitor_count DESC
        """
        cursor.execute(query, (region, month, year))
        results = cursor.fetchall()
        if results: return pd.DataFrame(results, columns=['name', 'value'])
        return pd.DataFrame(columns=['name', 'value'])
    except Exception as e:
        st.error(f"Error fetching tourist categories: {str(e)}")
        return None

def fetch_dwell_time(cursor, region, month, year):
    """Fetch dwell time data from database"""
    try:
        query = """
            SELECT time_range AS range, visitor_count AS value
            FROM visitor_dwell_time
            WHERE region_id = %s AND month = %s AND year = %s
            ORDER BY sort_order 
        """
        cursor.execute(query, (region, month, year))
        results = cursor.fetchall()
        if results: return pd.DataFrame(results, columns=['range', 'value'])
        return pd.DataFrame(columns=['range', 'value'])
    except Exception as e:
        st.error(f"Error fetching dwell time data: {str(e)}")
        return None

def fetch_age_gender(cursor, region, month, year):
    """Fetch age and gender distribution from database"""
    try:
        query = """
            SELECT age_group AS age, male_count AS male, female_count AS female
            FROM visitor_demographics
            WHERE region_id = %s AND month = %s AND year = %s
            ORDER BY sort_order 
        """
        cursor.execute(query, (region, month, year))
        results = cursor.fetchall()
        if results: return pd.DataFrame(results, columns=['age', 'male', 'female'])
        return pd.DataFrame(columns=['age', 'male', 'female'])
    except Exception as e:
        st.error(f"Error fetching age and gender data: {str(e)}")
        return None

def fetch_top_municipalities(cursor, region, month, year):
    """Fetch top municipalities data from database"""
    try:
        query = """
            SELECT municipality_name AS name, visitor_count AS value
            FROM top_visitor_municipalities
            WHERE region_id = %s AND month = %s AND year = %s
            ORDER BY visitor_count DESC
            LIMIT 5
        """
        cursor.execute(query, (region, month, year))
        results = cursor.fetchall()
        if results: return pd.DataFrame(results, columns=['name', 'value'])
        return pd.DataFrame(columns=['name', 'value'])
    except Exception as e:
        st.error(f"Error fetching top municipalities data: {str(e)}")
        return None

# --- Mock Data Generation (Reintroduced) ---
def get_sample_data(region, month, year):
    """Generate sample data similar to the React app for testing"""
    region_multiplier = {'bellinzonese': 1, 'ascona-locarno': 1.2, 'luganese': 1.5, 'mendrisiotto': 0.8}.get(region, 1)
    month_str = str(month)
    seasonal_multiplier = {'1': 0.7, '2': 0.8, '3': 0.9, '4': 1.0, '5': 1.2, '6': 1.3, '7': 1.5, '8': 1.5, '9': 1.2, '10': 1.0, '11': 0.8, '12': 0.7}.get(month_str, 1)
    year_str = str(year)
    year_multiplier = 1.1 if year_str == '2024' else 1
    total_multiplier = region_multiplier * seasonal_multiplier * year_multiplier
    tourist_categories = pd.DataFrame([
        {"name": "Swiss Commuters", "value": int(1200 * total_multiplier)},
        {"name": "Swiss Locals", "value": int(3500 * total_multiplier)},
        {"name": "Swiss Tourists", "value": int(2800 * total_multiplier)},
        {"name": "Foreign Workers", "value": int(800 * total_multiplier)},
        {"name": "Foreign Tourists", "value": int(1500 * total_multiplier)}
    ])
    dwell_time = pd.DataFrame([
        {"range": "0.5-1h", "value": int(500 * total_multiplier)}, {"range": "1-2h", "value": int(800 * total_multiplier)},
        {"range": "2-3h", "value": int(1200 * total_multiplier)}, {"range": "3-4h", "value": int(900 * total_multiplier)},
        {"range": "4-5h", "value": int(600 * total_multiplier)}, {"range": "5-6h", "value": int(400 * total_multiplier)},
        {"range": "6-7h", "value": int(300 * total_multiplier)}, {"range": "7-8h", "value": int(200 * total_multiplier)},
        {"range": "8-24h", "value": int(1000 * total_multiplier)}
    ])
    age_gender = pd.DataFrame([
        {"age": "0-19", "male": int(300 * total_multiplier), "female": int(280 * total_multiplier)},
        {"age": "20-39", "male": int(800 * total_multiplier), "female": int(750 * total_multiplier)},
        {"age": "40-64", "male": int(600 * total_multiplier), "female": int(650 * total_multiplier)},
        {"age": "65+", "male": int(200 * total_multiplier), "female": int(250 * total_multiplier)}
    ])
    # Simulate municipality names based on region
    if region == 'luganese': muni_names = ["Lugano", "Paradiso", "Massagno", "Agno", "Caslano"]
    elif region == 'ascona-locarno': muni_names = ["Locarno", "Ascona", "Minusio", "Muralto", "Tenero"]
    elif region == 'mendrisiotto': muni_names = ["Mendrisio", "Chiasso", "Stabio", "Balerna", "Coldrerio"]
    else: muni_names = ["Bellinzona", "Giubiasco", "Arbedo", "Sementina", "Biasca"]
    top_municipalities = pd.DataFrame([
        {"name": muni_names[0], "value": int(1200 * total_multiplier)}, {"name": muni_names[1], "value": int(900 * total_multiplier)},
        {"name": muni_names[2], "value": int(800 * total_multiplier)}, {"name": muni_names[3], "value": int(600 * total_multiplier)},
        {"name": muni_names[4], "value": int(500 * total_multiplier)}
    ])
    return {
        "tourist_categories": tourist_categories, "dwell_time": dwell_time,
        "age_gender": age_gender, "top_municipalities": top_municipalities
    }

# --- Dashboard UI Components --- 
def render_header():
    """Render the header section of the dashboard"""
    st.title("📊 Ticino Tourism Insights")
    st.markdown("#### Swisscom Mobility Insights Data Analysis")
    st.markdown("---")

def render_filters():
    """Render the filter controls using session_state and return selected values"""
    regions = [{"id": "bellinzonese", "name": "Bellinzonese"}, {"id": "ascona-locarno", "name": "Ascona-Locarno"},
               {"id": "luganese", "name": "Luganese"}, {"id": "mendrisiotto", "name": "Mendrisiotto"}]
    region_options = {r["name"]: r["id"] for r in regions}
    region_names = list(region_options.keys())
    months = [{"id": i, "name": datetime.date(2000, i, 1).strftime('%B')} for i in range(1, 13)]
    month_options = {m["name"]: m["id"] for m in months}
    month_names = list(month_options.keys())
    years = ["2023", "2024"]

    if 'swisscom_region' not in st.session_state: st.session_state.swisscom_region = region_names[0]
    if 'swisscom_month' not in st.session_state: st.session_state.swisscom_month = month_names[datetime.datetime.now().month - 1]
    if 'swisscom_year' not in st.session_state:
        current_year = str(datetime.datetime.now().year)
        st.session_state.swisscom_year = current_year if current_year in years else years[-1]

    # Add data source selection
    if 'data_source' not in st.session_state: 
        st.session_state.data_source = "Database Views"

    col1, col2, col3 = st.columns(3)
    with col1:
        current_region_index = region_names.index(st.session_state.swisscom_region) if st.session_state.swisscom_region in region_names else 0
        st.session_state.swisscom_region = st.selectbox("Region", options=region_names, index=current_region_index, key="sb_region")
        selected_region_id = region_options[st.session_state.swisscom_region]
    with col2:
        current_month_index = month_names.index(st.session_state.swisscom_month) if st.session_state.swisscom_month in month_names else datetime.datetime.now().month - 1
        st.session_state.swisscom_month = st.selectbox("Month", options=month_names, index=current_month_index, key="sb_month")
        selected_month_id = month_options[st.session_state.swisscom_month]
    with col3:
        current_year_index = years.index(st.session_state.swisscom_year) if st.session_state.swisscom_year in years else len(years)-1
        st.session_state.swisscom_year = st.selectbox("Year", options=years, index=current_year_index, key="sb_year")
        selected_year_id = int(st.session_state.swisscom_year)
    
    # Add a radio button to toggle between using database views and direct access
    # Use a separate key for the widget and initialize with session state value
    options = ["Database Views", "Direct from aoi_days_raw", "Sample Data"]
    # Find index of current or default value, handle case where value might not be in options yet
    current_value = st.session_state.get('data_source', "Database Views")
    try:
        default_index = options.index(current_value)
    except ValueError:
        default_index = 0 # Default to first option if current value is invalid
        
    data_source = st.radio(
        "Data Source Method",
        options,
        index=default_index, # Use index for initialization
        horizontal=True,
        key="data_source_widget", # Use a unique key for the widget
        help="Choose how to retrieve data. Use Database Views if you've created the SQL views, or Direct Access if you haven't."
    )
    # Update session state AFTER the widget is rendered
    st.session_state.data_source = data_source
    
    return selected_region_id, selected_month_id, selected_year_id

def render_tabs(data):
    """Render the tabbed interface for different charts"""
    if not isinstance(data, dict):
        st.warning("No data available to display.")
        return
        
    tab_titles = ["Overview", "Tourist Categories", "Dwell Time Analysis", "Demographics", "Geographic Distribution"]
    tabs = st.tabs(tab_titles)

    def plot_or_info(df, plot_func, chart_title, info_message="No data available for this chart."):
        if df is not None and not df.empty:
            try:
                fig = plot_func(df)
                st.plotly_chart(fig, use_container_width=True)
            except Exception as e:
                st.error(f"Error plotting {chart_title}: {e}")
        elif df is not None:
             st.info(info_message)

    # Tab 0: Overview
    with tabs[0]:
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Tourist Categories")
            def plot_pie(df): fig = px.pie(df, values='value', names='name', title="Visitor Mix", color_discrete_sequence=px.colors.qualitative.Set2); fig.update_traces(textposition='inside', textinfo='percent+label', hoverinfo='label+percent+value'); return fig
            plot_or_info(data.get("tourist_categories"), plot_pie, "Tourist Categories Pie")
        with col2:
            st.subheader("Dwell Time Distribution")
            def plot_dwell_bar(df): fig = px.bar(df, x='range', y='value', title="Time Spent by Visitors", labels={'range': 'Dwell Time Range', 'value': 'Number of Visitors'}, color_discrete_sequence=['#8884d8']); return fig
            plot_or_info(data.get("dwell_time"), plot_dwell_bar, "Dwell Time Bar")
            
    # Tab 1: Tourist Categories
    with tabs[1]: 
        st.subheader("Tourist Categories Distribution")
        def plot_tourist_bar(df): fig = px.bar(df, x='name', y='value', color='name', title="Visitor Count by Category", labels={'name': 'Category', 'value': 'Number of Visitors'}, color_discrete_sequence=px.colors.qualitative.Set2); return fig
        plot_or_info(data.get("tourist_categories"), plot_tourist_bar, "Tourist Categories Bar")
        
    # Tab 2: Dwell Time Analysis
    with tabs[2]: 
        st.subheader("Dwell Time Analysis")
        def plot_dwell_line(df):
            try: df['sort_key'] = df['range'].str.extract(r'(\d+(\.\d+)?)').astype(float); df = df.sort_values('sort_key').drop(columns='sort_key');
            except: pass 
            fig = px.line(df, x='range', y='value', title="Visitors by Dwell Time Range", labels={'range': 'Dwell Time Range', 'value': 'Number of Visitors'}, markers=True, line_shape='spline'); return fig
        plot_or_info(data.get("dwell_time"), plot_dwell_line, "Dwell Time Line")
            
    # Tab 3: Demographics
    with tabs[3]:
        st.subheader("Age and Gender Distribution")
        def plot_age_gender(df):
             df_age = df.copy(); df_age['male'] = pd.to_numeric(df_age['male'], errors='coerce').fillna(0); df_age['female'] = pd.to_numeric(df_age['female'], errors='coerce').fillna(0);
             fig = go.Figure(); fig.add_trace(go.Bar(x=df_age['age'], y=df_age['male'], name='Male', marker_color='#8884d8', hoverinfo='x+y+name')); fig.add_trace(go.Bar(x=df_age['age'], y=df_age['female'], name='Female', marker_color='#82ca9d', hoverinfo='x+y+name'));
             fig.update_layout(barmode='group', title="Visitors by Age Group and Gender", xaxis_title="Age Group", yaxis_title="Number of Visitors"); return fig
        plot_or_info(data.get("age_gender"), plot_age_gender, "Age/Gender Bar")
        
    # Tab 4: Geographic Distribution
    with tabs[4]:
        st.subheader("Top Municipalities by Visitors")
        def plot_muni_bar(df): 
            df_sorted = df.sort_values('value', ascending=False)
            fig = px.bar(df_sorted, x='name', y='value', title="Top 5 Municipalities by Visitor Count", labels={'name': 'Municipality', 'value': 'Number of Visitors'}, color_discrete_sequence=['#ff7f0e'])
            return fig
        plot_or_info(data.get("top_municipalities"), plot_muni_bar, "Top Municipalities Bar")

# --- Main App Logic --- 
def run_page():
    # Render header and filters first
    render_header()
    selected_region, selected_month, selected_year = render_filters()

    # Initialize data placeholder
    data = None
    use_mock_data = False
    error_occurred = False
    
    data_source_method = st.session_state.get('data_source', "Database Views") # Use the correct key

    conn, cursor = None, None # Initialize outside try

    # --- Main Try Block for Connection and Data Fetching ---
    try:
        # Check DB config validity ONLY IF we intend to use the DB
        if data_source_method != "Sample Data":
            if 'db_config' not in st.session_state or not all(st.session_state.db_config.values()):
                st.warning("Database connection details missing or incomplete. Using sample data.")
                use_mock_data = True
            else:
                # Attempt connection ONLY if config seems valid and not using sample data
                conn, cursor = connect_to_db()
                if conn and cursor:
                    st.session_state.db_connected = True 
                    try: # Inner try for data fetching logic
                        if data_source_method == "Direct from aoi_days_raw":
                            st.info("Using direct access to aoi_days_raw table.")
                            data = fetch_directly_from_raw(cursor, selected_region, selected_month, selected_year)
                            if data is None:
                                st.warning("No data found using direct access. Falling back to sample data.")
                                use_mock_data = True
                        else:  # Database Views (Default)
                            st.info("Using database views for data retrieval.")
                            tourist_categories_df = fetch_tourist_categories(cursor, selected_region, selected_month, selected_year)
                            dwell_time_df = fetch_dwell_time(cursor, selected_region, selected_month, selected_year)
                            age_gender_df = fetch_age_gender(cursor, selected_region, selected_month, selected_year)
                            top_municipalities_df = fetch_top_municipalities(cursor, selected_region, selected_month, selected_year)
                            
                            if None in [tourist_categories_df, dwell_time_df, age_gender_df, top_municipalities_df]:
                                st.error("Error fetching data from views. Falling back to direct access (if possible).")
                                error_occurred = True
                                data = fetch_directly_from_raw(cursor, selected_region, selected_month, selected_year)
                                if data is None:
                                    st.warning("Direct access fallback failed. Using sample data.")
                                    use_mock_data = True
                            else:
                                data = {
                                    "tourist_categories": tourist_categories_df, "dwell_time": dwell_time_df,
                                    "age_gender": age_gender_df, "top_municipalities": top_municipalities_df
                                }
                                if all(df.empty for df in data.values()):
                                    st.info("No data found in views. Trying direct access.")
                                    data = fetch_directly_from_raw(cursor, selected_region, selected_month, selected_year)
                                    if data is None:
                                        st.warning("Direct access fallback returned no data. Using sample data.")
                                        use_mock_data = True
                                else:
                                    st.success("Displaying data from database views.")
                                    
                    except Exception as e: # Catch errors during data fetching
                        st.error(f"An error occurred during data processing: {e}. Using sample data.")
                        use_mock_data = True
                        error_occurred = True
                
                # If connection failed (conn is None or cursor is None after connect_to_db call)
                elif not use_mock_data: # Check we haven't already decided to use mock data
                    st.session_state.db_connected = False 
                    # connect_to_db already showed the error
                    st.warning("Database connection failed. Using sample data.")
                    use_mock_data = True
                    error_occurred = True
        
        # If user selected Sample Data directly
        else: 
            use_mock_data = True
            st.info("Using sample data as requested.")

    # Catch unexpected errors outside connection/fetching (e.g., in filter logic)
    except Exception as e:
         st.error(f"An unexpected error occurred in the main process: {e}. Using sample data.")
         use_mock_data = True
         error_occurred = True
         if cursor: cursor.close() # Try to close if cursor exists
         if conn: conn.close() # Try to close if conn exists
         conn, cursor = None, None # Ensure they are reset
         
    finally: # Always ensure connection is closed if opened
        if cursor: cursor.close()
        if conn: conn.close()

    # Generate mock data if needed
    if use_mock_data:
        data = get_sample_data(selected_region, selected_month, selected_year)
        st.info("Using sample data for visualization.")
        
    # Render the dashboard tabs if data is available
    if data:
        render_tabs(data)
    elif not error_occurred:
        st.warning("No data available to display. Check connection or filters.")
    else:
        st.error("Failed to load or generate any data.")

# --- Sidebar Content --- 
def render_sidebar():
    st.sidebar.title("About This Page")
    st.sidebar.info(
        """
        This dashboard provides insights based on Swisscom mobility data,
        showing visitor categories, dwell times, demographics, and top locations.
        Select region, month, and year to filter the data.
        """
    )
    
    # Show database connection status
    if 'db_config' in st.session_state and all(st.session_state.db_config.values()):
        status = "🟢 Connected" if st.session_state.get('db_connected', False) else "⚪ Not Connected / Configured"
        st.sidebar.markdown(f"**DB Status**: {status}")
    
    # Show data source method explanation
    st.sidebar.markdown("---")
    st.sidebar.markdown("### Data Source Methods")
    st.sidebar.markdown("""
    - **Database Views**: Uses SQL views to transform data (recommended for performance)
    - **Direct from aoi_days_raw**: Directly queries and processes the raw JSON data 
    - **Sample Data**: Uses generated sample data for testing
    """)
    
    # Add instructions for database setup
    st.sidebar.markdown("---")
    st.sidebar.markdown("### Database Setup")
    with st.sidebar.expander("View SQL Setup Instructions"):
        st.markdown("""
        To use real data, you need to:
        
        1. Create the `aoi_days_raw` table
        2. Insert data into the table
        3. Create the supporting views
        
        Check the SQL schema documentation for detailed instructions.
        """)

# --- Keep only function calls needed when run as a page ---
render_sidebar()
run_page()