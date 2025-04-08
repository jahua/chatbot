# MasterCard Geospatial Data Analysis Project

## Database Schema Overview

The `master_card` table in the `data_lake` schema contains transaction data with geospatial information:

| Column | Type | Description |
|--------|------|-------------|
| id | integer | Unique identifier for each record |
| yr | integer | Year of the transaction |
| txn_date | date | Date of the transaction |
| industry | varchar | Industry category of the merchant |
| geo_name | varchar | Geographic location name (city/region) |
| bounding_box | geometry | Geospatial coordinates for the location |
| txn_amt | numeric | Transaction amount |
| txn_cnt | integer | Number of transactions |
| acct_cnt | integer | Number of unique accounts involved |
| avg_txn | numeric | Average transaction amount |
| yoy_txn_amt | numeric | Year-over-Year transaction amount change |
| yoy_txn_cnt | numeric | Year-over-Year transaction count change |
| yoy_acct_cnt | numeric | Year-over-Year account count change |

## SQL Queries for Analysis

### 1. Regional Transaction Volume Analysis

```sql
SELECT 
    geo_name,
    SUM(txn_amt) AS total_amount,
    AVG(avg_txn) AS average_transaction,
    SUM(txn_cnt) AS transaction_count,
    SUM(acct_cnt) AS account_count,
    COUNT(DISTINCT bounding_box) AS unique_locations
FROM 
    data_lake.master_card
WHERE 
    geo_name IN ('Bern', 'Zurich', 'Geneva', 'Basel', 'Lucerne')
    AND txn_amt > 0
GROUP BY 
    geo_name
ORDER BY 
    total_amount DESC;
```

**Expected Response:**
```
| geo_name | total_amount | average_transaction | transaction_count | account_count | unique_locations |
|----------|--------------|---------------------|-------------------|---------------|------------------|
| Zurich   | 4250000.00   | 125.50              | 32500             | 15200         | 42               |
| Geneva   | 3120000.00   | 118.75              | 26500             | 12800         | 36               |
| Basel    | 2850000.00   | 112.25              | 24200             | 10500         | 28               |
| Bern     | 2480000.00   | 105.80              | 21800             | 9800          | 25               |
| Lucerne  | 1950000.00   | 98.40               | 19300             | 8400          | 22               |
```

### 2. Industry Performance by Region

```sql
SELECT 
    industry,
    geo_name,
    SUM(txn_amt) AS total_amount,
    AVG(avg_txn) AS average_transaction,
    SUM(txn_cnt) AS transaction_count,
    SUM(acct_cnt) AS account_count,
    ROUND((SUM(yoy_txn_amt) / NULLIF(LAG(SUM(txn_amt)) OVER (PARTITION BY industry, geo_name ORDER BY MAX(yr)), 0)) * 100, 2) AS growth_percentage
FROM 
    data_lake.master_card
WHERE 
    geo_name IN ('Bern', 'Zurich', 'Geneva')
    AND txn_amt > 0
GROUP BY 
    industry, geo_name
ORDER BY 
    industry, total_amount DESC;
```

**Expected Response:**
```
| industry       | geo_name | total_amount | average_transaction | transaction_count | account_count | growth_percentage |
|----------------|----------|--------------|---------------------|-------------------|---------------|-------------------|
| Accommodation  | Zurich   | 875000.00    | 145.80              | 6800              | 3200          | 8.50              |
| Accommodation  | Geneva   | 720000.00    | 138.50              | 5400              | 2800          | 7.20              |
| Accommodation  | Bern     | 580000.00    | 132.20              | 4200              | 2100          | 6.40              |
| Food & Beverage| Zurich   | 980000.00    | 92.50               | 10500             | 6800          | 12.30             |
| Food & Beverage| Geneva   | 840000.00    | 88.75               | 9200              | 5900          | 10.80             |
| Food & Beverage| Bern     | 720000.00    | 85.40               | 8300              | 5200          | 9.50              |
| Retail         | Zurich   | 1250000.00   | 115.20              | 9800              | 7400          | 5.80              |
| Retail         | Geneva   | 970000.00    | 110.50              | 8600              | 6500          | 5.20              |
| Retail         | Bern     | 850000.00    | 105.80              | 7500              | 5800          | 4.90              |
```

## Geospatial Visualization Plan

### Python Script for Geospatial Visualization

```python
import pandas as pd
import psycopg2
import plotly.express as px
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Database connection parameters
db_params = {
    'host': os.getenv('POSTGRES_HOST'),
    'port': os.getenv('POSTGRES_PORT'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'database': os.getenv('POSTGRES_DB')
}

# Connect to the database
conn = psycopg2.connect(**db_params)
cursor = conn.cursor()

# Query to extract geospatial data
query = """
SELECT 
    geo_name,
    ST_AsText(bounding_box) as geometry,
    SUM(txn_amt) as total_amount,
    SUM(txn_cnt) as transaction_count,
    industry
FROM 
    data_lake.master_card
WHERE 
    bounding_box IS NOT NULL
    AND txn_amt > 0
GROUP BY 
    geo_name, bounding_box, industry
"""

cursor.execute(query)
data = cursor.fetchall()
cursor.close()
conn.close()

# Convert to DataFrame
df = pd.DataFrame(data, columns=['geo_name', 'geometry', 'total_amount', 'transaction_count', 'industry'])

# Extract coordinates from WKT geometry
def extract_coords(wkt):
    # Parse the WKT string to extract center coordinates
    # This is a simplified example - actual implementation would depend on geometry type
    if 'POINT' in wkt:
        coords = wkt.replace('POINT(', '').replace(')', '').split()
        return float(coords[1]), float(coords[0])  # lat, lon
    elif 'POLYGON' in wkt:
        # For polygons, calculate centroid (simplified)
        coords_str = wkt.replace('POLYGON((', '').replace('))', '').split(',')
        coords = [tuple(map(float, coord.split())) for coord in coords_str]
        lon = sum(c[0] for c in coords) / len(coords)
        lat = sum(c[1] for c in coords) / len(coords)
        return lat, lon
    return None, None

# Apply coordinate extraction
df['lat'], df['lon'] = zip(*df['geometry'].apply(extract_coords))

# Create interactive map
fig = px.scatter_mapbox(
    df,
    lat='lat',
    lon='lon',
    color='industry',
    size='total_amount',
    hover_name='geo_name',
    hover_data=['transaction_count', 'total_amount'],
    zoom=8,
    height=800,
    width=1200,
    title='Transaction Volume by Location and Industry'
)

fig.update_layout(mapbox_style='carto-positron')
fig.update_layout(margin={"r":0,"t":50,"l":0,"b":0})
fig.show()
```

## Analysis Plan

1. **Regional Economic Impact Assessment**
   - Compare transaction volumes across major Swiss cities
   - Identify economic hotspots based on transaction density
   - Analyze spending patterns by geographic region

2. **Industry Performance Analysis**
   - Evaluate which industries perform best in each region
   - Compare industry growth rates across different locations
   - Identify regional specializations based on industry concentration

3. **Customer Behavior Mapping**
   - Map customer account distribution geographically
   - Analyze average transaction amounts by location
   - Identify areas with high customer loyalty (repeat transactions)

4. **Growth Trend Visualization**
   - Plot YoY changes on maps to show regional growth patterns
   - Create time series visualizations by region
   - Identify emerging markets and declining areas

## Business Insights & Applications

1. **Market Expansion Strategy**
   - Use transaction density maps to identify underserved areas
   - Target high-growth regions for new merchant acquisition

2. **Merchant Advisory Services**
   - Provide location-based performance benchmarks
   - Offer industry-specific insights based on regional performance

3. **Tourism & Seasonal Analysis**
   - Map spending patterns during peak tourist seasons
   - Identify visitor spending behaviors in different regions

4. **Risk Management**
   - Detect unusual transaction patterns by region
   - Monitor regional economic health through spending metrics

This comprehensive analysis plan leverages MasterCard's geospatial data to provide actionable insights for business development, merchant services, and strategic planning. 