# Using Langchain for Geospatial Data Analysis

## Core Components

### 1. Data Loading and Processing
- **GeoPandas Integration**
  - Extends pandas capabilities with spatial operations
  - Supports various geometry types (points, polygons, etc.)
  - Uses shapely for geometric operations
  - Example: `GeoDataFrameLoader` for converting geospatial data to documents

### 2. Vector Database Integration
- **PostgreSQL with pgvector**
  - Efficient spatial indexing for vector search
  - Supports similarity search with geospatial context
  - Can store embeddings alongside spatial data

### 3. Retrieval Methods
- **Retriever Tools**
  - Create vector stores with location names and attributes
  - Use retrieval for context-aware location queries
  - Specialized retrieval for high-cardinality location columns

### 4. Query Processing
- **SQL Query Generation**
  - Convert natural language to SQL with spatial awareness
  - Handle fuzzy matching for location names
  - Support for PostGIS spatial functions in generated queries

## Example Workflow

```python
import geopandas as gpd
import pandas as pd
from langchain_community.document_loaders import GeoDataFrameLoader
from langchain_community.vectorstores import FAISS
from langchain_openai import OpenAIEmbeddings

# 1. Load geospatial data
gdf = gpd.read_file("path_to_geojson.geojson")

# 2. Convert to documents for RAG
loader = GeoDataFrameLoader(data_frame=gdf, page_content_column="geometry")
docs = loader.load()

# 3. Extract location names for vector search
locations = list(gdf["location_name"].unique())

# 4. Create vector store for location search
vector_db = FAISS.from_texts(locations, OpenAIEmbeddings())
retriever = vector_db.as_retriever(search_kwargs={"k": 5})

# 5. Create retriever tool for location matching
retriever_tool = create_retriever_tool(
    retriever,
    name="search_locations",
    description="Find exact location names from approximate user input"
)
```

## PostgreSQL Integration

```python
# Database connection parameters
db_params = {
    'host': os.getenv('POSTGRES_HOST'),
    'port': os.getenv('POSTGRES_PORT'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'database': os.getenv('POSTGRES_DB')
}

# Connect to PostgreSQL with PostGIS extension
conn = psycopg2.connect(**db_params)
```

## Visualization Component

```python
import matplotlib.pyplot as plt
import plotly.express as px

# Matplotlib visualization
fig, ax = plt.subplots(figsize=(10, 10))
gdf.plot(ax=ax, color="blue", alpha=0.5)
plt.title("Geospatial Data Visualization")
plt.show()

# Plotly interactive map
fig = px.scatter_mapbox(
    gdf, 
    lat="latitude", 
    lon="longitude",
    color="category",
    hover_name="location_name",
    zoom=8
)
fig.update_layout(mapbox_style="open-street-map")
fig.show()
```

## Use Cases

1. **Location-based RAG**
   - Retrieve documents relevant to specific geographic areas
   - Include spatial context in generated responses

2. **Spatial Data Querying**
   - Convert natural language queries to spatial SQL
   - Support for distance-based, intersection, and containment queries

3. **Location Entity Resolution**
   - Match imprecise location references to exact database entries
   - Handle variations in spelling and naming conventions 