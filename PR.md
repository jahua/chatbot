# PR: Fix Swiss vs International Tourist Visualization

## Problem
The system currently fails to properly visualize Swiss vs international tourist data when requested as a bar chart. When the LLM authentication fails, the fallback SQL query only retrieves total visitors without separating Swiss from international tourists, and the visualization defaults to a line chart.

## Root Causes
1. Authentication error with the language model service (nuwaapi.com)
2. Inadequate fallback SQL for tourist comparison queries
3. Visualization service not detecting Swiss vs international tourist data

## Solution

### 1. Add Specific Fallback for Swiss vs International Tourist Queries
```python
# In app/services/sql_generation_service.py - _get_fallback_sql method

# Check for Swiss vs international tourist comparison request
if (("swiss" in user_question.lower() and "international" in user_question.lower() 
     or "swiss" in user_question.lower() and "foreign" in user_question.lower())
    and "tourist" in user_question.lower() and "month" in user_question.lower()):
    
    # Special case for Swiss vs international tourists by month (optimized for bar chart)
    return f"""
    SELECT 
        d.month,
        d.month_name, 
        SUM(fv.swiss_tourists) as swiss_tourists,
        SUM(fv.foreign_tourists) as foreign_tourists
    FROM dw.fact_visitor fv
    JOIN dw.dim_date d ON fv.date_id = d.date_id
    WHERE d.year = {target_year}
    GROUP BY d.month, d.month_name
    ORDER BY d.month
    """
```

### 2. Enhance Visualization Type Selection
```python
# In app/services/visualization_service.py - _hybrid_visualization_selection method

# Special case for Swiss vs International tourists comparison
if (('swiss' in query.lower() and 'international' in query.lower() or 
     'swiss' in query.lower() and 'foreign' in query.lower()) and
    'tourist' in query.lower() and
    'month' in query.lower() and
    'swiss_tourists' in data.columns and
    'foreign_tourists' in data.columns):
    
    logger.info("Detected Swiss vs International tourists comparison request. Using bar chart.")
    return "bar"
```

### 3. Add Specialized Bar Chart Creation for Tourist Comparison
```python
# In app/services/visualization_service.py - _create_bar_chart method

# Special case for Swiss vs International tourists by month
if ('swiss_tourists' in df.columns and 'foreign_tourists' in df.columns and 
    'month' in df.columns and 'month_name' in df.columns):
    
    # Use month_name for better readability
    month_name_col = 'month_name'
    
    # Sort by month number to ensure chronological order
    if 'month' in df.columns:
        df = df.sort_values(by='month')
    
    # Create a grouped bar chart
    fig = go.Figure()
    
    # Add Swiss tourists bar
    fig.add_trace(go.Bar(
        x=df[month_name_col],
        y=df['swiss_tourists'],
        name='Swiss Tourists',
        marker_color='blue'
    ))
    
    # Add International/Foreign tourists bar
    fig.add_trace(go.Bar(
        x=df[month_name_col],
        y=df['foreign_tourists'],
        name='International Tourists',
        marker_color='red'
    ))
    
    # Update layout
    fig.update_layout(
        title="Swiss and International Tourists per Month",
        xaxis_title="Month",
        yaxis_title="Number of Tourists",
        barmode='group',
        legend_title="Tourist Type",
        template="plotly_dark"
    )
    
    logger.info("Created Swiss vs International tourists bar chart")
    
    # Convert to plotly json for frontend
    return {
        "type": "plotly",
        "data": json.loads(fig.to_json())
    }
```

## Implementation Notes
These changes enhance the fallback mechanisms for handling visualization requests when the LLM service is unavailable. After implementing these changes:

1. The system will correctly generate SQL that separates Swiss and international tourists 
2. The visualization type will be properly detected as a bar chart
3. The bar chart will be correctly formatted with appropriate colors and grouping

The PR also includes a test script (`test_api.py`) to verify the functionality.

## Testing
To test these changes, restart the Docker container after implementation and run:
```bash
python test_api.py "Show me a bar chart comparing Swiss tourists and foreign tourists per month in 2023"
``` 