from typing import List, Dict, Any
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
from datetime import datetime

def create_visitor_trend_chart(data: List[Dict[str, Any]], title: str = "Visitor Trends") -> go.Figure:
    """Create a line chart showing visitor trends over time"""
    df = pd.DataFrame(data)
    df['aoi_date'] = pd.to_datetime(df['aoi_date'])
    
    fig = go.Figure()
    
    # Add total visitors line
    fig.add_trace(go.Scatter(
        x=df['aoi_date'],
        y=df['total_visitors'],
        name='Total Visitors',
        line=dict(color='blue')
    ))
    
    # Add domestic visitors line
    fig.add_trace(go.Scatter(
        x=df['aoi_date'],
        y=df['domestic_visitors'],
        name='Domestic Visitors',
        line=dict(color='green')
    ))
    
    # Add international visitors line
    fig.add_trace(go.Scatter(
        x=df['aoi_date'],
        y=df['international_visitors'],
        name='International Visitors',
        line=dict(color='red')
    ))
    
    fig.update_layout(
        title=title,
        xaxis_title='Date',
        yaxis_title='Number of Visitors',
        hovermode='x unified'
    )
    
    return fig

def create_spending_trend_chart(data: List[Dict[str, Any]], title: str = "Spending Trends") -> go.Figure:
    """Create a line chart showing spending trends over time"""
    df = pd.DataFrame(data)
    df['txn_date'] = pd.to_datetime(df['txn_date'])
    
    fig = go.Figure()
    
    # Add total spending line
    fig.add_trace(go.Scatter(
        x=df['txn_date'],
        y=df['total_spending'],
        name='Total Spending',
        line=dict(color='blue')
    ))
    
    # Add transaction count line (secondary y-axis)
    fig.add_trace(go.Scatter(
        x=df['txn_date'],
        y=df['transaction_count'],
        name='Transaction Count',
        line=dict(color='green'),
        yaxis='y2'
    ))
    
    fig.update_layout(
        title=title,
        xaxis_title='Date',
        yaxis_title='Total Spending',
        yaxis2=dict(
            title='Transaction Count',
            overlaying='y',
            side='right'
        ),
        hovermode='x unified'
    )
    
    return fig

def create_demographic_pie_chart(data: Dict[str, Any], title: str = "Demographic Distribution") -> go.Figure:
    """Create a pie chart showing demographic distribution"""
    age_distribution = data['age_distribution']
    
    fig = go.Figure(data=[go.Pie(
        labels=list(age_distribution.keys()),
        values=list(age_distribution.values()),
        hole=.3
    )])
    
    fig.update_layout(
        title=title,
        annotations=[dict(text='Age Groups', x=0.5, y=0.5, font_size=20, showarrow=False)]
    )
    
    return fig

def create_origin_bar_chart(data: Dict[str, Any], title: str = "Top Visitor Origins") -> go.Figure:
    """Create a bar chart showing top visitor origins"""
    top_origins = data['top_origins']
    
    fig = go.Figure(data=[go.Bar(
        x=list(top_origins.keys()),
        y=list(top_origins.values())
    )])
    
    fig.update_layout(
        title=title,
        xaxis_title='Origin',
        yaxis_title='Number of Visitors',
        xaxis_tickangle=-45
    )
    
    return fig

def create_spending_heatmap(data: List[Dict[str, Any]], title: str = "Spending Heatmap") -> go.Figure:
    """Create a heatmap showing spending patterns by industry and geography"""
    df = pd.DataFrame(data)
    
    # Pivot the data for the heatmap
    pivot_df = df.pivot_table(
        values='total_spending',
        index='industry',
        columns='geo_name',
        aggfunc='sum'
    )
    
    fig = go.Figure(data=go.Heatmap(
        z=pivot_df.values,
        x=pivot_df.columns,
        y=pivot_df.index,
        colorscale='Viridis'
    ))
    
    fig.update_layout(
        title=title,
        xaxis_title='Geographic Area',
        yaxis_title='Industry'
    )
    
    return fig

def create_visitor_correlation_chart(
    visitor_data: List[Dict[str, Any]],
    spending_data: List[Dict[str, Any]],
    title: str = "Visitor-Spending Correlation"
) -> go.Figure:
    """Create a scatter plot showing correlation between visitors and spending"""
    # Convert to DataFrames
    visitor_df = pd.DataFrame(visitor_data)
    spending_df = pd.DataFrame(spending_data)
    
    # Merge the data
    merged_df = pd.merge(
        visitor_df,
        spending_df,
        left_on='aoi_date',
        right_on='txn_date',
        how='inner'
    )
    
    fig = go.Figure()
    
    # Add scatter plot
    fig.add_trace(go.Scatter(
        x=merged_df['total_visitors'],
        y=merged_df['total_spending'],
        mode='markers',
        marker=dict(
            size=10,
            color=merged_df['total_visitors'],
            colorscale='Viridis',
            showscale=True
        )
    ))
    
    fig.update_layout(
        title=title,
        xaxis_title='Number of Visitors',
        yaxis_title='Total Spending'
    )
    
    return fig 