from typing import List, Dict, Any, Optional
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import logging
import traceback
import base64
from io import BytesIO

logger = logging.getLogger(__name__)

def create_visitor_trend_chart(data: List[Dict[str, Any]], title: str = "Visitor Trends") -> plt.Figure:
    """Create a line chart showing visitor trends over time"""
    df = pd.DataFrame(data)
    df['aoi_date'] = pd.to_datetime(df['aoi_date'])
    
    # Create figure and axis
    plt.figure(figsize=(12, 6))
    
    # Plot total visitors
    plt.plot(df['aoi_date'], df['total_visitors'], label='Total Visitors', color='blue')
    
    # Plot domestic visitors
    plt.plot(df['aoi_date'], df['domestic_visitors'], label='Domestic Visitors', color='green')
    
    # Plot international visitors
    plt.plot(df['aoi_date'], df['international_visitors'], label='International Visitors', color='red')
    
    # Customize the plot
    plt.title(title)
    plt.xlabel('Date')
    plt.ylabel('Number of Visitors')
    plt.legend()
    plt.grid(True)
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    return plt.gcf()

def create_spending_trend_chart(data: List[Dict[str, Any]], title: str = "Spending Trends") -> plt.Figure:
    """Create a line chart showing spending trends over time"""
    df = pd.DataFrame(data)
    df['txn_date'] = pd.to_datetime(df['txn_date'])
    
    # Create figure and axis
    fig, ax1 = plt.subplots(figsize=(12, 6))
    
    # Plot total spending on primary y-axis
    ax1.plot(df['txn_date'], df['total_spending'], color='blue', label='Total Spending')
    ax1.set_xlabel('Date')
    ax1.set_ylabel('Total Spending', color='blue')
    ax1.tick_params(axis='y', labelcolor='blue')
    
    # Create secondary y-axis for transaction count
    ax2 = ax1.twinx()
    ax2.plot(df['txn_date'], df['transaction_count'], color='green', label='Transaction Count')
    ax2.set_ylabel('Transaction Count', color='green')
    ax2.tick_params(axis='y', labelcolor='green')
    
    # Customize the plot
    plt.title(title)
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left')
    plt.grid(True)
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    return plt.gcf()

def create_demographic_pie_chart(data: Dict[str, Any], title: str = "Demographic Distribution") -> plt.Figure:
    """Create a pie chart showing demographic distribution"""
    age_distribution = data['age_distribution']
    
    # Create figure
    plt.figure(figsize=(10, 6))
    
    # Create pie chart
    plt.pie(list(age_distribution.values()),
            labels=list(age_distribution.keys()),
            autopct='%1.1f%%',
            startangle=90,
            wedgeprops={'width': 0.7})
    
    # Customize the plot
    plt.title(title)
    plt.axis('equal')
    plt.tight_layout()
    
    return plt.gcf()

def create_origin_bar_chart(data: Dict[str, Any], title: str = "Top Visitor Origins") -> plt.Figure:
    """Create a bar chart showing top visitor origins"""
    top_origins = data['top_origins']
    
    # Create figure
    plt.figure(figsize=(12, 6))
    
    # Create bar chart
    bars = plt.bar(list(top_origins.keys()), list(top_origins.values()))
    
    # Add value labels
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height):,}',
                ha='center', va='bottom')
    
    # Customize the plot
    plt.title(title)
    plt.xlabel('Origin')
    plt.ylabel('Number of Visitors')
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    return plt.gcf()

def create_spending_heatmap(data: List[Dict[str, Any]], title: str = "Spending Heatmap") -> plt.Figure:
    """Create a heatmap showing spending patterns by industry and geography"""
    df = pd.DataFrame(data)
    
    # Pivot the data for the heatmap
    pivot_df = df.pivot_table(
        values='total_spending',
        index='industry',
        columns='geo_name',
        aggfunc='sum'
    )
    
    # Create figure
    plt.figure(figsize=(12, 8))
    
    # Create heatmap
    sns.heatmap(pivot_df, annot=True, fmt='.0f', cmap='YlOrRd')
    
    # Customize the plot
    plt.title(title)
    plt.xlabel('Geographic Area')
    plt.ylabel('Industry')
    plt.tight_layout()
    
    return plt.gcf()

def create_visitor_correlation_chart(
    visitor_data: List[Dict[str, Any]],
    spending_data: List[Dict[str, Any]],
    title: str = "Visitor-Spending Correlation"
) -> plt.Figure:
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
    
    # Create figure
    plt.figure(figsize=(10, 6))
    
    # Create scatter plot
    scatter = plt.scatter(merged_df['total_visitors'],
                         merged_df['total_spending'],
                         c=merged_df['total_visitors'],
                         cmap='viridis',
                         s=100)
    
    # Add colorbar
    plt.colorbar(scatter, label='Number of Visitors')
    
    # Customize the plot
    plt.title(title)
    plt.xlabel('Number of Visitors')
    plt.ylabel('Total Spending')
    plt.grid(True)
    plt.tight_layout()
    
    return plt.gcf()

def create_swiss_foreign_comparison_chart(swiss_tourists: int, foreign_tourists: int, title: str = "Swiss vs. Foreign Tourists Comparison") -> plt.Figure:
    """Create a bar chart comparing Swiss and foreign tourists"""
    # Create data for bar chart
    categories = ["Swiss Tourists", "Foreign Tourists"]
    values = [swiss_tourists, foreign_tourists]
    
    # Create figure
    plt.figure(figsize=(10, 6))
    
    # Create horizontal bar chart
    bars = plt.barh(categories, values, color=['#D81B60', '#1E88E5'])
    
    # Add value labels
    for bar in bars:
        width = bar.get_width()
        plt.text(width, bar.get_y() + bar.get_height()/2,
                f'{int(width):,}',
                ha='left', va='center')
    
    # Add percentage annotations
    total = sum(values)
    percentages = [value / total * 100 for value in values]
    
    for i, (value, percentage) in enumerate(zip(values, percentages)):
        plt.text(value/2, i,
                f"{percentage:.1f}%",
                ha='center', va='center',
                color='white', fontweight='bold')
    
    # Customize the plot
    plt.title(title)
    plt.xlabel('Number of Tourists')
    plt.tight_layout()
    
    return plt.gcf()

def figure_to_base64(fig: plt.Figure) -> str:
    """Convert a matplotlib figure to a base64-encoded PNG image"""
    buf = BytesIO()
    fig.savefig(buf, format='png', bbox_inches='tight')
    buf.seek(0)
    img_str = base64.b64encode(buf.read()).decode('utf-8')
    plt.close(fig)  # Close the figure to free memory
    return img_str

def create_visualization(data: List[Dict[str, Any]], query: str) -> Optional[str]:
    """Create a visualization based on the data and query type"""
    try:
        if not data:
            logger.warning("No data provided for visualization")
            return None
            
        df = pd.DataFrame(data)
        if df.empty:
            logger.warning("Empty DataFrame provided for visualization")
            return None
            
        # Set style
        plt.style.use('default')  # Use default style instead of seaborn
        
        # Handle peak tourism periods visualization
        if "peak" in query.lower() and "tourism" in query.lower():
            if 'aoi_date' in df.columns and 'total_visitors' in df.columns:
                # Convert date column
                df['aoi_date'] = pd.to_datetime(df['aoi_date'])
                df = df.sort_values('aoi_date')
                
                # Create figure
                plt.figure(figsize=(12, 6))
                
                # Create bar plot
                plt.bar(df['aoi_date'], df['total_visitors'], color='skyblue')
                
                # Customize the plot
                plt.title('Peak Tourism Periods in 2023')
                plt.xlabel('Date')
                plt.ylabel('Total Visitors')
                plt.xticks(rotation=45)
                plt.grid(True, linestyle='--', alpha=0.7)
                plt.tight_layout()
                
                return figure_to_base64(plt.gcf())
        
        # Handle weekly patterns visualization
        elif "weekly" in query.lower() or "week" in query.lower():
            if 'week_start' in df.columns and 'total_visitors' in df.columns:
                # Convert date column
                df['week_start'] = pd.to_datetime(df['week_start'])
                df = df.sort_values('week_start')
                
                # Create figure
                plt.figure(figsize=(12, 6))
                
                # Create line plot
                plt.plot(df['week_start'], df['total_visitors'], marker='o', color='blue', label='Total Visitors')
                
                # Add Swiss and foreign visitors if available
                if 'total_swiss_visitors' in df.columns and 'total_foreign_visitors' in df.columns:
                    plt.plot(df['week_start'], df['total_swiss_visitors'], marker='s', color='green', label='Swiss Visitors')
                    plt.plot(df['week_start'], df['total_foreign_visitors'], marker='^', color='red', label='Foreign Visitors')
                
                # Customize the plot
                plt.title('Weekly Visitor Patterns')
                plt.xlabel('Week Starting')
                plt.ylabel('Number of Visitors')
                plt.legend()
                plt.grid(True, linestyle='--', alpha=0.7)
                plt.xticks(rotation=45)
                plt.tight_layout()
                
                return figure_to_base64(plt.gcf())
        
        # Handle time series data
        elif 'aoi_date' in df.columns and ('total_visitors' in df.columns or 'visitors' in df.columns):
            # Convert date column
            df['aoi_date'] = pd.to_datetime(df['aoi_date'])
            df = df.sort_values('aoi_date')
            
            # Create figure
            plt.figure(figsize=(12, 6))
            
            # Create line plot
            plt.plot(df['aoi_date'], 
                    df['total_visitors'] if 'total_visitors' in df.columns else df['visitors'],
                    marker='o',
                    color='blue')
            
            # Customize the plot
            plt.title('Visitor Trends Over Time')
            plt.xlabel('Date')
            plt.ylabel('Number of Visitors')
            plt.grid(True, linestyle='--', alpha=0.7)
            plt.xticks(rotation=45)
            plt.tight_layout()
            
            return figure_to_base64(plt.gcf())
        
        # Handle Swiss vs Foreign tourists comparison
        elif 'swiss_tourists' in df.columns and 'foreign_tourists' in df.columns:
            # Create figure
            plt.figure(figsize=(10, 6))
            
            # Prepare data
            categories = ['Swiss Tourists', 'Foreign Tourists']
            values = [df['swiss_tourists'].sum(), df['foreign_tourists'].sum()]
            
            # Create bar plot
            bars = plt.bar(categories, values, color=['blue', 'red'])
            
            # Add value labels
            for bar in bars:
                height = bar.get_height()
                plt.text(bar.get_x() + bar.get_width()/2., height,
                        f'{int(height):,}',
                        ha='center', va='bottom')
            
            # Customize the plot
            plt.title('Swiss vs Foreign Tourists Comparison')
            plt.ylabel('Number of Tourists')
            plt.grid(True, linestyle='--', alpha=0.7)
            plt.tight_layout()
            
            return figure_to_base64(plt.gcf())
        
        # Default to a simple bar chart if no specific visualization is matched
        else:
            numeric_columns = df.select_dtypes(include=['int64', 'float64']).columns
            if len(numeric_columns) > 0:
                # Create figure
                plt.figure(figsize=(12, 6))
                
                # Create bar plot
                df[numeric_columns].plot(kind='bar')
                
                # Customize the plot
                plt.title('Data Overview')
                plt.xlabel('Index')
                plt.ylabel('Value')
                plt.grid(True, linestyle='--', alpha=0.7)
                plt.xticks(rotation=45)
                plt.tight_layout()
                
                return figure_to_base64(plt.gcf())
        
        return None
        
    except Exception as e:
        logger.error(f"Error creating visualization: {str(e)}")
        logger.error(traceback.format_exc())
        return None 