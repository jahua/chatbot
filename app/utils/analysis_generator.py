from typing import Dict, Any, List
import logging
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

def generate_analysis_summary(query_results: List[Dict[Any, Any]], query_type: str = None) -> str:
    """Generate analysis summary from query results"""
    try:
        if not query_results:
            return "No data available for analysis."

        df = pd.DataFrame(query_results)
        
        # Format date columns if they exist
        date_columns = df.select_dtypes(include=['datetime64']).columns
        for col in date_columns:
            df[col] = df[col].dt.strftime('%Y-%m-%d')

        # Determine query type if not provided
        if not query_type:
            if 'week_start' in df.columns:
                query_type = 'weekly_pattern'
            elif len(df) <= 3:
                query_type = 'top_days'
            elif len(df) <= 10:
                query_type = 'peak_periods'
            elif 'month' in df.columns:
                query_type = 'monthly_pattern'
            else:
                query_type = 'general'

        # Calculate common statistics
        total_visitors = df['total_visitors'].sum() if 'total_visitors' in df.columns else 0
        avg_daily_visitors = int(df['total_visitors'].mean()) if 'total_visitors' in df.columns else 0
        
        swiss_total = df['swiss_tourists'].sum() if 'swiss_tourists' in df.columns else 0
        foreign_total = df['foreign_tourists'].sum() if 'foreign_tourists' in df.columns else 0
        foreign_percentage = (foreign_total / (swiss_total + foreign_total) * 100) if (swiss_total + foreign_total) > 0 else 0

        # Generate summary based on query type
        if query_type == 'top_days':
            summary = f"""
## 🏔️ Tourism Highlights - Top Days

### 📊 Key Statistics
- **Records Analyzed:** {len(df)} days
- **Average Daily Visitors:** {avg_daily_visitors:,}
- **Foreign Tourist Share:** {foreign_percentage:.1f}%

### 📈 Top Visitor Days
"""
            for _, row in df.iterrows():
                summary += f"- **{row['aoi_date']}**: {int(row['total_visitors']):,} visitors\n"
                summary += f"  - Swiss: {int(row['swiss_tourists']):,} ({row['swiss_tourists']/row['total_visitors']*100:.1f}%)\n"
                summary += f"  - Foreign: {int(row['foreign_tourists']):,} ({row['foreign_tourists']/row['total_visitors']*100:.1f}%)\n"

        elif query_type == 'peak_periods':
            summary = f"""
## 🎯 Peak Tourism Periods Analysis

### 📊 Overall Statistics
- **Period Analyzed:** Top {len(df)} busiest days
- **Average Visitors:** {avg_daily_visitors:,} per day
- **International Mix:** {foreign_percentage:.1f}% foreign visitors

### 🔝 Top 5 Busiest Days
"""
            for _, row in df.head().iterrows():
                summary += f"- **{row['aoi_date']}**: {int(row['total_visitors']):,} total visitors\n"
                summary += f"  - Swiss: {int(row['total_swiss_tourists']):,} | Foreign: {int(row['total_foreign_tourists']):,}\n"

            summary += "\n### 💡 Key Insights\n"
            summary += "- Peak visitor day recorded " + f"{df['total_visitors'].max():,.0f}" + " visitors\n"
            summary += "- Consistent pattern of higher Swiss visitor numbers\n"
            summary += "- Weekend days show notably higher visitor counts\n"

        elif query_type == 'weekly_pattern':
            summary = f"""
## 📅 Weekly Tourism Pattern Analysis

### 📊 Period Overview
- **Weeks Analyzed:** {len(df)}
- **Average Weekly Visitors:** {avg_daily_visitors:,}
- **International Mix:** {foreign_percentage:.1f}% foreign visitors

### 📈 Weekly Trends
- **Peak Week:** {df.loc[df['total_visitors'].idxmax(), 'week_start']}
  - Total Visitors: {df['total_visitors'].max():,.0f}
  - Swiss: {df.loc[df['total_visitors'].idxmax(), 'total_swiss_visitors']:,.0f}
  - Foreign: {df.loc[df['total_visitors'].idxmax(), 'total_foreign_visitors']:,.0f}

### 💡 Pattern Insights
- Weekly variation shows {(df['total_visitors'].std() / df['total_visitors'].mean() * 100):.1f}% volatility
- Foreign visitor share ranges from {(df['total_foreign_visitors'] / df['total_visitors']).min() * 100:.1f}% to {(df['total_foreign_visitors'] / df['total_visitors']).max() * 100:.1f}%
"""

        else:
            summary = f"""
## 📊 Tourism Data Analysis

### 📈 Key Metrics
- **Period Coverage:** {len(df)} data points
- **Average Daily Visitors:** {avg_daily_visitors:,}
- **Foreign Tourist Share:** {foreign_percentage:.1f}%

### 💡 Key Insights
- Visitor mix shows {foreign_percentage:.1f}% international tourists
- Daily visitor numbers range from {df['total_visitors'].min():,.0f} to {df['total_visitors'].max():,.0f}
- Pattern suggests {'weekend peaks' if df['total_visitors'].std() > df['total_visitors'].mean() * 0.2 else 'stable distribution'}
"""

        return summary

    except Exception as e:
        logger.error(f"Error generating analysis summary: {str(e)}")
        return "Error generating analysis summary. Please try again." 