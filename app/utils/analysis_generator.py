from typing import Dict, Any, List
import logging
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

def format_results_as_markdown_table(query_results: List[Dict[Any, Any]]) -> str:
    """Format query results as a markdown table"""
    try:
        if not query_results:
            return "No data available."
            
        df = pd.DataFrame(query_results)
        
        # Convert Decimal to float for calculations
        for col in df.select_dtypes(include=['object']).columns:
            try:
                df[col] = pd.to_numeric(df[col], errors='ignore')
            except:
                pass
        
        # Format date columns if they exist
        date_columns = df.select_dtypes(include=['datetime64']).columns
        for col in date_columns:
            df[col] = df[col].dt.strftime('%Y-%m-%d')
            
        # Format numeric columns
        for col in df.select_dtypes(include=['float64', 'int64']).columns:
            if 'amount' in col.lower() or 'spending' in col.lower() or 'revenue' in col.lower():
                df[col] = df[col].apply(lambda x: f"CHF {x:,.2f}" if pd.notnull(x) else "")
            elif 'percentage' in col.lower() or 'pct' in col.lower() or 'ratio' in col.lower():
                df[col] = df[col].apply(lambda x: f"{x:.1f}%" if pd.notnull(x) else "")
            elif 'count' in col.lower() or 'visitors' in col.lower() or 'tourists' in col.lower():
                df[col] = df[col].apply(lambda x: f"{int(x):,}" if pd.notnull(x) else "")
            else:
                df[col] = df[col].apply(lambda x: f"{x:,.2f}" if pd.notnull(x) else "")
                
        # Create markdown table
        markdown = "| " + " | ".join(df.columns) + " |\n"
        markdown += "| " + " | ".join(["---"] * len(df.columns)) + " |\n"
        
        for _, row in df.iterrows():
            markdown += "| " + " | ".join([str(val) for val in row]) + " |\n"
            
        return markdown
    except Exception as e:
        logger.error(f"Error formatting results as markdown table: {str(e)}")
        return "Error formatting results as markdown table."

def generate_analysis_summary(query_results: List[Dict[Any, Any]], query_type: str = None) -> str:
    """Generate analysis summary from query results"""
    try:
        if not query_results:
            return "No data available for analysis."

        df = pd.DataFrame(query_results)
        
        # Convert Decimal to float for calculations
        for col in df.select_dtypes(include=['object']).columns:
            try:
                df[col] = pd.to_numeric(df[col], errors='ignore')
            except:
                pass
        
        # Format date columns if they exist
        date_columns = df.select_dtypes(include=['datetime64']).columns
        for col in date_columns:
            df[col] = df[col].dt.strftime('%Y-%m-%d')

        # Determine query type if not provided
        if not query_type:
            if 'industry' in df.columns:
                query_type = 'spending_pattern'
            elif 'week_start' in df.columns:
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
        if query_type == 'spending_pattern':
            total_amount = df['total_amount'].sum() if 'total_amount' in df.columns else 0
            total_transactions = df['total_transactions'].sum() if 'total_transactions' in df.columns else 0
            avg_transaction = total_amount / total_transactions if total_transactions > 0 else 0
            
            summary = f"""
## 💰 Spending Pattern Analysis

### 📊 Overall Statistics
- **Total Spending:** CHF {total_amount:,.2f}
- **Total Transactions:** {total_transactions:,}
- **Average Transaction:** CHF {avg_transaction:.2f}

### 🏢 Top Industries by Spending
"""
            # Add top 5 industries by spending
            if len(df) > 0 and 'total_amount' in df.columns:
                top_industries = df.nlargest(5, 'total_amount')
                for _, row in top_industries.iterrows():
                    pct = (row['total_amount'] / total_amount * 100) if total_amount > 0 else 0
                    summary += f"- **{row['industry']}**\n"
                    summary += f"  - Amount: CHF {row['total_amount']:,.2f} ({pct:.1f}%)\n"
                    summary += f"  - Transactions: {row['total_transactions']:,}\n"
                    if 'avg_transaction_amount' in row:
                        summary += f"  - Avg Transaction: CHF {row['avg_transaction_amount']:,.2f}\n"

            summary += "\n### 💡 Key Insights\n"
            if len(df) > 0 and 'total_amount' in df.columns:
                max_amount = df['total_amount'].max()
                summary += f"- Top industry accounts for {(max_amount / total_amount * 100):.1f}% of total spending\n"
            if 'unique_locations' in df.columns:
                total_locations = df['unique_locations'].sum()
                summary += f"- Transactions recorded across {total_locations:,} unique locations\n"
            if len(df) > 0 and 'total_amount' in df.columns and 'total_transactions' in df.columns:
                min_amt = df['total_amount'].min()
                min_txn = df['total_transactions'].min()
                max_amt = df['total_amount'].max()
                max_txn = df['total_transactions'].max()
                if min_txn > 0 and max_txn > 0:
                    summary += f"- Average transaction size varies from CHF {min_amt/min_txn:.2f} to CHF {max_amt/max_txn:.2f}\n"

            return summary

        elif query_type == 'top_days':
            summary = f"""
## 🏔️ Tourism Highlights - Top Days

### 📊 Key Statistics
- **Records Analyzed:** {len(df)} days
- **Average Daily Visitors:** {avg_daily_visitors:,}
- **Foreign Tourist Share:** {foreign_percentage:.1f}%

### 📈 Top Visitor Days
"""
            for _, row in df.iterrows():
                total = row['total_visitors']
                swiss = row['swiss_tourists']
                foreign = row['foreign_tourists']
                if total > 0:
                    summary += f"- **{row['aoi_date']}**: {int(total):,} visitors\n"
                    summary += f"  - Swiss: {int(swiss):,} ({swiss/total*100:.1f}%)\n"
                    summary += f"  - Foreign: {int(foreign):,} ({foreign/total*100:.1f}%)\n"

            return summary

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
                total = row['total_visitors']
                swiss = row['total_swiss_tourists']
                foreign = row['total_foreign_tourists']
                summary += f"- **{row['aoi_date']}**: {int(total):,} total visitors\n"
                summary += f"  - Swiss: {int(swiss):,} | Foreign: {int(foreign):,}\n"

            summary += "\n### 💡 Key Insights\n"
            if len(df) > 0 and 'total_visitors' in df.columns:
                max_visitors = df['total_visitors'].max()
                summary += f"- Peak visitor day recorded {max_visitors:,.0f} visitors\n"
            summary += "- Consistent pattern of higher Swiss visitor numbers\n"
            summary += "- Weekend days show notably higher visitor counts\n"

            return summary

        elif query_type == 'weekly_pattern':
            summary = f"""
## 📅 Weekly Tourism Pattern Analysis

### 📊 Period Overview
- **Weeks Analyzed:** {len(df)}
- **Average Weekly Visitors:** {avg_daily_visitors:,}
- **International Mix:** {foreign_percentage:.1f}% foreign visitors

### 📈 Weekly Trends
"""
            if len(df) > 0 and 'total_visitors' in df.columns:
                max_visitors_idx = df['total_visitors'].idxmax()
                max_week = df.loc[max_visitors_idx]
                summary += f"- **Peak Week:** {max_week['week_start']}\n"
                summary += f"  - Total Visitors: {max_week['total_visitors']:,.0f}\n"
                summary += f"  - Swiss: {max_week['total_swiss_visitors']:,.0f}\n"
                summary += f"  - Foreign: {max_week['total_foreign_visitors']:,.0f}\n"

            summary += "\n### 💡 Pattern Insights\n"
            if len(df) > 0 and 'total_visitors' in df.columns:
                std = df['total_visitors'].std()
                mean = df['total_visitors'].mean()
                if mean > 0:
                    volatility = (std / mean * 100)
                    summary += f"- Weekly variation shows {volatility:.1f}% volatility\n"
                if 'total_foreign_visitors' in df.columns and 'total_visitors' in df.columns:
                    foreign_ratio = df['total_foreign_visitors'] / df['total_visitors']
                    min_foreign = foreign_ratio.min() * 100
                    max_foreign = foreign_ratio.max() * 100
                    summary += f"- Foreign visitor share ranges from {min_foreign:.1f}% to {max_foreign:.1f}%\n"

            return summary

        else:
            summary = f"""
## 📊 Tourism Data Analysis

### 📈 Key Metrics
- **Period Coverage:** {len(df)} data points
- **Average Daily Visitors:** {avg_daily_visitors:,}
- **Foreign Tourist Share:** {foreign_percentage:.1f}%

### 💡 Key Insights
"""
            if len(df) > 0 and 'total_visitors' in df.columns:
                min_visitors = df['total_visitors'].min()
                max_visitors = df['total_visitors'].max()
                summary += f"- Visitor numbers range from {min_visitors:,.0f} to {max_visitors:,.0f}\n"
                std = df['total_visitors'].std()
                mean = df['total_visitors'].mean()
                if mean > 0:
                    volatility = (std / mean * 100)
                    pattern = 'weekend peaks' if volatility > 20 else 'stable distribution'
                    summary += f"- Pattern suggests {pattern}\n"

            return summary

    except Exception as e:
        logger.error(f"Error generating analysis summary: {str(e)}")
        return "Error generating analysis summary. Please try again." 