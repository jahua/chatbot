from typing import Dict, Any, List
import logging
import re

logger = logging.getLogger(__name__)

def generate_sql_query(message: str) -> str:
    """Generate SQL query based on user message"""
    try:
        # Convert message to lowercase for easier matching
        message = message.lower()
        
        # Check for spending pattern queries
        if any(word in message for word in ["spending", "transaction", "purchase", "industry", "industries"]):
            if "by industry" in message or "across industries" in message or "industry breakdown" in message:
                return """
                    SELECT 
                        industry,
                        SUM(txn_amt::numeric) as total_amount,
                        SUM(txn_cnt::numeric) as total_transactions,
                        AVG(txn_amt::numeric) as avg_transaction_amount
                    FROM data_lake.master_card
                    GROUP BY industry
                    ORDER BY total_amount DESC
                    LIMIT 15;  /* Limit results for performance */
                """
            elif "daily" in message or "by day" in message:
                return """
                    SELECT 
                        txn_date,
                        industry,
                        SUM(txn_amt::numeric) as total_amount,
                        SUM(txn_cnt::numeric) as total_transactions
                    FROM data_lake.master_card
                    WHERE txn_date >= CURRENT_DATE - INTERVAL '30 days'  /* Add time window */
                    GROUP BY txn_date, industry
                    ORDER BY txn_date DESC, total_amount DESC
                    LIMIT 100;  /* Limit results */
                """
            else:
                return """
                    SELECT 
                        industry,
                        SUM(txn_amt::numeric) as total_amount,
                        SUM(txn_cnt::numeric) as total_transactions,
                        AVG(txn_amt::numeric) as avg_transaction_amount,
                        COUNT(DISTINCT geo_name) as unique_locations
                    FROM data_lake.master_card
                    GROUP BY industry
                    ORDER BY total_amount DESC
                    LIMIT 15;  /* Limit results for performance */
                """
        
        # Check for specific date queries
        if "day" in message and "most visitors" in message and "2023" in message:
            return """
                SELECT aoi_date, 
                       SUM((visitors->>'swissTourist')::numeric) AS swiss_tourists, 
                       SUM((visitors->>'foreignTourist')::numeric) AS foreign_tourists,
                       SUM((visitors->>'swissTourist')::numeric) + SUM((visitors->>'foreignTourist')::numeric) AS total_visitors
                FROM data_lake.aoi_days_raw
                WHERE aoi_date >= '2023-01-01' AND aoi_date < '2024-01-01'
                GROUP BY aoi_date
                ORDER BY total_visitors DESC
                LIMIT 1;
            """
        
        # Check for weekly patterns
        elif "weekly" in message and "spring" in message and "2023" in message:
            return """
                SELECT DATE_TRUNC('week', aoi_date) AS week_start, 
                       SUM((visitors->>'swissTourist')::numeric) AS total_swiss_visitors, 
                       SUM((visitors->>'foreignTourist')::numeric) AS total_foreign_visitors, 
                       SUM((visitors->>'swissTourist')::numeric) + SUM((visitors->>'foreignTourist')::numeric) AS total_visitors 
                FROM data_lake.aoi_days_raw 
                WHERE aoi_date >= '2023-03-20' AND aoi_date < '2023-06-21' 
                GROUP BY week_start 
                ORDER BY week_start;
            """
        
        # Check for peak tourism queries
        elif "peak" in message and "tourism" in message and "2023" in message:
            return """
                SELECT aoi_date, 
                       SUM((visitors->>'swissTourist')::numeric) AS total_swiss_tourists, 
                       SUM((visitors->>'foreignTourist')::numeric) AS total_foreign_tourists, 
                       SUM((visitors->>'swissTourist')::numeric) + SUM((visitors->>'foreignTourist')::numeric) AS total_visitors 
                FROM data_lake.aoi_days_raw 
                WHERE aoi_date >= '2023-01-01' AND aoi_date < '2024-01-01' 
                GROUP BY aoi_date 
                ORDER BY total_visitors DESC 
                LIMIT 10;
            """
        
        # Check for Swiss vs foreign tourist comparison
        elif ("swiss" in message and "foreign" in message) or ("domestic" in message and "international" in message):
            if "april" in message and "2023" in message:
                return """
                    SELECT aoi_date,
                           SUM((visitors->>'swissTourist')::numeric) AS swiss_tourists,
                           SUM((visitors->>'foreignTourist')::numeric) AS foreign_tourists,
                           SUM((visitors->>'swissTourist')::numeric) + SUM((visitors->>'foreignTourist')::numeric) AS total_visitors
                    FROM data_lake.aoi_days_raw
                    WHERE aoi_date >= '2023-04-01' AND aoi_date < '2023-05-01'
                    GROUP BY aoi_date
                    ORDER BY aoi_date;
                """
            else:
                return """
                    SELECT DATE_TRUNC('month', aoi_date) AS month,
                           SUM((visitors->>'swissTourist')::numeric) AS swiss_tourists,
                           SUM((visitors->>'foreignTourist')::numeric) AS foreign_tourists,
                           SUM((visitors->>'swissTourist')::numeric) + SUM((visitors->>'foreignTourist')::numeric) AS total_visitors
                    FROM data_lake.aoi_days_raw
                    WHERE aoi_date >= '2023-01-01' AND aoi_date < '2024-01-01'
                    GROUP BY month
                    ORDER BY month;
                """
        
        # Check for summer peak days
        elif "summer" in message and "2023" in message and ("top" in message or "busiest" in message):
            return """
                SELECT aoi_date,
                       SUM((visitors->>'swissTourist')::numeric) AS swiss_tourists,
                       SUM((visitors->>'foreignTourist')::numeric) AS foreign_tourists,
                       SUM((visitors->>'swissTourist')::numeric) + SUM((visitors->>'foreignTourist')::numeric) AS total_visitors
                FROM data_lake.aoi_days_raw
                WHERE aoi_date >= '2023-06-21' AND aoi_date < '2023-09-23'
                GROUP BY aoi_date
                ORDER BY total_visitors DESC
                LIMIT 3;
            """
        
        # Default query for general tourism data
        else:
            return """
                SELECT aoi_date, 
                       SUM((visitors->>'swissTourist')::numeric) AS swiss_tourists, 
                       SUM((visitors->>'foreignTourist')::numeric) AS foreign_tourists,
                       SUM((visitors->>'swissTourist')::numeric) + SUM((visitors->>'foreignTourist')::numeric) AS total_visitors
                FROM data_lake.aoi_days_raw
                GROUP BY aoi_date
                ORDER BY aoi_date DESC
                LIMIT 100;
            """
            
    except Exception as e:
        logger.error(f"Error generating SQL query: {str(e)}")
        raise 