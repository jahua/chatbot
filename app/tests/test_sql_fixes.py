import unittest
import sys
import os
import logging

# Add the parent directory to the path to import the application modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.sql_generation_service import SQLGenerationService
from app.llm.openai_adapter import OpenAIAdapter

# Mock the OpenAI adapter for testing
class MockOpenAIAdapter:
    def __init__(self):
        pass
    
    async def agenerate_text(self, prompt):
        return "Mock response"

class TestSQLFixes(unittest.TestCase):
    def setUp(self):
        """Set up the test environment"""
        self.sql_service = SQLGenerationService(MockOpenAIAdapter())
    
    def test_extract_without_group_by(self):
        """Test SQL with EXTRACT but missing it in GROUP BY"""
        test_sql = """
        SELECT EXTRACT(year FROM d.full_date) AS year, 
               EXTRACT(month FROM d.full_date) AS month,
               SUM(fv.total_visitors) AS total_visitors
        FROM dw.fact_visitor fv
        JOIN dw.dim_date d ON fv.date_id = d.date_id
        GROUP BY year, month
        """
        fixed_sql = self.sql_service._fix_common_sql_errors(test_sql)
        
        # Check that the GROUP BY clause was fixed to include the full EXTRACT expressions
        self.assertIn("GROUP BY EXTRACT(year FROM d.full_date), EXTRACT(month FROM d.full_date)", fixed_sql)
        self.assertNotIn("GROUP BY year, month", fixed_sql)
    
    def test_incomplete_group_by(self):
        """Test SQL with missing columns in GROUP BY"""
        test_sql = """
        SELECT d.month_name, d.year, EXTRACT(week FROM d.full_date) AS week_num,
               SUM(fv.total_visitors) AS total_visitors
        FROM dw.fact_visitor fv
        JOIN dw.dim_date d ON fv.date_id = d.date_id
        GROUP BY d.month_name, d.year
        """
        fixed_sql = self.sql_service._fix_common_sql_errors(test_sql)
        
        # Check that the GROUP BY clause was fixed to include the missing EXTRACT expression
        self.assertIn("GROUP BY d.month_name, d.year, EXTRACT(week FROM d.full_date)", fixed_sql)
    
    def test_extract_function_in_group_by(self):
        """Test SQL that correctly uses the EXTRACT function in GROUP BY"""
        test_sql = """
        SELECT EXTRACT(year FROM d.full_date) AS year, 
               EXTRACT(month FROM d.full_date) AS month,
               SUM(fv.total_visitors) AS total_visitors
        FROM dw.fact_visitor fv
        JOIN dw.dim_date d ON fv.date_id = d.date_id
        GROUP BY EXTRACT(year FROM d.full_date), EXTRACT(month FROM d.full_date)
        """
        fixed_sql = self.sql_service._fix_common_sql_errors(test_sql)
        
        # The SQL is already correct, so it should remain unchanged
        self.assertEqual(test_sql.strip(), fixed_sql.strip())
    
    def test_preferred_approach(self):
        """Test SQL using the preferred approach with d.year and d.month directly"""
        test_sql = """
        SELECT d.year, d.month_name,
               SUM(fv.total_visitors) AS total_visitors
        FROM dw.fact_visitor fv
        JOIN dw.dim_date d ON fv.date_id = d.date_id
        GROUP BY d.year, d.month, d.month_name
        ORDER BY d.year, d.month
        """
        fixed_sql = self.sql_service._fix_common_sql_errors(test_sql)
        
        # The SQL is already using the preferred approach, so it should remain unchanged
        self.assertEqual(test_sql.strip(), fixed_sql.strip())
    
    def test_complex_query_with_multiple_extracts(self):
        """Test a more complex query with multiple EXTRACT functions"""
        test_sql = """
        WITH monthly_data AS (
            SELECT 
                EXTRACT(year FROM d.full_date) AS year,
                EXTRACT(month FROM d.full_date) AS month,
                EXTRACT(day FROM d.full_date) AS day,
                d.month_name,
                SUM(fv.swiss_tourists) AS swiss_tourists,
                SUM(fv.foreign_tourists) AS foreign_tourists
            FROM dw.fact_visitor fv
            JOIN dw.dim_date d ON fv.date_id = d.date_id
            WHERE d.year = 2023
            GROUP BY month, year, day, d.month_name
            ORDER BY year, month, day
        )
        SELECT year, month_name, SUM(swiss_tourists) AS total_swiss, SUM(foreign_tourists) AS total_foreign
        FROM monthly_data
        GROUP BY year, month_name
        ORDER BY year, month
        """
        fixed_sql = self.sql_service._fix_common_sql_errors(test_sql)
        
        # Check that all EXTRACT functions in the CTE are properly included in GROUP BY
        self.assertIn("GROUP BY EXTRACT(month FROM d.full_date), EXTRACT(year FROM d.full_date), EXTRACT(day FROM d.full_date), d.month_name", fixed_sql)
        
        # The main query's GROUP BY is still an issue (references month which isn't in SELECT), but our fix
        # focuses on the EXTRACT issues primarily
    
    def test_week_of_year_replacement(self):
        """Test the fix for d.week_of_year references"""
        test_sql = """
        SELECT d.week_of_year, SUM(fv.total_visitors) AS total_visitors
        FROM dw.fact_visitor fv
        JOIN dw.dim_date d ON fv.date_id = d.date_id
        GROUP BY d.week_of_year
        ORDER BY d.week_of_year
        """
        fixed_sql = self.sql_service._fix_common_sql_errors(test_sql)
        
        # Check that d.week_of_year is replaced with the EXTRACT expression
        self.assertIn("EXTRACT(WEEK FROM d.full_date) AS week_of_year", fixed_sql)
        self.assertIn("GROUP BY EXTRACT(WEEK FROM d.full_date)", fixed_sql)
        self.assertIn("ORDER BY EXTRACT(WEEK FROM d.full_date)", fixed_sql)

if __name__ == "__main__":
    # Set up basic logging
    logging.basicConfig(level=logging.INFO)
    unittest.main() 