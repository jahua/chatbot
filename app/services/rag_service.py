from typing import Dict, Any, Optional, List
from app.rag.tourism_rag import TourismRAG
from app.core.config import settings
from app.llm.openai_adapter import OpenAIAdapter
import logging
import json
from datetime import datetime
from sqlalchemy.orm import Session
from ..utils.sql_utils import (
    build_visitor_query,
    build_spending_query,
    build_demographics_query,
    build_origin_query
)
from ..db.models import AOIDay, GeoinsightsDataRaw

logger = logging.getLogger(__name__)

class RAGService:
    def __init__(self, db: Session):
        self.db = db
        # Initialize LLM adapter
        self.llm = OpenAIAdapter()
        
        # Initialize RAG pipeline
        self.rag = TourismRAG()
        
        logger.info("RAGService initialized successfully")
    
    async def process_message(self, message: str, conversation_id: Optional[str] = None) -> Dict[str, Any]:
        """Process a user message through the RAG pipeline"""
        try:
            # Process query through RAG
            rag_response = await self.rag.process_query(message)
            
            # Format response
            response = {
                "message": rag_response["response"],
                "conversation_id": conversation_id,
                "metadata": {
                    "source": "rag",
                    "chat_history": rag_response["chat_history"]
                }
            }
            
            return response
            
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")
            # Fallback to basic LLM response if RAG fails
            return await self._fallback_response(message, conversation_id)
    
    async def _fallback_response(self, message: str, conversation_id: Optional[str] = None) -> Dict[str, Any]:
        """Fallback to basic LLM response if RAG fails"""
        try:
            response = await self.llm.generate_response(message)
            return {
                "message": response,
                "conversation_id": conversation_id,
                "metadata": {
                    "source": "llm",
                    "fallback": True
                }
            }
        except Exception as e:
            logger.error(f"Error in fallback response: {str(e)}")
            raise 

    def get_visitor_statistics(
        self,
        aoi_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get visitor statistics for a specific AOI and date range"""
        query = build_visitor_query(aoi_id, start_date, end_date)
        result = self.db.execute(query)
        return [dict(row) for row in result]

    def get_spending_statistics(
        self,
        geo_name: Optional[str] = None,
        industry: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get spending statistics for a specific geographic area and industry"""
        query = build_spending_query(geo_name, industry, start_date, end_date)
        result = self.db.execute(query)
        return [dict(row) for row in result]

    def get_demographic_statistics(
        self,
        aoi_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get demographic statistics for a specific AOI and date range"""
        query = build_demographics_query(aoi_id, start_date, end_date)
        result = self.db.execute(query)
        return [dict(row) for row in result]

    def get_origin_statistics(
        self,
        aoi_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        origin_type: str = "foreign"
    ) -> List[Dict[str, Any]]:
        """Get visitor origin statistics for a specific AOI and date range"""
        query = build_origin_query(aoi_id, start_date, end_date, origin_type)
        result = self.db.execute(query)
        return [dict(row) for row in result]

    def analyze_visitor_trends(
        self,
        aoi_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Dict[str, Any]:
        """Analyze visitor trends including growth rates and patterns"""
        visitor_stats = self.get_visitor_statistics(aoi_id, start_date, end_date)
        if not visitor_stats:
            return {"error": "No data found for the specified parameters"}

        # Calculate basic statistics
        total_visitors = sum(row["total_visitors"] for row in visitor_stats)
        avg_visitors = total_visitors / len(visitor_stats)
        max_visitors = max(row["total_visitors"] for row in visitor_stats)
        min_visitors = min(row["total_visitors"] for row in visitor_stats)

        # Calculate growth rate
        first_day = visitor_stats[0]["total_visitors"]
        last_day = visitor_stats[-1]["total_visitors"]
        growth_rate = ((last_day - first_day) / first_day) * 100 if first_day > 0 else 0

        return {
            "total_visitors": total_visitors,
            "average_visitors": avg_visitors,
            "max_visitors": max_visitors,
            "min_visitors": min_visitors,
            "growth_rate": growth_rate,
            "data_points": len(visitor_stats)
        }

    def analyze_spending_patterns(
        self,
        geo_name: Optional[str] = None,
        industry: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Dict[str, Any]:
        """Analyze spending patterns including trends and averages"""
        spending_stats = self.get_spending_statistics(geo_name, industry, start_date, end_date)
        if not spending_stats:
            return {"error": "No data found for the specified parameters"}

        # Calculate basic statistics
        total_spending = sum(row["total_spending"] for row in spending_stats)
        total_transactions = sum(row["transaction_count"] for row in spending_stats)
        avg_ticket = sum(row["average_ticket"] for row in spending_stats) / len(spending_stats)

        # Calculate growth rate
        first_day = spending_stats[0]["total_spending"]
        last_day = spending_stats[-1]["total_spending"]
        growth_rate = ((last_day - first_day) / first_day) * 100 if first_day > 0 else 0

        return {
            "total_spending": total_spending,
            "total_transactions": total_transactions,
            "average_ticket": avg_ticket,
            "growth_rate": growth_rate,
            "data_points": len(spending_stats)
        }

    def analyze_demographic_patterns(
        self,
        aoi_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Dict[str, Any]:
        """Analyze demographic patterns including age and gender distributions"""
        demo_stats = self.get_demographic_statistics(aoi_id, start_date, end_date)
        if not demo_stats:
            return {"error": "No data found for the specified parameters"}

        # Calculate average demographics
        avg_male_proportion = sum(row["male_proportion"] for row in demo_stats) / len(demo_stats)
        avg_age_0_18 = sum(row["age_0_18"] for row in demo_stats) / len(demo_stats)
        avg_age_19_35 = sum(row["age_19_35"] for row in demo_stats) / len(demo_stats)
        avg_age_36_60 = sum(row["age_36_60"] for row in demo_stats) / len(demo_stats)
        avg_age_61_plus = sum(row["age_61_plus"] for row in demo_stats) / len(demo_stats)

        return {
            "male_proportion": avg_male_proportion,
            "age_distribution": {
                "0-18": avg_age_0_18,
                "19-35": avg_age_19_35,
                "36-60": avg_age_36_60,
                "61+": avg_age_61_plus
            },
            "data_points": len(demo_stats)
        }

    def analyze_visitor_origins(
        self,
        aoi_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        origin_type: str = "foreign"
    ) -> Dict[str, Any]:
        """Analyze visitor origins including top regions and trends"""
        origin_stats = self.get_origin_statistics(aoi_id, start_date, end_date, origin_type)
        if not origin_stats:
            return {"error": "No data found for the specified parameters"}

        # Group by origin and calculate totals
        origin_totals = {}
        for row in origin_stats:
            origin = row["origin_name"]
            if origin not in origin_totals:
                origin_totals[origin] = 0
            origin_totals[origin] += row["visitor_count"]

        # Sort by total visitors
        sorted_origins = sorted(origin_totals.items(), key=lambda x: x[1], reverse=True)
        top_origins = dict(sorted_origins[:10])  # Top 10 origins

        return {
            "top_origins": top_origins,
            "total_origins": len(origin_totals),
            "data_points": len(origin_stats)
        } 