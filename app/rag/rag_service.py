from typing import Any, Dict, List, Optional
from fastapi import Depends
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
from sqlalchemy import func, desc

from app.db.database import get_dw_db
from app.models.dw_models import FactVisitor, DimDate, DimRegion
from app.rag.dw_context_service import DWContextService
from app.rag.debug_service import DebugService
from app.core.config import settings

class DWContextService:
    def __init__(self, dw_db: Session):
        self.dw_db = dw_db
    
    async def get_dw_context(
        self,
        query: str,
        region_id: Optional[int] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """Get intelligent context from DW schema"""
        # Get base context about regions and dates
        context = {
            'schema_info': self._get_schema_info(),
            'available_regions': self._get_available_regions(),
            'date_range': self._get_date_range()
        }
        
        # Add region-specific context if provided
        if region_id:
            context.update({
                'region_context': self._get_region_context(region_id),
                'visitor_insights': self._get_visitor_insights(region_id, start_date, end_date),
                'trends': self._get_region_trends(region_id),
                'demographics': self._get_region_demographics(region_id)
            })
        
        # Generate intelligent prompt based on context
        context['intelligent_prompt'] = self._generate_intelligent_prompt(query, context)
        
        return context

    def _get_schema_info(self) -> Dict[str, Any]:
        """Get information about DW schema structure"""
        return {
            'fact_tables': ['fact_visitor'],
            'dimension_tables': ['dim_region', 'dim_date'],
            'key_metrics': [
                'total_visitors',
                'swiss_tourists',
                'foreign_tourists',
                'swiss_locals',
                'foreign_workers',
                'swiss_commuters'
            ],
            'json_metrics': [
                'demographics',
                'dwell_time',
                'top_foreign_countries',
                'top_swiss_cantons',
                'top_municipalities',
                'top_last_cantons',
                'top_last_municipalities'
            ],
            'metadata': [
                'aoi_id',
                'load_date',
                'ingestion_timestamp',
                'raw_content'
            ]
        }

    def _get_available_regions(self) -> List[Dict[str, Any]]:
        """Get list of available regions with their types"""
        regions = self.dw_db.query(
            DimRegion.region_id,
            DimRegion.region_name,
            DimRegion.region_type,
            DimRegion.region_name_de,
            DimRegion.region_name_fr,
            DimRegion.region_name_it
        ).all()
        
        return [
            {
                'id': r.region_id,
                'name': r.region_name,
                'type': r.region_type,
                'name_de': r.region_name_de,
                'name_fr': r.region_name_fr,
                'name_it': r.region_name_it
            }
            for r in regions
        ]

    def _get_date_range(self) -> Dict[str, Any]:
        """Get available date range in the DW"""
        date_range = self.dw_db.query(
            func.min(DimDate.date).label('min_date'),
            func.max(DimDate.date).label('max_date')
        ).first()
        
        return {
            'min_date': date_range.min_date.isoformat() if date_range.min_date else None,
            'max_date': date_range.max_date.isoformat() if date_range.max_date else None
        }

    def _get_region_context(self, region_id: int) -> Dict[str, Any]:
        """Get detailed context about a specific region"""
        region = self.dw_db.query(DimRegion).filter(
            DimRegion.region_id == region_id
        ).first()
        
        if not region:
            return {}
        
        # Get visitor statistics
        stats = self.dw_db.query(
            func.avg(FactVisitor.total_visitors).label('avg_visitors'),
            func.max(FactVisitor.total_visitors).label('max_visitors'),
            func.min(FactVisitor.total_visitors).label('min_visitors')
        ).filter(
            FactVisitor.region_id == region_id
        ).first()
        
        return {
            'region_info': {
                'name': region.region_name,
                'type': region.region_type,
                'name_de': region.region_name_de,
                'name_fr': region.region_name_fr,
                'name_it': region.region_name_it,
                'parent_id': region.parent_region_id
            },
            'visitor_stats': {
                'average_visitors': float(stats.avg_visitors) if stats.avg_visitors else 0,
                'max_visitors': float(stats.max_visitors) if stats.max_visitors else 0,
                'min_visitors': float(stats.min_visitors) if stats.min_visitors else 0
            }
        }

    def _get_visitor_insights(
        self,
        region_id: int,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """Get detailed visitor insights for a region"""
        query = self.dw_db.query(FactVisitor).filter(
            FactVisitor.region_id == region_id
        )
        
        if start_date and end_date:
            query = query.join(DimDate).filter(
                DimDate.date.between(start_date, end_date)
            )
        
        insights = query.order_by(desc(FactVisitor.date_id)).limit(12).all()
        
        return [
            {
                'date_id': insight.date_id,
                'total_visitors': float(insight.total_visitors) if insight.total_visitors else 0,
                'visitor_types': {
                    'swiss_tourists': float(insight.swiss_tourists) if insight.swiss_tourists else 0,
                    'foreign_tourists': float(insight.foreign_tourists) if insight.foreign_tourists else 0,
                    'swiss_locals': float(insight.swiss_locals) if insight.swiss_locals else 0,
                    'foreign_workers': float(insight.foreign_workers) if insight.foreign_workers else 0,
                    'swiss_commuters': float(insight.swiss_commuters) if insight.swiss_commuters else 0
                },
                'demographics': insight.demographics,
                'dwell_time': insight.dwell_time,
                'top_foreign_countries': insight.top_foreign_countries,
                'top_swiss_cantons': insight.top_swiss_cantons,
                'top_municipalities': insight.top_municipalities,
                'top_last_cantons': insight.top_last_cantons,
                'top_last_municipalities': insight.top_last_municipalities,
                'overnights_from_yesterday': insight.overnights_from_yesterday
            }
            for insight in insights
        ]

    def _get_region_trends(self, region_id: int) -> Dict[str, Any]:
        """Get trend analysis for a region"""
        # Get last 12 months of data
        insights = self.dw_db.query(FactVisitor).filter(
            FactVisitor.region_id == region_id
        ).order_by(desc(FactVisitor.date_id)).limit(12).all()
        
        if not insights:
            return {}
        
        total_visitors = [
            float(insight.total_visitors) if insight.total_visitors else 0
            for insight in insights
        ]
        
        return {
            'monthly_trends': total_visitors,
            'growth_rate': self._calculate_growth_rate(total_visitors),
            'seasonality': self._analyze_seasonality(total_visitors)
        }

    def _get_region_demographics(self, region_id: int) -> Dict[str, Any]:
        """Get demographic analysis for a region"""
        insights = self.dw_db.query(FactVisitor).filter(
            FactVisitor.region_id == region_id
        ).order_by(desc(FactVisitor.date_id)).limit(12).all()
        
        if not insights:
            return {}
        
        demographics = {}
        for insight in insights:
            if insight.demographics:
                for key, value in insight.demographics.items():
                    if key not in demographics:
                        demographics[key] = []
                    demographics[key].append(float(value) if value else 0)
        
        return {
            key: {
                'average': sum(values) / len(values),
                'trend': self._calculate_trend(values)
            }
            for key, values in demographics.items()
        }

    def _generate_intelligent_prompt(self, query: str, context: Dict[str, Any]) -> str:
        """Generate an intelligent prompt based on context and query"""
        prompt = f"Context for query: {query}\n\n"
        
        # Add schema information
        schema_info = context.get('schema_info', {})
        prompt += "Available data:\n"
        prompt += f"- Fact tables: {', '.join(schema_info.get('fact_tables', []))}\n"
        prompt += f"- Dimension tables: {', '.join(schema_info.get('dimension_tables', []))}\n"
        prompt += f"- Key metrics: {', '.join(schema_info.get('key_metrics', []))}\n"
        prompt += f"- JSON metrics: {', '.join(schema_info.get('json_metrics', []))}\n\n"
        
        # Add region information if available
        if 'region_context' in context:
            region_info = context['region_context'].get('region_info', {})
            prompt += f"Region: {region_info.get('name')} ({region_info.get('type')})\n"
            prompt += f"Available in languages: DE: {region_info.get('name_de')}, FR: {region_info.get('name_fr')}, IT: {region_info.get('name_it')}\n\n"
        
        # Add date range information
        date_range = context.get('date_range', {})
        prompt += f"Data available from {date_range.get('min_date')} to {date_range.get('max_date')}\n\n"
        
        # Add analysis considerations
        prompt += "Consider the following in your analysis:\n"
        prompt += "- Use appropriate visitor type metrics based on the query\n"
        prompt += "- Consider seasonal patterns in the data\n"
        prompt += "- Analyze demographic trends when relevant\n"
        prompt += "- Use geographic distribution data when available\n"
        
        return prompt

    def _calculate_growth_rate(self, values: List[float]) -> float:
        """Calculate growth rate between first and last value"""
        if not values or len(values) < 2:
            return 0.0
        return ((values[-1] / values[0]) - 1) * 100

    def _calculate_trend(self, values: List[float]) -> str:
        """Calculate trend direction"""
        if not values or len(values) < 2:
            return "stable"
        
        growth_rate = self._calculate_growth_rate(values)
        if growth_rate > 5:
            return "increasing"
        elif growth_rate < -5:
            return "decreasing"
        else:
            return "stable"

    def _analyze_seasonality(self, values: List[float]) -> Dict[str, Any]:
        """Analyze seasonality in the data"""
        if not values or len(values) < 12:
            return {"has_seasonality": False}
        
        # Calculate monthly averages
        monthly_avg = sum(values) / len(values)
        monthly_std = (sum((x - monthly_avg) ** 2 for x in values) / len(values)) ** 0.5
        
        # Check for significant seasonality
        has_seasonality = monthly_std > (monthly_avg * 0.2)  # 20% variation threshold
        
        return {
            "has_seasonality": has_seasonality,
            "monthly_average": monthly_avg,
            "monthly_std": monthly_std
        }

class RAGService:
    def __init__(self, dw_context_service: DWContextService):
        self.dw_context_service = dw_context_service
        self.debug_service = DebugService()
    
    def get_context_for_query(self, query: str) -> Dict[str, Any]:
        """Get relevant context from DW for a given query"""
        try:
            self.debug_service.start_step("Context Extraction", {"query": query})
            
            context = {
                'visitor_insights': [],
                'spending_insights': [],
                'region_metrics': {}
            }
            
            # Extract date range from query
            start_date, end_date = self._extract_date_range(query)
            self.debug_service.add_step_details({
                "date_range": {
                    "start": start_date.isoformat() if start_date else None,
                    "end": end_date.isoformat() if end_date else None
                }
            })
            
            # Extract region from query
            region_name = self._extract_region(query)
            if region_name:
                self.debug_service.add_step_details({"region": region_name})
                context['region_metrics'] = self.dw_context_service.get_region_metrics(region_name)
            
            # Get visitor insights
            self.debug_service.start_step("Visitor Insights Retrieval")
            context['visitor_insights'] = self.dw_context_service.get_visitor_insights(
                start_date=start_date,
                end_date=end_date
            )
            self.debug_service.add_step_details({
                "visitor_insights_count": len(context['visitor_insights'])
            })
            self.debug_service.end_step()
            
            # Get spending insights
            self.debug_service.start_step("Spending Insights Retrieval")
            context['spending_insights'] = self.dw_context_service.get_spending_insights(
                start_date=start_date,
                end_date=end_date
            )
            self.debug_service.add_step_details({
                "spending_insights_count": len(context['spending_insights'])
            })
            self.debug_service.end_step()
            
            self.debug_service.end_step()
            self.debug_service.log_flow_summary()
            
            return context
            
        except Exception as e:
            self.debug_service.end_step(error=e)
            self.debug_service.log_flow_summary()
            raise
    
    def _extract_date_range(self, query: str) -> tuple[Optional[datetime], Optional[datetime]]:
        """Extract date range from query"""
        try:
            self.debug_service.start_step("Date Range Extraction", {"query": query})
            
            # Default to last year if no specific date mentioned
            end_date = datetime.now()
            start_date = end_date - timedelta(days=365)
            
            # TODO: Implement more sophisticated date extraction
            # For now, return default range
            self.debug_service.end_step()
            return start_date, end_date
            
        except Exception as e:
            self.debug_service.end_step(error=e)
            raise
    
    def _extract_region(self, query: str) -> Optional[str]:
        """Extract region name from query"""
        try:
            self.debug_service.start_step("Region Extraction", {"query": query})
            
            # TODO: Implement region name extraction
            # For now, return None to get all regions
            self.debug_service.end_step()
            return None
            
        except Exception as e:
            self.debug_service.end_step(error=e)
            raise 