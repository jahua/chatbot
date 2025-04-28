from typing import List, Dict, Any, Optional
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import func, desc
from app.models.dw_models import FactVisitor, FactSpending, DimRegion, DimDate, DimIndustry
from app.core.config import settings
from app.rag.debug_service import DebugService

class DWContextService:
    def __init__(self, dw_db: Session = None):
        """Initialize the DWContextService with a database session.
        
        Args:
            dw_db: A SQLAlchemy Session object, not a generator function
        """
        # If dw_db is a generator, get the next value
        if hasattr(dw_db, '__next__'):
            try:
                self.dw_db = next(dw_db)
            except StopIteration:
                self.dw_db = None
        else:
            self.dw_db = dw_db
        self.debug_service = DebugService()
    
    def get_visitor_insights(
        self,
        region_id: Optional[int] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """Get visitor insights from DW for RAG context"""
        try:
            self.debug_service.start_step("Visitor Insights Query", {
                "region_id": region_id,
                "start_date": start_date.isoformat() if start_date else None,
                "end_date": end_date.isoformat() if end_date else None
            })
            
            query = self.dw_db.query(
                FactVisitor,
                DimDate.full_date
            ).join(
                DimDate,
                FactVisitor.date_id == DimDate.date_id
            )
            
            if region_id:
                query = query.filter(FactVisitor.region_id == region_id)
            
            if start_date and end_date:
                query = query.filter(
                    DimDate.full_date.between(start_date, end_date)
                )
            
            results = query.order_by(desc(FactVisitor.total_visitors)).limit(10).all()
            
            self.debug_service.add_step_details({
                "result_count": len(results)
            })
            self.debug_service.end_step()
            
            return [self._format_visitor_insight(r) for r in results]
            
        except Exception as e:
            self.debug_service.end_step(error=e)
            raise
    
    def get_spending_insights(
        self,
        region_id: Optional[int] = None,
        industry_id: Optional[int] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """Get spending insights from DW for RAG context"""
        try:
            self.debug_service.start_step("Spending Insights Query", {
                "region_id": region_id,
                "industry_id": industry_id,
                "start_date": start_date.isoformat() if start_date else None,
                "end_date": end_date.isoformat() if end_date else None
            })
            
            query = self.dw_db.query(
                FactSpending,
                DimDate.date,
                DimIndustry.industry_name
            ).join(
                DimDate,
                FactSpending.date_id == DimDate.date_id
            ).join(
                DimIndustry,
                FactSpending.industry_id == DimIndustry.industry_id
            )
            
            if region_id:
                query = query.filter(FactSpending.region_id == region_id)
            
            if industry_id:
                query = query.filter(FactSpending.industry_id == industry_id)
            
            if start_date and end_date:
                query = query.filter(
                    DimDate.date.between(start_date, end_date)
                )
            
            results = query.order_by(desc(FactSpending.total_spending)).limit(10).all()
            
            self.debug_service.add_step_details({
                "result_count": len(results)
            })
            self.debug_service.end_step()
            
            return [self._format_spending_insight(r) for r in results]
            
        except Exception as e:
            self.debug_service.end_step(error=e)
            raise
    
    def get_region_metrics(self, region_name: str) -> Dict[str, Any]:
        """Get comprehensive metrics for a region"""
        try:
            self.debug_service.start_step("Region Metrics Query", {"region_name": region_name})
            
            region = self.dw_db.query(DimRegion).filter(
                DimRegion.region_name == region_name
            ).first()
            
            if not region:
                self.debug_service.end_step()
                return {}
            
            # Get visitor metrics
            visitor_metrics = self.dw_db.query(
                func.avg(FactVisitor.total_visitors).label('avg_visitors'),
                func.max(FactVisitor.total_visitors).label('max_visitors'),
                func.min(FactVisitor.total_visitors).label('min_visitors')
            ).filter(
                FactVisitor.region_id == region.region_id
            ).first()
            
            # Get spending metrics
            spending_metrics = self.dw_db.query(
                func.avg(FactSpending.total_spending).label('avg_spending'),
                func.max(FactSpending.total_spending).label('max_spending'),
                func.min(FactSpending.total_spending).label('min_spending')
            ).filter(
                FactSpending.region_id == region.region_id
            ).first()
            
            self.debug_service.add_step_details({
                "region_found": True,
                "region_id": region.region_id
            })
            self.debug_service.end_step()
            
            return {
                'region_name': region.region_name,
                'region_type': region.region_type,
                'visitor_metrics': {
                    'average_visitors': float(visitor_metrics.avg_visitors) if visitor_metrics.avg_visitors else 0,
                    'max_visitors': float(visitor_metrics.max_visitors) if visitor_metrics.max_visitors else 0,
                    'min_visitors': float(visitor_metrics.min_visitors) if visitor_metrics.min_visitors else 0
                },
                'spending_metrics': {
                    'average_spending': float(spending_metrics.avg_spending) if spending_metrics.avg_spending else 0,
                    'max_spending': float(spending_metrics.max_spending) if spending_metrics.max_spending else 0,
                    'min_spending': float(spending_metrics.min_spending) if spending_metrics.min_spending else 0
                }
            }
            
        except Exception as e:
            self.debug_service.end_step(error=e)
            raise
    
    def get_visitor_demographics(self, region_id: int) -> Dict[str, Any]:
        """Get visitor demographics for a region"""
        result = self.dw_db.query(FactVisitor.demographics).filter(
            FactVisitor.region_id == region_id
        ).first()
        
        return result[0] if result else {}
    
    def _format_visitor_insight(self, record: tuple) -> Dict[str, Any]:
        """Format visitor record for RAG context"""
        try:
            self.debug_service.start_step("Format Visitor Insight")
            
            fact_visitor, date = record
            insight = {
                'date': date.isoformat(),
                'date_id': fact_visitor.date_id,
                'region_id': fact_visitor.region_id,
                'total_visitors': float(fact_visitor.total_visitors) if fact_visitor.total_visitors else 0,
                'visitor_types': {
                    'swiss_tourists': float(fact_visitor.swiss_tourists) if fact_visitor.swiss_tourists else 0,
                    'foreign_tourists': float(fact_visitor.foreign_tourists) if fact_visitor.foreign_tourists else 0,
                    'swiss_locals': float(fact_visitor.swiss_locals) if fact_visitor.swiss_locals else 0,
                    'foreign_workers': float(fact_visitor.foreign_workers) if fact_visitor.foreign_workers else 0,
                    'swiss_commuters': float(fact_visitor.swiss_commuters) if fact_visitor.swiss_commuters else 0
                },
                'demographics': fact_visitor.demographics,
                'dwell_time': fact_visitor.dwell_time,
                'top_foreign_countries': fact_visitor.top_foreign_countries,
                'top_swiss_cantons': fact_visitor.top_swiss_cantons,
                'top_municipalities': fact_visitor.top_municipalities
            }
            
            self.debug_service.end_step()
            return insight
            
        except Exception as e:
            self.debug_service.end_step(error=e)
            raise
    
    def _format_spending_insight(self, record: tuple) -> Dict[str, Any]:
        """Format spending record for RAG context"""
        try:
            self.debug_service.start_step("Format Spending Insight")
            
            fact_spending, date, industry_name = record
            insight = {
                'date': date.isoformat(),
                'date_id': fact_spending.date_id,
                'region_id': fact_spending.region_id,
                'industry_id': fact_spending.industry_id,
                'industry_name': industry_name,
                'total_spending': float(fact_spending.total_spending) if fact_spending.total_spending else 0,
                'avg_transaction': float(fact_spending.avg_transaction) if fact_spending.avg_transaction else 0,
                'geo_latitude': float(fact_spending.geo_latitude) if fact_spending.geo_latitude else None,
                'geo_longitude': float(fact_spending.geo_longitude) if fact_spending.geo_longitude else None
            }
            
            self.debug_service.end_step()
            return insight
            
        except Exception as e:
            self.debug_service.end_step(error=e)
            raise
    
    async def get_dw_context(
        self,
        query: str,
        region_id: Optional[int] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """Get intelligent context from DW schema"""
        try:
            self.debug_service.start_step("DW Context Generation", {
                "query": query,
                "region_id": region_id,
                "start_date": start_date.isoformat() if start_date else None,
                "end_date": end_date.isoformat() if end_date else None
            })
            
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
                    'visitor_insights': self.get_visitor_insights(region_id, start_date, end_date),
                    'trends': self._get_region_trends(region_id),
                    'demographics': self.get_visitor_demographics(region_id)
                })
            
            # Generate intelligent prompt based on context
            context['intelligent_prompt'] = self._generate_intelligent_prompt(query, context)
            
            self.debug_service.end_step()
            return context
            
        except Exception as e:
            self.debug_service.end_step(error=e)
            raise

    def _get_schema_info(self) -> Dict[str, Any]:
        """Get information about DW schema structure"""
        try:
            self.debug_service.start_step("Get Schema Info")
            
            # Get fact tables
            fact_tables = [
                'fact_visitor',
                'fact_spending'
            ]
            
            # Get dimension tables
            dimension_tables = [
                'dim_region',
                'dim_date',
                'dim_industry'
            ]
            
            # Get key metrics from fact tables
            key_metrics = [
                'total_visitors',
                'swiss_tourists',
                'foreign_tourists',
                'swiss_locals',
                'foreign_workers',
                'swiss_commuters',
                'total_spending',
                'avg_transaction'
            ]
            
            # Get JSON metrics
            json_metrics = [
                'demographics',
                'dwell_time',
                'top_foreign_countries',
                'top_swiss_cantons',
                'top_municipalities'
            ]
            
            schema_info = {
                'fact_tables': fact_tables,
                'dimension_tables': dimension_tables,
                'key_metrics': key_metrics,
                'json_metrics': json_metrics
            }
            
            self.debug_service.add_step_details(schema_info)
            self.debug_service.end_step()
            
            return schema_info
            
        except Exception as e:
            self.debug_service.end_step(error=e)
            return {
                'fact_tables': [],
                'dimension_tables': [],
                'key_metrics': [],
                'json_metrics': []
            }

    def _get_available_regions(self) -> List[Dict[str, Any]]:
        """Get list of available regions with their types"""
        regions = self.dw_db.query(
            DimRegion.region_id,
            DimRegion.region_name,
            DimRegion.region_type
        ).all()
        
        return [
            {
                'id': r.region_id,
                'name': r.region_name,
                'type': r.region_type
            }
            for r in regions
        ]

    def _get_date_range(self) -> Dict[str, Any]:
        """Get available date range in the DW"""
        date_range = self.dw_db.query(
            func.min(DimDate.full_date).label('min_date'),
            func.max(DimDate.full_date).label('max_date')
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
                'type': region.region_type
            },
            'visitor_stats': {
                'average_visitors': float(stats.avg_visitors) if stats.avg_visitors else 0,
                'max_visitors': float(stats.max_visitors) if stats.max_visitors else 0,
                'min_visitors': float(stats.min_visitors) if stats.min_visitors else 0
            }
        }

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
            prompt += f"Region: {region_info.get('name')} ({region_info.get('type')})\n\n"
        
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
        
    def get_highest_spending_industry(
        self,
        region_id: Optional[int] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """Get the industry with the highest total spending"""
        try:
            self.debug_service.start_step("Highest Spending Industry Query", {
                "region_id": region_id,
                "start_date": start_date.isoformat() if start_date else None,
                "end_date": end_date.isoformat() if end_date else None
            })
            
            # Build a query to sum total_spending by industry
            query = self.dw_db.query(
                DimIndustry.industry_id,
                DimIndustry.industry_name,
                func.sum(FactSpending.total_spending).label('total_industry_spending')
            ).join(
                FactSpending,
                FactSpending.industry_id == DimIndustry.industry_id
            ).join(
                DimDate,
                FactSpending.date_id == DimDate.date_id
            )
            
            # Apply filters if provided
            if region_id:
                query = query.filter(FactSpending.region_id == region_id)
            
            if start_date and end_date:
                query = query.filter(
                    DimDate.date.between(start_date, end_date)
                )
            
            # Group by industry and order by total spending in descending order
            result = query.group_by(
                DimIndustry.industry_id, 
                DimIndustry.industry_name
            ).order_by(
                desc('total_industry_spending')
            ).first()
            
            if not result:
                self.debug_service.add_step_details({
                    "result": "No spending data found for the given parameters"
                })
                self.debug_service.end_step()
                return {
                    "found": False,
                    "message": "No spending data found"
                }
            
            # Format the result
            highest_spending = {
                "found": True,
                "industry_id": result.industry_id,
                "industry_name": result.industry_name,
                "total_spending": float(result.total_industry_spending) if result.total_industry_spending else 0
            }
            
            # Get all industries and their spending for comparison
            all_industries = query.group_by(
                DimIndustry.industry_id, 
                DimIndustry.industry_name
            ).order_by(
                desc('total_industry_spending')
            ).limit(10).all()
            
            highest_spending["all_top_industries"] = [
                {
                    "industry_id": ind.industry_id,
                    "industry_name": ind.industry_name,
                    "total_spending": float(ind.total_industry_spending) if ind.total_industry_spending else 0
                }
                for ind in all_industries
            ]
            
            self.debug_service.add_step_details({
                "result": highest_spending
            })
            self.debug_service.end_step()
            
            return highest_spending
            
        except Exception as e:
            self.debug_service.end_step(error=e)
            raise 