from typing import Any, Dict, List, Optional
from fastapi import Depends
from sqlalchemy.orm import Session
from datetime import datetime
from sqlalchemy import func, desc

from app.db.database import get_dw_db
from app.rag.dw_context_service import DWContextService
from app.models.dw_models import FactVisitor, DimDate, DimRegion

class DWAnalyticsAgent:
    def __init__(
        self,
        dw_db: Session = Depends(get_dw_db),
        dw_context_service: Optional[DWContextService] = None
    ):
        self.dw_db = dw_db
        self.dw_context_service = dw_context_service or DWContextService(dw_db=dw_db)

    async def process_query(
        self,
        query: str,
        region_id: Optional[int] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """Process query with DW context and generate analysis"""
        # Get DW context
        context = await self.dw_context_service.get_dw_context(
            query,
            region_id=region_id,
            start_date=start_date,
            end_date=end_date
        )
        
        # Generate analysis based on context
        analysis = {
            'query': query,
            'context': context,
            'analysis': self._generate_analysis(context)
        }
        
        return analysis

    def _generate_analysis(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Generate comprehensive analysis based on DW context"""
        analysis = {
            'schema_overview': self._analyze_schema(context),
            'region_analysis': self._analyze_regions(context),
            'temporal_analysis': self._analyze_temporal_patterns(context),
            'demographic_analysis': self._analyze_demographics(context),
            'recommendations': self._generate_recommendations(context)
        }
        
        return analysis

    def _analyze_schema(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze DW schema structure and metrics"""
        schema_info = context.get('schema_info', {})
        
        return {
            'fact_tables': len(schema_info.get('fact_tables', [])),
            'dimension_tables': len(schema_info.get('dimension_tables', [])),
            'key_metrics': len(schema_info.get('key_metrics', [])),
            'json_metrics': len(schema_info.get('json_metrics', [])),
            'data_completeness': self._assess_data_completeness(context)
        }

    def _analyze_regions(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze region data and relationships"""
        regions = context.get('available_regions', [])
        region_context = context.get('region_context', {})
        
        return {
            'total_regions': len(regions),
            'region_types': self._count_region_types(regions),
            'current_region': region_context.get('region_info', {}),
            'visitor_statistics': region_context.get('visitor_stats', {})
        }

    def _analyze_temporal_patterns(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze temporal patterns in visitor data"""
        trends = context.get('trends', {})
        date_range = context.get('date_range', {})
        
        return {
            'date_range': date_range,
            'growth_rate': trends.get('growth_rate', 0),
            'seasonality': trends.get('seasonality', {}),
            'trend_direction': self._determine_trend_direction(trends)
        }

    def _analyze_demographics(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze demographic patterns and changes"""
        demographics = context.get('demographics', {})
        
        return {
            'demographic_metrics': len(demographics),
            'trends': {
                key: value.get('trend', 'stable')
                for key, value in demographics.items()
            },
            'averages': {
                key: value.get('average', 0)
                for key, value in demographics.items()
            }
        }

    def _generate_recommendations(self, context: Dict[str, Any]) -> List[str]:
        """Generate data-driven recommendations"""
        recommendations = []
        
        # Analyze trends and patterns
        trends = context.get('trends', {})
        if trends.get('growth_rate', 0) < -5:
            recommendations.append("Consider implementing promotional campaigns to boost visitor numbers")
        
        seasonality = trends.get('seasonality', {})
        if seasonality:
            peak_month = seasonality.get('peak_season')
            off_month = seasonality.get('off_season')
            recommendations.append(
                f"Focus marketing efforts during off-season (month {off_month}) "
                f"to balance visitor distribution"
            )
        
        # Analyze demographics
        demographics = context.get('demographics', {})
        for key, value in demographics.items():
            if value.get('trend') == 'decreasing':
                recommendations.append(
                    f"Investigate reasons for decreasing {key} and develop "
                    "targeted strategies to address this trend"
                )
        
        return recommendations

    def _count_region_types(self, regions: List[Dict[str, Any]]) -> Dict[str, int]:
        """Count regions by type"""
        type_counts = {}
        for region in regions:
            region_type = region.get('type', 'unknown')
            type_counts[region_type] = type_counts.get(region_type, 0) + 1
        return type_counts

    def _determine_trend_direction(self, trends: Dict[str, Any]) -> str:
        """Determine overall trend direction"""
        growth_rate = trends.get('growth_rate', 0)
        if growth_rate > 5:
            return "strongly increasing"
        elif growth_rate > 0:
            return "slightly increasing"
        elif growth_rate < -5:
            return "strongly decreasing"
        elif growth_rate < 0:
            return "slightly decreasing"
        return "stable"

    def _assess_data_completeness(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Assess completeness of DW data"""
        return {
            'fact_tables': self._check_table_completeness('fact_visitor'),
            'dimension_tables': {
                'dim_region': self._check_table_completeness('dim_region'),
                'dim_date': self._check_table_completeness('dim_date')
            }
        }

    def _check_table_completeness(self, table_name: str) -> Dict[str, Any]:
        """Check completeness of a specific table"""
        try:
            count = self.dw_db.query(func.count()).select_from(
                getattr(FactVisitor if table_name == 'fact_visitor' else
                       DimRegion if table_name == 'dim_region' else
                       DimDate, 'id')
            ).scalar()
            
            return {
                'has_data': count > 0,
                'record_count': count
            }
        except Exception:
            return {
                'has_data': False,
                'record_count': 0
            } 