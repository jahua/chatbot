from typing import Dict, List, Any, Optional
from langchain.tools import Tool
from langchain.memory import ConversationBufferMemory
from app.agents.base_agent import BaseAgent
from app.agents.visualization_agent import VisualizationAgent
from app.core.config import settings
from app.services.geo_insights_service import GeoInsightsService
from app.services.geo_visualization_service import GeoVisualizationService
import logging

logger = logging.getLogger(__name__)

class GeoInsightAgent(BaseAgent):
    def __init__(self, llm, memory: Optional[ConversationBufferMemory] = None):
        # Initialize services
        self.geo_service = GeoInsightsService()
        self.viz_service = GeoVisualizationService()
        self.viz_agent = VisualizationAgent()
        
        # Define geospatial-specific tools
        tools = [
            Tool(
                name="spatial_query",
                func=self._execute_spatial_query,
                description="Execute spatial queries to analyze geographic patterns and relationships"
            ),
            Tool(
                name="location_search",
                func=self._search_locations,
                description="Search for locations and their attributes in the database"
            ),
            Tool(
                name="spatial_analysis",
                func=self._perform_spatial_analysis,
                description="Perform spatial analysis like clustering, hotspot detection, and pattern analysis"
            ),
            Tool(
                name="geographic_visualization",
                func=self._create_geographic_visualization,
                description="Create geographic visualizations like maps, heatmaps, and flow maps"
            ),
            Tool(
                name="region_insights",
                func=self._get_region_insights,
                description="Get detailed insights about a specific region"
            ),
            Tool(
                name="spatial_patterns",
                func=self._analyze_spatial_patterns,
                description="Analyze spatial patterns within a region"
            ),
            Tool(
                name="hotspot_detection",
                func=self._detect_hotspots,
                description="Detect hotspots of activity within a region"
            )
        ]
        
        # Initialize base agent with geospatial tools
        super().__init__(llm=llm, tools=tools, memory=memory)
        
        # Update the system prompt for geospatial focus
        self.prompt = self.prompt.partial(
            system="""You are a geospatial data analyst specializing in tourism and location-based insights.
            You have access to various tools for spatial analysis, geographic visualization, and location-based queries.
            
            Current tools available:
            {tools}
            
            Use these tools to help users understand geographic patterns, spatial relationships, and location-based insights.
            Always explain your spatial reasoning and provide clear, map-based visualizations when appropriate.
            
            When creating visualizations:
            1. Use maps for geographic data
            2. Use heatmaps for density analysis
            3. Use flow maps for movement patterns
            4. Use charts for statistical comparisons
            5. Always include proper legends and labels."""
        )
        
        logger.info("GeoInsightAgent initialized successfully")
    
    async def _execute_spatial_query(self, query: str) -> Dict[str, Any]:
        """Execute a spatial query using PostGIS functions"""
        try:
            # Implementation will use PostGIS functions
            # This is a placeholder for the actual implementation
            return {"success": True, "result": "Spatial query executed"}
        except Exception as e:
            logger.error(f"Error executing spatial query: {str(e)}")
            return {"success": False, "error": str(e)}
    
    async def _search_locations(self, location_query: str) -> Dict[str, Any]:
        """Search for locations and their attributes"""
        try:
            # Implementation will use vector search for locations
            # This is a placeholder for the actual implementation
            return {"success": True, "result": "Location search completed"}
        except Exception as e:
            logger.error(f"Error searching locations: {str(e)}")
            return {"success": False, "error": str(e)}
    
    async def _perform_spatial_analysis(self, analysis_params: Dict[str, Any]) -> Dict[str, Any]:
        """Perform spatial analysis operations"""
        try:
            # Implementation will use spatial analysis functions
            # This is a placeholder for the actual implementation
            return {"success": True, "result": "Spatial analysis completed"}
        except Exception as e:
            logger.error(f"Error performing spatial analysis: {str(e)}")
            return {"success": False, "error": str(e)}
    
    async def _create_geographic_visualization(self, viz_params: Dict[str, Any]) -> Dict[str, Any]:
        """Create geographic visualizations"""
        try:
            viz_type = viz_params.get('type', 'map')
            data = viz_params.get('data', {})
            
            if viz_type == 'map':
                return self.viz_service.create_region_map(data)
            elif viz_type == 'hotspot':
                return self.viz_service.create_hotspot_map(data)
            elif viz_type == 'pattern':
                return self.viz_service.create_spatial_pattern_chart(data)
            elif viz_type == 'comparison':
                return self.viz_service.create_region_comparison(data)
            else:
                # Fall back to standard visualization agent for non-geographic charts
                return await self.viz_agent.generate_visualization(
                    data=data.get('values', []),
                    visualization_type=viz_type,
                    **viz_params.get('options', {})
                )
        except Exception as e:
            logger.error(f"Error creating visualization: {str(e)}")
            return {"success": False, "error": str(e)}
    
    async def _get_region_insights(self, region_id: str) -> Dict[str, Any]:
        """Get detailed insights about a specific region"""
        try:
            insights = await self.geo_service.get_region_insights(region_id)
            return {"success": True, "result": insights}
        except Exception as e:
            logger.error(f"Error getting region insights: {str(e)}")
            return {"success": False, "error": str(e)}
    
    async def _analyze_spatial_patterns(self, region_id: str) -> Dict[str, Any]:
        """Analyze spatial patterns within a region"""
        try:
            patterns = await self.geo_service.get_spatial_patterns(region_id)
            return {"success": True, "result": patterns}
        except Exception as e:
            logger.error(f"Error analyzing spatial patterns: {str(e)}")
            return {"success": False, "error": str(e)}
    
    async def _detect_hotspots(self, region_id: str) -> Dict[str, Any]:
        """Detect hotspots of activity within a region"""
        try:
            hotspots = await self.geo_service.get_hotspots(region_id)
            return {"success": True, "result": hotspots}
        except Exception as e:
            logger.error(f"Error detecting hotspots: {str(e)}")
            return {"success": False, "error": str(e)} 