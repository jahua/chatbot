from typing import Dict, List, Any, Optional
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from langchain.tools import Tool
from .base_agent import BaseAgent
from app.llm.openai_adapter import openai_adapter
from langchain.llms.base import BaseLLM
from langchain.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.pydantic_v1 import BaseModel, Field
from app.core.langsmith_config import get_traceable_decorator

class VisualizationInput(BaseModel):
    data: List[Dict[str, Any]] = Field(description="The data to visualize")
    visualization_type: str = Field(description="Type of visualization to create")
    x_col: Optional[str] = Field(description="Column to use for x-axis")
    y_col: Optional[str] = Field(description="Column to use for y-axis")
    color_col: Optional[str] = Field(description="Column to use for color")
    title: Optional[str] = Field(description="Title of the visualization")

class VisualizationOutput(BaseModel):
    success: bool = Field(description="Whether visualization was successful")
    visualization: Optional[Dict[str, Any]] = Field(description="The generated visualization")
    error: Optional[str] = Field(description="Error message if visualization failed")

class VisualizationAgent(BaseAgent):
    def __init__(self, model_name: str = "openai"):
        self.model_name = model_name
        self.llm = self._get_llm()
        self.traceable = get_traceable_decorator()

        # Define visualization-specific tools
        tools = [
            Tool(
                name="create_line_chart",
                func=self._create_line_chart,
                description="Create a line chart from time series data"
            ),
            Tool(
                name="create_bar_chart",
                func=self._create_bar_chart,
                description="Create a bar chart from categorical data"
            ),
            Tool(
                name="create_pie_chart",
                func=self._create_pie_chart,
                description="Create a pie chart from percentage data"
            ),
            Tool(
                name="create_scatter_plot",
                func=self._create_scatter_plot,
                description="Create a scatter plot from numerical data"
            ),
            Tool(
                name="create_heatmap",
                func=self._create_heatmap,
                description="Create a heatmap from matrix data"
            )
        ]
        
        super().__init__(llm=self.llm, tools=tools)
    
    def _get_llm(self) -> BaseLLM:
        """Get the appropriate LLM based on model name"""
        if self.model_name == "openai":
            from app.llm.openai_adapter import OpenAIAdapter
            return OpenAIAdapter()
        elif self.model_name == "gemini":
            from app.llm.gemini_adapter import GeminiAdapter
            return GeminiAdapter()
        elif self.model_name == "ollama":
            from app.llm.ollama_adapter import OllamaAdapter
            return OllamaAdapter()
        elif self.model_name == "vanna":
            from app.llm.vanna_adapter import VannaAdapter
            return VannaAdapter()
        else:
            raise ValueError(f"Unsupported model: {self.model_name}")
    
    def _create_line_chart(self, data: List[Dict[str, Any]], x_col: str, y_col: str, 
                          title: str = None) -> Dict[str, Any]:
        """Create a line chart from time series data"""
        df = pd.DataFrame(data)
        fig = px.line(df, x=x_col, y=y_col, title=title)
        return {
            "type": "line",
            "figure": fig.to_json(),
            "data": data
        }
    
    def _create_bar_chart(self, data: List[Dict[str, Any]], x_col: str, y_col: str,
                         title: str = None) -> Dict[str, Any]:
        """Create a bar chart from categorical data"""
        df = pd.DataFrame(data)
        fig = px.bar(df, x=x_col, y=y_col, title=title)
        return {
            "type": "bar",
            "figure": fig.to_json(),
            "data": data
        }
    
    def _create_pie_chart(self, data: List[Dict[str, Any]], names_col: str, values_col: str,
                         title: str = None) -> Dict[str, Any]:
        """Create a pie chart from percentage data"""
        df = pd.DataFrame(data)
        fig = px.pie(df, names=names_col, values=values_col, title=title)
        return {
            "type": "pie",
            "figure": fig.to_json(),
            "data": data
        }
    
    def _create_scatter_plot(self, data: List[Dict[str, Any]], x_col: str, y_col: str,
                           color_col: str = None, title: str = None) -> Dict[str, Any]:
        """Create a scatter plot from numerical data"""
        df = pd.DataFrame(data)
        fig = px.scatter(df, x=x_col, y=y_col, color=color_col, title=title)
        return {
            "type": "scatter",
            "figure": fig.to_json(),
            "data": data
        }
    
    def _create_heatmap(self, data: List[Dict[str, Any]], x_col: str, y_col: str, values_col: str,
                       title: str = None) -> Dict[str, Any]:
        """Create a heatmap from matrix data"""
        df = pd.DataFrame(data)
        pivot_df = df.pivot(index=y_col, columns=x_col, values=values_col)
        fig = px.imshow(pivot_df, title=title)
        return {
            "type": "heatmap",
            "figure": fig.to_json(),
            "data": data
        }
    
    @traceable
    async def generate_visualization(self, data: List[Dict[str, Any]], 
                                   visualization_type: str, **kwargs) -> Dict[str, Any]:
        """Generate visualization based on data and type"""
        try:
            # Convert data to DataFrame
            df = pd.DataFrame(data)

            # Create visualization based on type
            if visualization_type == "line":
                fig = px.line(
                    df,
                    x=kwargs.get("x_col"),
                    y=kwargs.get("y_col"),
                    title=kwargs.get("title")
                )
            elif visualization_type == "bar":
                fig = px.bar(
                    df,
                    x=kwargs.get("x_col"),
                    y=kwargs.get("y_col"),
                    title=kwargs.get("title")
                )
            elif visualization_type == "pie":
                fig = px.pie(
                    df,
                    names=kwargs.get("names_col"),
                    values=kwargs.get("values_col"),
                    title=kwargs.get("title")
                )
            elif visualization_type == "scatter":
                fig = px.scatter(
                    df,
                    x=kwargs.get("x_col"),
                    y=kwargs.get("y_col"),
                    color=kwargs.get("color_col"),
                    title=kwargs.get("title")
                )
            elif visualization_type == "heatmap":
                fig = px.imshow(
                    df.pivot(
                        index=kwargs.get("y_col"),
                        columns=kwargs.get("x_col"),
                        values=kwargs.get("values_col")
                    ),
                    title=kwargs.get("title")
                )
            else:
                raise ValueError(f"Unsupported visualization type: {visualization_type}")

            # Convert to JSON for frontend
            return VisualizationOutput(
                success=True,
                visualization={
                    "figure": fig.to_json(),
                    "type": visualization_type
                }
            ).dict()
            
        except Exception as e:
            return VisualizationOutput(
                success=False,
                error=str(e)
            ).dict() 