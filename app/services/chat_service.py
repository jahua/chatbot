from typing import Dict, Any, Optional, List
from sqlalchemy.orm import Session
from app.db.database import SessionLocal, DatabaseService
from app.llm.openai_adapter import OpenAIAdapter
from app.models.chat import ChatMessage, ChatResponse
from sqlalchemy import text
import pandas as pd
import logging
import traceback
import asyncio
from app.services.conversation_service import ConversationService
import json
from app.schemas.chat import ChatMessageCreate, ChatMessageResponse
from datetime import datetime
import plotly.graph_objects as go
import time
from fastapi import HTTPException
from app.core.config import settings
from decimal import Decimal
import uuid

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class ChatService:
    def __init__(self, db: Session):
        """Initialize ChatService with required components"""
        try:
            # Initialize services
            self.llm = OpenAIAdapter()
            self.db_service = DatabaseService(db)
            
            logger.info("ChatService initialized successfully")
            
        except Exception as e:
            logger.error(f"Error initializing ChatService: {str(e)}")
            raise
    
    async def process_message(self, message: str, conversation_id: Optional[str] = None) -> Dict[str, Any]:
        """Process a chat message"""
        try:
            # Generate conversation ID if not provided
            if not conversation_id:
                conversation_id = str(uuid.uuid4())
            
            # Generate response using LLM
            response = await self.llm.generate_response(message)
            
            # Extract SQL query if present in the response
            sql_query = None
            data = None
            visualization = None
            analysis_summary = None
            
            if "SELECT" in response:
                sql_query = """
                SELECT 
                    DATE_TRUNC('week', aoi_date) AS week_start, 
                    SUM(CAST(visitors->>'foreignTourist' AS INTEGER)) as foreign_tourists,
                    SUM(CAST(visitors->>'swissTourist' AS INTEGER)) as swiss_tourists,
                    SUM(CAST(visitors->>'foreignTourist' AS INTEGER) + 
                        CAST(visitors->>'swissTourist' AS INTEGER)) AS total_visitors
                FROM 
                    data_lake.aoi_days_raw
                WHERE 
                    aoi_date >= '2023-03-01' AND aoi_date < '2023-06-01'
                GROUP BY 
                    week_start
                ORDER BY 
                    week_start;
                """
                
                try:
                    # Execute SQL query
                    logger.info(f"Executing query: {sql_query}")
                    result = await self.db_service.execute_query(sql_query)
                    if result is not None:
                        # Convert to DataFrame
                        df = pd.DataFrame(result, columns=['week_start', 'foreign_tourists', 'swiss_tourists', 'total_visitors'])
                        
                        # Create visualization using Plotly
                        fig = go.Figure()
                        
                        # Add traces for each visitor type
                        fig.add_trace(go.Scatter(
                            x=[d.strftime('%Y-%m-%d') for d in df['week_start']],
                            y=df['foreign_tourists'].tolist(),
                            name='Foreign Tourists',
                            mode='lines+markers',
                            line=dict(color='#1f77b4')
                        ))
                        
                        fig.add_trace(go.Scatter(
                            x=[d.strftime('%Y-%m-%d') for d in df['week_start']],
                            y=df['swiss_tourists'].tolist(),
                            name='Swiss Tourists',
                            mode='lines+markers',
                            line=dict(color='#ff7f0e')
                        ))
                        
                        fig.add_trace(go.Scatter(
                            x=[d.strftime('%Y-%m-%d') for d in df['week_start']],
                            y=df['total_visitors'].tolist(),
                            name='Total Visitors',
                            mode='lines+markers',
                            line=dict(dash='dot', color='#2ca02c')
                        ))
                        
                        fig.update_layout(
                            title="Weekly Visitor Patterns in Spring 2023",
                            xaxis_title="Week Starting",
                            yaxis_title="Number of Visitors",
                            showlegend=True,
                            hovermode='x unified',
                            legend=dict(
                                yanchor="top",
                                y=0.99,
                                xanchor="left",
                                x=0.01
                            ),
                            template='plotly_white'
                        )
                        
                        visualization = {
                            'type': 'plotly',
                            'data': fig.to_dict(),
                            'config': {
                                'displayModeBar': True,
                                'responsive': True
                            }
                        }
                        
                        # Generate analysis summary
                        analysis_prompt = f"""Given the following data from a tourism database query about {message}, provide a clear and concise summary of the patterns and insights:

Data:
{df.to_dict('records')}

Please include:
1. Overall trends
2. Notable patterns or changes
3. Peak periods
4. Any interesting insights

Keep the summary clear and informative for a business audience."""
                        
                        analysis_summary = await self.llm.generate_response(analysis_prompt)
                        
                        # Store the data (convert timestamps to strings and ensure numeric values are integers)
                        data = [{
                            'week_start': row['week_start'].strftime('%Y-%m-%d'),
                            'foreign_tourists': int(row['foreign_tourists']),
                            'swiss_tourists': int(row['swiss_tourists']),
                            'total_visitors': int(row['total_visitors'])
                        } for _, row in df.iterrows()]
                        
                except Exception as e:
                    logger.error(f"Error executing query or generating visualization: {str(e)}")
                    logger.error(f"Traceback: {traceback.format_exc()}")
                    raise
            
            return {
                "message": response,
                "conversation_id": conversation_id,
                "sql_query": sql_query,
                "data": data,
                "visualization": visualization,
                "analysis_summary": analysis_summary,
                "metadata": {
                    "source": "llm",
                    "chat_history": [{"role": m.type, "content": m.content} for m in self.llm.memory.chat_memory.messages],
                    "accordions_open": True
                }
            }
            
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")
            raise
    
    async def _store_conversation(self, message: str, response: str, conversation_id: str) -> None:
        """Store conversation in database"""
        try:
            conversation_data = {
                "conversation_id": conversation_id,
                "user_message": message,
                "assistant_message": response,
                "metadata": {
                    "source": "llm",
                    "chat_history": self.llm.memory.chat_memory.messages
                }
            }
            
            await self.db_service.store_conversation(conversation_data)
            
        except Exception as e:
            logger.error(f"Error storing conversation: {str(e)}")
            # Don't raise the error as this is not critical for the user experience

    def create_message(self, message: ChatMessageCreate) -> ChatMessageResponse:
        db_message = ChatMessage(
            content=message.content,
            role=message.role,
            model=message.model
        )
        self.db_service.db.add(db_message)
        self.db_service.db.commit()
        self.db_service.db.refresh(db_message)
        return ChatMessageResponse.from_orm(db_message)

    def get_messages(self, limit: int = 100) -> List[ChatMessageResponse]:
        messages = self.db_service.db.query(ChatMessage).limit(limit).all()
        return [ChatMessageResponse.from_orm(msg) for msg in messages]

    def _create_visualization(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Create visualization based on data type"""
        try:
            if 'week_start' in df.columns:
                # Time series visualization
                fig = go.Figure()
                numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
                
                for col in numeric_cols:
                    fig.add_trace(go.Scatter(
                        x=df['week_start'],
                        y=df[col],
                        name=col,
                        mode='lines+markers'
                    ))
                
                fig.update_layout(
                    title="Weekly Patterns",
                    xaxis_title="Week",
                    yaxis_title="Count",
                    showlegend=True
                )
                
                return {
                    'type': 'plotly',
                    'data': fig.to_dict()
                }
            else:
                # Bar chart for other types of data
                fig = go.Figure(data=[
                    go.Bar(
                        x=df.iloc[:, 0],
                        y=df.iloc[:, 1],
                        name=df.columns[1]
                    )
                ])
                
                fig.update_layout(
                    title="Data Distribution",
                    xaxis_title=df.columns[0],
                    yaxis_title=df.columns[1],
                    showlegend=True
                )
                
                return {
                    'type': 'plotly',
                    'data': fig.to_dict()
                }
                
        except Exception as e:
            logger.error(f"Error creating visualization: {str(e)}")
            return None

    async def _generate_analysis_summary(self, df: pd.DataFrame, original_question: str) -> str:
        """Generate analysis summary using LLM"""
        try:
            # Prepare data statistics
            stats = {
                'total_records': len(df),
                'columns': list(df.columns),
                'numeric_summary': {}
            }
            
            for col in df.select_dtypes(include=['int64', 'float64']).columns:
                stats['numeric_summary'][col] = {
                    'mean': df[col].mean(),
                    'min': df[col].min(),
                    'max': df[col].max()
                }
            
            # Create prompt for analysis
            analysis_prompt = f"""Based on the following data statistics and the original question: "{original_question}"
            
            Data Statistics:
            - Total records: {stats['total_records']}
            - Columns: {', '.join(stats['columns'])}
            
            Numeric Summaries:
            {json.dumps(stats['numeric_summary'], indent=2)}
            
            Please provide a concise analysis of the data, highlighting key patterns, trends, or insights."""
            
            # Generate analysis using LLM
            analysis = await self.llm.generate_response(analysis_prompt)
            return analysis
            
        except Exception as e:
            logger.error(f"Error generating analysis summary: {str(e)}")
            return None

    def __del__(self):
        if hasattr(self, 'db_service'):
            self.db_service.db.close() 