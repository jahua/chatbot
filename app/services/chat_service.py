from typing import Dict, Any, Optional, List
from sqlalchemy.orm import Session
from app.db.database import SessionLocal, DatabaseService
from app.llm.claude_adapter import ClaudeAdapter
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

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class ChatService:
    def __init__(self, db_service: DatabaseService, llm_adapter: ClaudeAdapter):
        self.db_service = db_service
        self.claude_adapter = llm_adapter
        logger.info("ChatService initialized successfully")

    async def process_message(self, request: ChatMessage) -> ChatResponse:
        """Process a chat message and return a response"""
        logger.debug(f"Processing message: {request.message}")
        logger.debug(f"Session ID: {request.session_id}")
        
        try:
            # Generate SQL query
            logger.debug("Generating SQL query...")
            sql_query = await self.claude_adapter.generate_sql(
                request.message,
                "data_lake.aoi_visitors (visitors JSONB, aoi_date DATE)"
            )
            logger.debug(f"Generated SQL: {sql_query}")
            
            # Execute SQL query
            logger.debug("Executing SQL query...")
            results = await self.claude_adapter.execute_sql(sql_query)
            logger.debug(f"Query results: {results}")
            
            # Generate response
            logger.debug("Generating response...")
            response = await self.claude_adapter.generate_response(
                request.message,
                sql_query,
                results
            )
            logger.debug(f"Generated response: {response}")
            
            return ChatResponse(
                message=response,
                sql_query=sql_query,
                results=results
            )
            
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")
            logger.error(traceback.format_exc())
            raise HTTPException(
                status_code=500,
                detail=f"Error processing message: {str(e)}"
            )

    def _load_schema_summary(self) -> str:
        logger.debug("Loading schema summary...")
        try:
            with open("app/schema/schema_summary.txt", "r") as f:
                summary = f.read()
            logger.debug("Schema summary loaded successfully")
            return summary
        except Exception as e:
            logger.error(f"Error loading schema summary: {e}")
            raise

    async def process_message_with_visualization(self, message: str, session_id: str) -> Dict:
        """Process a message and return a response with visualization"""
        logger.debug(f"Processing message for session {session_id}: {message}")
        start_time = time.time()
        
        try:
            # Generate SQL query
            logger.debug("Generating SQL query...")
            sql_query = await self.claude_adapter.generate_sql(
                message,
                "data_lake.aoi_visitors (visitors JSONB, aoi_date DATE)"
            )
            logger.debug(f"Generated SQL: {sql_query}")
            
            # Execute query with timeout
            logger.debug("Executing SQL query...")
            try:
                results = await self.claude_adapter.execute_sql(sql_query)
                logger.debug(f"Query executed successfully. Results: {results}")
            except asyncio.TimeoutError:
                logger.error("SQL query execution timed out")
                raise Exception("Database query timed out. Please try a simpler query.")
            
            # Process results
            logger.debug("Processing query results...")
            if "week_number" in results:
                logger.debug("Processing weekly pattern data...")
                if any("spring" in col.lower() for col in results):
                    logger.debug("Processing spring-specific weekly pattern")
                else:
                    logger.debug("Processing general weekly pattern")
                    
                # Create visualization
                logger.debug("Creating visualization...")
                fig = self._create_weekly_pattern_plot(results)
                
                # Calculate statistics
                logger.debug("Calculating statistics...")
                stats = self._calculate_visitor_statistics(results)
                logger.debug(f"Statistics calculated: {stats}")
                
                response = {
                    "type": "weekly_pattern",
                    "plot": fig,
                    "statistics": stats,
                    "message": "Here are the weekly visitor patterns you requested."
                }
            else:
                logger.debug("Processing general query results...")
                response = {
                    "type": "general",
                    "data": results,
                    "message": "Here are the results of your query."
                }
            
            execution_time = time.time() - start_time
            logger.debug(f"Message processing completed in {execution_time:.2f} seconds")
            return response
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error processing message: {error_msg}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            
            return {
                "type": "error",
                "message": f"An error occurred: {error_msg}"
            }
        finally:
            logger.debug(f"Request processing completed for session {session_id}")

    def _create_weekly_pattern_plot(self, df: pd.DataFrame) -> go.Figure:
        logger.debug("Creating weekly pattern plot...")
        try:
            fig = go.Figure()
            fig.add_trace(go.Bar(x=df['week_number'], 
                               y=df['swiss_tourists'], 
                               name='Swiss Tourists'))
            fig.add_trace(go.Bar(x=df['week_number'], 
                               y=df['foreign_tourists'], 
                               name='Foreign Tourists'))
            fig.update_layout(
                title='Weekly Visitor Patterns in 2023',
                xaxis_title='Week Number',
                yaxis_title='Number of Visitors',
                barmode='stack'
            )
            logger.debug("Plot created successfully")
            return fig
        except Exception as e:
            logger.error(f"Error creating plot: {e}")
            raise
            
    def _calculate_visitor_statistics(self, df: pd.DataFrame) -> Dict:
        logger.debug("Calculating visitor statistics...")
        try:
            peak_week = df.loc[df['total_visitors'].idxmax()]
            stats = {
                'peak_week': int(peak_week['week_number']),
                'peak_visitors': int(peak_week['total_visitors']),
                'avg_swiss': int(df['swiss_tourists'].mean()),
                'avg_foreign': int(df['foreign_tourists'].mean()),
                'avg_total': int(df['total_visitors'].mean())
            }
            logger.debug("Statistics calculated successfully")
            return stats
        except Exception as e:
            logger.error(f"Error calculating statistics: {e}")
            raise

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

    def __del__(self):
        if hasattr(self, 'db_service'):
            self.db_service.db.close() 