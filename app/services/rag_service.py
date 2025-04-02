from typing import Dict, Any, Optional
from app.rag.tourism_rag import TourismRAG
from app.core.config import settings
from app.llm.claude_adapter import ClaudeAdapter
import logging

logger = logging.getLogger(__name__)

class RAGService:
    def __init__(self):
        # Initialize LLM adapter
        self.llm = ClaudeAdapter()
        
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