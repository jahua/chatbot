from sqlalchemy.orm import Session
from app.db.models import conversation_history
from app.core.config import settings
import chromadb
import json
from datetime import datetime
from typing import Dict, List, Optional, Any
from sqlalchemy.exc import ProgrammingError
import logging

logger = logging.getLogger(__name__)

class ConversationService:
    def __init__(self, db: Session):
        self.db = db
        try:
            # Create an ephemeral client (in-memory)
            self.chroma_client = chromadb.EphemeralClient()
            
            # Get or create the collection
            self.collection = self.chroma_client.get_or_create_collection(
                name="conversations",
                metadata={"hnsw:space": "cosine"}
            )
            
        except Exception as e:
            print(f"Error initializing ChromaDB client: {str(e)}")
            raise
        
    def save_conversation(self, session_id: str, prompt: str, sql_query: str, 
                         response: str, schema_context: str, metadata: dict = None):
        """Save conversation to both relational DB and vector store"""
        try:
            # Create conversation history record
            result = self.db.execute(
                conversation_history.insert().values(
                    session_id=session_id,
                    prompt=prompt,
                    sql_query=sql_query,
                    response=response,
                    schema_context=schema_context,
                    query_metadata=metadata or {}
                )
            )
            self.db.commit()
            
            # Get the inserted record's ID
            conversation_id = result.inserted_primary_key[0]
            
            # Generate embedding and save to vector store
            # Note: In a real implementation, you would use a proper embedding model
            # For now, we'll use a simple text-based embedding
            vector_embedding = self._generate_embedding(prompt)
            
            # Update the record with the embedding
            self.db.execute(
                conversation_history.update()
                .where(conversation_history.c.id == conversation_id)
                .values(vector_embedding=vector_embedding)
            )
            self.db.commit()
            
            # Save to vector store
            metadata_dict = {
                "conversation_id": conversation_id,
                "session_id": session_id,
                "sql_query": sql_query,
                "created_at": datetime.utcnow().isoformat(),
                **(metadata or {})
            }
            
            self.collection.add(
                documents=[prompt],
                embeddings=[vector_embedding],
                metadatas=[metadata_dict],
                ids=[str(conversation_id)]
            )
            
            # Return the conversation record
            result = self.db.execute(
                conversation_history.select()
                .where(conversation_history.c.id == conversation_id)
            ).first()
            
            return result
        except ProgrammingError as e:
            # Handle case where table doesn't exist yet
            logger.warning(f"Database error in save_conversation: {str(e)}")
            self.db.rollback()
            return None
        except Exception as e:
            logger.error(f"Error in save_conversation: {str(e)}")
            self.db.rollback()
            return None
    
    def find_similar_conversations(self, prompt: str, limit: int = 5):
        """Find similar conversations using vector similarity search"""
        try:
            # Generate embedding for the prompt
            vector_embedding = self._generate_embedding(prompt)
            
            # Search in vector store
            results = self.collection.query(
                query_embeddings=[vector_embedding],
                n_results=limit
            )
            
            # Get full conversation details from relational DB
            conversation_ids = [int(id) for id in results['ids'][0]]
            conversations = self.db.execute(
                conversation_history.select()
                .where(conversation_history.c.id.in_(conversation_ids))
            ).all()
            
            return conversations
        except ProgrammingError as e:
            # Handle case where table doesn't exist yet
            logger.warning(f"Database error in find_similar_conversations: {str(e)}")
            return []
        except Exception as e:
            logger.error(f"Error in find_similar_conversations: {str(e)}")
            return []
    
    def _generate_embedding(self, text: str) -> list:
        """Generate a simple embedding for text (placeholder)"""
        # In a real implementation, you would use a proper embedding model
        # For now, we'll just use character counts as a simple embedding
        # This is NOT a good embedding strategy for production!
        embedding = [0] * 100  # 100-dimensional embedding
        for i, char in enumerate(text):
            pos = i % 100
            embedding[pos] += ord(char)
        # Normalize
        import math
        norm = math.sqrt(sum(x*x for x in embedding))
        return [x/norm for x in embedding] if norm > 0 else embedding
    
    def get_conversation_history(self, session_id: str):
        """Get all conversations for a session"""
        return self.db.execute(
            conversation_history.select()
            .where(conversation_history.c.session_id == session_id)
            .order_by(conversation_history.c.created_at.desc())
        ).all() 