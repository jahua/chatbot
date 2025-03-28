from chromadb import Client, Settings
from app.core.config import settings
from typing import List, Dict, Any
import json

# Initialize ChromaDB client
client = Client()

# Create or get the collection
collection = client.get_or_create_collection(
    name="tourism_data",
    metadata={"hnsw:space": "cosine"}
)

vector_store = {
    "client": client,
    "collection": collection
}

class VectorStore:
    def __init__(self):
        self.client = Client()
        self.collection = self.client.get_or_create_collection(
            name="tourism_data",
            metadata={"hnsw:space": "cosine"}
        )
    
    def add_schema_documents(self, schema_info: Dict[str, Dict[str, List[Dict[str, Any]]]]):
        """Add database schema information to the vector store"""
        # Process tables
        for table_name, columns in schema_info["tables"].items():
            for column in columns:
                doc = f"Table {table_name} has column {column['column_name']} of type {column['data_type']}"
                metadata = {
                    "type": "table",
                    "table_name": table_name,
                    "column_name": column["column_name"],
                    "data_type": column["data_type"]
                }
                self.collection.add(
                    documents=[doc],
                    metadatas=[metadata],
                    ids=[f"table_{table_name}_{column['column_name']}"]
                )
        
        # Process foreign keys
        for table_name, foreign_keys in schema_info["foreign_keys"].items():
            for fk in foreign_keys:
                doc = f"Table {table_name} has foreign key {fk['column']} referencing {fk['references']}"
                metadata = {
                    "type": "foreign_key",
                    "table_name": table_name,
                    "column_name": fk["column"],
                    "references": fk["references"]
                }
                self.collection.add(
                    documents=[doc],
                    metadatas=[metadata],
                    ids=[f"fk_{table_name}_{fk['column']}"]
                )
    
    def query_schema(self, query: str, n_results: int = 5) -> List[Dict[str, Any]]:
        """Query the schema information using natural language"""
        results = self.collection.query(
            query_texts=[query],
            n_results=n_results
        )
        
        return [
            {
                "document": doc,
                "metadata": meta,
                "distance": dist
            }
            for doc, meta, dist in zip(
                results["documents"][0],
                results["metadatas"][0],
                results["distances"][0]
            )
        ]
    
    def get_schema_context(self, query: str) -> Dict[str, Any]:
        """Get relevant schema context for a given query"""
        results = self.query_schema(query)
        
        # Extract table and column information from results
        tables = {}
        for result in results:
            meta = result["metadata"]
            if meta["type"] == "table":
                table_name = meta["table_name"]
                if table_name not in tables:
                    tables[table_name] = []
                tables[table_name].append({
                    "column_name": meta["column_name"],
                    "data_type": meta["data_type"]
                })
        
        return {
            "tables": tables,
            "context": "\n".join([f"- {result['document']}" for result in results])
        }

    def add_documents(self, documents: List[str], metadatas: List[Dict[str, Any]] = None):
        """Add documents to the vector store"""
        if not metadatas:
            metadatas = [{"source": f"doc_{i}"} for i in range(len(documents))]
        
        self.collection.add(
            documents=documents,
            metadatas=metadatas,
            ids=[f"doc_{i}" for i in range(len(documents))]
        )

    def search(self, query: str, n_results: int = 5) -> List[Dict[str, Any]]:
        """Search for similar documents"""
        results = self.collection.query(
            query_texts=[query],
            n_results=n_results
        )
        return results

# Initialize vector store
vector_store = VectorStore() 