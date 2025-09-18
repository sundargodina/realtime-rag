from sentence_transformers import SentenceTransformer
import chromadb
from fastapi import FastAPI
import uvicorn
from state import collection, documents, model
from streaming import DocumentStreamer, start_background_processor

# Initialize components
app = FastAPI(title="Real-Time RAG System")
client = chromadb.PersistentClient(path="./rag_db")
collection = client.get_or_create_collection("documents")
model = SentenceTransformer('all-MiniLM-L6-v2')
streamer = DocumentStreamer()

# Store for tracking documents
documents = {}

# Start background processor
start_background_processor()

@app.post("/add-document")
async def add_document(doc_id: str, content: str):
    """Add document via streaming pipeline"""
    # Send to Kafka instead of direct update
    streamer.send_update(doc_id, content, "update")
    return {"message": f"Document {doc_id} queued for processing"}

@app.delete("/delete-document/{doc_id}")
async def delete_document(doc_id: str):
    """Delete document via streaming"""
    streamer.send_update(doc_id, "", "delete")
    return {"message": f"Document {doc_id} queued for deletion"}

@app.post("/search")
async def search_documents(query: str, limit: int = 5):
    """Search documents"""
    results = collection.query(
        query_texts=[query],
        n_results=limit
    )
    
    return {
        "query": query,
        "results": [
            {
                "id": results['ids'][0][i],
                "content": results['documents'][0][i],
                "score": results['distances'][0][i]
            }
            for i in range(len(results['ids'][0]))
        ]
    }

@app.get("/")
async def root():
    return {
        "message": "Real-Time RAG System is running!", 
        "docs_count": len(documents),
        "features": ["Real-time updates", "Semantic search", "Vector storage"]
    }

@app.get("/docs")
async def list_documents():
    """List all documents"""
    return {"documents": list(documents.keys())}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)