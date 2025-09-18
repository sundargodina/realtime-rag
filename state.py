import chromadb
from sentence_transformers import SentenceTransformer

# Initialize shared components
client = chromadb.PersistentClient(path="./rag_db")
collection = client.get_or_create_collection("documents")
model = SentenceTransformer('all-MiniLM-L6-v2')

# Shared document store
documents = {}

print("Database components initialized")