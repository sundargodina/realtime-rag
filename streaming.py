from kafka import KafkaProducer, KafkaConsumer
import json
import threading
import time
from state import collection, documents

class DocumentStreamer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        
    def send_update(self, doc_id, content, action="update"):
        message = {
            "doc_id": doc_id,
            "content": content,
            "action": action,
            "timestamp": time.time()
        }
        future = self.producer.send('document-updates', message)
	
        try:
            record_metadata = future.get(timeout=10)
            print(f" Sent {action} for {doc_id} "
              f"(topic={record_metadata.topic}, "
              f"partition={record_metadata.partition}, "
              f"offset={record_metadata.offset})")
        except Exception as e:
            print(f" Failed to send message: {e}")
        self.producer.flush()

class DocumentProcessor:
    def __init__(self):
        self.consumer = KafkaConsumer(
            'document-updates',
            bootstrap_servers=['localhost:9092'],
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
    def start_processing(self):
        print("Starting document processor...")
        for message in self.consumer:
            update = message.value
            self.process_update(update)
            
    def process_update(self, update):
        doc_id = update['doc_id']
        content = update['content']
        action = update['action']
        
        print(f"Processing {action} for {doc_id}")
        
        if action == "update":
            # Update in-memory store
            documents[doc_id] = content
            
            # Update vector database
            collection.upsert(
                ids=[doc_id],
                documents=[content]
            )
            
        elif action == "delete":
            # Remove from stores
            if doc_id in documents:
                del documents[doc_id]
            try:
                collection.delete(ids=[doc_id])
            except:
                pass
                
        print(f"✅ Processed {action} for {doc_id}")

# Start processor in background
def start_background_processor():
    processor = DocumentProcessor()
    thread = threading.Thread(target=processor.start_processing, daemon=True)
    thread.start()
    return thread

if __name__ == "__main__":
    # Test streaming
    streamer = DocumentStreamer()
    streamer.send_update("streaming_doc", "This is a streaming document update!", "update")
