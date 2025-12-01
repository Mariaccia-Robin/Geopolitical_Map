import json
import uuid
import hashlib  # <--- Added for deterministic IDs
from typing import List, Dict, Any
from qdrant_client import QdrantClient
from qdrant_client.http import models
from sentence_transformers import SentenceTransformer
from tqdm import tqdm
import torch

def check_device():
    if torch.cuda.is_available():
        return "cuda"
    elif torch.backends.mps.is_available():
        return "mps" 
    else:
        return "cpu"

device = check_device()
print(f"Running on: {device}")

COLLECTION_NAME = "Geopolitical_Knowledge_Base"
VECTOR_SIZE = 384
BATCH_SIZE = 512
LOCAL_DB_PATH = "./qdrant_storage"

def get_local_client() -> QdrantClient:
    return QdrantClient(path=LOCAL_DB_PATH)

def setup_qdrant(client: QdrantClient):
    if not client.collection_exists(collection_name=COLLECTION_NAME):
        client.create_collection(
            collection_name=COLLECTION_NAME,
            vectors_config=models.VectorParams(
                size=VECTOR_SIZE,
                distance=models.Distance.COSINE
            )
        )

def load_chunks(file_path: str) -> List[Dict[str, Any]]:
    with open(file_path, 'r', encoding='utf-8') as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            f.seek(0)
            return [json.loads(line) for line in f if line.strip()]

def ingest_to_qdrant(file_path: str):
    client = get_local_client()
    setup_qdrant(client)
    
    model = SentenceTransformer('all-MiniLM-L6-v2', device=device)
    data = load_chunks(file_path)
    
    for i in tqdm(range(0, len(data), BATCH_SIZE), desc="Ingesting Batches", unit="batch"):
        batch = data[i : i + BATCH_SIZE]
        
        texts = [item.get("text", "") for item in batch]
        embeddings = model.encode(texts)
        
        points = []
        for item, vector in zip(batch, embeddings):
            # Generate a consistent ID based on the text content
            text_content = item.get("text", "")
            doc_id = hashlib.md5(text_content.encode('utf-8')).hexdigest()
            
            points.append(models.PointStruct(
                id=doc_id,  # <--- Uses hash instead of random UUID
                vector=vector.tolist(),
                payload={
                    "original_id": item.get("id"),
                    "title": item.get("title"),
                    "text": item.get("text"),
                    "metadata": item.get("metadata"),
                    "type": item.get("type"),
                    "tags": item.get("tags")
                }
            ))
        
        client.upload_points(
            collection_name=COLLECTION_NAME,
            points=points
        )