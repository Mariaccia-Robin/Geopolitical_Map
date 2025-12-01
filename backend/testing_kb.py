from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer

COLLECTION_NAME = "Geopolitical_Knowledge_Base"
LOCAL_DB_PATH = "./qdrant_storage"
OUTPUT_FILE = "search_results.txt"

def test_database():
    client = QdrantClient(path=LOCAL_DB_PATH)
    model = SentenceTransformer('all-MiniLM-L6-v2')
    
    query_text = "2024 and 2025 recent events degrading relations between france and algeria"
    query_vector = model.encode(query_text).tolist()

    try:
        results = client.query_points(
            collection_name=COLLECTION_NAME,
            query=query_vector,
            limit=5,
        ).points

        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            f.write(f"Query: {query_text}\n")
            f.write("="*50 + "\n\n")
            
            for i, hit in enumerate(results, 1):
                title = hit.payload.get('title', 'No Title')
                text = hit.payload.get('text', 'No Text')
                score = hit.score
                
                f.write(f"Result {i} (Score: {score:.4f})\n")
                f.write(f"Title: {title}\n")
                f.write("-" * 20 + "\n")
                f.write(f"{text}\n")
                f.write("\n" + "="*50 + "\n\n")
        
        print(f"✅ Results exported to {OUTPUT_FILE}")

    except Exception as e:
        print(f"❌ Error: {e}")
