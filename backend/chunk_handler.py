import json
import hashlib
import re
from tqdm import tqdm
from langchain_text_splitters import RecursiveCharacterTextSplitter

def stream_docs(file_path):
    buffer = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            if "--- DOC START ---" in line:
                if buffer:
                    yield "".join(buffer)
                buffer = []
            else:
                buffer.append(line)
        if buffer:
            yield "".join(buffer)

def generate_chunks(input_file: str, output_file: str):
    text_splitter = RecursiveCharacterTextSplitter.from_tiktoken_encoder(
        model_name="gpt-4o",
        chunk_size=300,
        chunk_overlap=50
    )

    seen_hashes = set()
    
    print(f"Chunking {input_file} to {output_file}...")

    with open(output_file, 'w', encoding='utf-8') as out_f:
        for raw_doc in tqdm(stream_docs(input_file)):
            match = re.search(r'TITLE: (.*?)\nCONTENT:\n(.*)', raw_doc, re.DOTALL)
            if not match:
                continue
                
            title = match.group(1).strip()
            content = match.group(2).strip()

            if not content:
                continue

            chunks = text_splitter.split_text(content)

            for chunk in chunks:
                content_hash = hashlib.md5(chunk.encode('utf-8')).hexdigest()
                
                if content_hash not in seen_hashes:
                    seen_hashes.add(content_hash)
                    
                    record = {
                        "id": content_hash,
                        "title": title,
                        "text": chunk,
                        "metadata": {
                            "source": title,
                            "type": "geopolitical_event"
                        }
                    }
                    
                    out_f.write(json.dumps(record) + "\n")
                    
                    
        