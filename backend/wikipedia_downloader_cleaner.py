import requests
import pandas as pd
import re
import time
import json
import os
import html
from tqdm import tqdm

# --- CONFIGURATION ---
WIKI_API_URL = "https://en.wikipedia.org/w/api.php"
USER_AGENT = "Geopolitical-Map/3.0 (Contact: robin.mariaccia@gmail.com)"
CLEAN_CORPUS_FILE = "rag_corpus_clean.txt"

# ==============================================================================
# FUNCTION 1: DOWNLOAD RAW CONTENT
# ==============================================================================
def download_corpus(input_file = str, output_file = str, limit_debug: int = None):
    """
    Downloads raw Wikitext for 'KEPT' articles and saves them line-by-line as JSON.
    This preserves the exact raw state from Wikipedia for later processing.
    """
    INPUT_CSV = input_file
    RAW_CORPUS_FILE = output_file
    try:
        df = pd.read_csv(INPUT_CSV)
        if 'keep' not in df.columns:
            raise ValueError(f"Column 'keep' missing in {INPUT_CSV}.")
    except Exception as e:
        print(f"Error loading CSV: {e}")
        return

    titles_to_fetch = df[df['keep'] == 'KEPT']['title'].tolist()
    
    if limit_debug:
        titles_to_fetch = titles_to_fetch[:limit_debug]
        print(f"DEBUG MODE: Limiting to first {limit_debug} articles.")

    print(f"Downloading {len(titles_to_fetch)} articles...")

    session = requests.Session()
    session.headers.update({'User-Agent': USER_AGENT})
    
    # Check if file exists to resume? (Simple version: overwrite)
    # We open in 'a' (append) mode if you wanted to resume, but 'w' for fresh start
    with open(RAW_CORPUS_FILE, 'w', encoding='utf-8') as f:
        
        batch_size = 50
        for i in tqdm(range(0, len(titles_to_fetch), batch_size), desc="Downloading Batches"):
            batch_titles = titles_to_fetch[i:i + batch_size]
            title_string = "|".join(batch_titles)

            params = {
                "action": "query",
                "prop": "revisions",
                "titles": title_string,
                "rvprop": "content",
                "format": "json",
                "rvslots": "main"
            }

            try:
                response = session.get(WIKI_API_URL, params=params, timeout=15)
                response.raise_for_status()
                data = response.json()
                
                pages = data.get('query', {}).get('pages', {})
                
                for page_id, page in pages.items():
                    title = page.get('title', 'Unknown')
                    
                    # Extract raw wikitext safely
                    raw_text = ""
                    revisions = page.get('revisions', [])
                    if revisions:
                        slot = revisions[0].get('slots', {}).get('main', {})
                        raw_text = slot.get('*', '')
                        # Fallback for older API structure
                        if not raw_text and '*' in revisions[0]:
                            raw_text = revisions[0]['*']
                    
                    # Save RAW entry as JSON line
                    if raw_text:
                        entry = {"title": title, "raw_content": raw_text}
                        f.write(json.dumps(entry, ensure_ascii=False) + "\n")

            except Exception as e:
                print(f"Batch Error: {e}")
            
            time.sleep(0.1) # Rate limit

    print(f"Raw corpus saved to {RAW_CORPUS_FILE}")


# ==============================================================================
# FUNCTION 2: CLEAN AND STRUCTURE
# ==============================================================================
def clean_wikitext_logic(text: str) -> str:
    """
    Refined cleaning logic to handle tables, graphs, HTML entities, and messy links.
    """
    
    # 0. Strip the entire REFERENCES/SOURCES/EXTERNAL LINKS sections
    text = re.sub(r'\n=+\s*(References|Citations|Sources|External links|See also|Notes|Further reading|Bibliography)\s*=+\n.*', 
                  '\n', text, flags=re.IGNORECASE | re.DOTALL)
    
    # 1. Remove Timeline/Graph Scripting (NEW)
    # Catches the raw "EasyTimeline" syntax seen in the South Korea entry
    text = re.sub(r'^\s*(ImageSize|PlotArea|Period|TimeAxis|ScaleMajor|ScaleMinor|PlotData|DateFormat|Define|Legend|BarData|Colors).*$', 
                  '', text, flags=re.MULTILINE)

    # 2. REMOVE TABLES ({| ... |})
    text = re.sub(r'\{\|.*?\|\}', '', text, flags=re.DOTALL)

    # 3. Remove HTML comments text = re.sub(r'', '', text, flags=re.DOTALL)

    # 4. Remove HTML tags (<ref>, <div>, etc)
    text = re.sub(r'<[^>]+>', '', text)

    # 5. Remove Templates {{...}} (Recursive)
    pattern = re.compile(r'\{\{[^{}]*?\}\}')
    for _ in range(20): 
        text, count = pattern.subn('', text)
        if count == 0: break

    # 6. Handle File/Image Links (Nested Handling)
    # Covers standard [[File:...]] blocks
    text = re.sub(r'\[\[(File|Image):(?:[^\[\]]|\[\[[^\[\]]*\]\])*\]\]', '', text, flags=re.IGNORECASE)

    # 7. Remove Residual File Lines (NEW)
    # Catches "File:Name.jpg|Caption" lines that lacked brackets (seen in Saudi Arabia/South Africa)
    text = re.sub(r'^File:.*$', '', text, flags=re.MULTILINE)

    # 8. Handle Category Links (Remove)
    text = re.sub(r'\[\[Category:.*?\]\]', '', text)

    # 9. Clean Internal Links: [[Target|Label]] -> Label
    text = re.sub(r'\[\[(?:[^|\]]+\|)?([^\]]+)\]\]', r'\1', text)

    # 10. Clean External Links (Bracketed)
    text = re.sub(r'\[https?://.*?\]', '', text)

    # 11. Clean Floating URLs (NEW)
    # Removes bare https:// links not caught by brackets
    text = re.sub(r'https?://\S+', '', text)

    # 12. Headers: == History == -> History
    text = re.sub(r'=+\s*([^=]+)\s*=+', r'\1', text)

    # 13. Bold/Italic formatting
    text = re.sub(r"''+", '', text)

    # 14. Fix Artifacts (bullet points, etc.)
    text = re.sub(r'^\*+', '', text, flags=re.MULTILINE)

    # 15. Decode HTML Entities (NEW)
    # Converts &nbsp; -> space, &ndash; -> -, etc.
    text = html.unescape(text)

    # 16. Normalize Whitespace
    text = re.sub(r'\n{3,}', '\n\n', text)
    
    return text.strip()

def process_corpus(input_file = str, output_file = str):
    
    if not os.path.exists(input_file):
        return

    print("Processing and cleaning raw corpus (Streaming Mode)...")

    # Open both files simultaneously for streaming
    with open(input_file, 'r', encoding='utf-8') as fin, \
         open(output_file, 'w', encoding='utf-8') as fout:
        
        # Iterate over file object directly (lazy loading)
        for line in tqdm(fin, desc="Cleaning"):
            try:
                data = json.loads(line)
                cleaned_text = clean_wikitext_logic(data['raw_content'])
                
                if len(cleaned_text) < 50: 
                    continue

                entry = (
                    f"--- DOC START ---\n"
                    f"TITLE: {data['title']}\n"
                    f"CONTENT:\n"
                    f"{cleaned_text}\n"
                )
                
                fout.write(entry + "\n")
                
            except json.JSONDecodeError:
                continue

    print(f"Cleaned corpus saved to {output_file}")
