import requests
import csv
from typing import List, Dict, Any, Set
from tqdm import tqdm
import re
import pandas as pd

PROCESSED_CATEGORIES: Set[str] = set()

def get_category_members_recursive(
    category_name: str, 
    titles_data: List[Dict[str, Any]],
    user_agent: str,
    S: requests.Session,
    pbar: tqdm,
    current_depth: int,
    max_depth: int
) -> None:
    
    if category_name in PROCESSED_CATEGORIES:
        return
        
    PROCESSED_CATEGORIES.add(category_name)
    pbar.update(1)
    pbar.set_description(f"Depth {current_depth}") 
    pbar.set_postfix({"Articles": len(titles_data)}) 

    URL = "https://en.wikipedia.org/w/api.php"
    HEADERS = {'User-Agent': user_agent}

    PARAMS: Dict[str, Any] = {
        "action": "query",
        "format": "json",
        "generator": "categorymembers",
        "gcmtitle": category_name,
        "gcmlimit": "max",
        "gcmtype": "page|subcat",
        "prop": "revisions",
        "rvprop": "ids"
    }

    max_iterations = 5000 
    iteration_count = 0

    while iteration_count < max_iterations:
        iteration_count += 1
        
        try:
            R = S.get(url=URL, params=PARAMS, headers=HEADERS, timeout=30)
            R.raise_for_status()
            DATA = R.json()

        except requests.exceptions.RequestException:
            return
        except ValueError:
            return

        pages_dict = DATA.get("query", {}).get("pages", {})
        subcategories_to_process = []
        
        for page_id, page in pages_dict.items():
            if page.get('ns') == 0:
                revisions = page.get('revisions', [])
                current_revid = revisions[0]['revid'] if revisions else 0
                titles_data.append({
                    'title': page['title'], 
                    'revid': current_revid
                })
            
            elif page.get('ns') == 14:
                if current_depth < max_depth:
                    subcategories_to_process.append(page['title'])
        
        pbar.set_postfix({"Articles": len(titles_data)})

        if "continue" in DATA:
            PARAMS["gcmcontinue"] = DATA["continue"]["gcmcontinue"]
        else:
            break
            
    for subcat_title in subcategories_to_process:
        get_category_members_recursive(
            subcat_title, 
            titles_data, 
            user_agent, 
            S, 
            pbar, 
            current_depth + 1, 
            max_depth
        )


def scrape_bilateral_relations_data(
    start_category: str = "Category:Bilateral relations by country",
    filename: str = "wiki_bilateral_relations.csv",
    max_depth: int = 2,
    user_agent: str = "Geopolitical-Map/2.0 (Contact: robin.mariaccia@gmail.com)"
) -> None:
    
    print(f"Starting recursive data acquisition (Max Depth: {max_depth})...")
    S = requests.Session()
    titles_data: List[Dict[str, Any]] = []
    
    PROCESSED_CATEGORIES.clear()
    
    with tqdm(desc="Init", unit="cat") as pbar:
        get_category_members_recursive(
            start_category, 
            titles_data, 
            user_agent, 
            S, 
            pbar, 
            current_depth=0, 
            max_depth=max_depth
        )
            
    if titles_data:
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['title', 'revid']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(titles_data)
            print(f"Finished. Successfully wrote {len(titles_data)} titles to {filename}")
            df = pd.read_csv('wiki_bilateral_relations.csv')
            
            df['keep'] = df['title'].apply(categorize_title)
            output_df = df[['title', 'revid', 'keep']]
            output_df.to_csv('wiki_bilateral_relations.csv', index=False)
        except IOError as e:
            print(f"Error writing to CSV file: {e}")
    else:
        print("Completed traversal, but no articles were found.")


PRIORITY_KEEPS = [
    "great game", "shootdown", "spy case", "cold war", "incident", "scandal"
]

NOISE_KEYWORDS_RAW = [
    # Substring matches
    "football", "soccer", "olympic", "championship", "tournament",
    "movie", "music", "song", "album", "orchestra", "festival",
    "museum", "exhibition", "theatre", "species", "garden",
    "list of ambassadors", "list of high commissioners", "list of consuls",
    "list of diplomatic missions", "list of twin towns", "sister cities",
    # Exact word matches
    "film", "park", "cup", "game", "match", "sport", "race"
]

INTERESTING_KEYWORDS_RAW = [
    "relations", "embassy", "consulate", "liaison", "mission",
    "summit", "visit", "trip", "dialogue", "conference", "forum",
    "treaty", "accord", "agreement", "memorandum", "declaration", "protocol",
    "alliance", "partnership", "cooperation", "recognition",
    "affair", "election", "referendum", "protest",
    "rights", "democracy", "government", "office", "institute",
    "reconciliation", "repatriation", "party", "authority", "council",
    "committee", "bloc", "federation", "conquest", "skirmish",
    "pact", "talks", "hotline", "nunciature", "ambassador",
    "diplomat", "high commission",
    "war", "conflict", "dispute", "crisis", "tension", "standoff",
    "invasion", "occupation", "annexation", "coup", "uprising", "insurgency",
    "terror", "bombing", "attack", "airstrike", "hostage", "sanction",
    "intelligence", "espionage", "surveillance", "cyber",
    "operation", "assassination", "clash",
    "massacre", "hack", "arrest", "detention", "prisoner",
    "border", "boundary", "territory", "claim", "eez", "continental shelf",
    "maritime", "naval", "patrol", "coast guard", "joint exercise",
    "island", "archipelago", "trade", "tariff", "pipeline", "refugee", 
    "migration", "deportation", "asylum", "loan", "debt",
    "railway", "highway"
]

# Strict words: Must be exact whole words (e.g. "aid" but not "raid")
STRICT_INTEREST_WORDS = ["dam", "act", "aid", "trip", "vote", "gas", "oil", "ban", "party", "zone"]

def compile_patterns():
    # Priority: Substring match
    p_pattern = re.compile(r"|".join(map(re.escape, PRIORITY_KEEPS)), re.IGNORECASE)

    # Noise: Mix of whole words (\b) and substrings
    exact_noise = ["film", "park", "cup", "game", "match", "sport", "race"]
    substring_noise = [w for w in NOISE_KEYWORDS_RAW if w not in exact_noise]
    
    noise_regex_parts = [re.escape(w) for w in substring_noise] + \
                        [r'\b' + re.escape(w) + r'\b' for w in exact_noise]
    n_pattern = re.compile(r"|".join(noise_regex_parts), re.IGNORECASE)

    # Interest: Mix of strict words and substrings
    substring_interest = [w for w in INTERESTING_KEYWORDS_RAW if w not in STRICT_INTEREST_WORDS]
    interest_regex_parts = [re.escape(w) for w in substring_interest] + \
                           [r'\b' + re.escape(w) + r'\b' for w in STRICT_INTEREST_WORDS]
    i_pattern = re.compile(r"|".join(interest_regex_parts), re.IGNORECASE)
    
    return p_pattern, n_pattern, i_pattern

P_PATTERN, N_PATTERN, I_PATTERN = compile_patterns()

def categorize_title(title):
    if not isinstance(title, str):
        return "IGNORED"
    
    # 1. Priority Check
    if P_PATTERN.search(title):
        return "KEPT"
        
    # 2. Conditional Noise (Flight)
    if "flight" in title.lower():
        return "IGNORED"
        
    # 3. Standard Noise Check
    if N_PATTERN.search(title):
        return "IGNORED"
        
    # 4. Interest Check
    if I_PATTERN.search(title):
        return "KEPT"
        
    return "IGNORED"














