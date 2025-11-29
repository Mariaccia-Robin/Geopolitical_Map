import wikipediaapi
import pandas as pd
from tqdm import tqdm

USER_AGENT = 'Geopolitical_Map/1.0 (contact: robin.mariaccia@gmail.com)'

wiki = wikipediaapi.Wikipedia(
    user_agent=USER_AGENT,
    language='en'
)

INTERESTING_KEYWORDS = [
    # Diplomacy
    "relations", "embassy", "consulate", "liaison", "mission", 
    "summit", "visit", "trip", "dialogue", "conference", "forum",
    "treaty", "accord", "agreement", "memorandum", "declaration", "protocol",
    "alliance", "partnership", "cooperation", "recognition",
    
    # Conflict & Security
    "war", "conflict", "dispute", "crisis", "incident", "tension", "standoff",
    "invasion", "occupation", "annexation", "coup", "uprising", "insurgency",
    "terror", "bombing", "attack", "airstrike", "hostage", "sanction",
    "intelligence", "espionage", "surveillance", "cyber",
    
    # Border & Maritime
    "border", "boundary", "territory", "eez", "continental shelf",
    "maritime", "naval", "patrol", "coast guard", "joint exercise",
    
    # Material & People
    "trade", "tariff", "pipeline", "dam", "refugee", "migration", 
    "deportation", "asylum", "aid", "loan", "debt"
]

NOISE_KEYWORDS = [
    "football", "soccer", "cricket", "rugby", "basketball", "volleyball",
    "championship", "tournament", "cup", "copa", "league", "olympic", "sport",
    "film", "movie", "novel", "series", "season", "episode", "video game",
    "music", "song", "album", "band", "orchestra", "festival",
    "species", "fauna", "flora", "biota",
    "coat of arms", "flag of", "national anthem", "symbol",
    "outline of", "timeline of", "list of twin towns",
    "airline", "flight"
]

def analyze_title(title):
    """
    Decides if a page title is relevant based on keyword heuristics.

    Parameters
    ----------
    title : str
        The Wikipedia page title to check.

    Returns
    -------
    tuple
        (status, reason) where status is 'KEPT' or 'IGNORED'.
    """
    title_lower = " " + title.lower() + " " # Padding for simple whole-word matching
    
    # 1. Noise Check
    for noise in NOISE_KEYWORDS:
        if noise in title_lower:
            return "IGNORED", f"Noise: {noise}"
            
    # 2. Interest Check (Strict)
    for interest in INTERESTING_KEYWORDS:
        # Force whole word for short dangerously short keywords
        if interest in ["dam", "line", "act", "aid"]: 
             if f" {interest} " in title_lower: 
                 return "KEPT", f"Match: {interest}"
        elif interest in title_lower:
             return "KEPT", f"Match: {interest}"
             
    return "IGNORED", "No Keywords"

def drill_down(category_obj, audit_log, visited_global, depth=0):
    """
    Recursively traverses a Wikipedia category tree to identify and harvest relevant 
    geopolitical pages based on keyword analysis.

    This function inspects members of a category. If a member is a page, it analyzes 
    relevance. If a member is a sub-category, it checks for relevance and recurses 
    downward until `max_depth` is reached.

    Parameters
    ----------
    category_obj : wikipediaapi.WikipediaPage
        The current category object to inspect (e.g., "Category:Bilateral relations of France").
    audit_log : list of dict
        A mutable list where the processing status (KEPT/IGNORED), reason, and metadata 
        for each visited page are recorded as dictionaries.
    visited_global : set of str
        A set containing the titles of all pages and categories visited so far across 
        the entire execution. Used to prevent infinite recursion and duplicate processing.
    depth : int, optional
        The current depth level in the recursive stack. The default is 0.
    max_depth : int, optional
        The maximum number of sub-category levels to descend into. The default is 2.

    Returns
    -------
    dict
        A dictionary mapping page titles (str) to their corresponding WikipediaPage objects 
        for all pages marked as "KEPT".
    """
    found_pages = {}
    
    #Lower than depth 2 is mostly noise events which wont help the RAG 
    #Depth 0 = bilateral relation of country X
    #Depth 1 = country X and Country Y relations
    #Depth 2 = war between country X and country Y
    #Depth 3 = Battle of 1995 between X and Y (useless on geopolitical stance)
    if depth > 2:
        return found_pages

    for member in category_obj.categorymembers.values():
        
        #avoid infinite loop bug cause by circular folders
        if member.title in visited_global:
            continue
        visited_global.add(member.title)
        
        #If it's a page, we get the info from it
        if member.ns == wikipediaapi.Namespace.MAIN:
            status, reason = analyze_title(member.title)
            
            try:
                rev_id = member.lastrevid
            except Exception as e:
                print(f"Error for {member.title}: {e}") 
                rev_id = "ERROR"
                    
            audit_log.append({
                "Title": member.title,
                "Type": "Deep Event",
                "Status": status,
                "Reason": reason,
                "Revision_ID": rev_id, 
                "Source_Category": category_obj.title,
                "Depth": depth
            })

            if status == "KEPT":
                found_pages[member.title] = member

        # If it's a folder again, we go deeper down the hole
        elif member.ns == wikipediaapi.Namespace.CATEGORY:
            status, reason = analyze_title(member.title)
            
            if status == "KEPT":
                #DIGGY DIGGY HOLE
                sub_results = drill_down(member, audit_log, visited_global, depth + 1)
                found_pages.update(sub_results)

    return found_pages


def harvest_world_data():
    """
    Orchestrates the global harvesting of geopolitical Wikipedia pages by iterating 
    through country-level bilateral relation categories.

    Saves a csv of all checked pages (wether we want to keep them or not) and their metadata for download later on.
    This function is only meant to be called once to create the index of all wikipedia pages we want to download from.
    Once the index is created this function becomes irrelevant, unless new articles pop up.
    So it could be good to run this function from time to time to check if new articles have appeared.
    
    Returns
    -------
    list of str
        A list containing the titles of all Wikipedia pages that were selected 
        ("KEPT") based on the keyword analysis logic.
    """
    print("INITIALIZING GLOBAL HARVEST...")
    print("Fetching Country List from Wikipedia (Level 1)...")
    
    root_cat = wiki.page("Category:Bilateral relations by country")
    country_categories = list(root_cat.categorymembers.values())
    
    # Filter to ensure we only have actual category folders
    country_categories = [c for c in country_categories if c.ns == wikipediaapi.Namespace.CATEGORY]
    
    print(f"Found {len(country_categories)} Country Categories. Starting Harvest.")

    audit_log = []       
    final_pages = {}     
    visited_global = set() 

    for country_cat in tqdm(country_categories, desc="Harvesting Countries", unit="country"):
        for member in country_cat.categorymembers.values():
            
            #avoid infinite loop bug cause by circular folders
            if member.title in visited_global:
                continue
            
            # If it's a direct relation page we just pull it
            if member.ns == wikipediaapi.Namespace.MAIN:
                visited_global.add(member.title)
                status, reason = analyze_title(member.title)
                
                try:
                    rev_id = member.lastrevid
                except Exception as e:
                    print(f"Error for {member.title}: {e}") 
                    rev_id = "ERROR"
                
                audit_log.append({
                    "Title": member.title,
                    "Type": "Direct Page",
                    "Status": status,
                    "Reason": reason,
                    "Revision_ID": rev_id,
                    "Source_Category": country_cat.title,
                    "Depth": 0
                })
                
                if status == "KEPT":
                    final_pages[member.title] = member

            # if its not a direct relation page and is a folder, we enter the rabbit hole
            elif member.ns == wikipediaapi.Namespace.CATEGORY:
                if "relations" in member.title.lower():
                    visited_global.add(member.title)
                    
                    # UNLEASH THE DRILL
                    found_items = drill_down(member, audit_log, visited_global, depth=1)
                    
                    if found_items:
                        final_pages.update(found_items)


    print("\n HARVEST COMPLETE.")
    print(f" Total Pages Selected: {len(final_pages)}")
    print(f" Total Pages Checked:  {len(audit_log)}")
    
    print("Saving Audit Log to 'harvest_audit.csv'...")
    df = pd.DataFrame(audit_log)
    df.to_csv("harvest_audit.csv", index=False)
    
    print("Done. Check 'harvest_audit.csv' for details.")
    return list(final_pages.keys())
