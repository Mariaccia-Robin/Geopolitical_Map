# -*- coding: utf-8 -*-
"""
Created on Fri Nov 28 21:34:35 2025

@author: robin
"""
#from .wikipedia_scraper import harvest_world_data

#from .create_wikipedia_index import scrape_bilateral_relations_data
from .wikipedia_downloader_cleaner import download_corpus, process_corpus
from .chunk_handler import generate_chunks
from .qdrant_handler import ingest_to_qdrant
from .testing_kb import test_database

def update_corpus():
    """
    Orchestrates the end-to-end data pipeline for updating the knowledge base.

    This function sequentially executes:
    1. Harvesting raw data from external sources (currently only Wikipedia).
    2. Transforming and cleaning text for model consumption.
    3. Tokenizing processed text.
    4. Updating the RAG vector database with new embeddings.

    Returns
    -------
    None
    """
    #Builds the wikipedia index with everysingle wikipedia page about international relations 
    #APPROX TIME : 30 minutes
    #scrape_bilateral_relations_data()
    
    #Downloads wikipedia files listed in the wikipedia index file
    #APPROX TIME : 7m30
    #download_corpus(input_file = 'wiki_bilateral_relations.csv', output_file = 'rag_corpus_clean.jsonl') 
    
    # Step 2: Cleans the corpus
    #APPROX TIME : Almost instantaneous
    #process_corpus(input_file = 'rag_corpus_raw.jsonl', output_file = 'rag_corpus_clean.txt')
    
    #Step 3 : Chunks and deduplicate the cleaned corpus, then sends it to the RAG database.
    #APPROX TIME : 3 minutes
    #generate_chunks(input_file = 'rag_corpus_clean.txt', output_file = 'rag_corpus_chunked.jsonl')
    
    #Step 4 : sending to qdrant
    # APPROX TIME : 50 min with CPU, 8 min with GPU, but because im writing on my local disk, once dockerized it will be much faster.
    #ingest_to_qdrant("rag_corpus_chunked.jsonl")
    
    #Step5 : testing the knowledgebase : 
    test_database()

    return
            
    