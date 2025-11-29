# -*- coding: utf-8 -*-
"""
Created on Fri Nov 28 21:34:35 2025

@author: robin
"""
from .wikipedia_scraper import harvest_world_data
    
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
    
    #Creates the wikipedia index
    harvest_world_data()
    #download_world_data()
    return
            
    