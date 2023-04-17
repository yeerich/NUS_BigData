from difflib import SequenceMatcher

import pandas as pd
import numpy as np

# Find similar jobs based on the input job
# Function to calculate string similarity ratio
def similarity_ratio(a, b):
        return SequenceMatcher(None, a, b).ratio()

# Function to find similar job titles
def find_similar_job_titles(df_title, job_title, similarity_threshold):
    similar_job_titles = []
    count_df = 0
    for title in df_title['title']:
        similarity = similarity_ratio(job_title.lower(), title.lower())
        if similarity >= similarity_threshold:
            similar_job_titles.append(count_df)
        count_df += 1
    return similar_job_titles