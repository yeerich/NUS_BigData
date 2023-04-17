import streamlit as st
import pandas as pd
import numpy as np
import pickle
import scipy
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import CountVectorizer
from similarjobs import find_similar_job_titles, similarity_ratio

st.set_page_config(page_title="Bridging the Gap - Matching Jobs to Skills", layout="wide")

# Store the initial value of widgets in session state
if "visibility" not in st.session_state:
    st.session_state.visibility = "visible"


# Load data
@st.cache_data
def load_data():
    with open('data/course_subset_data.pickle', 'rb') as handle:
        course_subset_data = pickle.load(handle)
    
    with open('data/jobs_subset_data.pickle', 'rb') as handle:
        jobs_subset_data = pickle.load(handle)
        
    with open('data/tfidf.pickle', 'rb') as handle:
        tfidf = pickle.load(handle)
        
    tfidf_matrix = scipy.sparse.load_npz('data/tfidf_matrix.npz')
    
    return course_subset_data, jobs_subset_data, tfidf_matrix, tfidf

course_subset_data, jobs_subset_data, tfidf_matrix, tfidf = load_data()


title = "Course Recommendation Engine"
st.title(title)

st.write("First of all, welcome! This is the place where you can input the jobs of your interest and it will return the relevant courses based on the typical skills required.")
st.markdown("##")

# Add search panel and search button
user_input = st.text_input("Enter job here 👇.  For example, 'Data Scientist'",)

if user_input:
    st.write("You entered: ", user_input)
    
    # add widgets on sidebar
    recommended_course_num = st.sidebar.slider("Recommended course number", min_value=5, max_value=10);
    show_score = st.sidebar.checkbox("Show score")

    # Create a subset of dataframe for job title
    df_title = pd.DataFrame(jobs_subset_data['title'])

    # Find similar job titles for a given job title
    job_title = user_input
    similarity_threshold = 0.9
    similar_titles = find_similar_job_titles(df_title, job_title, similarity_threshold)

    sim_jobs_subset_data = pd.DataFrame(columns = jobs_subset_data.columns)

    if len(similar_titles) == 0:
        st.write("Please enter a job of your interest.")
    else:
        for title in similar_titles:
            new_row = pd.Series(jobs_subset_data.iloc[title], name=-1)
            sim_jobs_subset_data = sim_jobs_subset_data.append(new_row)
            sim_jobs_subset_data.index += 1
        
        sim_jobs_subset_data = sim_jobs_subset_data.sort_index()
            
        for ngram in range(1,4):
            CV_job = CountVectorizer(ngram_range=(ngram, ngram))
            CV_X = CV_job.fit_transform(sim_jobs_subset_data['skills_extracts'])

            # Get the feature names (words)
            feature_names = CV_job.get_feature_names_out()

            # Sum the occurrences of each word
            word_counts = CV_X.sum(axis=0)

            # Convert the word counts to a list
            word_counts = word_counts.tolist()[0]

            # Create a dictionary of word counts with word as key and count as value
            word_count_dict = dict(zip(feature_names, word_counts))

            # Sort the word count dictionary by value (count) in descending order
            sorted_word_count_dict = dict(sorted(word_count_dict.items(), key=lambda x: x[1], reverse=True))

            # Get the top 10 most frequent words (skill sets)
            top_10_ngram_words = list(sorted_word_count_dict.keys())[:10]

            skills_query = ', '.join(top_10_ngram_words)
            
            skills_vector = tfidf.transform([skills_query]) # transform the query into a vector (sparse matrix)
            skills_sim = cosine_similarity(skills_vector, tfidf_matrix).flatten() # calculate the similarity between the query and all the courses, flatten the matrix into a vector

            top_10_indices = skills_sim.argsort()[::-1][:recommended_course_num]
            st.write("Recommended courses for:")
            st.write(skills_query)
            recommendations = pd.DataFrame()
            
            recommended_course = []
            for i in top_10_indices:
                course = course_subset_data.iloc[i]
                course['Similarity Score'] = skills_sim[i]
                recommended_course.append(course[['title', 'referenceNumber', 'combined_features', 'Similarity Score']])
                
            recommendations = pd.concat(recommended_course, axis=1).T
            
            # When score is clicked, show the similarity score              
            if show_score:
                recommendations.columns = ['Course Title', 'Course Reference Number', 'Course Skills', 'Similarity Score']
            else:
                recommendations = recommendations[['title', 'referenceNumber', 'combined_features']]
                recommendations.columns = ['Course Title', 'Course Reference Number', 'Course Skills']
            
            recommendations.reset_index(drop=True, inplace=True)
            recommendations.index = np.arange(1, len(recommendations) + 1)
            
            st.dataframe(recommendations, use_container_width=True)
            st.markdown("##")