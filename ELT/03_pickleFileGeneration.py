# Databricks notebook source
import pandas as pd
import pickle
from pyspark.sql.functions import col, to_date, current_date, date_sub
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import CountVectorizer
import scipy

print(pd.__version__)
print(pickle.format_version)

# COMMAND ----------

ls

# COMMAND ----------

# j_df = spark.read.table("jobswithdescription")
# # uncomment and run if you want to know the breakdown of the posting_date
# j_df.groupBy(col('posting_date')).count().orderBy(col('posting_date').asc()).display()

# COMMAND ----------

j_df = spark.read.table("jobswithdescription")
cutoff_days = 30

# uncomment and run if you want to know the breakdown of the posting_date
# j_df.groupBy(col('posting_date')).count().orderBy(col('posting_date').asc()).display()

j_df = j_df.withColumn("posting_date", to_date(col("posting_date"), "yyyyMMdd"))
j_df_filtered = j_df.filter((col("posting_date").isNotNull()) & (col("posting_date").between(date_sub(current_date(), cutoff_days), current_date())))
# print(j_df_filtered.count())
# display(j_df_filtered)


jobs_data = j_df_filtered.toPandas()

col_subset_jobs = ['jobPostId','title','skills_extracts']

#start the cleansing
jobs_subset_data = jobs_data[col_subset_jobs]
jobs_subset_data = jobs_subset_data.dropna()
jobs_subset_data['skills_extracts_length'] = jobs_subset_data['skills_extracts'].apply(lambda x: len(x))
jobs_subset_data = jobs_subset_data[jobs_subset_data['skills_extracts_length'] >= 2 | jobs_subset_data['skills_extracts'].str.contains('r')]   
jobs_subset_data.drop(['skills_extracts_length'], axis=1, inplace=True)
jobs_subset_data["combined_features"] = jobs_subset_data["title"] + ", "+ jobs_subset_data["skills_extracts"]

# display(jobs_subset_data)
print(jobs_subset_data.info())

# package as pickle file
with open('../data/jobs_subset_data.pickle', 'wb') as w:
    pickle.dump(jobs_subset_data, w, protocol=pickle.HIGHEST_PROTOCOL)
print("successfully exported jobs_subset_data.pickle file")

# COMMAND ----------

# MAGIC %md #for courses

# COMMAND ----------

# create the course pickle file
course_df = spark.read.table("coursewithdescription")
course_data = course_df.toPandas()

print(course_data['title'].count())

col_subset_course = ['referenceNumber','title','SkillsTaxonomy']
course_subset_data = course_data[col_subset_course] 
course_subset_data = course_subset_data.dropna()
course_subset_data['combined_features'] = course_subset_data['title'].str.lower() + ", " + course_subset_data['SkillsTaxonomy']

course_subset_data['SkillsTaxonomy_length'] = course_subset_data['SkillsTaxonomy'].apply(lambda x: len(x))

# remove courses with less than 2 characters, skills names are usually more than 3 characters
course_subset_data = course_subset_data[course_subset_data['SkillsTaxonomy_length'] >= 2]

course_subset_data.drop(['SkillsTaxonomy_length'], axis=1, inplace=True) ; course_subset_data
course_subset_data['combined_features'] = course_subset_data['title'] + ", " + course_subset_data['SkillsTaxonomy']

with open('../data/course_subset_data.pickle', 'wb') as w:
    pickle.dump(course_subset_data, w, protocol=pickle.HIGHEST_PROTOCOL)
print("successfully course_subset_data.pickle file")


# display(course_subset_data)

# Create the course pickle file

# COMMAND ----------

# MAGIC %md #tfid-generation

# COMMAND ----------

# %sql
# DESCRIBE HISTORY "dbfs:/user/hive/warehouse/coursewithdescription"

# COMMAND ----------

tfidf = TfidfVectorizer(ngram_range=(1, 4)) # up to 4-grams, all combinations of the 4 words
tfidf.fit(course_subset_data['combined_features'])  # fit the vectorizer to the corpus
tfidf_matrix = tfidf.transform(course_subset_data['combined_features'])

# save tfidf_matrix as sparse matrix
scipy.sparse.save_npz('../data/tfidf_matrix.npz', tfidf_matrix) #sparse matrix
print("successfully created tfidf_matrix file in data folder\n")
# print(tfidf_matrix)

# save tfidf pickle file
with open('../data/tfidf.pickle', 'wb') as handle:
    pickle.dump(tfidf, handle, protocol=pickle.HIGHEST_PROTOCOL)
print("successfully created tfidfpickle file in data folder")

# COMMAND ----------

# temp = df_filtered.groupBy(col('posting_date')).count().orderBy(col('posting_date').asc())
# df_count = df_filtered.groupBy(col("posting_date")).agg(sum("count"))

