# Databricks notebook source
pip install -q spacy skillNer bs4

# COMMAND ----------

!python -m spacy download en_core_web_lg -q

# COMMAND ----------

import requests
import json
import pandas as pd
import datetime
import math
import time
import random
import re
import warnings
import os
import spacy
import bs4
from bs4 import BeautifulSoup
from spacy.matcher import PhraseMatcher
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import explode
from pyspark.sql.functions import concat


# load default skills data base
from skillNer.general_params import SKILL_DB
# import skill extractor
from skillNer.skill_extractor_class import SkillExtractor



from pyspark.sql.functions import col,concat_ws
from pyspark.sql.types import StringType

warnings.filterwarnings('ignore')
pd.set_option('display.max_colwidth', 100)


# COMMAND ----------

# init params of skill extractor
nlp = spacy.load("en_core_web_lg")
# init skill extractor
skill_extractor = SkillExtractor(nlp, SKILL_DB, PhraseMatcher)

# COMMAND ----------

def clean_html(html):
    soup = BeautifulSoup(html, 'html.parser')
    cleaned_text = soup.get_text()
    cleaned_text = cleaned_text.replace('&', 'and')
    cleaned_text = re.sub(r'[^a-zA-Z0-9\s]', '', cleaned_text)
    cleaned_text = cleaned_text.replace('\n\n', ' ')
    cleaned_text = cleaned_text.replace('\n', ' ')
    cleaned_text = cleaned_text.replace('  ', ' ')
    cleaned_text = cleaned_text.lower()
    return cleaned_text


def getJobDescription(uuid):
    url = 'https://api.mycareersfuture.gov.sg/v2/jobs/{}'.format(uuid)
    payload = {""}
    headers = {
        'Content-Type': 'application/json',
        'Mcf-Client': 'jobseeker',
        'Origin': 'https://www.mycareersfuture.gov.sg',
        'Referer': 'https://www.mycareersfuture.gov.sg/'}
    response = requests.get(url, params = headers)
    response_json = response.json()
    cleaned_description = clean_html(response_json['description'])
    return(cleaned_description)

# Define the UDF
@udf(returnType=StringType())
def job_description_udf(uuid):
    url = 'https://api.mycareersfuture.gov.sg/v2/jobs/{}'.format(uuid)
    payload = {""}
    headers = {
        'Content-Type': 'application/json',
        'Mcf-Client': 'jobseeker',
        'Origin': 'https://www.mycareersfuture.gov.sg',
        'Referer': 'https://www.mycareersfuture.gov.sg/'}
    response = requests.get(url, params=headers)
    response_json = response.json()
    cleaned_description = clean_html(response_json['description'])
    return cleaned_description



# print(getJobDescription("ea7e1b236eb25b54c60b92455743d994")[:300])

testDesc = getJobDescription("ea7e1b236eb25b54c60b92455743d994")
print(testDesc)

# COMMAND ----------

def extractSkills(job_description): 
    try:
        annotations = skill_extractor.annotate(job_description)
        # print(annotations['results']['ngram_scored'])
        full_matches_doc_node_values = [match['doc_node_value'] for match in annotations['results']['full_matches']]
        ngram_scored_doc_node_values = [match['doc_node_value'] for match in annotations['results']['ngram_scored']]
        # print(ngram_scored_doc_node_values)
        combined_doc_node_values = set(full_matches_doc_node_values + ngram_scored_doc_node_values)
        output = ', '.join(map(str, combined_doc_node_values))
        print("SkillsTaxonomy \t:",output)
        # print(combined_doc_node_values)
        assert len(output) > 0, "Output must not be empty"
    except Exception as e:
        print("Error: ", str(e))
        output = ''
    return output
extract_skills_udf = udf(extractSkills, StringType())

extractSkills(testDesc)

# COMMAND ----------

# MAGIC %%time
# MAGIC # get the current date and time
# MAGIC now = datetime.datetime.now()
# MAGIC 
# MAGIC # format the current date and time
# MAGIC formatted_date = now.strftime("%Y-%b-%d_%H-%M-%S")
# MAGIC # load_date_time = now.strftime("%d%m%Y")
# MAGIC load_date_time = now.strftime("%Y%m%d")
# MAGIC # print the formatted date and time
# MAGIC print("load_date_time: ", load_date_time)  
# MAGIC 
# MAGIC # declare parameters
# MAGIC page_job_number = 0
# MAGIC page = 0
# MAGIC current_job_number = 0
# MAGIC jobs_shown_limit = 100 # max at 100 jobs per page, else will break the api call
# MAGIC keyword_search = ''
# MAGIC 
# MAGIC jobs = None #clear the jobs dataframe
# MAGIC columns = [
# MAGIC     'job_id',
# MAGIC     'title',
# MAGIC     'company_name',
# MAGIC     'uen',
# MAGIC     'employment_type',
# MAGIC     'level',
# MAGIC     'category',
# MAGIC     'salary_min',
# MAGIC     'salary_max',
# MAGIC     'salary_type',
# MAGIC     'skills',
# MAGIC     'job_uuid'
# MAGIC ]
# MAGIC jobs = pd.DataFrame(columns=columns)
# MAGIC print('the shape of jobs df: ',jobs.shape)
# MAGIC 
# MAGIC 
# MAGIC def getTotalJobs():
# MAGIC     url = 'https://api.mycareersfuture.gov.sg/v2/search?limit=100&page=0' 
# MAGIC     payload = {"search":keyword_search,"sortBy":["new_posting_date"]}
# MAGIC     headers = {
# MAGIC         'Content-Type': 'application/json',
# MAGIC         'Mcf-Client': 'jobseeker',
# MAGIC         'Origin': 'https://www.mycareersfuture.gov.sg',
# MAGIC         'Referer': 'https://www.mycareersfuture.gov.sg/'}
# MAGIC     response = requests.post(url, headers=headers, data=json.dumps(payload))
# MAGIC     response_json = response.json()
# MAGIC     total_jobs_count = response_json['countWithoutFilters']
# MAGIC     print("total number of jobs avialable:",total_jobs_count)       #total number of jobs
# MAGIC     return(total_jobs_count)
# MAGIC 
# MAGIC 
# MAGIC total_number_pages = (getTotalJobs()+99) // 100
# MAGIC print('total number of pages to run: ',total_number_pages,'\n\n')
# MAGIC print('-------------------------------------------------------------')
# MAGIC 
# MAGIC 
# MAGIC total_number_pages =6 #(total_number_pages-1) # extracting 1000 records everyday on a to get the delta jobs
# MAGIC for page in range(0,total_number_pages-1):
# MAGIC # declare the post request
# MAGIC     url = 'https://api.mycareersfuture.gov.sg/v2/search?limit={}&page={}'.format(jobs_shown_limit, page)
# MAGIC 
# MAGIC     payload = {"search":keyword_search,"sortBy":["new_posting_date"]}
# MAGIC     headers = {
# MAGIC         'Content-Type': 'application/json',
# MAGIC         'Mcf-Client': 'jobseeker',
# MAGIC         'Origin': 'https://www.mycareersfuture.gov.sg',
# MAGIC         'Referer': 'https://www.mycareersfuture.gov.sg/'
# MAGIC     }
# MAGIC 
# MAGIC     response = requests.post(url, headers=headers, data=json.dumps(payload))
# MAGIC     response_json = response.json() # convert the response to json for feature extraction
# MAGIC     # print(response_json)  # print the whole json for
# MAGIC 
# MAGIC     for current_job_number in range(0,100): #single page is limited to 100 jobs inforamtion
# MAGIC         job_id = response_json['results'][current_job_number]['metadata']['jobPostId']   #jobPostId
# MAGIC         title = response_json['results'][current_job_number]['title']   #title
# MAGIC         company_name = response_json['results'][current_job_number]['postedCompany']["name"]     # posted-conmpany name
# MAGIC         uen = response_json['results'][current_job_number]['postedCompany']["uen"] 
# MAGIC         employment_type = response_json['results'][current_job_number]['employmentTypes'][0]["employmentType"]  #employmentType , Permanent
# MAGIC         level = response_json['results'][current_job_number]['positionLevels'][0]["position"] #Non-executive
# MAGIC         category =response_json['results'][current_job_number]['categories'][0]['category'] 
# MAGIC         salary_min = response_json['results'][current_job_number]['salary']['minimum'] #3000
# MAGIC         salary_max = response_json['results'][current_job_number]['salary']['maximum'] #5500
# MAGIC         salary_type = response_json['results'][current_job_number]['salary']['type']['salaryType'] #monthly
# MAGIC         job_uuid = response_json['results'][current_job_number]['uuid']
# MAGIC   
# MAGIC         # get the list of skills
# MAGIC         skills = []
# MAGIC         for item in response_json['results'][current_job_number]['skills']:
# MAGIC             skills.append(item['skill'])
# MAGIC         # print(skills)
# MAGIC         page_job_number +=1
# MAGIC 
# MAGIC         jobs.loc[page_job_number] = [job_id, title, company_name, uen, employment_type, level, category, salary_min, salary_max, salary_type, skills,job_uuid]
# MAGIC         print("current job number = {}, current page: {}, current job number: {}".format(page_job_number, page, current_job_number))
# MAGIC jobs["load_date"] = load_date_time
# MAGIC # display(jobs)

# COMMAND ----------

spark_jobs = spark.createDataFrame(jobs)
# spark_jobs.describe().display()
spark_jobs = spark_jobs.withColumn('skills', col('skills').cast(StringType()))
spark_jobs = spark_jobs.withColumn('skills', concat_ws(',', 'skills'))
try:
    spark_jobs.write.format("parquet").mode("append").partitionBy("load_date").save("dbfs:/FileStore/tables/bronze_careersFuture/")  # this is the bronze layer parquet file 
    print("successfully loaded to bronze layer partitioned by: ",load_date_time)
except: print("already loaded today \t:", load_date_time)

# COMMAND ----------

# start jobs silver layer ingestion

spark_jobs_df = spark_jobs.dropDuplicates(['job_id']).dropna(subset=['job_id']) #de-dup dataset
# spark_jobs_df.describe().display()
print("total number of unique jobs \t:",spark_jobs_df.select('job_id').distinct().count())



    
base_jobs_table = spark.read.table('jobswithdescription') #read data from delta table (gold layer table jobswithdescription)

#perform a join statement which returns only jobs_id not found in gold layer table
df_filtered_jobs = spark_jobs_df.join(base_jobs_table,spark_jobs_df.job_id == base_jobs_table.jobPostId , how='left_anti') 
# df_filtered_jobs.display()

print("delta jobs count: ",df_filtered_jobs.count())

if df_filtered_jobs.count() != 0: #if there is delta record is not null
    df_filtered_jobs = spark_jobs.withColumn("job_description", job_description_udf(spark_jobs["job_uuid"])) #generate the jobs_description using the API
    df_filtered_jobs = df_filtered_jobs.withColumn("combined_description", concat_ws(" ", df_filtered_jobs["skills"], df_filtered_jobs["job_description"]))

    # check if silver layer delta table exists, else create the silver jobs table
    if spark.catalog._jcatalog.tableExists("jobsdata"):
        df_filtered_jobs.write.format("delta").mode("append").save("dbfs:/user/hive/warehouse/jobsdata")
        print("\n delta data appended to jobsdata table")
    else:
        # create Delta table
        df_filtered_jobs.write.format("delta").saveAsTable("jobsdata")  # create the table
        print("\n jobsdata table created")

# gold layer processing
print("starting gold layer ingestion")
gold_jobs = df_filtered_jobs.select("job_id",'title',"skills","job_description","combined_description")
gold_jobs = gold_jobs.withColumnRenamed("job_id", "jobPostId").withColumnRenamed("skills", "requiredSkills")

#execute the skills extraction
gold_jobs = gold_jobs.withColumn("skills_extracted", extract_skills_udf(gold_jobs["combined_description"]))

gold_jobs = gold_jobs.withColumnRenamed("job_description", "description").withColumnRenamed("skills_extracted", "skills_extracts")
gold_jobs.write.format("delta").mode("append").save("dbfs:/user/hive/warehouse/jobswithdescription") #append delta records to the gold table

else: print("all jobs records are in the gold table, there is no delta data")
