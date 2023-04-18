# Databricks notebook source
pip install -q requests_oauthlib spacy skillNer

# COMMAND ----------

!python -m spacy download en_core_web_lg -q

# COMMAND ----------

from requests_oauthlib import OAuth2Session
import json
import requests
import pandas as pd
from pandas.io.json import json_normalize
from urllib.request import urlopen, Request
import requests
import re
from datetime import datetime , timedelta

from pyspark.sql.functions import col, current_date, StringType

import spacy
from spacy.matcher import PhraseMatcher
# load default skills data base
from skillNer.general_params import SKILL_DB
# import skill extractor
from skillNer.skill_extractor_class import SkillExtractor

# COMMAND ----------

# init params of skill extractor
nlp = spacy.load("en_core_web_lg")
# init skill extractor
skill_extractor = SkillExtractor(nlp, SKILL_DB, PhraseMatcher)

# COMMAND ----------

today = datetime.today()
now = datetime.now()

# format the date as yyyymmdd
today_string = today.strftime('%Y%m%d')
print("today's date \t\t:",today_string)

one_year_ago = today - timedelta(days=365)

# format the date as yyyy-mm-dd
one_year_ago_string = one_year_ago.strftime('%Y%m%d')
print("effective cutoff date \t:",one_year_ago_string)

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
        # print("SkillsTaxonomy \t:",output)
        # print(combined_doc_node_values)
    except:
        output = ''
    return output

# COMMAND ----------

try:
    request_url = 'https://public-api.ssg-wsg.sg/courses/directory?'
    client_id = '680c1489f1594eb0b0c2223524d0cb83'
    client_secret = 'ZDI4YzliMDMtNjQ5Mi00MDJhLWE4ZTctZjEzNzU2MmE1YjYw'

    "function to fetch the token from the url inserted"
    from oauthlib.oauth2 import BackendApplicationClient

    client = BackendApplicationClient(client_id=client_id)
    oauth = OAuth2Session(client=client)
    token = oauth.fetch_token(
        token_url='https://public-api.ssg-wsg.sg/dp-oauth/oauth/token',
        client_id=client_id,
        client_secret=client_secret
    )
    

    #Empty Dataframe
    dfs = []

    #Start loop at 0
    page = 0
    #keyword = 'data'
    pageSize = 100
    

    #     Get the number of pages to run
    response =  oauth.get(request_url + f"&pageSize={pageSize}"+ f"&page={page}" + f"&taggingCodes=FULL" + f"&courseSupportEndDate={one_year_ago_string}" "&retrieveType=FULL" ).json()
    total_courses_count = response['meta']['total']
    print("total number of effective courses \t:",total_courses_count)
    total_number_of_pages = (total_courses_count+99)//100 # total number of pages
    print("total number of pages to run \t\t:",total_number_of_pages)
    
    
    while page <= 0: #(total_number_of_pages-1): # uncomment this for the complete list of 
      response =  oauth.get(request_url + f"&pageSize={pageSize}"+ f"&page={page}" + f"&taggingCodes=FULL" + f"&courseSupportEndDate={one_year_ago_string}" "&retrieveType=FULL" ).json()
      print("Current Page: \t",page)
      #Access the list in the json payload
      course_data = response['data']['courses']
      #Convert to dataframe
      course_df = pd.json_normalize(course_data)

      #Specify Selected Column Names  
      course_df =  course_df.loc[:, course_df.columns.isin(['seoName', 'title', 'taggings', 'tags', 'content', 'areaOfTrainings','fieldOfStudies1', 'fieldOfStudies2', 'objective','specialisation','outcome', 'referenceNumber', 'skillsConnectReferenceNumber', 'skillsFrameworks', 'skillsFutureCreditReferenceNumber'])]

      #
      dfs.append(course_df)
      page += 1

except:
    #print("An error has occurred. Please check data input")
    pass

#Union everything

combined = pd.concat(dfs)

# COMMAND ----------

# count=1
# if response['status'] ==200:
#     try:
#         print("action code here")
#     except:
#         count-=1
#         time.sleep(100)
#         print("Maxed out API call for this minute, sleeping for 100 seconds")

# COMMAND ----------

# def getTotalJobs():
#     url = 'https://api.mycareersfuture.gov.sg/v2/search?limit=100&page=0' 
#     payload = {"search":keyword_search,"sortBy":["new_posting_date"]}
#     headers = {
#         'Content-Type': 'application/json',
#         'Mcf-Client': 'jobseeker',
#         'Origin': 'https://www.mycareersfuture.gov.sg',
#         'Referer': 'https://www.mycareersfuture.gov.sg/'}
#     response = requests.post(url, headers=headers, data=json.dumps(payload))
#     response_json = response.json()
#     total_jobs_count = response_json['countWithoutFilters']
#     print("total number of jobs avialable:",total_jobs_count)       #total number of jobs
#     return(total_jobs_count)


# COMMAND ----------

###big data distributed compute starts here

skillsFuture_raw = combined.astype(str) #change to all strings
skillsFuture_raw['load_date'] = now.strftime("%Y%m%d")
skillsFuture_raw.dtypes #confirm if all columns attributes are string




# spark_skillsFuture.write.format("parquet").partitionBy("load_date").save("dbfs:/FileStore/tables/bronze_skillsFuture/")  # this is the bronze layer parquet file 



spark_skillsFuture = spark.createDataFrame(skillsFuture_raw)  #make it into a spark df
spark_skillsFuture_cleaned = spark_skillsFuture.dropDuplicates(['referenceNumber']).dropna(subset=['referenceNumber']) #drop the duplicates
print("total number of unique course reference number \t:",spark_skillsFuture_cleaned.select('referenceNumber').distinct().count())
# print(spark_skillsFuture_cleaned.columns)

# check if Delta table exists, else create the silver course table
if spark.catalog._jcatalog.tableExists("courseData"):
    print("\n courseData table exists")
else:
    # create Delta table
    spark_skillsFuture_cleaned.write.format("delta").saveAsTable("courseData")  # create the table
    print("\n courseData table created")

#this runs correctly when the gold layer table is avail
base_skillFutureDescription_table = spark.read.table('coursewithdescription') #read data from delta table (gold layer table -  skillsFuture course data)

df_filtered = spark_skillsFuture_cleaned.join(base_skillFutureDescription_table, on='referenceNumber', how='left_anti') #perform a join statement which returns only referenceNumber not found in gold layer table

# @@@@@@@@@@@@@@@@@@@ this used as a bypass, just in case that the courses are not updated @@@@@@@@@@@@@@@@@@@
# df_filtered = spark_skillsFuture_cleaned  
# @@@@@@@@@@@@@@@@@@@ this used as a bypass, just in case that the courses are not updated @@@@@@@@@@@@@@@@@@@

if df_filtered.count() != 0: #check if there are any new records
    df_filtered.display()
    
    df_filtered.write.format("delta").mode("append").save("dbfs:/user/hive/warehouse/coursedata")
    print("perform the decription extraction here...")
    
    skillcourse = df_filtered.toPandas()
    skillcourse['concat'] = skillcourse['objective'].map(str) + '' + skillcourse['title'].map(str) + '' + skillcourse['fieldOfStudies1'].map(str) + '' + skillcourse['content'].map(str) 
    skillcourse["SkillsTaxonomy"] = skillcourse['objective'].apply(lambda x: extractSkills(x)) #extract the skills
    print("SkillsTaxonomy successfully extracted")
    try:
        spark_skillcourse = spark.createDataFrame(skillcourse)
        try:
            spark_skillcourse = spark_skillcourse.drop('load_date')
            print("load_date column successfully dropped")
            spark_skillcourse = spark_skillcourse.select([col(c).cast(StringType()) for c in df.columns])
            # print(spark_skillcourse.printSchema())
            spark_skillcourse.write.format("delta").mode("append").save("dbfs:/user/hive/warehouse/coursewithdescription")
            print("successfully appended new delta course data into gold table")
        except:
            spark_skillcourse = spark_skillcourse.select([col(c).cast(StringType()) for c in df.columns])
            # print(spark_skillcourse.printSchema())
            spark_skillcourse.write.format("delta").mode("append").save("dbfs:/user/hive/warehouse/coursewithdescription")
            print("successfully appended new delta course data into gold table")
    except: print("spark df contains error")
else:
    print("\n there isnt any new course available.")
    print("ending program...")

# COMMAND ----------

# %sql
# describe history  coursewithdescription;
# RESTORE coursewithdescription TO VERSION AS OF 0
