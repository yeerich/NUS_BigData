# Databricks notebook source
import pandas as pd
import pickle

print(pd.__version__)
print(pickle.format_version)

# COMMAND ----------

# df = spark.read.table("jobswithdescription").toPandas()
df.head(1)

# COMMAND ----------

data_dict = {'Name': ['Alice', 'Bob', 'Charlie','Alex', 'Richard'], 'Age': [25, 30, 35,30, 29]}

# Create a DataFrame from the dictionary
df = pd.DataFrame(data_dict)

# COMMAND ----------

course_df = spark.read.table("coursewithdescription").toPandas()

# COMMAND ----------

with open('./course.pickle', 'wb') as w:
    pickle.dump(course_df, w, protocol=pickle.HIGHEST_PROTOCOL)

# COMMAND ----------

jobs_df = spark.read.table("jobswithdescription").toPandas()
jobs_df = jobs_df[:10000] # there is a size limitation
print(jobs_df.memory_usage(deep=True).sum())

# COMMAND ----------

with open('./jobs.pickle', 'wb') as w:
    pickle.dump(jobs_df, w, protocol=pickle.HIGHEST_PROTOCOL)

# COMMAND ----------

with open('./course.pickle', 'rb') as f:
    # Load the DataFrame from the pickle file
    data = pickle.load(f)
display(data)

# COMMAND ----------

ls

# COMMAND ----------

ls

# COMMAND ----------

# MAGIC %ls

# COMMAND ----------


