# Databricks notebook source
import pandas as pd
import pickle
from pyspark.sql.functions import col, to_date, current_date, date_sub

print(pd.__version__)
print(pickle.format_version)

# COMMAND ----------

ls

# COMMAND ----------

j_df = spark.read.table("jobswithdescription")
# uncomment and run if you want to know the breakdown of the posting_date
# j_df.groupBy(col('posting_date')).count().orderBy(col('posting_date').asc()).display()

j_df = j_df.withColumn("posting_date", to_date(col("posting_date"), "yyyyMMdd"))
j_df_filtered = j_df.filter((col("posting_date").isNotNull()) & (col("posting_date").between(date_sub(current_date(), 7), current_date())))
# print(j_df_filtered.count())
# display(j_df_filtered)


j_df_pickle = j_df_filtered.toPandas()
print(j_df_pickle['posting_date'].count())
# package as pickle file
with open('../data/jobs_subset_data.pickle', 'wb') as w:
    pickle.dump(j_df_pickle, w, protocol=pickle.HIGHEST_PROTOCOL)
print("successfully exported jobs_subset_data.pickle file")

# COMMAND ----------

# MAGIC %md #for courses

# COMMAND ----------

c_df = spark.read.table("coursewithdescription")
# # c_df.display()
# # print(c_df.columns)

c_df_filtered = c_df.filter(col("referenceNumber").isNotNull())
# print(c_df_filtered.count())
# # # # display(j_df_filtered)


c_df_pickle = c_df_filtered.toPandas()

# package as pickle file
with open('../data/course_subset_data.pickle', 'wb') as w:
    pickle.dump(c_df_pickle, w, protocol=pickle.HIGHEST_PROTOCOL)
print("successfully course_subset_data.pickle file")

# COMMAND ----------

# temp = df_filtered.groupBy(col('posting_date')).count().orderBy(col('posting_date').asc())
# df_count = df_filtered.groupBy(col("posting_date")).agg(sum("count"))

