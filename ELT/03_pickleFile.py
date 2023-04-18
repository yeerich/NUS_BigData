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

with open('./wr_test.pickle', 'wb') as w:
    # Write the DataFrame to the pickle file
    pickle.dump(df, w, protocol=pickle.HIGHEST_PROTOCOL)

# COMMAND ----------

with open('./wr_test.pickle', 'rb') as f:
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


