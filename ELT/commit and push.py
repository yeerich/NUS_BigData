# Databricks notebook source
# MAGIC %sh
# MAGIC git commit -m "to commit and push automatically"

# COMMAND ----------

pwd

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /Workspace/Repos/e0938906@u.nus.edu/Big_data_final/
# MAGIC git add .
# MAGIC git commit -m "Automated commit message"
# MAGIC git push origin $(git rev-parse --abbrev-ref HEAD)

# COMMAND ----------

# MAGIC %sh 
# MAGIC git rev-parse --show-toplevel
