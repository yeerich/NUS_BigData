# Databricks notebook source
course_pickle = spark.read.table("jobswithdescription")
# save data for future use
with open('course_subset_data.pickle', 'wb') as handle:
    pickle.dump(course_subset_data, handle, protocol=pickle.HIGHEST_PROTOCOL)

rdd = sc.parallelize(course_subset_data)
rdd.saveAsPickleFile('course_subset_data.pickle')
