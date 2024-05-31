#!/usr/bin/env python
# coding: utf-8

# ## Migrating from Spark to BigQuery via Dataproc -- Part 1
# 
# * [Part 1](01_spark.ipynb): The original Spark code, now running on Dataproc (lift-and-shift).
# * [Part 2](02_gcs.ipynb): Replace HDFS by Google Cloud Storage. This enables job-specific-clusters. (cloud-native)
# * [Part 3](03_automate.ipynb): Automate everything, so that we can run in a job-specific cluster. (cloud-optimized)
# * [Part 4](04_bigquery.ipynb): Load CSV into BigQuery, use BigQuery. (modernize)
# * [Part 5](05_functions.ipynb): Using Cloud Functions, launch analysis every time there is a new file in the bucket. (serverless)
# 

# ### Copy data to HDFS
# 
# The Spark code in this notebook is based loosely on the [code](https://github.com/dipanjanS/data_science_for_all/blob/master/tds_spark_sql_intro/Working%20with%20SQL%20at%20Scale%20-%20Spark%20SQL%20Tutorial.ipynb) accompanying [this post](https://opensource.com/article/19/3/apache-spark-and-dataframes-tutorial) by Dipanjan Sarkar. I am using it to illustrate migrating a Spark analytics workload to BigQuery via Dataproc.
# 
# The data itself comes from the 1999 KDD competition. Let's grab 10% of the data to use as an illustration.

# ### Reading in data
# 
# The data are CSV files. In Spark, these can be read using textFile and splitting rows on commas.

# In[34]:


get_ipython().run_cell_magic('writefile', 'spark_analysis.py', '\nimport matplotlib\nmatplotlib.use(\'agg\')\n\nimport argparse\nparser = argparse.ArgumentParser()\nparser.add_argument("--bucket", help="bucket for input and output")\nargs = parser.parse_args()\n\nBUCKET = args.bucket\n')


# In[36]:


get_ipython().run_cell_magic('writefile', '-a spark_analysis.py', '\nfrom pyspark.sql import SparkSession, SQLContext, Row\n\ngcs_bucket=\'iot-project-1-416514\'\nspark = SparkSession.builder.appName("kdd").getOrCreate()\nsc = spark.sparkContext\ndata_file = "gs://"+gcs_bucket+"//kddcup.data_10_percent.gz"\nraw_rdd = sc.textFile(data_file).cache()\nraw_rdd.take(5)\n')


# In[38]:


get_ipython().run_cell_magic('writefile', '-a spark_analysis.py', '\ncsv_rdd = raw_rdd.map(lambda row: row.split(","))\nparsed_rdd = csv_rdd.map(lambda r: Row(\n    duration=int(r[0]), \n    protocol_type=r[1],\n    service=r[2],\n    flag=r[3],\n    src_bytes=int(r[4]),\n    dst_bytes=int(r[5]),\n    wrong_fragment=int(r[7]),\n    urgent=int(r[8]),\n    hot=int(r[9]),\n    num_failed_logins=int(r[10]),\n    num_compromised=int(r[12]),\n    su_attempted=r[14],\n    num_root=int(r[15]),\n    num_file_creations=int(r[16]),\n    label=r[-1]\n    )\n)\nparsed_rdd.take(5)\n')


# ### Spark analysis
# 
# One way to analyze data in Spark is to call methods on a dataframe.

# In[40]:


get_ipython().run_cell_magic('writefile', '-a spark_analysis.py', "\nsqlContext = SQLContext(sc)\ndf = sqlContext.createDataFrame(parsed_rdd)\nconnections_by_protocol = df.groupBy('protocol_type').count().orderBy('count', ascending=False)\nconnections_by_protocol.show()\n")


# Another way is to use Spark SQL

# In[42]:


get_ipython().run_cell_magic('writefile', '-a spark_analysis.py', '\ndf.registerTempTable("connections")\nattack_stats = sqlContext.sql("""\n                           SELECT \n                             protocol_type, \n                             CASE label\n                               WHEN \'normal.\' THEN \'no attack\'\n                               ELSE \'attack\'\n                             END AS state,\n                             COUNT(*) as total_freq,\n                             ROUND(AVG(src_bytes), 2) as mean_src_bytes,\n                             ROUND(AVG(dst_bytes), 2) as mean_dst_bytes,\n                             ROUND(AVG(duration), 2) as mean_duration,\n                             SUM(num_failed_logins) as total_failed_logins,\n                             SUM(num_compromised) as total_compromised,\n                             SUM(num_file_creations) as total_file_creations,\n                             SUM(su_attempted) as total_root_attempts,\n                             SUM(num_root) as total_root_acceses\n                           FROM connections\n                           GROUP BY protocol_type, state\n                           ORDER BY 3 DESC\n                           """)\nattack_stats.show()\n')


# In[43]:


get_ipython().run_cell_magic('writefile', '-a spark_analysis.py', "\nax = attack_stats.toPandas().plot.bar(x='protocol_type', subplots=True, figsize=(10,25))\n")


# In[44]:


get_ipython().run_cell_magic('writefile', '-a spark_analysis.py', "\nax[0].get_figure().savefig('report.png');\n")


# In[45]:


get_ipython().run_cell_magic('writefile', '-a spark_analysis.py', "\nimport google.cloud.storage as gcs\nbucket = gcs.Client().get_bucket(BUCKET)\nfor blob in bucket.list_blobs(prefix='sparktodp/'):\n    blob.delete()\nbucket.blob('sparktodp/report.png').upload_from_filename('report.png')\n")


# In[46]:


get_ipython().run_cell_magic('writefile', '-a spark_analysis.py', '\nconnections_by_protocol.write.format("csv").mode("overwrite").save(\n    "gs://{}/sparktodp/connections_by_protocol".format(BUCKET))\n')


# In[47]:


BUCKET_list = get_ipython().getoutput("gcloud info --format='value(config.project)'")
BUCKET=BUCKET_list[0]
print('Writing to {}'.format(BUCKET))
get_ipython().system('/opt/conda/miniconda3/bin/python spark_analysis.py --bucket=$BUCKET')


# In[48]:


get_ipython().system('gcloud storage ls gs://$BUCKET/sparktodp/**')


# In[49]:


get_ipython().system('gcloud storage cp spark_analysis.py gs://$BUCKET/sparktodp/spark_analysis.py')


# Copyright 2019 Google Inc. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
