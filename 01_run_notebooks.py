# Databricks notebook source
# MAGIC %md 
# MAGIC **STEP 1**
# MAGIC 
# MAGIC This notebook runs the pipeline that creates the line-level table for further analysis

# COMMAND ----------

import datetime as dt

from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text('batch', '')
dbutils.widgets.text('input_db', '')
dbutils.widgets.text('output_table_prefix', '')

# COMMAND ----------

collab_db = dbutils.widgets.get('input_db')
batch = dbutils.widgets.get('batch').lower()
output_table_prefix = dbutils.widgets.get('output_table_prefix')

# COMMAND ----------

batch_mailing_param = {
  "db": collab_db,
  "batch": batch,
  "output_table_prefix": output_table_prefix
}

# COMMAND ----------

# Return patient details view
line_level_table = dbutils.notebook.run( './notebooks/create_line_level_table', 
                                        0, {**batch_mailing_param} )

if collab_db not in line_level_table:
  line_level_table = f"{collab_db}.{line_level_table}"
print(line_level_table)

# COMMAND ----------


