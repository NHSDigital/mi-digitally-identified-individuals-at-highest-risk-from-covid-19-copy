# Databricks notebook source
import datetime as dt
import functools as ft

from pyspark.sql import functions as F
from pyspark import StorageLevel

import nhsdccl.util as U

# COMMAND ----------

# MAGIC %run ./util

# COMMAND ----------

# MAGIC %run "./data_sources"

# COMMAND ----------

# MAGIC %md
# MAGIC # Parameters

# COMMAND ----------

# Tcolumn names
PID_FIELD = 'nhs_number'
SEX_FIELD = 'gender_desc'
AGE_FIELD = 'age_band'
ETHNIC_FIELD = 'ethnic_group'
DETAILED_ETHNIC_FIELD = 'ethnicity_description'
VSTATUS_FIELD = 'vaccine_status'
COHORT_FIELD = 'cohort'
COUNT_FIELD = 'count'

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text('db', '')
dbutils.widgets.text('batch', '')
dbutils.widgets.text('output_table_prefix', '')

# COMMAND ----------

# get widget parameters
db = dbutils.widgets.get('db')
batch = dbutils.widgets.get('batch').lower()
output_table_prefix = dbutils.widgets.get('output_table_prefix')

# COMMAND ----------

# info for output table name
now = dt.datetime.now()
run_time_str = now.strftime("%Y%m%d%H%M")

# input table name
table = f"{INPUT_TABLE_PREFIX}_{batch}_{INPUT_TABLE_SUFFIX}"

# COMMAND ----------

# MAGIC %md # Process data

# COMMAND ----------

# read table with snapshot of cohort
df_mabs = spark.table(f'{db}.{table}')

# get the time that the batch is from for the output table name
job_id = df_mabs.select('job_id').collect()[0][0]
cohort_date = job_id[0:8]
print(job_id, cohort_date)

# COMMAND ----------

df_condition_flags = (df_mabs
  .select(
    F.col('nhsnumber').alias(PID_FIELD),
    *condition_columns # defined in data_sources.py
  )
)

cols_to_select = [
  'job_id',
  F.col('nhsnumber').alias(PID_FIELD),
  'dateofbirth',
  'age',
  'postcode',
  'conditions'
]
  
df_mabs = df_mabs.select(*cols_to_select)

# COMMAND ----------

# MAGIC %md
# MAGIC # Get demographics data

# COMMAND ----------

df_mabs_pds = (df_mabs
  .join(pds_df, on = PID_FIELD, how = "left")
)

intermediate_table = f"{output_table_prefix}_temp_{batch}_{run_time_str}".lower()

# for efficiency - save table so far
U.create_table(spark, df = df_mabs_pds, db_or_asset = db, table = intermediate_table,
                 overwrite = True,
                 owner = db)

# COMMAND ----------

df_mabs_pds = spark.table(f'{db}.{intermediate_table}')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Gender

# COMMAND ----------

df_mabs_output_with_gender = (df_mabs_pds
  .transform(add_gender_description_column('gender_code'))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Age bands

# COMMAND ----------

df_mabs_output_with_age_bands = (df_mabs_output_with_gender
  .join(age_band_df, 'age', how="left")
  .na.fill("Unknown", ["age_band"])
  .drop('age', 'value')
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ethnicity

# COMMAND ----------

df_mabs_output_with_ethnicity = (df_mabs_output_with_age_bands
  .join(ethnicity_linkage_with_categories, on = ["nhs_number"], how = "left")
  .na.fill("Not Stated", [DETAILED_ETHNIC_FIELD, ETHNIC_FIELD])
)

# COMMAND ----------

df_mabs_with_demographics = df_mabs_output_with_ethnicity

# COMMAND ----------

# MAGIC %md # Explode Conditions

# COMMAND ----------

df_nmabs_algo_map_groups_exploded = (df_condition_flags
  .select(
    'nhs_number',
    F.explode(
      F.split(
        F.concat_ws(',',
          *[F.when(F.col(condition_column) == 1, F.lit(condition_column)) for condition_column in condition_columns],
        ),
        ','
      )
    ).alias('algo_map_group')
  )
)                 

df_nmabs_conditions_mapped_to_cohorts = (df_nmabs_algo_map_groups_exploded
  .join(algo_map_group_cohort_mapping, 'algo_map_group', 'left')
)

# COMMAND ----------

# MAGIC %md # Create table

# COMMAND ----------

output_table = f"{output_table_prefix}_line_level_eligible_cohort_batch_{batch}_{cohort_date}_{run_time_str}"
print(output_table)

final_cols = [PID_FIELD, SEX_FIELD, AGE_FIELD, DETAILED_ETHNIC_FIELD, ETHNIC_FIELD, COHORT_FIELD, 'algo_map_group']

df_final = df_mabs_with_demographics.join(df_nmabs_conditions_mapped_to_cohorts, on = [PID_FIELD], how = 'inner').select(final_cols)


# COMMAND ----------

U.create_table(spark, df = df_final, db_or_asset = db, table = output_table,
                 overwrite = True,
                 owner = db)

dbutils.notebook.exit(output_table)
