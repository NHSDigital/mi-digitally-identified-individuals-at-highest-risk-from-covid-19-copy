# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

dbutils.widgets.text('db')
dbutils.widgets.text('line_level_table')

collab_db = dbutils.widgets.get('db')
line_level_table_name = dbutils.widgets.get('line_level_table')

# COMMAND ----------

PID_FIELD = 'nhs_number'
SEX_FIELD = 'gender_desc'
AGE_FIELD = 'age_band'
ETHNIC_FIELD = 'ethnic_group'
DETAILED_ETHNIC_FIELD = 'ethnicity_description'
COHORT_FIELD = 'cohort'
COUNT_FIELD = 'count'

TOTAL_NAME = 'Total'
OUTPUT_COHORT_FIELD = 'Clinical condition group'

# COMMAND ----------

df = spark.table(f'{collab_db}.{line_level_table_name}')

# COMMAND ----------

# MAGIC %md # Prep all conditions row

# COMMAND ----------

df_all = df.agg(F.countDistinct(PID_FIELD).alias(TOTAL_NAME)).select(F.lit(TOTAL_NAME).alias(COHORT_FIELD), TOTAL_NAME)

# COMMAND ----------

def prep_total_counts(df, field):
  
  df_all = df.groupBy(field).agg(F.countDistinct(PID_FIELD).alias('count')).select(field, F.lit(TOTAL_NAME).alias(COHORT_FIELD), 'count')
  
  return df_all.groupBy(COHORT_FIELD).pivot(field).sum()

# COMMAND ----------

df_sex_pivot = prep_total_counts(df, SEX_FIELD).drop('Unknown')

# COMMAND ----------

df_ethnic_pivot = prep_total_counts(df, ETHNIC_FIELD)

# COMMAND ----------

df_age_pivot = prep_total_counts(df, AGE_FIELD)

# COMMAND ----------

df_total_for_union = (
  df_all
  .join(df_sex_pivot, on = COHORT_FIELD, how = 'inner')
  .join(df_age_pivot, on = COHORT_FIELD, how = 'inner')
  .join(df_ethnic_pivot, on = COHORT_FIELD, how = 'inner')
)

# COMMAND ----------

# MAGIC %md # Condition details

# COMMAND ----------

df_condition_all = df.groupBy(COHORT_FIELD).agg(F.countDistinct(PID_FIELD).alias(TOTAL_NAME)).select(COHORT_FIELD, TOTAL_NAME))

# COMMAND ----------

def prep_condition_pivot(df, field):

  df_all = df.groupBy(COHORT_FIELD, field).agg(F.countDistinct(PID_FIELD).alias('count')).select(field, COHORT_FIELD, 'count')
  
  return df_all.groupBy(COHORT_FIELD).pivot(field).sum()

# COMMAND ----------

df_condition_sex = prep_condition_pivot(df, SEX_FIELD).drop('Unknown')

# COMMAND ----------

df_condition_ecat = prep_condition_pivot(df, ETHNIC_FIELD)

# COMMAND ----------

df_condition_age = prep_condition_pivot(df, AGE_FIELD)

# COMMAND ----------

df_condition_counts = (
  df_condition_all
  .join(df_condition_sex, on = COHORT_FIELD, how = 'inner')
  .join(df_condition_age, on = COHORT_FIELD, how = 'inner')
  .join(df_condition_ecat, on = COHORT_FIELD, how = 'inner')
)

# COMMAND ----------

# MAGIC %md # All together for Download

# COMMAND ----------

df_final = df_total_for_union.unionByName(df_condition_counts)

# COMMAND ----------

# export these results to S3 bucket
display(df_final.withColumnRenamed(COHORT_FIELD, OUTPUT_COHORT_FIELD))

# COMMAND ----------


