# Databricks notebook source
# MAGIC %md Functions

# COMMAND ----------

import datetime as dt
import pandas as pd

import pyspark.sql.functions as F
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.dataframe import DataFrame

# COMMAND ----------

def transform(self, f):
    return f(self)
DataFrame.transform = transform

# COMMAND ----------

def pc(df, pid_field = 'nhs_number'):
  print('count: ' + str(df.count()))
  print('distinct nhs number: ' + str(df.select(pid_field).distinct().count()))

# COMMAND ----------

def add_gender_description_column(gender_code_column):
  return lambda df: (df
    .withColumn(gender_code_column,
      F.when(F.col(gender_code_column).isNull(), 0)
       .when(F.col(gender_code_column) == 9, 0)
       .otherwise(F.col(gender_code_column))
    )
    .withColumn('gender_desc',
      F.when(F.col(gender_code_column) == 1, 'Male')
       .when(F.col(gender_code_column) == 2, 'Female')
       .when(F.col(gender_code_column) == 0, 'Unknown')
       .otherwise('Unknown')
    )   
  )

# COMMAND ----------

def ageband_func(age):
  
  if age is None:
    return None
  
  # almost 12 year olds are cohorted 
  # so include age == 11 in '12 to 16'
  if   age >= 11 and age <=16:    return '12 to 16' 
  elif age >= 17 and age <=19:    return '17 to 19'
  elif age >= 20 and age <=29:    return '20 to 29'
  elif age >= 30 and age <=39:    return '30 to 39'
  elif age >= 40 and age <=49:    return '40 to 49'
  elif age >= 50 and age <=59:    return '50 to 59'
  elif age >= 60 and age <=69:    return '60 to 69'
  elif age >= 70 and age <=79:    return '70 to 79'
  elif age >= 80 and age <=89:    return '80 to 89'
  elif age >= 90 and age <=120:    return '90+'
  else: return None
  
ageband_udf = udf(ageband_func)
 
def add_age_band_pub(df_data, age_col, output_col_name = "age_band"):
  return df_data.select("*", ageband_udf(age_col).alias(output_col_name))
