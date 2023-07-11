# Databricks notebook source
import pyspark.sql.functions as F
import pyspark.sql.types as T

# COMMAND ----------

# MAGIC %md ## Condition mapping

# COMMAND ----------

condition_columns = [
  'has_downs_syndrome',
  'has_sickle_cell_immuno_def',
  'has_solid_organ_transplant',
  'has_immunosuppression_disorder',
  'has_immunosuppression_drugs',
  'has_immunosuppression_steroids_3months',
  'has_immunosuppression_steroids_12months',
  'has_radio_last_6_months',
  'has_cirrhosis_liver_gp',
  'has_cirrhosis_liver_hes',
  'has_ckd_4_5_or_dialysis_gp',
  'has_ckd_4_5_or_dialysis_hes',
  'has_rare_neurological_disease',
  'has_myelodysplastic_syndrome',
  'has_haematological_malignancy_gp',
  'has_haematological_malignancy_hes',
  'has_haematological_malignancy_chemo_hes',
  'has_chemotherapy_any_code_sact_12months',
  'has_chemotherapy_hes_3months',
  'has_chemotherapy_any_code_sact_3months',
  'has_chemotherapy',
  'has_immunosuppression',
  'has_cirrhosis_liver',
  'has_ckd_4_5_or_dialysis',
]

algo_map_group_cohort_mapping = spark.createDataFrame([
  ('has_downs_syndrome', "Down's syndrome and chromosomal disorders known to affect immune competence"),
  ('has_sickle_cell_immuno_def', 'Sickle cell disease'),
  ('has_solid_organ_transplant', 'Organ transplant'),
  ('has_immunosuppression_disorder', 'Conditions affecting the immune system, including HIV/AIDS'),
  ('has_immunosuppression_drugs', 'Autoimmune or inflammatory conditions'),
  ('has_immunosuppression_steroids_3months', 'Autoimmune or inflammatory conditions'),
  ('has_immunosuppression_steroids_12months', 'Autoimmune or inflammatory conditions'),
  ('has_radio_last_6_months', 'Cancer'),
  ('has_cirrhosis_liver_gp', 'Severe liver disease'),
  ('has_cirrhosis_liver_hes', 'Severe liver disease'),
  ('has_ckd_4_5_or_dialysis_gp', 'Chronic kidney disease (CKD)'),
  ('has_ckd_4_5_or_dialysis_hes', 'Chronic kidney disease (CKD)'),
  ('has_rare_neurological_disease', 'Rare neurological conditions'),
  ('has_myelodysplastic_syndrome', 'Conditions affecting the blood'),
  ('has_haematological_malignancy_gp', 'Conditions affecting the blood'),
  ('has_haematological_malignancy_hes', 'Conditions affecting the blood'),
  ('has_haematological_malignancy_chemo_hes', 'Conditions affecting the blood'),
  ('has_chemotherapy_any_code_sact_12months', 'Cancer'),
  ('has_chemotherapy_hes_3months', 'Cancer'),
  ('has_chemotherapy_any_code_sact_3months', 'Cancer'),
  ('has_chemotherapy', 'Cancer'),
  ('has_immunosuppression', 'Conditions affecting the immune system, including HIV/AIDS'),
  ('has_cirrhosis_liver', 'Severe liver disease'),
  ('has_ckd_4_5_or_dialysis', 'Chronic kidney disease (CKD)'),
], ['algo_map_group', 'cohort']) 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demographics

# COMMAND ----------

ethnic_category_code = spark.table('{ECAT_DB}.{ECAT_TABLE}')

ethnicity_mapping = spark.createDataFrame([
('A', 'White - British', 'White'),
('B', 'White - Irish', 'White'),
('C', 'White - Any other White background', 'White'),
('D', 'Mixed - White and Black Caribbean', 'Mixed/Multiple ethnic groups'),
('E', 'Mixed - White and Black African', 'Mixed/Multiple ethnic groups'),
('F', 'Mixed - White and Asian', 'Mixed/Multiple ethnic groups'),
('G', 'Mixed - Any other mixed background', 'Mixed/Multiple ethnic groups'),
('H', 'Asian or Asian British - Indian', 'Asian/Asian British'),
('J', 'Asian or Asian British - Pakistani', 'Asian/Asian British'),
('K', 'Asian or Asian British - Bangladeshi', 'Asian/Asian British'),
('L', 'Asian or Asian British - Any other Asian background', 'Asian/Asian British'),
('M', 'Black or Black British - Caribbean', 'Black/African/Caribbean/Black British'),
('N', 'Black or Black British - African', 'Black/African/Caribbean/Black British'),
('P', 'Black or Black British - Any other Black background', 'Black/African/Caribbean/Black British'),
('R', 'Other Ethnic Groups - Chinese', 'Asian/Asian British'),
('S', 'Other Ethnic Groups - Any other ethnic group', 'Other ethnic group'),
('T', 'Traveller', 'White'),
('W', 'Arab', 'Other ethnic group'),
('Z', 'Not stated', 'Not stated'),
('99', 'Not known', 'Not known'),
], ['ethnic_category_code', 'ethnicity_description', 'ethnic_group'])

ethnicity_linkage_with_categories = (ethnic_category_code
  .join(ethnicity_mapping, 'ethnic_category_code', how = 'left' )
  .select(
    'nhs_number',
    'ETHNIC_CATEGORY_CODE',
    'ethnicity_description',
    'ethnic_group'
  )
)


# COMMAND ----------

# MAGIC %md ### Age bands

# COMMAND ----------

# if age is below 12 or above 120 then age_band is null
# this table goes up to age 2000 years 
acceptable_ages = [(i, i) for i in range(0, 2000)] 

age_schema = T.StructType([
  T.StructField("id", T.IntegerType(), True),
  T.StructField("age", T.IntegerType(), True)
])

df_age_list = spark.createDataFrame(data = acceptable_ages, schema = age_schema)

# 10 year age bands in util.py
age_band_df = add_age_band_pub(df_age_list, "age").drop("id")

# COMMAND ----------

# MAGIC %md ### PDS: gender

# COMMAND ----------

pds_df = spark.sql("SELECT NHS_NUMBER, gender.gender as gender_code from {PDS_DB}.{PDS_TABLE}")
