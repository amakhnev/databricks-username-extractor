# Databricks notebook source
# MAGIC %md
# MAGIC Now, we want to generate usernames for subset of companies, with certain sic codes (only from certain industry). 
# MAGIC The generator function generates max 5 usernames, with 3 to 20 symbols, together with rank (1 to 100) showing how likely company is going to pick this username. 
# MAGIC After we generate all usernames, we combine them together and keep only 10 companies with highers rank for any selected username 
# MAGIC

# COMMAND ----------

# MAGIC %run ./env_config

# COMMAND ----------

# MAGIC %run "../libraries/username_generator"

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType

# Define the schema for the UDF return type
username_schema = ArrayType(StructType([
    StructField("username", StringType(), False),
    StructField("score", IntegerType(), False)
]))

# Define UDF for username generation
generate_usernames_udf = udf(generate_usernames, username_schema)

# COMMAND ----------

# Import necessary libraries
from pyspark.sql.functions import udf, col, explode, monotonically_increasing_id, row_number, desc
from pyspark.sql.window import Window

# Read in the silver tables
df_companies = spark.read.format('delta').load(base_file_path[current_env]+'silver/companies')
df_relationship = spark.read.format('delta').load(base_file_path[current_env]+'silver/company_sic_relationship')


# Filter companies with specific SIC codes
df_filtered_relationship = df_relationship.where(col('sic_code').isin(sic_codes[current_env]))

# Join to get company names and keep only companies in case of multiple sic_codes
df_company_names = df_filtered_relationship.join(df_companies, 'company_number') \
                        .drop('sic_code') \
                        .distinct()    
df_company_names.show()
# Generate potential usernames
df_usernames = df_company_names.withColumn('username_score', explode(generate_usernames_udf('company_name')))
df_usernames = df_usernames.select(
    col('company_number'),
    col('company_name'),
    col('username_score.username').alias('username'),
    col('username_score.score').alias('score')
)

# Rank companies record within usernames and then filer it from low-rank companies
windowSpec  = Window.partitionBy('username').orderBy(desc('score'))
df_usernames = df_usernames.withColumn('rank', row_number().over(windowSpec))
df_usernames = df_usernames.filter((col('rank') <= username_rank_filter_threshold[current_env]) | (col('score')==100))

# # Order and show to validate results
# df_usernames_ordered = df_usernames.orderBy( 'username',desc('score'))

# # Show the top records (change the number inside show() to see more or fewer records)
# df_usernames_ordered.show(50)

# Save the final DataFrame to the gold layer
df_usernames.write.format('delta').mode('overwrite').save(base_file_path[current_env] + 'gold/usernames')
