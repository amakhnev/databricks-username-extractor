# Databricks notebook source
# MAGIC %md 
# MAGIC CSV files are downloaded from [Companies house website](https://download.companieshouse.gov.uk/en_output.html)
# MAGIC
# MAGIC Small file was prepared by copying first 256 records from file containing companies starting from U. Will use that file for development, and place it into /FileStore/tables/dev/bronze folder
# MAGIC

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"])

# COMMAND ----------

# MAGIC %run ./env_config

# COMMAND ----------

# Ensure file is uploaded by running command
dbutils.fs.ls(base_file_path[current_env]+'bronze/')


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC next, will complete transformation from bronze to silver layer. 
# MAGIC In Silver layer we keep companies, SIC codes and relationship between both
# MAGIC
# MAGIC
# MAGIC * First thing, we filter inactive or dormant companies
# MAGIC * next, we create company table, SIC table and relationship
# MAGIC * And finally write it into silver tables.
# MAGIC
# MAGIC

# COMMAND ----------

df_bronze = spark.read.csv(base_file_path[current_env]+'bronze/'+bronze_file_names[current_env]['companies'], header=True, inferSchema=True)

# COMMAND ----------

from pyspark.sql.functions import col

df_companies = df_bronze.filter(
    (col('CompanyStatus') == 'Active') &
    (col('`Accounts.AccountCategory`') != 'DORMANT')
).select(
    col('CompanyName').alias('company_name'),
    col(' CompanyNumber').alias('company_number'),
    col('IncorporationDate').alias('incorporation_date')
).distinct()

df_companies.show()

# COMMAND ----------

from pyspark.sql.functions import explode, array, split, filter, col

# Specify the column names with periods wrapped in backticks
sic_columns = [col("`SICCode.SicText_1`"), col("`SICCode.SicText_2`"), col("`SICCode.SicText_3`"), col("`SICCode.SicText_4`")]

df_sic_codes = df_bronze.select(*sic_columns) \
                         .distinct()

# Creating a single list of distinct SIC codes, filtering it, removing special 'None Supplied' and splitting into code and text
df_sic_codes = df_sic_codes.withColumn('sic_codes', array(*sic_columns)) \
                           .select(explode('sic_codes').alias('sic_code')) \
                           .na.drop() \
                           .filter(col('sic_code') != 'None Supplied') \
                           .distinct() \
                           .withColumn('code', split(col('sic_code'), ' - ').getItem(0)) \
                           .withColumn('name', split(col('sic_code'), ' - ').getItem(1)) \
                           .drop('sic_code') 


# Show the DataFrame to verify 
df_sic_codes.show()





# COMMAND ----------

from pyspark.sql.functions import col, array, explode, filter

# Assuming 'sic_columns' contains the column names like 'SICCode.SicText_1', etc.
# and you want to explode them into separate rows.
df_relationship = df_bronze.withColumn('sic_codes', array(*sic_columns)) \
                            .select(col(' CompanyNumber').alias('company_number'), explode('sic_codes').alias('sic_text')) \
                            .filter((col('sic_text')!='') & (col('sic_text') != 'None Supplied'))

# Split the 'sic_text' into 'code' and dropping original column
df_relationship = df_relationship.withColumn('sic_code', split(col('sic_text'), ' - ').getItem(0))
df_relationship = df_relationship.drop('sic_text')


df_relationship.show()




# COMMAND ----------

# Write the company DataFrame to the silver layer
df_companies.write.format('delta').mode('overwrite').save(base_file_path[current_env] + 'silver/companies')

# Write the SIC codes DataFrame to the silver layer
df_sic_codes.write.format('delta').mode('overwrite').save(base_file_path[current_env] + 'silver/sic_codes')

# Write the relationship DataFrame to the silver layer
df_relationship.write.format('delta').mode('overwrite').save(base_file_path[current_env] + 'silver/company_sic_relationship')

# Ensure dfs are saved command
dbutils.fs.ls(base_file_path[current_env] + 'silver/companies')
