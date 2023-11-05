# Databricks notebook source
# MAGIC %md
# MAGIC Now, we want to generate usernames for subset of companies, with certain sic codes (only from certain industry). 
# MAGIC The generator function generates max 5 usernames, with 3 to 20 symbols, together with rank (1 to 100) showing how likely company is going to pick this username. 
# MAGIC After we generate all usernames, we combine them together and keep only 10 companies with highers rank for any selected username 
# MAGIC

# COMMAND ----------

# Import necessary libraries
from pyspark.sql.functions import udf, col, explode, monotonically_increasing_id, row_number, desc
from pyspark.sql.window import Window
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType

import re


def generate_usernames(company_name):
    # List of UK incorporation terms to ignore
    incorporation_terms = ['limited', 'ltd', 'llp', 'plc', 'group', 'holdings', '(uk)']
    
    # Clean the company name
    clean_name = company_name.lower()
    for term in incorporation_terms:
        clean_name = clean_name.replace(term, '')
    
    # Remove any non-allowed characters and replace spaces with underscores
    clean_name = re.sub(r'[^a-z0-9 ]', '', clean_name)  # Keep only lowercase, numbers, and space
    clean_name = re.sub(r'\s+', ' ', clean_name.strip())  # Replace one or more spaces with a single space

    # Generate acronyms
    acronym = ''.join(part[0] for part in clean_name.split() if part)
    potential_usernames = {acronym} if 3 <= len(acronym) <= 20 else set()

    # Generate potential usernames by splitting the cleaned name and combining parts
    name_parts = clean_name.split()
    for i in range(len(name_parts)):
        for j in range(i+1, len(name_parts) + 1):
            username = '_'.join(name_parts[i:j])
            if 3 <= len(username) <= 20:  # Check the length constraints
                potential_usernames.add(username)
                if '_' in username:
                    # Adding acronym variations by replacing parts with their initials
                    for k in range(i, j):
                        temp_parts = name_parts[i:j]  # Create a new sublist
                        temp_username = "_".join([part[0] if idx == k else part for idx, part in enumerate(temp_parts)])
                        if 3 <= len(temp_username) <= 20:
                            potential_usernames.add(temp_username)

    # Relevance score calculations
    usernames_with_scores = []
    clean_name_without_spaces = clean_name.replace(' ', '')
    for username in potential_usernames:
        if username == clean_name_without_spaces or username == acronym:
            score = 100
        else:
            # Calculate percentage of username in the company name
            count = clean_name_without_spaces.count(username.replace('_', ''))
            score = (count * len(username.replace('_', '')) / len(clean_name_without_spaces)) * 100
        usernames_with_scores.append((username, round(score)))

    # Sort by relevance score and limit to 5 usernames
    usernames_with_scores.sort(key=lambda x: x[1], reverse=True)
    return usernames_with_scores[:5]
# Define the schema for the UDF return type
username_schema = ArrayType(StructType([
    StructField("username", StringType(), False),
    StructField("score", IntegerType(), False)
]))

# Define UDF for username generation
generate_usernames_udf = udf(generate_usernames, username_schema)

# Read in the silver tables
df_companies = spark.read.format('delta').load('/FileStore/tables/dev/silver/companies')
df_relationship = spark.read.format('delta').load('/FileStore/tables/dev/silver/company_sic_relationship')

# SIC codes of interest
sic_codes = ['41202', '41201', '42110', '42120', '42130', '42210', '42220', '42910', '42990', '43210', '43220', '43290', '43110', '43120', '43130', '43991', '43310', '43320', '43330', '43341', '43342', '43910', '43390', '43999']

# Filter companies with specific SIC codes
df_filtered_relationship = df_relationship.where(col('sic_code').isin(sic_codes))

# Join to get company names and keep only companies in case of multiple sic_codes
df_company_names = df_filtered_relationship.join(df_companies, 'company_number') \
                        .drop('sic_code') \
                        .distinct()    

# Generate potential usernames
df_usernames = df_company_names.withColumn('username_score', explode(generate_usernames_udf('company_name')))
df_usernames = df_usernames.select(
    col('company_number'),
    col('company_name'),
    col('username_score.username').alias('username'),
    col('username_score.score').alias('score')
)

# Rank companies record within usernames
windowSpec  = Window.partitionBy('username').orderBy(desc('score'))
df_usernames = df_usernames.withColumn('rank', row_number().over(windowSpec))


df_usernames = df_usernames.filter((col('rank') <= 10) | (col('score')==100))

df_usernames.show()

# Save the final DataFrame to the gold layer
df_usernames.write.format('delta').mode('overwrite').save('/FileStore/tables/dev/gold/usernames')
