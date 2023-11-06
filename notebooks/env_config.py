# Databricks notebook source
# Environment-specific configuration settings

# Current environment
current_env = 'dev'  

# Base file path for the tables
base_file_path = {
    'dev': '/FileStore/tables/dev/',
    'prod': '/FileStore/tables/prod/'
}

# Bronze company filename
bronze_file_names = {
    'dev': 
        {'companies':'companies.csv'},
    'prod': 
        {'companies':'BasicCompanyDataAsOneFile-2023-11-01_filtered.csv'}
}


# SIC codes relevant for different environments
sic_codes = {
    'dev': ['43390', '43210', '62090','62020'], 
    'prod': ['41202', '41201', '42110', '42120', '42130', '42210', '42220', '42910', '42990', '43210', '43220', '43290', '43110', '43120', '43130', '43991', '43310', '43320', '43330', '43341', '43342', '43910', '43390', '43999']
}

# Rank filter for usernames
username_rank_filter_threshold = {
    'dev': 3,
    'prod': 20
}


