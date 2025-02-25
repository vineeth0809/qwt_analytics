import snowflake.snowpark.functions as F 
def model(dbt, session):
    dbt.config(materialized = 'table')
    df = dbt.source('qwt_raw','raw_customers')

    return df