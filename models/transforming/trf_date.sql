{{config(materialized='table', schema=env_var('DBT_TRANSFORMSCHEMA', 'TRANSFORMING_DEV'))}}


{% set min_orderdate = get_mindate() %}
{% set max_orderdate = get_maxdate() %}

{{ dbt_date.get_date_dimension(min_orderdate[0], max_orderdate[0]) }}