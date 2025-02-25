-- code generation failed!
{{ config(materialized = 'table', alias = 'stage_customers')}}

select * from {{ source("qwt_raw", "raw_customers")}}