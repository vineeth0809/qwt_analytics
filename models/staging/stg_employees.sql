-- code generation failed!
{{ config(materialized = 'table')}}

select * from {{ source("qwt_raw", "raw_employees")}}