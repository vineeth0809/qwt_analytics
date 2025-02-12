{{ config(materialized = 'table', alias = 'stage_products')}}

select * from {{ source("qwt_raw", "raw_products")}}