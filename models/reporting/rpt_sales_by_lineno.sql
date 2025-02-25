{{config(materialized = 'view', schema = 'reporting_dev')}}

{% set v_linnos = get_line_numbers() %}
select 
orderid,

{% for linno in v_linnos -%}

sum(case when lineno={{linno}} then linesalesamount end) as lineno{{linno}}_sales,

{% endfor %}
sum(linesalesamount) as total_sales
from {{ ref('fct_orders') }}
group by orderid