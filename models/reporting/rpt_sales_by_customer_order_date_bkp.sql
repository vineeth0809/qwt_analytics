{{config(materialized = 'view', schema = 'reporting_dev')}}

select
c.companyname,
c.contactname,
min(o.orderdate) as first_order_date,
max(o.orderdate) as recent_order_date,
count(distinct o.orderid) as total_orders,
count(distinct p.productid) as total_products,
sum(o.quantity) as total_quantity,
sum(o.linesalesamount) as total_sales,
avg(o.margin) as avg_margin
 from
{{ref('fct_orders')}} as o 
inner join {{ref("fct_customers")}} as c on o.customerid = c.customerid
inner join {{ref("dim_products")}} as p on p.productid = o.productid
group by c.companyname, c.contactname
order by total_sales desc