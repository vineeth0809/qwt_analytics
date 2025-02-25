
{{config(materialized='table', schema=env_var('DBT_TRANSFORMSCHEMA', 'TRANSFORMING_DEV'))}}

select 
p.productid,
p.productname,
c.categoryname,
s.companyname as suppliercompany,
s.ContactName as suppliercontact,
s.city as suppliercity,
s.country as suppliercountry,
p.quantityperunit,
p.unitcost,
p.unitsinstock,
p.unitsonorder,
TO_DECIMAL(p.unitprice - p.unitcost,9,2) as profit,
IFF(p.unitsonorder > p.unitsinstock,'Not Available', 'Available') as productavailablity
 from
{{ref("stg_products")}} as p left join {{ref('trf_suppliers')}} as s 
on p.supplierid=s.supplierid left join {{ref('lkp_catagory')}} as c 
on c.categoryid = p.categoryid