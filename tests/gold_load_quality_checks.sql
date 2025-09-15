-- ================================================================================
--		Quality check for golder dimension customer table
-- ================================================================================

select * from gold.dim_customers;

select customer_key,count(*) from gold.dim_customers group by customer_key having count(*)>1;

select distinct gender from gold.dim_customers;

select distinct marital_status from gold.dim_customers;

-- ================================================================================
--		Quality check for golder dimension product table
-- ================================================================================

select * from gold.dim_products;

-- ================================================================================
--		Quality check for gold fact sales table
-- ================================================================================

select * from gold.fact_sales;

SELECT *
from gold.fact_sales f1
LEFT JOIN gold.dim_customers d1
ON f1.customer_key = d1.customer_key
LEFT JOIN gold.dim_products d2
ON f1.product_key = d2.product_key
where d2.product_key is null or f1.customer_key is null
