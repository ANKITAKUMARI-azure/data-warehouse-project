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

