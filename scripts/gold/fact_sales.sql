select * from gold.dim_customers;

select * from gold.dim_products;

select * from [silver].[crm_sales_details];

-- =====================================================================================
--	Joining the sales table with dimension customer and product table from gold layer
-- =====================================================================================

SELECT *
FROM [silver].[crm_sales_details] s1
LEFT JOIN gold.dim_customers g1
ON s1.sls_cust_id = g1.customer_id
LEFT JOIN gold.dim_products g2
ON s1.sls_prd_key = g2.product_number

-- =====================================================================================
--	created connection with sales table and dimension customer ,product table
-- =====================================================================================

SELECT [sls_ord_num] as order_number
	  ,pr.product_key as product_key
	  ,cu.customer_key as customer_key
      ,[sls_order_dt] as order_date
      ,[sls_ship_dt] as shipping_date
      ,[sls_due_dt] as due_date
      ,[sls_sales] as sales_amount
      ,[sls_quantity] as quantity
      ,[sls_price] as price
FROM [silver].[crm_sales_details] s1
LEFT JOIN gold.dim_customers cu
ON s1.sls_cust_id = cu.customer_id
LEFT JOIN gold.dim_products pr
ON s1.sls_prd_key = pr.product_number

-- ================================================================================
---Create views
-- ================================================================================

create view gold.fact_sales as
SELECT [sls_ord_num] as order_number
	  ,pr.product_key as product_key
	  ,cu.customer_key as customer_key
      ,[sls_order_dt] as order_date
      ,[sls_ship_dt] as shipping_date
      ,[sls_due_dt] as due_date
      ,[sls_sales] as sales_amount
      ,[sls_quantity] as quantity
      ,[sls_price] as price
FROM [silver].[crm_sales_details] s1
LEFT JOIN gold.dim_customers cu
ON s1.sls_cust_id = cu.customer_id
LEFT JOIN gold.dim_products pr
ON s1.sls_prd_key = pr.product_number
