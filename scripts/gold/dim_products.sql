select * from silver.crm_prd_info;

select * from silver.erp_px_cat_g1v2;

-- ================================================================================
--Filtering the historical data from the table using end date is null
-- ================================================================================

select
	p1.prd_id,
	p1.cat_id,
	p1.prd_key,
	p1.prd_nm,
	p1.prd_cost,
	p1.prd_line,
	p1.prd_start_dt,
	p1.prd_end_dt,
	p2.cat,
	p2.subcat,
	p2.maintenance
from silver.crm_prd_info p1
left join silver.erp_px_cat_g1v2 p2
on p1.cat_id = p2.id
where p1.prd_end_dt is null;

-- ================================================================================
--checking duplicates in the primary key after joining the product information
-- ================================================================================

with cte as (
	select
		p1.prd_id,
		p1.cat_id,
		p1.prd_key,
		p1.prd_nm,
		p1.prd_cost,
		p1.prd_line,
		p1.prd_start_dt,
		p1.prd_end_dt,
		p2.cat,
		p2.subcat,
		p2.maintenance
	from silver.crm_prd_info p1
	left join silver.erp_px_cat_g1v2 p2
	on p1.cat_id = p2.id
	where p1.prd_end_dt is null
)
select prd_id,count(*) 
from cte 
group by prd_id 
having count(*)>1;

-- ================================================================================
---Transforming the data with correct column headers and orders
-- ================================================================================

select
	p1.prd_id as product_id,
	p1.prd_key as product_key,
	p1.prd_nm as product_name,
	p1.cat_id as category_id,
	p2.cat as category,
	p2.subcat as subcategory,
	p2.maintenance as maintenance,
	p1.prd_cost as cost,
	p1.prd_line as product_line,
	p1.prd_start_dt as product_start_date
from silver.crm_prd_info p1
left join silver.erp_px_cat_g1v2 p2
on p1.cat_id = p2.id
where p1.prd_end_dt is null;

-- ================================================================================
---Deciding whether it is a dimension or fact table
-- ================================================================================

select
	row_number() over( order by p1.prd_start_dt, p1.prd_key ) as product_key,
	p1.prd_id as product_id,
	p1.prd_key as product_number,
	p1.prd_nm as product_name,
	p1.cat_id as category_id,
	p2.cat as category,
	p2.subcat as subcategory,
	p2.maintenance as maintenance,
	p1.prd_cost as cost,
	p1.prd_line as product_line,
	p1.prd_start_dt as product_start_date
from silver.crm_prd_info p1
left join silver.erp_px_cat_g1v2 p2
on p1.cat_id = p2.id
where p1.prd_end_dt is null;

-- ================================================================================
---Create views
-- ================================================================================

create view gold.dim_products as
select
	row_number() over( order by p1.prd_start_dt, p1.prd_key ) as product_key,
	p1.prd_id as product_id,
	p1.prd_key as product_number,
	p1.prd_nm as product_name,
	p1.cat_id as category_id,
	p2.cat as category,
	p2.subcat as subcategory,
	p2.maintenance as maintenance,
	p1.prd_cost as cost,
	p1.prd_line as product_line,
	p1.prd_start_dt as product_start_date
from silver.crm_prd_info p1
left join silver.erp_px_cat_g1v2 p2
on p1.cat_id = p2.id
where p1.prd_end_dt is null;
