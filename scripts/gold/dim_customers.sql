select * from silver.crm_cust_info;

select * from silver.erp_cust_az12;

select * from silver.erp_loc_a101;

-- ================================================================================
--checking duplicates in the primary key after joining the customer infromation
-- ================================================================================

with cte as (
		select 
		c1.cst_id,
		c1.cst_key,
		c1.cst_firstname,
		c1.cst_lastname,
		c1.cst_marital_status,
		c1.cst_gndr,
		c2.gen,
		c1.cst_create_date,
		c2.bdate,
		c3.cntry
		from silver.crm_cust_info c1
		left join silver.erp_cust_az12 c2
		on c1.cst_key = c2.cid
		left join silver.erp_loc_a101 c3
		on c1.cst_key = c3.cid
)
select cst_id,count(*) from cte group by cst_id having count(*)>1;

-- ================================================================================
--checking gender information 
-- ================================================================================

select distinct
	c1.cst_gndr,
	c2.gen
from silver.crm_cust_info c1
left join silver.erp_cust_az12 c2
on c1.cst_key = c2.cid
left join silver.erp_loc_a101 c3
on c1.cst_key = c3.cid;

-- ================================================================================
---Transforming the data for the gender column along with correct column headers
-- ================================================================================

select
	c1.cst_id as customer_id,
	c1.cst_key as customer_number,
	c1.cst_firstname as first_name,
	c1.cst_lastname as last_name,
	c3.cntry as country,
	c1.cst_marital_status as marital_status,
	case when c1.cst_gndr != c2.gen and c1.cst_gndr = 'N/A' and (c2.gen is not null or c2.gen != 'N/A') then c2.gen
		when c1.cst_gndr != c2.gen and c2.gen = 'N/A' and (c1.cst_gndr is not null or c1.cst_gndr != 'N/A') then c1.cst_gndr
		else c1.cst_gndr
	end as gender,
	c2.bdate as birth_date,
	c1.cst_create_date as create_date
from silver.crm_cust_info c1
left join silver.erp_cust_az12 c2
on c1.cst_key = c2.cid
left join silver.erp_loc_a101 c3
on c1.cst_key = c3.cid;

-- ================================================================================
---Deciding whether it is a dimension or fact table
-- ================================================================================

select
	row_number() over( order by c1.cst_id) as customer_key,
	c1.cst_id as customer_id,
	c1.cst_key as customer_number,
	c1.cst_firstname as first_name,
	c1.cst_lastname as last_name,
	c3.cntry as country,
	c1.cst_marital_status as marital_status,
	case when c1.cst_gndr != 'N/A' then c1.cst_gndr
		else coalesce(c2.gen,'N/A')
	end as gender,
	c2.bdate as birth_date,
	c1.cst_create_date as create_date
from silver.crm_cust_info c1
left join silver.erp_cust_az12 c2
on c1.cst_key = c2.cid
left join silver.erp_loc_a101 c3
on c1.cst_key = c3.cid;

-- ================================================================================
---Create views
-- ================================================================================

create view gold.dim_customers as
select
	row_number() over( order by c1.cst_id) as customer_key,
	c1.cst_id as customer_id,
	c1.cst_key as customer_number,
	c1.cst_firstname as first_name,
	c1.cst_lastname as last_name,
	c3.cntry as country,
	c1.cst_marital_status as marital_status,
	case when c1.cst_gndr != 'N/A' then c1.cst_gndr
		else coalesce(c2.gen,'N/A')
	end as gender,
	c2.bdate as birth_date,
	c1.cst_create_date as create_date
from silver.crm_cust_info c1
left join silver.erp_cust_az12 c2
on c1.cst_key = c2.cid
left join silver.erp_loc_a101 c3
on c1.cst_key = c3.cid;
