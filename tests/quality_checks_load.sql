/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

/*
=================================================================================================================
Checking quality of silver.crm_cust_info
=================================================================================================================
*/

select * from silver.[crm_cust_info];

select cst_id,count(*) from silver.[crm_cust_info] group by cst_id having count(*)>1 or cst_id is null;

select distinct cst_gndr from silver.[crm_cust_info] ;

select distinct cst_marital_status from silver.[crm_cust_info];

select cst_firstname, cst_lastname from silver.[crm_cust_info] 
where cst_firstname != trim(cst_firstname) or cst_lastname != trim(cst_lastname) ;

/*
=================================================================================================================
Checking quality of silver.crm_prd_info
=================================================================================================================
*/

select * from silver.crm_prd_info;

select prd_id,count(*) from silver.crm_prd_info group by prd_id having count(*)>1 or prd_id is null;

select distinct prd_line from silver.crm_prd_info;

select * from silver.crm_prd_info where prd_start_dt > prd_end_dt;

/*
=================================================================================================================
Checking quality of silver.crm_sales_details
=================================================================================================================
*/

select * from silver.crm_sales_details;

select * from silver.crm_sales_details where sls_ord_num is null or sls_prd_key is null or sls_cust_id is null;

/*
=================================================================================================================
Checking quality of silver.erp_cust_az12
=================================================================================================================
*/

select * from silver.erp_cust_az12;

select * from silver.erp_cust_az12 where cid is null or bdate is null or gen is null;

select distinct gen from silver.erp_cust_az12;

/*
=================================================================================================================
Checking quality of silver.erp_loc_a101
=================================================================================================================
*/

select * from silver.erp_loc_a101;

select distinct cntry from silver.erp_loc_a101;

/*
=================================================================================================================
Checking quality of silver.erp_px_cat_g1v2
=================================================================================================================
*/

select * from silver.[erp_px_cat_g1v2];

select * from silver.[erp_px_cat_g1v2] where id is null;

select distinct cat from silver.[erp_px_cat_g1v2];

select distinct subcat from silver.[erp_px_cat_g1v2];

select distinct maintenance from silver.[erp_px_cat_g1v2];
