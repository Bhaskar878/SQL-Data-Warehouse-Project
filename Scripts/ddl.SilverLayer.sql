if OBJECT_ID('Silver.crm_prd_info','U') is not null
drop table Silver.crm_prd_info;
create table Silver.crm_prd_info
(
    prd_id int,
    cat_id varchar(50),
    prd_key varchar(50),
    prd_nm varchar(50),
    prd_cost int,
    prd_line varchar(50),
    prd_start_dt date,
    prd_end_dt date,
    dwh_create_dt datetime default getdate()
);

if OBJECT_ID('Silver.crm_sales_details','U') is not null
drop table Silver.crm_sales_details;
create table Silver.crm_sales_details
(
    sls_ord_num varchar(50),
    sls_prd_key varchar(50),
    sls_cust_id int,
      sla_order_dt date,
      sls_ship_dt date,
    sls_due_dt date,
    sls_sales int,
    sls_quantity int,
    sls_price int,
    dwh_create_dt datetime default getdate()
);

if OBJECT_ID('Silver.erp_loc_a101','U') is not null
drop table Silver.erp_loc_a101;
create table Silver.erp_loc_a101
(
    cid varchar(50),
    cntry varchar(50),
    dwh_create_dt datetime default getdate()
);

if OBJECT_ID('Silver.erp_cust_az12','U') is not null
drop table Silver.erp_cust_az12;
create table Silver.erp_cust_az12
(
    cid varchar(50),
    bdate date,
    gen varchar(50),
    dwh_create_dt datetime default getdate()
);

if OBJECT_ID('Silver.erp_px_cat_g1v2','U') is not null
drop table Silver.erp_px_cat_g1v2;
create table Silver.erp_px_cat_g1v2
(
    id varchar(50),
    cat varchar(50),
    subcat varchar(50),
    maintenance varchar(50),
    dwh_create_dt datetime default getdate()
);

if OBJECT_ID('Silver.crm_cust_info','U') is not null  -- Fixed the schema name
drop table Silver.crm_cust_info;
create table Silver.crm_cust_info
(
    cst_id int,
    csr_key varchar(50),
    cst_firstname varchar(50),
    cst_lastname varchar(50),
    cst_material_status varchar(50),
    cst_gndr varchar(50),
    cst_create_date date,
    dwh_create_dt datetime default getdate()
);
