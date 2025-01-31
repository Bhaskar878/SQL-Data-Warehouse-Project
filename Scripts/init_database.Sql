use master
-- to check database exists or not and drop the database
IF EXISTS (SELECT 1 FROM sys.databases where name='DataWarehouse')
begin 
	alter database DataWarehouse set single_user with rollback immediate;
	Drop database DataWarehouse;
end;
-- create database
create database DataWarehouse;
go
-- use database
use DataWarehouse;
go
--- create schema
create schema bronze;
go

create schema Silver;
go

create schema Gold;
