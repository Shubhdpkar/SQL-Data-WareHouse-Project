## SQL-Data-Warehouse-Project

## 📊 Enterprise Data Engineering Case Study
A hands-on project showcasing modern data engineering practices using SQL Server 2022 for ETL, Data Warehousing, and analytical modeling. The project simulates a real-world corporate data platform environment, complete with automation scripts, Git versioning, and documentation.

## 🔍 Project Overview
This project is designed as an end-to-end data platform implementation covering:

Data ingestion from multiple operational systems (CRM, ERP).

Cleansing & standardization of raw data in a structured Medallion Architecture (Bronze → Silver → Gold).

Data transformation using SQL scripts and stored procedures.

Dimensional modeling to create a star schema for analytics.

Automated ETL process with execution time tracking and error handling.

GitHub integration for version control, with clearly separated folders for DDL, DML, and ETL logic.

## 🏗️ Architecture & Layers
Medallion Architecture
Bronze Layer – Raw data staging area

Bulk load from CSV files (CRM & ERP datasets) using BULK INSERT.

No transformation, just initial structure alignment.

Folder: /scripts/bronze

Silver Layer – Cleansed & standardized data

Duplicate removal, date formatting, data enrichment, and standardization (country names, gender, category mapping).

Implemented with a single stored procedure silver.load_silver including:

Table truncation

Transformation queries

Execution time logging per table and total batch

TRY…CATCH error handling with detailed messages

Folder: /scripts/silver

Gold Layer – Analytics-ready dimensional model

Creation of dimension and fact tables:

gold.dim_customers

gold.dim_products

gold.fact_sales

Joins CRM & ERP data into a star schema for BI tools.

Folder: /scripts/gold

## Data FLow Schema Design Data Model

![Architecture_v001-Data_Architecture](https://github.com/user-attachments/assets/07fe0b2b-0b32-492f-9c21-a841f3705007)

![Data_Model](https://github.com/user-attachments/assets/47d2b401-15bc-488f-891b-e4a0f7029558)

![DataFlow](https://github.com/user-attachments/assets/6d3f8a0d-a579-41f4-8a29-b1b702de4b59)




## 📂 Source Systems & Data

##CRM System

cust_info.csv – Customer master data

prd_info.csv – Product master data

sales_details.csv – Sales transactions

ERP System

cust_az12.csv – Customer demographic data (birthdate, gender)

loc_a101.csv – Customer location mapping

px_cat_g1v2.csv – Product category hierarchy

🛠️ Features Implemented
Data Loading Automation

Bulk insert from CSV to Bronze tables.

Silver layer procedure cleans and loads data to analytics-ready format.

Data Quality Rules Applied

Date validation (0 or invalid formats set to NULL).

Country code mapping (DE → Germany, US/USA → United States).

Gender normalization (M/F → Male/Female).

Sales calculation fixes (sales = quantity × price if missing/wrong).

Removal of duplicates using ROW_NUMBER().

Performance Tracking

Each table load time is printed.

Total batch duration is logged.

Error Handling

TRY…CATCH blocks in ETL procedures.

Prints detailed error number, state, and message.

Dimensional Modeling

Customer Dimension with demographic enrichment.

Product Dimension with category/subcategory mapping.

Sales Fact with keys from dimensions for BI reporting.

## 📊 Schema Design
Star Schema in Gold Layer

           dim_customers
                |
                |
fact_sales  ---- product_key ---- dim_products

dim_customers → Merged from CRM + ERP customer tables.

dim_products → Product details joined with ERP product category.

fact_sales → Sales transactions linked to products and customers.

## 🚀 How to Run the Project
Clone the repository

bash
Copy
Edit
git clone https://github.com/<your-username>/SQL-Data-Warehouse-Project.git
Run DDL scripts (in order)

/scripts/bronze/ddl.bronze.sql

/scripts/silver/ddl.silver.sql

/scripts/gold/ddl.gold.sql

Load data

Run bronze.load_bronze stored procedure.

Run silver.load_silver stored procedure.

Create gold views

Execute /scripts/gold/views.sql to build dim_customers, dim_products, and fact_sales.

Query the gold layer for BI or analytics.

## 📈 Reporting Ready
The Gold Layer views can be directly connected to Power BI, Tableau, or Excel Pivot Tables for interactive reporting and dashboards.

## 📌 Key Learnings from this Project
Building a data warehouse from scratch using SQL Server.

Applying Medallion Architecture in SQL-based environments.

Writing clean, modular SQL scripts for better maintainability.

Implementing data quality rules at the ETL stage.

Using Git to version control SQL scripts.

Creating dimensional models for analytics.
