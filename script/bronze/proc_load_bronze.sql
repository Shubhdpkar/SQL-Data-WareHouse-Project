/* ================================================================================================
   Script:     load_bronze_procedure.sql
   Purpose:    Stored procedure to load all Bronze layer tables from raw CSV files into the 
               Data Warehouse. 
               This process truncates existing data in Bronze tables and reloads fresh data from 
               the source CRM and ERP CSV datasets.

   Author:     [Your Name]
   Date:       [Date]
   
   Notes:      
               1. Bronze layer is the raw ingestion layer — data is loaded as-is with no transformation.
               2. Load durations for each table and total batch time are printed to the console.
               3. Error handling is implemented with TRY...CATCH to log load failures.
               4. All file paths are local and must be updated if deployed to another environment.
               5. The procedure assumes:
                  - Database: DataWarehouse
                  - Schemas: bronze
                  - Tables already created with appropriate structure (see create_bronze_tables.sql).
               6. The BULK INSERT command requires the SQL Server service account to have file system access 
                  to the specified paths.
               7. Sales table is loaded without cleaning invalid date formats — cleaning will occur later 
                  in the ETL pipeline.
================================================================================================ */

-- Execute procedure:
-- EXEC bronze.load_bronze;

CREATE OR ALTER PROCEDURE bronze.load_bronze 
AS 
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================================';
        PRINT 'Loading Bronze layer';
        PRINT '================================================================';

        PRINT '****************************************************************';
        PRINT 'Loading CRM Tables';
        PRINT '****************************************************************';

        -- CRM Customer Info
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>> Inserting data into Table: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM 'W:\SQL_Udemy\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
        PRINT '>> *****************';


        -- CRM Product Info
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_prod_info';
        TRUNCATE TABLE bronze.crm_prod_info;

        PRINT '>> Inserting data into Table: bronze.crm_prod_info';
        BULK INSERT bronze.crm_prod_info
        FROM 'W:\SQL_Udemy\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
        PRINT '>> *****************';


        -- CRM Sales Details (loaded raw, no date cleaning at this stage)
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>> Inserting data into Table: bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM 'W:\SQL_Udemy\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
        PRINT '>> *****************';


        PRINT '****************************************************************';
        PRINT 'Loading ERP Tables';
        PRINT '****************************************************************';

        -- ERP Location
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT '>> Inserting data into Table: bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM 'W:\SQL_Udemy\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
        PRINT '>> *****************';


        -- ERP Customer
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '>> Inserting data into Table: bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM 'W:\SQL_Udemy\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
        PRINT '>> *****************';


        -- ERP Product Category
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT '>> Inserting data into Table: bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'W:\SQL_Udemy\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
        PRINT '>> *****************';

        -- Total batch time
        SET @batch_end_time = GETDATE();
        PRINT '============================================================';
        PRINT '>> Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' Seconds';
        PRINT '============================================================';
        
   END TRY
   BEGIN CATCH
        PRINT '============================================================';
        PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
        PRINT 'Error Message:' + ERROR_MESSAGE();
        PRINT 'Error Number:' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State:' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '============================================================';
   END CATCH
END;
