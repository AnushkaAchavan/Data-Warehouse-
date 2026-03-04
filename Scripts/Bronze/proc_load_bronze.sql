/* 
=================================================================
Stored Procedure : Load Bronze Layer(Source -> Bronze)
=================================================================
Script Purpose:
  This stored procedures loads data into the 'bronze' schema from external CSV files.
  It performs the following actions:
  - Truncate the bronze tables before loading data.
  - Uses the 'BULK INSERT' command to load data from csv Files to bronze tables.

Parameters:
  None.
This stored procedure does not accept any parmeters or return any values.

Usage Example:
  EXEC bronz.load_bronze;
================================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN 
	DECLARE @start_time DATETIME,@end_time DATETIME,@batch_start DATETIME,@batch_end DATETIME; 
    BEGIN TRY
	    SET @batch_start = GETDATE();
		PRINT'=================================';
		PRINT 'Loading Bronze Layer';
		PRINT'=================================';

		PRINT'---------------------------------';
		PRINT'Loading CRM Tables';
		PRINT'---------------------------------';

		SET @start_time = GETDATE();
		PRINT '<< Inseting Data Info:bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\ANUSHKA CHAVAN\OneDrive\Documents\ds\Data_warehouse\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT'<< Load Duaration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT'--------------------'

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT'<<Inseting Data Info:bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\ANUSHKA CHAVAN\OneDrive\Documents\ds\Data_warehouse\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'<< Load Duaration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT'--------------------'

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT'<<Insert Data Info:bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\ANUSHKA CHAVAN\OneDrive\Documents\ds\Data_warehouse\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'<< Load Duaration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT'--------------------'


		PRINT'---------------------------------';
		PRINT'Loading ERP Tables';
		PRINT'---------------------------------';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_CUST_AZ12;
		PRINT '<<Insert Data Info:bronze.erp_CUST_AZ12';
		BULK INSERT bronze.erp_CUST_AZ12
		FROM 'C:\Users\ANUSHKA CHAVAN\OneDrive\Documents\ds\Data_warehouse\sql-data-warehouse-project-main\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'<< Load Duaration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT'--------------------'
	    
		SET @start_time = GETDATE();
		PRINT'<<Insert Data Info:bronze.erp_LOC_A101';
		BULK INSERT bronze.erp_LOC_A101
		FROM 'C:\Users\ANUSHKA CHAVAN\OneDrive\Documents\ds\Data_warehouse\sql-data-warehouse-project-main\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'<< Load Duaration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT'--------------------'


		SET @start_time = GETDATE();
		PRINT'<<Insert Data Info:bronze.erp_PX_CAT_G1V2';
		BULK INSERT bronze.erp_PX_CAT_G1V2
		FROM 'C:\Users\ANUSHKA CHAVAN\OneDrive\Documents\ds\Data_warehouse\sql-data-warehouse-project-main\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);SET @end_time = GETDATE();
		PRINT'<< Load Duaration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT'--------------------'
		SET @batch_end = GETDATE();
		PRINT'Bronze layer loading time:' + CAST(DATEDIFF(second,@batch_start,@batch_end) AS NVARCHAR) + 'seconds';

	END TRY
	BEGIN CATCH
		PRINT '================================================='
		PRINT'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT'Error Message' + ERROR_MESSAGE();
		PRINT'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '================================================='
	END CATCH
END;
