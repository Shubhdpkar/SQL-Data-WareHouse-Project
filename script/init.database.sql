/*

### 🏗️ Database Initialization

The `init_datawarehouse.sql` script sets up the foundational structure for our data warehouse project.
It creates:
- A `DataWarehouse` database

- Three logical layers as schemas:
  - **bronze**: Raw ingested data
  - **silver**: Cleaned/transformed data
  - **gold**: Business-ready data models

This layered approach follows modern data lakehouse/warehouse best practices.

📂 Location: `scripts/init_datawarehouse.sql`

*/




-- Create Database 'DataWarehouse'

Use master;

Create Database DataWarehouse;

Use DataWarehouse;

Create Schema bronze;
Go
Create Schema silver;
Go
Create Schema gold;
Go

