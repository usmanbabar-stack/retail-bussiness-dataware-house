
================================================================================
Retail Business Data Warehousing - Hybrid Join Project
================================================================================

Author: Usman Babar
Student ID: i221984
Course: Data Warehousing
Date: November 17, 2025

================================================================================
Project Introduction
================================================================================
This repository, **Retail Business Data Warehousing**, provides a professional, end-to-end implementation of a retail data warehouse using a hybrid join ETL strategy. The project is designed to mirror real-world business scenarios, integrating large-scale transactional, customer, and product data into a high-performance star schema for advanced business intelligence and OLAP analytics.

Key Features:
- **Hybrid Join ETL**: High-performance, multi-threaded hybrid join algorithm for scalable and efficient data integration.
- **Star Schema Design**: Fact table and five dimension tables, with robust indexing and referential integrity.
- **OLAP Query Suite**: 20 advanced OLAP queries for comprehensive business analysis, including drill-down, roll-up, slicing, dicing, ranking, and time-series analytics.
- **Professional Data Engineering**: Adheres to best practices in ETL, schema design, and SQL development for enterprise-grade reliability and scalability.

This project is ideal for students, data engineers, and business professionals who want to learn, demonstrate, or deploy a complete retail data warehousing solution—from raw data ingestion to actionable analytics.

================================================================================
PROJECT FILES
================================================================================

customermasterdata.csv - Customer master data (5, 891 records)
productmasterdata.csv= Product master data (3, 631 records)
transactionaldata.csv - transactional data (550, 068 records)
walmarthybridjoinetl.py - HYBRIDJOIN python ETL script.
1create star schema.sql. - SQL script to create schema of database.
3olapqueries.sql - SQL query with 20 OLAP queries.
README.txt                         - This file


================================================================================
INSTALLATION STEPS
================================================================================



Download SQL server 2019 express downloaded in Microsoft site.
During installation:
   - Choose "Basic" installation
   - Record the name of the server ( default: localhost\SQLEXPRESS)
   - Power on windows Authentication.

STEP 4: SQL SERver management studio (SSMS)
|human|>STEP 4: SQL SERver management studio (SSMS)

Install SSMS in Microsoft site.
Install default integration.
Start SSMS and log in to localhostEXPRESS.

================================================================================
PROJECT SETUP AND EXECUTION
================================================================================

STEP 1: PREPARE DATA FILES

Copy all project files to a folder (e.g. D:\DWCODE).
Make sure that the three CSVs are located in the same folder:
   - customermasterdata.csv
   - productmasterdata.csv
   - transactionaldata.csv

STEP 2: PRODUce DATABase and SCHEMA.

Open SQL server management studio (SSMS).
Connect to your SQL Server (localhost\SQLEXPRESS)
Click File > Open > File
Select "1createstarschema.sql"
Click Execute (or press F5)
Verify output shows:
   - Database WalmartDW created successfully.
   - Dimension tables were built successfully.
   - Fact table has been created successfully.
   - Indexes made successfully.
   - Views created successfully

STEP 3: RUN ETL PIPELINE (HYBRidjoin)

Open Command Prompt
Navigate to project folder:

Run the Python script:
   py walmarthybridjoinetl.py

Being asked, fill in connection information:
   Server name [localhost\SQLEXPRESS]: (Enter to have default)
   Database name [WalmartDW]: (Hit the Enter button to default)
   Authentication type [1]: 1 (Press Enter to Windows Auth)

Patience (about 2-5 minutes)
Verify output shows:
   - Writing Disk Buffer to Master Data.
   - Running multi-threaded hybridjoin algorithm.
   - Joined with Success: 547,217 records (99.48%)
   - Importing DUN Dimensions to SQL server.
   - LOADING FACTSALES TABLE
   - DATA VERIFICATION
   - ETL COMPLETE!

STEP 4: RUN OLAP QUERIES

SQL server management studio (SSMS)
Click File > Open > File
Select "3olapqueries.sql"
Click Execute (or press F5)
Examine all the 20 queries in Results pane.
The results of each query will be presented one at a time.

================================================================================
TROUBLESHOOTING
================================================================================

Low-level programming record insurgency:acket: python is not understood.
SOLUTION: Put Python into system PATH or use Long path to python.exe

ISSUE 2: "ODBC Driver not found"
SOLUTION: ODBC Driver 17 or 18 SQL Server should be downloaded and installed:
https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server

ISSUE 3: "Unable to access SQL server.
SOLUTION: 
Check the service SQL Server Express (check Services)
Check server name (SQL ServerConfiguration Manager)
Firewall check windows settings.

ISSUE 4: Database WalmartDW is already there.
"""
|human|>SOLUTION: remove and recreate database:
changing the break labels to SSMS: run: DROP DATABase WalmartDW; GO.
And again run 1createstarschema.sql.

ISSUE 5: ModuleNotFoundError No module named pandas.
SOLUTION: Error: install a needed package:
pip install pandas

ISSUE 6: ETL script takes excessive time to run (longer than 10 minutes)
SOLUTION: 
Shut other unused applications to release RAM.
Determine whether the antivirus is scanning the CSV files.
Make sure CSVs are not in a network drive but in local disk.

ISSUE 7: "Login failed to user" (SQL Authentication)
SOLUTION: Do windows authentication (option 1) or check sql credentials.

================================================================================
EXECUTION TIME ESTIMATES
================================================================================

Step 1: Create Schema           - 5 seconds
Step 2: ETL Pipeline Run- 2-5 minutes.
Step 3: Performance OLAP Queries- 1-2 minutes.

Total Time: 5-10 minutes
 using a return needle
|human|>Total Time: 5-10 minutes with a return needle

================================================================================
PROJECT DELIVERABLES
================================================================================

This project includes:

STAR SCHEMA DESIGN
   - 1 Fact table (FactSales)
   -5 dimension tables ( Customer, Product, Date, Store and Supplier)
   - Foreign key constraints
   - 15 performance indexes

HYBRIDJOIN Implementation of ETL.
   - Synchronous producer-consumer pattern.
   - Hash table with 10,000 slots
   - Processing queue of 5000 capacity.
   - Thread safe communication stream buffer.
   - 99.48% join success rate
   - Throughput: ~26,773 records/second

OLAP ANALYSIS SQL (20 queries)
   - Drill-Down (Q1, Q11, Q13, Q14)
   - Roll-Up (Q17)
   - Slicing (Q2, Q18)
   - Dicing (Q1, Q10)
   - Ranking (Q1, Q5, Q8, Q11)
   - Time-Series Analysis (Q4, Q9, Q12)
   - Growth Analysis (Q9, Q12)
   - Comparative Analysis (Q10, Q18)
   - Statistical Analysis (Q15, Q19)
   - Basket Analysis (Q16)
   - Materialized Views (Q20)

================================================================================