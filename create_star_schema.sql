USE master;
GO


IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'November2025DW')
BEGIN
    CREATE DATABASE November2025DW;
    PRINT 'Database November2025DW created successfully.';
END
ELSE
BEGIN
    PRINT 'Database November2025DW already exists.';
END
GO

-- Create transactional2025 database (Transactional Database)
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'transactional2025')
BEGIN
    CREATE DATABASE transactional2025;
    PRINT 'Database transactional2025 created successfully.';
END
ELSE
BEGIN
    PRINT 'Database transactional2025 already exists.';
END
GO

USE November2025DW;
GO

IF OBJECT_ID('dbo.Fact_Sales', 'U') IS NOT NULL DROP TABLE dbo.Fact_Sales;
IF OBJECT_ID('dbo.Dim_Date', 'U') IS NOT NULL DROP TABLE dbo.Dim_Date;
IF OBJECT_ID('dbo.Dim_Customer', 'U') IS NOT NULL DROP TABLE dbo.Dim_Customer;
IF OBJECT_ID('dbo.Dim_Product', 'U') IS NOT NULL DROP TABLE dbo.Dim_Product;
IF OBJECT_ID('dbo.Dim_Store', 'U') IS NOT NULL DROP TABLE dbo.Dim_Store;
IF OBJECT_ID('dbo.Dim_Supplier', 'U') IS NOT NULL DROP TABLE dbo.Dim_Supplier;
GO

PRINT 'Creating dimension tables...';
GO

CREATE TABLE dbo.Dim_Customer (
    Customer_SK INT PRIMARY KEY IDENTITY(1,1),
    Customer_ID INT NOT NULL UNIQUE,
    Gender VARCHAR(10),
    Age VARCHAR(10),
    Occupation INT,
    City_Category VARCHAR(5),
    Stay_In_Current_City_Years VARCHAR(10),
    Marital_Status INT
);
GO

CREATE TABLE dbo.Dim_Product (
    Product_SK INT PRIMARY KEY IDENTITY(1,1),
    Product_ID VARCHAR(50) NOT NULL UNIQUE,
    Product_Category VARCHAR(100),
    Price DECIMAL(10, 2)
);
GO

CREATE TABLE dbo.Dim_Date (
    Date_SK INT PRIMARY KEY IDENTITY(1,1),
    Date DATE NOT NULL UNIQUE,
    Year INT NOT NULL,
    Month INT NOT NULL,
    Day INT NOT NULL,
    Quarter INT NOT NULL,
    Day_of_Week INT NOT NULL,
    Day_Name VARCHAR(20),
    Day_Type VARCHAR(20),
    Month_Name VARCHAR(20)
);
GO

CREATE TABLE dbo.Dim_Store (
    Store_SK INT PRIMARY KEY IDENTITY(1,1),
    Store_ID INT NOT NULL UNIQUE,
    Store_Name VARCHAR(100)
);
GO

CREATE TABLE dbo.Dim_Supplier (
    Supplier_SK INT PRIMARY KEY IDENTITY(1,1),
    Supplier_ID INT NOT NULL UNIQUE,
    Supplier_Name VARCHAR(100)
);
GO

PRINT 'Dimension tables created successfully.';
GO

PRINT 'Creating fact table...';
GO

CREATE TABLE dbo.Fact_Sales (
    Sales_SK INT PRIMARY KEY IDENTITY(1,1),
    Order_ID INT NOT NULL,
    Customer_SK INT NOT NULL,
    Product_SK INT NOT NULL,
    Date_SK INT NOT NULL,
    Store_SK INT NOT NULL,
    Supplier_SK INT NOT NULL,
    Quantity INT NOT NULL,
    Total_Revenue DECIMAL(12, 2) NOT NULL,
    
    CONSTRAINT FK_Customer FOREIGN KEY (Customer_SK) REFERENCES dbo.Dim_Customer(Customer_SK),
    CONSTRAINT FK_Product FOREIGN KEY (Product_SK) REFERENCES dbo.Dim_Product(Product_SK),
    CONSTRAINT FK_Date FOREIGN KEY (Date_SK) REFERENCES dbo.Dim_Date(Date_SK),
    CONSTRAINT FK_Store FOREIGN KEY (Store_SK) REFERENCES dbo.Dim_Store(Store_SK),
    CONSTRAINT FK_Supplier FOREIGN KEY (Supplier_SK) REFERENCES dbo.Dim_Supplier(Supplier_SK)
);
GO

PRINT 'Fact table created successfully.';
GO

PRINT 'Creating indexes...';
GO

CREATE NONCLUSTERED INDEX IDX_Customer_ID ON dbo.Dim_Customer(Customer_ID);
CREATE NONCLUSTERED INDEX IDX_Product_ID ON dbo.Dim_Product(Product_ID);
CREATE NONCLUSTERED INDEX IDX_Date ON dbo.Dim_Date(Date);
CREATE NONCLUSTERED INDEX IDX_Store_ID ON dbo.Dim_Store(Store_ID);
CREATE NONCLUSTERED INDEX IDX_Supplier_ID ON dbo.Dim_Supplier(Supplier_ID);

CREATE NONCLUSTERED INDEX IDX_Fact_Customer_SK ON dbo.Fact_Sales(Customer_SK);
CREATE NONCLUSTERED INDEX IDX_Fact_Product_SK ON dbo.Fact_Sales(Product_SK);
CREATE NONCLUSTERED INDEX IDX_Fact_Date_SK ON dbo.Fact_Sales(Date_SK);
CREATE NONCLUSTERED INDEX IDX_Fact_Store_SK ON dbo.Fact_Sales(Store_SK);
CREATE NONCLUSTERED INDEX IDX_Fact_Supplier_SK ON dbo.Fact_Sales(Supplier_SK);

CREATE NONCLUSTERED INDEX IDX_Fact_Date_Product ON dbo.Fact_Sales(Date_SK, Product_SK);
CREATE NONCLUSTERED INDEX IDX_Fact_Date_Customer ON dbo.Fact_Sales(Date_SK, Customer_SK);
CREATE NONCLUSTERED INDEX IDX_Fact_Store_Date ON dbo.Fact_Sales(Store_SK, Date_SK);
GO

PRINT 'Indexes created successfully.';
GO

PRINT 'Creating views...';
GO

IF OBJECT_ID('dbo.STORE_QUARTERLY_SALES', 'V') IS NOT NULL 
    DROP VIEW dbo.STORE_QUARTERLY_SALES;
GO

CREATE VIEW dbo.STORE_QUARTERLY_SALES AS
SELECT 
    d.Year,
    d.Quarter,
    s.Store_SK,
    s.Store_Name,
    SUM(f.Total_Revenue) AS Total_Revenue,
    SUM(f.Quantity) AS Total_Quantity,
    COUNT(f.Order_ID) AS Total_Orders,
    AVG(f.Total_Revenue) AS Avg_Order_Value
FROM dbo.Fact_Sales f
JOIN dbo.Dim_Date d ON f.Date_SK = d.Date_SK
JOIN dbo.Dim_Store s ON f.Store_SK = s.Store_SK
GROUP BY d.Year, d.Quarter, s.Store_SK, s.Store_Name;
GO

PRINT 'Views created successfully.';
GO

PRINT '';
PRINT '============================================================================';
PRINT 'STAR SCHEMA CREATION COMPLETE!';
PRINT '============================================================================';
PRINT '';
PRINT 'Tables Created:';
PRINT '  - Dim_Customer';
PRINT '  - Dim_Product';
PRINT '  - Dim_Date';
PRINT '  - Dim_Store';
PRINT '  - Dim_Supplier';
PRINT '  - Fact_Sales';
PRINT '';
PRINT 'Views Created:';
PRINT '  - STORE_QUARTERLY_SALES';
PRINT '';
PRINT 'Next Steps:';
PRINT '  1. Run: py walmart_hybridjoin_etl.py (to load data via HYBRIDJOIN)';
PRINT '  2. Run: 3_olap_queries.sql (for business intelligence analysis)';
PRINT '';
PRINT '============================================================================';
GO
