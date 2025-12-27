USE November2025DW;
GO

PRINT '============================================================================';
PRINT 'STARTING OLAP ANALYSIS QUERIES';
PRINT '============================================================================';
PRINT '';
GO

PRINT 'Executing Q1: Top Revenue Products by Month and Day Type...';
GO

WITH Monthly_Product_Revenue AS (
    SELECT 
        d.Year,
        d.Month,
        d.Month_Name,
        d.Day_Type,
        p.Product_ID,
        p.Product_Category,
        SUM(f.Total_Revenue) AS Total_Revenue,
        ROW_NUMBER() OVER (PARTITION BY d.Year, d.Month, d.Day_Type ORDER BY SUM(f.Total_Revenue) DESC) AS Revenue_Rank
    FROM Fact_Sales f
    JOIN Dim_Date d ON f.Date_SK = d.Date_SK
    JOIN Dim_Product p ON f.Product_SK = p.Product_SK
    GROUP BY d.Year, d.Month, d.Month_Name, d.Day_Type, p.Product_ID, p.Product_Category
)
SELECT 
    Year,
    Month,
    Month_Name,
    Day_Type,
    Product_ID,
    Product_Category,
    Total_Revenue,
    Revenue_Rank
FROM Monthly_Product_Revenue
WHERE Revenue_Rank <= 5
ORDER BY Year, Month, Day_Type, Revenue_Rank;


SELECT 
    c.Gender,
    c.Age,
    c.City_Category,
    COUNT(DISTINCT f.Customer_SK) AS Customer_Count,
    SUM(f.Total_Revenue) AS Total_Purchase_Amount,
    AVG(f.Total_Revenue) AS Avg_Purchase_Amount,
    SUM(f.Quantity) AS Total_Quantity
FROM Fact_Sales f
JOIN Dim_Customer c ON f.Customer_SK = c.Customer_SK
GROUP BY c.Gender, c.Age, c.City_Category
ORDER BY Total_Purchase_Amount DESC;


SELECT 
    c.Occupation,
    p.Product_Category,
    SUM(f.Total_Revenue) AS Total_Sales,
    SUM(f.Quantity) AS Total_Quantity,
    COUNT(DISTINCT f.Customer_SK) AS Unique_Customers
FROM Fact_Sales f
JOIN Dim_Customer c ON f.Customer_SK = c.Customer_SK
JOIN Dim_Product p ON f.Product_SK = p.Product_SK
GROUP BY c.Occupation, p.Product_Category
ORDER BY c.Occupation, Total_Sales DESC;


SELECT 
    d.Year,
    d.Quarter,
    c.Gender,
    c.Age,
    SUM(f.Total_Revenue) AS Total_Purchases,
    SUM(f.Quantity) AS Total_Quantity,
    COUNT(DISTINCT f.Order_ID) AS Total_Orders
FROM Fact_Sales f
JOIN Dim_Customer c ON f.Customer_SK = c.Customer_SK
JOIN Dim_Date d ON f.Date_SK = d.Date_SK
GROUP BY d.Year, d.Quarter, c.Gender, c.Age
ORDER BY d.Year, d.Quarter, Total_Purchases DESC;


WITH Occupation_Category_Sales AS (
    SELECT 
        c.Occupation,
        p.Product_Category,
        SUM(f.Total_Revenue) AS Total_Sales,
        ROW_NUMBER() OVER (PARTITION BY p.Product_Category ORDER BY SUM(f.Total_Revenue) DESC) AS Sales_Rank
    FROM Fact_Sales f
    JOIN Dim_Customer c ON f.Customer_SK = c.Customer_SK
    JOIN Dim_Product p ON f.Product_SK = p.Product_SK
    GROUP BY c.Occupation, p.Product_Category
)
SELECT 
    Occupation,
    Product_Category,
    Total_Sales,
    Sales_Rank
FROM Occupation_Category_Sales
WHERE Sales_Rank <= 5
ORDER BY Product_Category, Sales_Rank;


SELECT 
    d.Year,
    d.Month,
    c.City_Category,
    c.Marital_Status,
    SUM(f.Total_Revenue) AS Total_Revenue,
    COUNT(DISTINCT f.Customer_SK) AS Unique_Customers,
    AVG(f.Total_Revenue) AS Avg_Purchase_Value
FROM Fact_Sales f
JOIN Dim_Customer c ON f.Customer_SK = c.Customer_SK
JOIN Dim_Date d ON f.Date_SK = d.Date_SK
GROUP BY d.Year, d.Month, c.City_Category, c.Marital_Status
ORDER BY d.Year, d.Month, Total_Revenue DESC;


SELECT 
    c.Stay_In_Current_City_Years,
    c.Gender,
    COUNT(DISTINCT f.Customer_SK) AS Customer_Count,
    SUM(f.Total_Revenue) AS Total_Revenue,
    AVG(f.Total_Revenue) AS Avg_Purchase_Amount,
    SUM(f.Quantity) AS Total_Quantity
FROM Fact_Sales f
JOIN Dim_Customer c ON f.Customer_SK = c.Customer_SK
GROUP BY c.Stay_In_Current_City_Years, c.Gender
ORDER BY c.Stay_In_Current_City_Years, c.Gender;


WITH City_Category_Revenue AS (
    SELECT 
        c.City_Category,
        p.Product_Category,
        SUM(f.Total_Revenue) AS Total_Revenue,
        ROW_NUMBER() OVER (PARTITION BY p.Product_Category ORDER BY SUM(f.Total_Revenue) DESC) AS Revenue_Rank
    FROM Fact_Sales f
    JOIN Dim_Customer c ON f.Customer_SK = c.Customer_SK
    JOIN Dim_Product p ON f.Product_SK = p.Product_SK
    GROUP BY c.City_Category, p.Product_Category
)
SELECT 
    City_Category,
    Product_Category,
    Total_Revenue,
    Revenue_Rank
FROM City_Category_Revenue
WHERE Revenue_Rank <= 5
ORDER BY Product_Category, Revenue_Rank;


WITH Monthly_Sales AS (
    SELECT 
        d.Year,
        d.Month,
        p.Product_Category,
        SUM(f.Total_Revenue) AS Current_Month_Revenue,
        LAG(SUM(f.Total_Revenue)) OVER (PARTITION BY p.Product_Category ORDER BY d.Year, d.Month) AS Previous_Month_Revenue
    FROM Fact_Sales f
    JOIN Dim_Date d ON f.Date_SK = d.Date_SK
    JOIN Dim_Product p ON f.Product_SK = p.Product_SK
    GROUP BY d.Year, d.Month, p.Product_Category
)
SELECT 
    Year,
    Month,
    Product_Category,
    Current_Month_Revenue,
    Previous_Month_Revenue,
    CASE 
        WHEN Previous_Month_Revenue IS NULL OR Previous_Month_Revenue = 0 THEN 0
        ELSE ((Current_Month_Revenue - Previous_Month_Revenue) / Previous_Month_Revenue) * 100
    END AS Growth_Percentage
FROM Monthly_Sales
ORDER BY Product_Category, Year, Month;


SELECT 
    d.Year,
    c.Age,
    d.Day_Type,
    SUM(f.Total_Revenue) AS Total_Sales,
    SUM(f.Quantity) AS Total_Quantity,
    COUNT(DISTINCT f.Order_ID) AS Total_Orders,
    AVG(f.Total_Revenue) AS Avg_Order_Value
FROM Fact_Sales f
JOIN Dim_Customer c ON f.Customer_SK = c.Customer_SK
JOIN Dim_Date d ON f.Date_SK = d.Date_SK
GROUP BY d.Year, c.Age, d.Day_Type
ORDER BY d.Year, c.Age, d.Day_Type;


WITH Product_Revenue_Detail AS (
    SELECT 
        d.Year,
        d.Month,
        d.Day_Type,
        p.Product_ID,
        p.Product_Category,
        SUM(f.Total_Revenue) AS Total_Revenue,
        SUM(f.Quantity) AS Total_Quantity,
        ROW_NUMBER() OVER (PARTITION BY d.Year, d.Month, d.Day_Type ORDER BY SUM(f.Total_Revenue) DESC) AS Revenue_Rank
    FROM Fact_Sales f
    JOIN Dim_Date d ON f.Date_SK = d.Date_SK
    JOIN Dim_Product p ON f.Product_SK = p.Product_SK
    GROUP BY d.Year, d.Month, d.Day_Type, p.Product_ID, p.Product_Category
)
SELECT 
    Year,
    Month,
    Day_Type,
    Product_ID,
    Product_Category,
    Total_Revenue,
    Total_Quantity,
    Revenue_Rank
FROM Product_Revenue_Detail
WHERE Revenue_Rank <= 5
ORDER BY Year, Month, Day_Type, Revenue_Rank;


WITH Quarterly_Store_Revenue AS (
    SELECT 
        d.Year,
        d.Quarter,
        s.Store_SK,
        s.Store_Name,
        SUM(f.Total_Revenue) AS Current_Quarter_Revenue,
        LAG(SUM(f.Total_Revenue)) OVER (PARTITION BY s.Store_SK ORDER BY d.Year, d.Quarter) AS Previous_Quarter_Revenue
    FROM Fact_Sales f
    JOIN Dim_Date d ON f.Date_SK = d.Date_SK
    JOIN Dim_Store s ON f.Store_SK = s.Store_SK
    WHERE d.Year = 2017
    GROUP BY d.Year, d.Quarter, s.Store_SK, s.Store_Name
)
SELECT 
    Year,
    Quarter,
    Store_Name,
    Current_Quarter_Revenue,
    Previous_Quarter_Revenue,
    CASE 
        WHEN Previous_Quarter_Revenue IS NULL OR Previous_Quarter_Revenue = 0 THEN 0
        ELSE ((Current_Quarter_Revenue - Previous_Quarter_Revenue) / Previous_Quarter_Revenue) * 100
    END AS Growth_Rate_Percentage
FROM Quarterly_Store_Revenue
ORDER BY Store_Name, Quarter;


SELECT 
    s.Store_Name,
    sup.Supplier_Name,
    p.Product_ID,
    p.Product_Category,
    SUM(f.Total_Revenue) AS Total_Sales,
    SUM(f.Quantity) AS Total_Quantity,
    COUNT(DISTINCT f.Order_ID) AS Total_Orders
FROM Fact_Sales f
JOIN Dim_Store s ON f.Store_SK = s.Store_SK
JOIN Dim_Supplier sup ON f.Supplier_SK = sup.Supplier_SK
JOIN Dim_Product p ON f.Product_SK = p.Product_SK
GROUP BY s.Store_Name, sup.Supplier_Name, p.Product_ID, p.Product_Category
ORDER BY s.Store_Name, sup.Supplier_Name, Total_Sales DESC;


SELECT 
    d.Year,
    d.Quarter,
    d.Month_Name,
    p.Product_ID,
    p.Product_Category,
    SUM(f.Total_Revenue) AS Total_Sales,
    SUM(f.Quantity) AS Total_Quantity,
    CASE 
        WHEN d.Month IN (12, 1, 2) THEN 'Winter'
        WHEN d.Month IN (3, 4, 5) THEN 'Spring'
        WHEN d.Month IN (6, 7, 8) THEN 'Summer'
        ELSE 'Fall'
    END AS Season
FROM Fact_Sales f
JOIN Dim_Date d ON f.Date_SK = d.Date_SK
JOIN Dim_Product p ON f.Product_SK = p.Product_SK
GROUP BY d.Year, d.Quarter, d.Month_Name, d.Month, p.Product_ID, p.Product_Category
ORDER BY p.Product_Category, d.Year, d.Quarter;


WITH Monthly_Revenue AS (
    SELECT 
        d.Year,
        d.Month,
        s.Store_Name,
        sup.Supplier_Name,
        SUM(f.Total_Revenue) AS Monthly_Revenue
    FROM Fact_Sales f
    JOIN Dim_Date d ON f.Date_SK = d.Date_SK
    JOIN Dim_Store s ON f.Store_SK = s.Store_SK
    JOIN Dim_Supplier sup ON f.Supplier_SK = sup.Supplier_SK
    GROUP BY d.Year, d.Month, s.Store_Name, sup.Supplier_Name
),
Revenue_Changes AS (
    SELECT 
        Year,
        Month,
        Store_Name,
        Supplier_Name,
        Monthly_Revenue,
        LAG(Monthly_Revenue) OVER (PARTITION BY Store_Name, Supplier_Name ORDER BY Year, Month) AS Previous_Month_Revenue,
        ABS(Monthly_Revenue - LAG(Monthly_Revenue) OVER (PARTITION BY Store_Name, Supplier_Name ORDER BY Year, Month)) AS Revenue_Change
    FROM Monthly_Revenue
)
SELECT 
    Store_Name,
    Supplier_Name,
    AVG(Monthly_Revenue) AS Avg_Monthly_Revenue,
    STDEV(Monthly_Revenue) AS Revenue_Std_Dev,
    (STDEV(Monthly_Revenue) / AVG(Monthly_Revenue)) * 100 AS Volatility_Percentage
FROM Revenue_Changes
WHERE Previous_Month_Revenue IS NOT NULL
GROUP BY Store_Name, Supplier_Name
ORDER BY Volatility_Percentage DESC;


WITH Product_Pairs AS (
    SELECT 
        f1.Product_SK AS Product1_SK,
        f2.Product_SK AS Product2_SK,
        COUNT(DISTINCT f1.Customer_SK) AS Co_Purchase_Count
    FROM Fact_Sales f1
    JOIN Fact_Sales f2 ON f1.Customer_SK = f2.Customer_SK AND f1.Product_SK < f2.Product_SK
    GROUP BY f1.Product_SK, f2.Product_SK
),
Ranked_Pairs AS (
    SELECT 
        p1.Product_ID AS Product1_ID,
        p1.Product_Category AS Product1_Category,
        p2.Product_ID AS Product2_ID,
        p2.Product_Category AS Product2_Category,
        pp.Co_Purchase_Count,
        ROW_NUMBER() OVER (ORDER BY pp.Co_Purchase_Count DESC) AS Affinity_Rank
    FROM Product_Pairs pp
    JOIN Dim_Product p1 ON pp.Product1_SK = p1.Product_SK
    JOIN Dim_Product p2 ON pp.Product2_SK = p2.Product_SK
)
SELECT 
    Product1_ID,
    Product1_Category,
    Product2_ID,
    Product2_Category,
    Co_Purchase_Count
FROM Ranked_Pairs
WHERE Affinity_Rank <= 3;


SELECT 
    d.Year,
    s.Store_Name,
    sup.Supplier_Name,
    p.Product_Category,
    SUM(f.Total_Revenue) AS Total_Revenue,
    SUM(f.Quantity) AS Total_Quantity
FROM Fact_Sales f
JOIN Dim_Date d ON f.Date_SK = d.Date_SK
JOIN Dim_Store s ON f.Store_SK = s.Store_SK
JOIN Dim_Supplier sup ON f.Supplier_SK = sup.Supplier_SK
JOIN Dim_Product p ON f.Product_SK = p.Product_SK
GROUP BY ROLLUP(d.Year, s.Store_Name, sup.Supplier_Name, p.Product_Category)
ORDER BY d.Year, s.Store_Name, sup.Supplier_Name, p.Product_Category;


SELECT 
    d.Year,
    CASE 
        WHEN d.Month BETWEEN 1 AND 6 THEN 'H1'
        ELSE 'H2'
    END AS Half_Year,
    p.Product_ID,
    p.Product_Category,
    SUM(f.Total_Revenue) AS Total_Revenue,
    SUM(f.Quantity) AS Total_Quantity,
    COUNT(DISTINCT f.Order_ID) AS Total_Orders,
    AVG(f.Total_Revenue) AS Avg_Order_Value
FROM Fact_Sales f
JOIN Dim_Date d ON f.Date_SK = d.Date_SK
JOIN Dim_Product p ON f.Product_SK = p.Product_SK
GROUP BY d.Year, 
    CASE 
        WHEN d.Month BETWEEN 1 AND 6 THEN 'H1'
        ELSE 'H2'
    END,
    p.Product_ID, 
    p.Product_Category
ORDER BY d.Year, Half_Year, Total_Revenue DESC;


WITH Daily_Product_Revenue AS (
    SELECT 
        d.Date,
        p.Product_ID,
        p.Product_Category,
        SUM(f.Total_Revenue) AS Daily_Revenue
    FROM Fact_Sales f
    JOIN Dim_Date d ON f.Date_SK = d.Date_SK
    JOIN Dim_Product p ON f.Product_SK = p.Product_SK
    GROUP BY d.Date, p.Product_ID, p.Product_Category
),
Revenue_Statistics AS (
    SELECT 
        Product_ID,
        Product_Category,
        AVG(Daily_Revenue) AS Avg_Daily_Revenue,
        STDEV(Daily_Revenue) AS Std_Dev_Revenue
    FROM Daily_Product_Revenue
    GROUP BY Product_ID, Product_Category
)
SELECT 
    dpr.Date,
    dpr.Product_ID,
    dpr.Product_Category,
    dpr.Daily_Revenue,
    rs.Avg_Daily_Revenue,
    rs.Std_Dev_Revenue,
    CASE 
        WHEN dpr.Daily_Revenue > (rs.Avg_Daily_Revenue + 2 * rs.Std_Dev_Revenue) THEN 'High Spike'
        WHEN dpr.Daily_Revenue < (rs.Avg_Daily_Revenue - 2 * rs.Std_Dev_Revenue) THEN 'Low Spike'
        ELSE 'Normal'
    END AS Revenue_Status
FROM Daily_Product_Revenue dpr
JOIN Revenue_Statistics rs ON dpr.Product_ID = rs.Product_ID
WHERE dpr.Daily_Revenue > (rs.Avg_Daily_Revenue + 2 * rs.Std_Dev_Revenue)
   OR dpr.Daily_Revenue < (rs.Avg_Daily_Revenue - 2 * rs.Std_Dev_Revenue)
ORDER BY dpr.Date, dpr.Product_ID;


SELECT 
    Year,
    Quarter,
    Store_Name,
    Total_Revenue,
    Total_Quantity,
    Total_Orders,
    Avg_Order_Value,
    ROW_NUMBER() OVER (PARTITION BY Year, Quarter ORDER BY Total_Revenue DESC) AS Store_Rank
FROM STORE_QUARTERLY_SALES
ORDER BY Year, Quarter, Total_Revenue DESC;

SELECT 
    d.Year,
    d.Quarter,
    s.Store_SK,
    s.Store_Name,
    SUM(f.Total_Revenue) AS Total_Revenue,
    SUM(f.Quantity) AS Total_Quantity,
    COUNT(f.Order_ID) AS Total_Orders,
    AVG(f.Total_Revenue) AS Avg_Order_Value
FROM Fact_Sales f
JOIN Dim_Date d ON f.Date_SK = d.Date_SK
JOIN Dim_Store s ON f.Store_SK = s.Store_SK
GROUP BY d.Year, d.Quarter, s.Store_SK, s.Store_Name
ORDER BY d.Year, d.Quarter, Total_Revenue DESC;
