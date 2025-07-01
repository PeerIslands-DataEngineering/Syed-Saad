-- 1. Top 5 Most Expensive Products
SELECT ProductName, UnitPrice
FROM Products
ORDER BY UnitPrice DESC
LIMIT 5;

-- 2. Products Out of Stock
SELECT ProductName
FROM Products
WHERE UnitsInStock = 0;

-- 3. Titles Shared by More Than One Employee
SELECT TitleOfCourtesy, COUNT(*) AS Count
FROM Employees
GROUP BY TitleOfCourtesy
HAVING COUNT(*) > 1;

-- 4. Average Freight Cost per Customer
SELECT CustomerID, AVG(Freight) AS AvgFreight
FROM Orders
GROUP BY CustomerID;

-- 5. Second Most Expensive Product
SELECT ProductName, UnitPrice
FROM Products
WHERE UnitPrice = (
    SELECT MAX(UnitPrice)
    FROM Products
    WHERE UnitPrice < (
        SELECT MAX(UnitPrice)
        FROM Products
    )
);

-- 6. Products Priced Above Category Average
SELECT ProductName, UnitPrice
FROM Products p
WHERE UnitPrice > (
    SELECT AVG(UnitPrice)
    FROM Products
    WHERE CategoryID = p.CategoryID
);

-- 7. Rank Products by Price per Supplier
SELECT 
    ProductName, 
    SupplierID, 
    UnitPrice,
    RANK() OVER (PARTITION BY SupplierID ORDER BY UnitPrice DESC) AS PriceRank
FROM Products;

-- 8. First Names Shared by Multiple Employees
SELECT FirstName, COUNT(*) AS Count
FROM Employees
GROUP BY FirstName
HAVING COUNT(*) > 1;
