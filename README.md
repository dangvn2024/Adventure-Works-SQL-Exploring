# Adventure-Works-SQL-Exploring
Utilize Google BigQuery SQL to explore a bycicle manufacturer on across multiple departments

## Table of Contents:
1. [Introduction](#data)
2. [Sales Department](#cau3)
3. [Production Department](#cau4)
4. [Purchasing Department](#cau5)

<div id='data'/>

## 1. Introduction 

The adventureworks2019 dataset is stored in Google BigQuery. This dataset contains all information about this business from 2011 to 2014. Based on this dataset, the author has formulated exploratory questions for each department to query using SQL. The results can be used as tables for creating a strategic dashboard.

To query and work with this dataset, the author uses the Google BigQuery tool to write and execute SQL queries.

Dataset: **adventureworks2019** (public Google BigQuery dataset)

Dataset dictionary: Please reach file "Data Dictionary" attached above

Dataset Schema: https://i0.wp.com/improveandrepeat.com/wp-content/uploads/2018/12/AdvWorksOLTPSchemaVisio.png?ssl=1

Dataset access: 
- Log in to your Google Cloud Platform account and create a new project.
- Navigate to the BigQuery console and select your newly created project.
- In the navigation panel, select "Add Data" and then "Star a project by name".
- Enter the project name "adventurework2019"

<div id='cau3'/>

## 2. Sales Department

### 2.1  Calculate Quantity of items, Sales value & Order quantity by each Subcategory in L12M

~~~~sql
WITH max_date as(
  select date_sub(date(max(ModifiedDate)), INTERVAL 12 month) as md
  from `adventureworks2019.Sales.SalesOrderDetail`
  )

select 
      format_datetime('%b %Y', a.ModifiedDate) month
      ,c.Name
      ,sum(a.OrderQty) qty_item
      ,sum(a.LineTotal) total_sales
      ,count(distinct a.SalesOrderID) order_cnt
FROM `adventureworks2019.Sales.SalesOrderDetail` a 
left join `adventureworks2019.Production.Product` b
  on a.ProductID = b.ProductID
left join `adventureworks2019.Production.ProductSubcategory` c
  on b.ProductSubcategoryID = cast(c.ProductSubcategoryID as string)

where date(a.ModifiedDate) >=  (select md from max_date )
group by 1,2
order by 2,1;

~~~~

**First rows:**

| month    | Name             | qty_item | total_sales      | order_cnt |
|----------|------------------|----------|------------------|-----------|
| Apr 2014 | Bib-Shorts        | 4        | 233.974          | 1         |
| Feb 2014 | Bib-Shorts        | 4        | 233.974          | 2         |
| Jul 2013 | Bib-Shorts        | 2        | 116.987          | 1         |
| Jun 2013 | Bib-Shorts        | 2        | 116.987          | 1         |
| Apr 2014 | Bike Racks        | 45       | 5400.0           | 45        |
| Aug 2013 | Bike Racks        | 222      | 17387.184        | 63        |
| Dec 2013 | Bike Racks        | 162      | 12582.288        | 48        |
| Feb 2014 | Bike Racks        | 27       | 3240.0           | 27        |
| Jan 2014 | Bike Racks        | 161      | 12840.0          | 53        |
| Jul 2013 | Bike Racks        | 422      | 29802.3          | 75        |
| Jun 2013 | Bike Racks        | 363      | 24684.0          | 57        |
| Jun 2014 | Bike Racks        | 20       | 2400.0           | 20        |
| Mar 2014 | Bike Racks        | 492      | 35588.712        | 100       |
| May 2014 | Bike Racks        | 284      | 21704.688        | 81        |

**Insights**

Top-selling Subcategories: Bike Racks and Jerseys generate significant sales volumes, with March 2014 and July 2013 standing out as strong sales months.

Seasonal Trends: Mountain Bikes and Road Bikes experience peak sales during summer months, indicating the need for increased stock during this period.

High-quantity Add-ons: Lower-cost items like Socks and Caps sell in large quantities, suggesting they are popular as add-on purchases.

Peak Sales Months: March and May 2014 show heightened demand across several subcategories, indicating potential promotional activities or seasonal demand spikes.


### 2.2  Calculate % YoY growth rate by SubCategory & release top 3 category with highest grow rate. Can use metric: quantity_item. Round results to 2 decimal

~~~~sql

with 
sale_info as (
  SELECT 
      FORMAT_TIMESTAMP("%Y", a.ModifiedDate) as yr
      , c.Name
      , sum(a.OrderQty) as qty_item

  FROM `adventureworks2019.Sales.SalesOrderDetail` a 
  LEFT JOIN `adventureworks2019.Production.Product` b on a.ProductID = b.ProductID
  LEFT JOIN `adventureworks2019.Production.ProductSubcategory` c on cast(b.ProductSubcategoryID as int) = c.ProductSubcategoryID

  GROUP BY 1,2
  ORDER BY 2 asc , 1 desc
),

sale_diff as (
  select *
  , lead (qty_item) over (partition by Name order by yr desc) as prv_qty
  , round(qty_item / (lead (qty_item) over (partition by Name order by yr desc)) -1,2) as qty_diff
  from sale_info
  order by 5 desc 
),

rk_qty_diff as (
  select *
      ,dense_rank() over( order by qty_diff desc) dk
  from sale_diff
)

select distinct Name
      , qty_item
      , prv_qty
      , qty_diff
from rk_qty_diff 
where dk <=3
order by dk ;
~~~~
| year | Name            | qty_item | prv_qty | qty_diff | rk |
|------|-----------------|----------|---------|----------|----|
| 2012 | Mountain Frames | 3168     | 510     | 5.21     | 1  |
| 2013 | Socks           | 2724     | 523     | 4.21     | 2  |
| 2012 | Road Frames     | 5564     | 1137    | 3.89     | 3  |

### 2.3  Ranking Top 3 TeritoryID with biggest Order quantity of every year. If there's TerritoryID with same quantity in a year

~~~~sql

WITH rd_q3 AS (

    SELECT 
        FORMAT_TIMESTAMP("%Y", a.ModifiedDate) as yr
        ,TerritoryId
        ,SUM (OrderQty) AS order_cnt

    FROM `adventureworks2019.Sales.SalesOrderDetail` AS a
    LEFT JOIN `adventureworks2019.Sales.SalesOrderHeader`  AS b  
        USING (SalesOrderId)

    GROUP BY 1,2
    ORDER BY 1 DESC
    ),

    rk_raw AS (

    SELECT 
        *
        ,DENSE_RANK () OVER (PARTITION BY year ORDER BY order_cnt DESC) AS rk
    FROM rd_q3
    ORDER BY year DESC
        )

SELECT *
FROM rk_raw
WHERE rk <= 3;
~~~~

| year | TerritoryId | order_cnt | rk |
|------|-------------|-----------|----|
| 2014 | 4           | 11632     | 1  |
| 2014 | 6           | 9711      | 2  |
| 2014 | 1           | 8823      | 3  |
| 2013 | 4           | 26682     | 1  |
| 2013 | 6           | 22553     | 2  |
| 2013 | 1           | 17452     | 3  |
| 2012 | 4           | 17553     | 1  |
| 2012 | 6           | 14412     | 2  |
| 2012 | 1           | 8537      | 3  |
| 2011 | 4           | 3238      | 1  |
| 2011 | 6           | 2705      | 2  |
| 2011 | 1           | 1964      | 3  |

### 2.4  Calc Total Discount Cost belongs to Seasonal Discount for each SubCategory

~~~~sql
with disc_cost_info AS (
    SELECT 
        DISTINCT a.*
        ,c.Name
        ,d.DiscountPct
        ,d.Type
        ,a.OrderQty * d.DiscountPct * UnitPrice as disc_cost
    FROM `adventureworks2019.Sales.SalesOrderDetail` as a
    LEFT JOIN `adventureworks2019.Production.Product` as b
        ON a.ProductID = b.ProductID
    LEFT JOIN `adventureworks2019.Production.ProductSubcategory` as c
        ON cast(b.ProductSubcategoryID as int) = c.ProductSubcategoryID
    LEFT JOIN `adventureworks2019.Sales.SpecialOffer` as d
        ON a.SpecialOfferID = d.SpecialOfferID

    WHERE lower(d.Type) like '%seasonal discount%'
    )

SELECT 
    FORMAT_TIMESTAMP("%Y", ModifiedDate) AS year
    ,Name
    ,SUM(disc_cost) AS total_cost
FROM disc_cost_info
GROUP BY 1,2;
~~~~

| year | Name    | total_cost   |
|------|---------|--------------|
| 2012 | Helmets | 827.64732    |
| 2013 | Helmets | 1606.041     |


### 2.4  Retention rate of Customer in 2014 with status of Successfully Shipped (Cohort Analysis)
~~~~sql

with 
info as (
  select  
      extract(month from ModifiedDate) as month_no
      , extract(year from ModifiedDate) as year_no
      , CustomerID
      , count(Distinct SalesOrderID) as order_cnt
  from `adventureworks2019.Sales.SalesOrderHeader`
  where FORMAT_TIMESTAMP("%Y", ModifiedDate) = '2014'
  and Status = 5
  group by 1,2,3
  order by 3,1 
),

row_num as
  select *
      , row_number() over (partition by CustomerID order by month_no) as row_numb
  from info 
), 

first_order as (   
  select *
  from row_num
  where row_numb = 1
), 

month_gap as (
  select 
      a.CustomerID
      , b.month_no as month_join
      , a.month_no as month_order
      , a.order_cnt
      , concat('M - ',a.month_no - b.month_no) as month_diff
  from info a 
  left join first_order b 
  on a.CustomerID = b.CustomerID
  order by 1,3
)

select month_join
      , month_diff 
      , count(distinct CustomerID) as customer_cnt
from month_gap
group by 1,2
order by 1,2;
~~~~

| month_join | month_diff | customer_cnt |
|------------|------------|--------------|
| 1          | M - 0      | 2076         |
| 1          | M - 1      | 78           |
| 1          | M - 2      | 89           |
| 1          | M - 3      | 252          |
| 1          | M - 4      | 96           |
| 1          | M - 5      | 61           |
| 1          | M - 6      | 18           |
| 2          | M - 0      | 1805         |
| 2          | M - 1      | 51           |
| 2          | M - 2      | 61           |
| 2          | M - 3      | 234          |
| 2          | M - 4      | 58           |
| 2          | M - 5      | 8            |
| 3          | M - 0      | 1918         |
| 3          | M - 1      | 43           |
| 3          | M - 2      | 58           |
| 3          | M - 3      | 44           |
| 3          | M - 4      | 11           |
| 4          | M - 0      | 1906         |
| 4          | M - 1      | 34           |


**Analysis Based on the Provided Data:**

From the given data, each row represents a cohort of customers who joined in a specific month (month_join) and tracks their behavior over several months (month_diff).
Cohorts in "M - 0" represent the first month customers made a purchase or engaged with the company.
As time progresses (in month_diff), we see how many of those customers continue to engage (retained) over the following months.
Key Insights from Retention Data:

Strong Initial Engagement: In the month they joined ("M - 0"), each cohort starts with a relatively high number of customers (e.g., 2076 in month 1, 1805 in month 2, etc.).
Sharp Drop in Retention: Moving from "M - 0" to subsequent months (M - 1, M - 2, and so on), there is a noticeable drop in the number of retained customers. For example, in the first cohort, only 78 customers remained active in "M - 1" compared to the initial 2076.
Retention Stabilizes in Later Months: After the first few months, retention levels out, and the number of active customers becomes more stable, although much lower than the initial count. This pattern suggests that most customer churn happens early on, but those who remain are more likely to stay longer.
Retention Strategy:

The data shows that customer retention is a major challenge after the initial month. Most customers drop off within the first 2-3 months.
Improving the onboarding process, offering post-purchase engagement, or loyalty programs could help increase retention rates in the early months.


<div id='cau4'/>

## 3. Production Department
 


### 3.1 Trend of Stock level & MoM diff % by all product in 2011. If %gr rate is null then 0. Round to 1 decimal
~~~~sql

WITH rd_q6 AS (
    SELECT  
        b.Name
        ,EXTRACT (month FROM a.ModifiedDate) AS month 
        ,EXTRACT (year FROM a.ModifiedDate) AS year
        ,Sum (StockedQTy) AS stock_qty
    FROM `adventureworks2019.Production.WorkOrder` AS a
    LEFT JOIN `adventureworks2019.Production.Product` AS b
        USING (ProductID)
    GROUP BY 1,2,3
    ORDER BY 1,3,2 DESC 
    ),

    prev_stock AS (
    SELECT 
        *
        ,LAG (stock_qty) OVER (PARTITION BY Name ORDER BY year, month) AS  stock_prv
    FROM rd_q6
    ORDER BY 1,3,2 DESC
    )
SELECT 
    *
    ,ROUND ((stock_qty - stock_prv) / stock_prv *100 ,1) AS diff
FROM prev_stock 
WHERE year = 2011;  

~~~~

| Name                     | Month | Year | Stock Qty | Stock Previous | Difference (%) |
|--------------------------|-------|------|-----------|----------------|----------------|
| BB Ball Bearing           | 12    | 2011 | 8475      | 14544          | -41.7          |
| BB Ball Bearing           | 11    | 2011 | 14544     | 19175          | -24.2          |
| BB Ball Bearing           | 10    | 2011 | 19175     | 8845           | 116.8          |
| BB Ball Bearing           | 9     | 2011 | 8845      | 9666           | -8.5           |
| BB Ball Bearing           | 8     | 2011 | 9666      | 12837          | -24.7          |
| BB Ball Bearing           | 7     | 2011 | 12837     | 5259           | 144.1          |
| BB Ball Bearing           | 6     | 2011 | 5259      |                |                |
| Blade                    | 12    | 2011 | 1842      | 3598           | -48.8          |
| Blade                    | 11    | 2011 | 3598      | 4670           | -23.0          |
| ...                       | ...   | ...  | ...       | ...            | ...            |
CSV Format

 
### 3.2 Calc Ratio of Stock / Sales in 2011 by product name, by month. Order results by month desc, ratio desc. Round Ratio to 1 decimal mom yoy
~~~~sql

WITH sales_info AS (
      SELECT  
        EXTRACT (month FROM a.ModifiedDate) AS month 
        ,EXTRACT (year FROM a.ModifiedDate) AS year
        ,ProductId
        ,Name
        ,SUM (OrderQty) AS sales
      FROM `adventureworks2019.Sales.SalesOrderDetail` AS a
      LEFT JOIN `adventureworks2019.Production.Product` AS b
        USING (ProductID)
      WHERE EXTRACT (year FROM a.ModifiedDate) = 2011
      GROUP BY 2,1,4,3
      ORDER BY 2,1 DESC ,4
    ),
    stock_info AS (
    SELECT 
        EXTRACT (month FROM ModifiedDate) AS month 
        ,EXTRACT (year FROM ModifiedDate) AS year 
        ,ProductID
        ,SUM (StockedQty) AS stock_cnt
    FROM adventureworks2019.Production.WorkOrder
    WHERE EXTRACT (year FROM ModifiedDate) = 2011
    GROUP BY 1,2,3
    )

SELECT 
    a.month
    ,a.year
    ,a.ProductID
    ,Name
    ,COALESCE(sales, 0) AS sales_cnt           
    ,COALESCE(stock_cnt, 0) AS stock_cnt       
    ,ROUND ( COALESCE(stock_cnt, 0) / sales  ,1) AS ratio 
FROM sales_info AS a
LEFT JOIN stock_info AS b
    ON a.ProductID = b.ProductID
    AND a.year = b.year
    AND a.month = b.month
ORDER BY 1 DESC, ratio DESC
;

~~~~

| month | year | ProductID | Name                                | sales | stock_cnt | ratio |
|-------|------|------------|-------------------------------------|-------|-----------|-------|
| 12    | 2011 | 745        | "HL Mountain Frame - Black, 48"     | 1     | 27        | 27.0  |
| 12    | 2011 | 743        | "HL Mountain Frame - Black, 42"     | 1     | 26        | 26.0  |
| 12    | 2011 | 748        | "HL Mountain Frame - Silver, 38"    | 2     | 32        | 16.0  |
| 12    | 2011 | 722        | "LL Road Frame - Black, 58"         | 4     | 47        | 11.8  |
| 12    | 2011 | 747        | "HL Mountain Frame - Black, 38"     | 3     | 31        | 10.3  |
| 12    | 2011 | 726        | "LL Road Frame - Red, 48"           | 5     | 36        | 7.2   |
| 12    | 2011 | 738        | "LL Road Frame - Black, 52"         | 10    | 64        | 6.4   |
| 12    | 2011 | 730        | "LL Road Frame - Red, 62"           | 7     | 38        | 5.4   |
| 12    | 2011 | 741        | "HL Mountain Frame - Silver, 48"    | 5     | 27        | 5.4   |
| 12    | 2011 | 725        | "LL Road Frame - Red, 44"           | 12    | 53        | 4.4   |
| 12    | 2011 | 729        | "LL Road Frame - Red, 60"           | 10    | 43        | 4.3   |
| 12    | 2011 | 732        | "ML Road Frame - Red, 48"           | 10    | 16        | 1.6   |
| 12    | 2011 | 750        | "Road-150 Red, 44"                  | 25    | 38        | 1.5   |
| 12    | 2011 | 751        | "Road-150 Red, 48"                  | 32    | 47        | 1.5   |
| 12    | 2011 | 775        | "Mountain-100 Black, 38"            | 23    | 28        | 1.2   |
| ...                       | ...   | ...  | ...       | ...            | ...            |





<div id='cau5'/>

## 4. Purchasing Department

### 4.1 No of order and value at Pending status in 2014

~~~~sql

SELECT
      EXTRACT (year FROM ModifiedDate) AS year
      ,Status
      ,COUNT (DISTINCT PurchaseOrderID) AS order_cnt
      ,SUM (TotalDue) AS value
FROM `adventureworks2019.Purchasing.PurchaseOrderHeader` 
WHERE status = 1
    AND EXTRACT (year FROM ModifiedDate) = 2014
GROUP BY 1,2
ORDER BY 1;
~~~~

| year | Status | order_cnt | value         |
|------|--------|-----------|---------------|
| 2014 | 1      | 224       | 3873579.0123  |



