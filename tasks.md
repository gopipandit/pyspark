# Advanced Spark Practice Tasks

## 1. Data Cleaning & Transformation
1. **Handle Missing Values**: Identify and replace null values in `sales` and `customers` datasets with appropriate defaults (e.g., `0` for numeric columns, "Unknown" for strings).  
2. **Data Type Optimization**: Convert `order_date` and `ship_date` to DateType and optimize data types (e.g., changing `unit_price`, `discount`, `profit` to FloatType).  
3. **Extract Date Parts**: Add columns for `year`, `month`, `day` to `orders` using `order_date`.  

## 2. Joins & Complex Queries
4. **Customer Purchase Analysis**: Find customers who have placed more than 5 orders and have an average order value greater than $500.  
5. **Shipping Time Analysis**: Calculate the average delivery time (difference between `ship_date` and `order_date`) per region.  
6. **Top-Selling Products**: Identify the top 5 most profitable products by summing up `profit` from `sales`.  
7. **Category-Wise Revenue**: Compute the total revenue for each `product_category` by joining `sales` with `products`.  
8. **Order Discount Analysis**: Find orders where the total discount given is greater than 20% of the total sales value.  

## 3. Window Functions & Ranking
9. **Customer Ranking**: Rank customers within each `region` based on their total spending.  
10. **Running Total of Sales**: Calculate a cumulative sum of `sales` over time for each `customer_id`.  
11. **First Purchase Date**: Find the first purchase date for each `customer_id` using a window function.  
12. **Top Product Per Category**: Identify the most sold `product_name` in each `product_category` using ranking functions.  
13. **Sales Growth Rate**: Calculate the month-over-month growth percentage of total `sales`.  

## 4. Aggregations & Grouping
14. **Quarterly Sales Performance**: Aggregate total sales per quarter and compare with the previous quarter.  
15. **Shipping Cost Analysis**: Compute the total shipping cost per `ship_mode` and identify the most expensive mode.  
16. **Customer Retention Rate**: Identify customers who placed at least one order every year in the last 3 years.  
17. **Product Performance Analysis**: Compute the profit margin for each `product_name` and rank them based on profitability.  

## 5. Performance Optimization & Advanced Spark Techniques
18. **Broadcast Joins**: Implement a broadcast join to optimize a small dataset join (e.g., `customers` with `sales`).  
19. **Bucketing & Partitioning**: Partition `sales` data by `order_date` (monthly) to improve query performance.  
20. **UDF for Custom Business Logic**: Write a UDF to classify orders into "High Profit", "Moderate Profit", and "Low Profit" based on `profit` percentage.  
