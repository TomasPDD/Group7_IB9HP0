library(readr)
library(RSQLite)
library(dplyr)
library(ggplot2)

#data visualisation

# Connect to the SQLite database
con <- dbConnect(RSQLite::SQLite(), dbname = "database/new_database.db")

# SQL query to count old customers 
query_old_customers <- "SELECT COUNT(*) as count FROM (SELECT customer_id FROM orders GROUP BY customer_id HAVING COUNT(product_id) > 1)"
old_customers <- dbGetQuery(con, query_old_customers)$count

# SQL query to count new customers 
query_new_customers <- "SELECT COUNT(*) as count FROM (SELECT customer_id FROM orders GROUP BY customer_id HAVING COUNT(product_id) = 1)"
new_customers <- dbGetQuery(con, query_new_customers)$count

# Create a data frame for the pie chart
pie_data <- data.frame(
  Category = c("Return Customers", "Churn Customers"),
  Count = c(old_customers, new_customers)
)

# Create the pie chart
plot1 <- ggplot(pie_data, aes(x = "", y = Count, fill = Category)) +
  geom_bar(width = 1, stat = "identity") +
  coord_polar("y", start = 0) +
  theme_void() +
  theme(legend.title = element_blank()) +
  labs(fill = "Customer Type") +
  scale_fill_manual(values = c("Return Customers" = "blue", "Churn Customers" = "red")) +
  geom_text(aes(label = scales::percent(Count / sum(Count))), position = position_stack(vjust = 0.5))

plot1
# 
# plot2 <- ggplot(payments, aes(x = payment_amount)) +
#   geom_histogram(bins = 30, fill = "blue", color = "black") +
#   labs(title = "Distribution of Payment Amounts",
#        x = "Payment Amount",
#        y = "Frequency") +
#   theme_minimal()
# 
# plot2

# Write your SQL query as a string
average_scores_per_category_sql <- "
SELECT
  cp.category_name,
  AVG(r.score) AS average_score
FROM
  reviews AS r
LEFT JOIN
  product_categories AS cp
ON
  r.product_id = cp.product_id
GROUP BY
  cp.category_name
ORDER BY
  average_score DESC
LIMIT 10
"

# Execute the query and fetch the results
average_scores_per_category <- dbGetQuery(con, average_scores_per_category_sql)





plot3 <- ggplot(average_scores_per_category, aes(x = reorder(category_name, average_score), y = average_score, fill = average_score)) +
  geom_col() +
  scale_fill_gradient(low = "blue", high = "red") +
  labs(title = "Average Review Score Per Category",
       x = "Category",
       y = "Average Score") +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

plot3 




# SQL query to aggregate sales by category and month
sales_by_category_month_sql <- "
  SELECT 
    strftime('%Y-%m',o.date) AS month, 
    pc.category_name, 
    COUNT(*) AS sales_count
  FROM 
    orders o
    LEFT JOIN products p ON o.product_id = p.product_id
    LEFT JOIN product_categories pc ON p.product_id = pc.product_id
  GROUP BY 
    month, 
    pc.category_name
"

# Execute the query and store the results in an R data frame
sales_by_category_month <- dbGetQuery(con, sales_by_category_month_sql)



# SQL query to identify the best-selling category for each month
best_sellers_by_month_sql <- "
  WITH MonthlySales AS (
    SELECT 
      strftime('%m', o.date) AS month, 
      pc.category_name, 
      COUNT(*) AS sales_count
    FROM 
      orders o
      LEFT JOIN products p ON o.product_id = p.product_id
      LEFT JOIN product_categories pc ON p.product_id = pc.product_id
    GROUP BY 
      month, 
      pc.category_name
  ),
  RankedSales AS (
    SELECT
      month,
      category_name,
      sales_count,
      RANK() OVER (PARTITION BY month ORDER BY sales_count DESC) as rank
    FROM 
      MonthlySales
  )
  SELECT 
    month,
    category_name,
    sales_count
  FROM 
    RankedSales
  WHERE 
    rank = 1
"

# Execute the query and store the results in an R data frame
best_sellers_by_month <- dbGetQuery(con, best_sellers_by_month_sql)



# Use the formatted_month directly for plotting as it is already in 'YYYY-MM' format
plot4 <- ggplot(best_sellers_by_month, aes(x = month, y = sales_count, fill = category_name)) +
  geom_col() +
  labs(title = "Best-Selling Product Categories by Month",
       x = "Month",
       y = "Sales Count") +
  scale_fill_discrete(name = "Category") +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))










payments$date <- as.Date(payments$date)


query1 <- "
SELECT 
  strftime('%Y-%m',date) AS month_year, 
  SUM(payment_amount) AS total_payment
FROM payments
GROUP BY month_year
"
# Execute the query and fetch results
monthly_payments <- dbGetQuery(con, query1)

# Plotting the graph
plot5 <- ggplot(monthly_payments, aes(x = month_year, y = total_payment, group = 1)) +
  geom_line(color = "#00BFC4") + # Line with a specific color
  geom_point(color = "#F8766D", size = 3) + # Points with a specific color and size
  theme_minimal() + 
  labs(title = "Total Payments by Month",
       x = "Month and Year",
       y = "Total Payment",
       caption = "Data from payment dataset") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1), # Rotate x-axis labels
        plot.title = element_text(hjust = 0.5)) # Center the title


plot5 


# Convert 'date' to Date format if it's not already
orders$date <- as.Date(orders$date)

# SQL query to extract month from date and count orders
query <- "
SELECT 
    strftime('%m', date) AS month, 
    COUNT(*) AS number_of_orders
FROM 
    orders
GROUP BY 
    month
ORDER BY 
    month
"

# Execute the query and fetch results
monthly_orders <- dbGetQuery(con, query)


# Create the plot
plot6 <- ggplot(monthly_orders, aes(x = month, y = number_of_orders)) +
  geom_bar(stat = "identity", fill = "steelblue") +
  theme_minimal() +
  labs(title = "Number of Orders per Month",
       x = "Month",
       y = "Number of Orders")

plot6

ggsave("figures/plot1.png", plot1)
#ggsave("figures/plot2.png", plot2)
ggsave("figures/plot3.png", plot3)
ggsave("figures/plot4.png", plot4)
ggsave("figures/plot5.png", plot5)
ggsave("figures/plot6.png", plot6)


# Disconnect from the database
RSQLite::dbDisconnect(con)




