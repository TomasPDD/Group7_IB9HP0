library(readr)
library(RSQLite)

# Connect to the database
my_connection <- RSQLite::dbConnect(RSQLite::SQLite(), "database/database.db")

# Read each CSV file and write it to the corresponding database table
data_tables <- list(
  customers = "data.upload/customer_data.csv",
  logistics = "data.upload/logistics_data.csv",
  orders = "data.upload/orders_data.csv",
  payments = "data.upload/payment_data.csv",
  products = "data.upload/product_data.csv",
  product_categories = "data.upload/product_category_data.csv",
  reviews = "data.upload/reviews_data.csv",
  suppliers = "data.upload/supplier_data.csv",
  supplies = "data.upload/supplies_data.csv"
)

for (table_name in names(data_tables)) {
  csv_file <- data_tables[[table_name]]
  data <- readr::read_csv(csv_file)
  RSQLite::dbWriteTable(my_connection, table_name, data, overwrite = TRUE)  # Overwrite existing tables
}

# Close the database connection
RSQLite::dbDisconnect(my_connection)

