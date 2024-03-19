library(readr)
library(RSQLite)

# Connect to the database
my_connection <- RSQLite::dbConnect(RSQLite::SQLite(), "database/database.db")

# Read each CSV file and write it to the corresponding database table
data_tables <- list(
  customers = "data.upload/customer_data.csv",
  logistics = "data.upload/logistics_data.csv",
  orders = "data.upload/order_data.csv",
  payments = "data.upload/payment_data.csv",
  products = "data.upload/product_data.csv",
  product_categories = "data.upload/product_category_data.csv",
  reviews = "data.upload/reviews_data.csv",
  suppliers = "data.upload/supplier_data.csv"
#  supplies = "data.upload/supplies_data.csv"
)

# Create the tables first
for (table_name in names(data_tables)) {
  tryCatch({
    RSQLite::dbCreateTable(my_connection, table_name, fields = NULL)
  }, error = function(err) {
    cat("Table already exists:", table_name, "\n")
  })
}

# Read and write data to the tables (with individual printing)
for (table_name in names(data_tables)) {
  csv_file <- data_tables[[table_name]]
  data <- readr::read_csv(csv_file)
  RSQLite::dbWriteTable(my_connection, table_name, data, overwrite = TRUE)  # Overwrite existing data
  
  # Print the data for this table
  print(paste("Data for table", table_name))
  print(data)
}

# Read and write data to the tables
for (table_name in names(data_tables)) {
  csv_file <- data_tables[[table_name]]
  data <- readr::read_csv(csv_file)
  RSQLite::dbWriteTable(my_connection, table_name, data, overwrite = TRUE)  # Overwrite existing tables
}

# Close the database connection
RSQLite::dbDisconnect(my_connection)

print("Initial data loaded successfully.")