library(readr)
library(dplyr)
library(RSQLite)

# Connect to the database
my_connection <- dbConnect(RSQLite::SQLite(), "database/database.db")

# Define data tables to load new data
data_tables <- list(
  customers = "new.data.upload/customer_data.csv",
  logistics = "new.data.upload/logistics_data.csv",
  orders = "new.data.upload/orders_data.csv",
  payments = "new.data.upload/payment_data.csv",
  products = "new.data.upload/product_data.csv",
  product_categories = "new.data.upload/product_category_data.csv",
  reviews = "new.data.upload/reviews_data.csv",
  suppliers = "new.data.upload/supplier_data.csv",
  supplies = "new.data.upload/supplies_data.csv"
)

# Loop through each table
for (table_name in names(data_tables)) {
  csv_file <- data_tables[[table_name]]
  
  # Read new data from CSV
  new_data <- readr::read_csv(csv_file)
  
  # Check if the table exists
  if (dbExistsTable(my_connection, table_name)) {
    # Read existing data from the database
    existing_data <- tbl(my_connection, table_name) %>% collect()
    
    # Combine existing and new data
    combined_data <- bind_rows(existing_data, new_data)
    
    # Write combined data back to the table
    RSQLite::dbWriteTable(my_connection, table_name, combined_data, overwrite = TRUE)
  } else {
    # Print a message if the table doesn't exist
    print(paste0("Table", table_name, "does not exist for loading new data."))
  }
}

# Close the database connection
RSQLite::dbDisconnect(my_connection)

print("New data loaded.")
