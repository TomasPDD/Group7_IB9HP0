library(readr)
library(dplyr)
library(RSQLite)

# Connect to the database
my_connection <- dbConnect(RSQLite::SQLite(), "database/database.db")

# Define data tables and their identifier column names
data_tables <- list(
  customers = c("new.data.upload/customer_data.csv", "customer_id"),  # Identifier column: customer_id
  logistics = c("new.data.upload/logistics_data.csv", "tracking_id"),  # Identifier column: tracking_id
#  orders = c("new.data.upload/order_data.csv", "order_id"),  # Identifier column: order_id
  payments = c("new.data.upload/payment_data.csv", "payment_id"),  # Identifier column: payment_id
  products = c("new.data.upload/product_data.csv","product_id"), # Identifier column: product_id
  product_categories = c("new.data.upload/product_category_data.csv","category_id"), # Identifier column: category_id
  reviews = c("new.data.upload/reviews_data.csv","review_id"), # Identifier column: review_id
  suppliers = c("new.data.upload/supplier_data.csv","supplier_id") # Identifier column: supplier_id
)

# Loop through each table
for (table_info in data_tables) {
  csv_file <- table_info[1]
  identifier_column <- table_info[2]
  
  # Read new data from CSV
  new_data <- readr::read_csv(csv_file)
  
  # Check if the table exists
  if (dbExistsTable(my_connection, table_name)) {
    # Get existing data's identifier
    existing_data <- tbl(my_connection, table_name) %>% select({{ identifier_column }})
    
    # Filter new data based on the specified identifier column
    new_data <- new_data %>% filter(!{{ identifier_column }} %in% existing_data[[identifier_column]])
    
    # If there's new data after filtering
    if (nrow(new_data) > 0) {
      # Write new data to the table
      RSQLite::dbWriteTable(my_connection, table_name, new_data, overwrite = TRUE)
      print(paste0("Loaded", nrow(new_data), "new records to", table_name))
    } else {
      print(paste0("No new data found for", table_name))
    }
  } else {
    # Print a message if the table doesn't exist
    print(paste0("Table", table_name, "does not exist for loading new data."))
  }
}

# Close the database connection
RSQLite::dbDisconnect(my_connection)