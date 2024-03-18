library(readr)
library(RSQLite)
library(dplyr)

# Connect to the database
my_connection <- dbConnect(SQLite(), "database/database.db")

# Loop through each table and CSV file
for (table_name in names(data_tables)) {
  csv_file <- data_tables[[table_name]]
  
  # Read data from CSV
  new_data <- read_csv(csv_file)
  
  # Check if the table exists
  if (dbExistsTable(my_connection, table_name)) {
    # Read existing data from the database
    existing_data <- tbl(my_connection, table_name) %>% collect()
    
    # Combine existing and new data
    combined_data <- bind_rows(existing_data, new_data)
    
    # Overwrite the existing table with the combined data
    dbWriteTable(my_connection, table_name, combined_data, overwrite = TRUE)
  } else {
    # Create a new table if it doesn't exist
    dbWriteTable(my_connection, table_name, new_data)
  }
}

# Close the database connection
dbDisconnect(my_connection)

