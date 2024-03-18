library(RSQLite)

my_connection <- dbConnect(RSQLite::SQLite(), "database/database.db")

data_customers <- dbReadTable(my_connection, "customers")

# Check for missing values in customers
missing_values_customers <- sum(is.na(data_customers))
if (missing_values_customers > 0) {
  stop("There are missing values in customers data")
}

# Validate phone number format
valid_phone_numbers <- grepl("^[7]\\d{9}$", data_customers$phone_numbers)  # Regular expression for 7 followed by 9 digits
if (!all(valid_phone_numbers)) {
  stop("Some phone numbers have invalid format (Start with 7 and have 10 digits)")
}

# # Validate email domain
# valid_email_domains <- c("gmail.com", "outlook.com", "yahoo.com", "hotmail.com", "icloud.com")
# valid_emails <- grepl(paste0("\\b", valid_email_domains, "\\b"), data_customers$email, ignore.case = TRUE)
# if (!all(valid_emails)) {
#   stop("Some email addresses have invalid domains")
# }

data_logistics <- dbReadTable(my_connection, "logistics")

# Check for missing values in logistics
missing_values_logistics <- sum(is.na(data_logistics))
if (missing_values_logistics > 0) {
  stop("There are missing values in logistics data")
}

data_orders <- dbReadTable(my_connection, "orders")
# Check for missing values in orders
missing_values_orders <- sum(is.na(data_orders))
if (missing_values_orders > 0) {
  stop("There are missing values in the order data")
}

data_products <- dbReadTable(my_connection, "products")
# Check for missing values in products
missing_values_products <- sum(is.na(data_products))
if (missing_values_products > 0) {
  stop("There are missing values in the products data")
}

data_products_categories <- dbReadTable(my_connection, "products_categories")
# Check for missing values in categories
missing_values_categories <- sum(is.na(data_categories))
if (missing_values_categories > 0) {
  stop("There are missing values in the products categories data")
}

data_reviews <- dbReadTable(my_connection, "reviews")
# Check for missing values in reviews
missing_values_reviews <- sum(is.na(data_reviews))
if (missing_values_reviews > 0) {
  stop("There are missing values in reviews data")
}

data_suppliers <- dbReadTable(my_connection, "suppliers")
# Check for missing values in suppliers
missing_values_suppliers <- sum(is.na(data_suppliers))
if (missing_values_suppliers > 0) {
  stop("There are missing values in suppliers data")
}

# data_supplies <- dbReadTable(my_connection, "supplies")
# # Check for missing values in supplies
# missing_values_supplies <- sum(is.na(data_supplies))
# if (missing_values_supplies > 0) {
#   stop("There are missing values in supplies data")
# }


