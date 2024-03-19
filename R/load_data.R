library(readr)
library(RSQLite)



# Connect to the database
con <- RSQLite::dbConnect(RSQLite::SQLite(), "database/new_database.db")

# Create customers
customer <- readr::read_csv("data.upload/customer_data.csv")
customer <- na.omit(customer)

dbExecute(con, "CREATE TABLE IF NOT EXISTS customers (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(25),
    last_name VARCHAR(25),
    street_name VARCHAR(100),
    street_number INT,
    email VARCHAR(100),
    password VARCHAR(500),
    phone_numbers INT(10) NOT NULL,
    zip_code VARCHAR(6)
);")

# Remove the duplicate entries, keeping the first occurrence
customer <- customer[!duplicated(customer$customer_id), ]

# First Name - Characters and Max Length
validate_firstname <- function(firstname) {
  !is.na(firstname) && all(grepl("^[[:alpha:]]+$", firstname)) && nchar(firstname) <= 25
}

# Apply the validation function to the CUSTOMER_FIRSTNAME column
valid_firstname <- sapply(customer$first_name, validate_firstname)

# Keep only the rows with valid first names
customer <- customer[valid_firstname, ]

# Last Name

validate_lastname <- function(lastname) {
  !is.na(lastname) && all(grepl("^[-'[:alpha:][:space:]]+$", lastname)) && nchar(lastname) <= 25
}

# Apply the validation function to the CUSTOMER_FIRSTNAME column
valid_lastname <- sapply(customer$last_name, validate_lastname)

# Keep only the rows with valid first and last names
customer <- customer[valid_lastname, ]

# Check if each entry in phone_numbers is numeric
valid_numeric <- function(phone) {
  !is.na(phone) && !is.na(as.numeric(phone)) && nchar(phone) ==10
}
num <- sapply(customer$phone_numbers, valid_numeric)

customer <- customer[num,]

# Check for duplicates in phone_numbers
customer <- customer[!duplicated(customer$phone_numbers), ]


# Email - Contains "@" and Valid Domain, should be unique

# Define the valid domain names
valid_domains <- c("gmail.com", "outlook.com", "yahoo.com", "hotmail.com", "icloud.com")

# Function to check if the domain is valid
is_valid_domain <- function(domain) {
  domain %in% valid_domains
}

# Function to extract domain from email and check if it's valid
valid_email <- function(email) {
  parts <- strsplit(email, "@")[[1]]
  if (length(parts) == 2) {
    is_valid_domain(parts[2])
  } else {
    FALSE
  }
}

# Filter out rows with invalid email domains
customer <- customer[sapply(customer$email, valid_email), , drop = FALSE]


# Check for duplicate rows based on email
customer <- customer[!duplicated(customer$email), ]

# Check if zip code is numeric and has length 6

valid_zip <- function(zipcode) {
  !is.na(zipcode) && !is.na(as.numeric(zipcode)) && nchar(zipcode) ==6
}
zip <- sapply(customer$zip_code, valid_zip)

customer <- customer[zip,]

# Check for numeric in street number
valid_street_num <- function(street_num) {
  !is.na(street_num) && !is.na(as.numeric(street_num)) && as.numeric(street_num) > 0
}
street <- sapply(customer$street_number, valid_street_num)

customer <- customer[street,]
# Check for character in street name

valid_street_name <- function(street_name) {
  !is.na(street_name) && all(grepl("^[-_'[:alpha:] ]+$", street_name, ignore.case = TRUE))
}

valid_street <- sapply(customer$street_name, valid_street_name)

customer <- customer[valid_street, ]


# Create Suppliers
supplier <- readr::read_csv("data.upload/supplier_data.csv")
supplier <- na.omit(supplier)

dbExecute(con, "CREATE TABLE IF NOT EXISTS suppliers (
    supplier_id INT PRIMARY KEY,
    supplier_name VARCHAR(255),
    contact_number VARCHAR(20),
    street_number INT,
    street_name VARCHAR(50),
    email VARCHAR(500)
);")

# Supplier ID - Uniqueness
supplier <- supplier[!duplicated(supplier$supplier_id), ]


# Supplier Name - Not Empty and Max Length
validate_name <- function(name) {
  !is.na(name) && all(grepl("^[[:alpha:]]+$", name)) && nchar(name) <= 25
}


# Street Number - Positive Integer

valid_street_num <- function(street_num) {
  !is.na(street_num) && !is.na(as.numeric(street_num)) && as.numeric(street_num) > 0
}
sup_street <- sapply(supplier$street_number, valid_street_num)

supplier <- supplier[sup_street,]

# Street Name - Not Empty and Max Length

sup_street_name <- sapply(supplier$street_name, valid_street_name)

supplier <- supplier[sup_street_name, ]

# Contact Number - Numeric, Length and Uniqueness

contact <- sapply(supplier$contact_number, valid_numeric)

supplier <- supplier[contact,]

# Check for duplicates in phone_numbers
supplier <- supplier[!duplicated(supplier$contact_number), ]

# Email - Contains "@" and Valid Domain

# Filter out rows with invalid email domains
supplier <- supplier[sapply(supplier$email, valid_email), , drop = FALSE]


# Check for duplicate rows based on email
supplier <- supplier[!duplicated(supplier$email), ]



# Create Product
product <- readr::read_csv("data.upload/product_data.csv")
product <- na.omit(product)

dbExecute(con, "CREATE TABLE IF NOT EXISTS products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(255),
    supplier_id INT,
    quantity INT,
    price DECIMAL(10, 2),
    FOREIGN KEY (supplier_id) REFERENCES supplier(supplier_id)
    );")
# Remove duplicate entries, keeping the first occurrence
product <- product[!duplicated(product$product_id), ]

# Product Name - Characters and Max Length
validate_productname <- function(productname) {
  !is.na(productname) && nchar(productname) <= 200
}

# Apply the validation function to the PRODUCT_NAME column
valid_productname <- sapply(product$product_name, validate_productname)

# Keep only the rows with valid product names
product <- product[valid_productname, ]

# Supplier ID - Numeric
valid_supplier_id <- function(supplier_id) {
  !is.na(supplier_id) && !is.na(as.numeric(supplier_id))
}
supplier_id <- sapply(product$supplier_id, valid_supplier_id)

product <- product[supplier_id, ]


# Quantity - Numeric
valid_quantity <- function(quantity) {
  !is.na(quantity) && !is.na(as.numeric(quantity)) && as.numeric(quantity) >= 0
}
quantity <- sapply(product$quantity, valid_quantity)

product <- product[quantity, ]

# Price - Numeric
valid_price <- function(price) {
  !is.na(price) && !is.na(as.numeric(price)) && as.numeric(price) >= 0
}
price <- sapply(product$price, valid_price)

product <- product[price, ]





# Create Reviews
reviews <- readr::read_csv("data.upload/reviews_data.csv")
reviews <- na.omit(reviews)

dbExecute(con, "CREATE TABLE IF NOT EXISTS reviews (
    review_id INT PRIMARY KEY,
    product_id INT,
    customer_id INT,
    score INT,
    FOREIGN KEY (product_id) REFERENCES orders(order_id),
    FOREIGN KEY (customer_id) REFERENCES customer(customer_id)
);")
# Remove duplicate entries, keeping the first occurrence
reviews <- reviews[!duplicated(reviews$review_id), ]

# Product ID - Numeric
valid_product_id <- function(product_id) {
  !is.na(product_id) && !is.na(as.numeric(product_id))
}
product_id <- sapply(reviews$product_id, valid_product_id)

reviews <- reviews[product_id, ]

# Customer ID - Numeric
valid_customer_id <- function(customer_id) {
  !is.na(customer_id) && !is.na(as.numeric(customer_id))
}
customer_id <- sapply(reviews$customer_id, valid_customer_id)

reviews <- reviews[customer_id, ]

# Score - Numeric and within range (0-10)
valid_score <- function(score) {
  !is.na(score) && !is.na(as.numeric(score)) && as.numeric(score) >= 0 && as.numeric(score) <= 10
}
score <- sapply(reviews$score, valid_score)

reviews <- reviews[score, ]


# Create Categories
product_category  <- readr::read_csv("data.upload/product_category_data.csv")
product_category <- na.omit(product_category)

dbExecute(con, "CREATE TABLE IF NOT EXISTS product_categories  (
    category_id INT PRIMARY KEY,
    product_id INT,
    category_name VARCHAR(255),
    FOREIGN KEY (product_id) REFERENCES product(product_id)
);")
# Remove duplicate entries, keeping the first occurrence
product_category <- product_category[!duplicated(product_category$category_id), ]

# Product ID - Numeric
valid_product_id <- function(product_id) {
  !is.na(product_id) && !is.na(as.numeric(product_id))
}
product_id <- sapply(product_category$product_id, valid_product_id)

product_category <- product_category[product_id, ]

# Category Name - Characters and Max Length
validate_category_name <- function(category_name) {
  !is.na(category_name) && nchar(category_name) <= 255
}

valid_category_name <- sapply(product_category$category_name, validate_category_name)

product_category <- product_category[valid_category_name, ]


# Create Payments
payment <- readr::read_csv("data.upload/payment_data.csv")

dbExecute(con, "CREATE TABLE IF NOT EXISTS payments_table (
    payment_id INT PRIMARY KEY,
    payment_amount DECIMAL(10, 2),
    date DATE, 
    time TIME
);")
# Remove duplicate entries, keeping the first occurrence
payment <- payment[!duplicated(payment$payment_id), ]

# Payment ID - Numeric
valid_payment_id <- function(payment_id) {
  !is.na(payment_id) && !is.na(as.numeric(payment_id))
}
payment_id <- sapply(payment$payment_id, valid_payment_id)

payment <- payment[payment_id, ]

# Payment Amount - Numeric and non-negative
valid_payment_amount <- function(payment_amount) {
  !is.na(payment_amount) && !is.na(as.numeric(payment_amount)) && as.numeric(payment_amount) >= 0
}
payment_amount <- sapply(payment$payment_amount, valid_payment_amount)

payment <- payment[payment_amount, ]


# Create supplies

dbExecute(con, "CREATE TABLE IF NOT EXISTS supplies (
    supplier_id INT,
    product_id INT,
    FOREIGN KEY (supplier_id) REFERENCES supplier(supplier_id),
    FOREIGN KEY (product_id) REFERENCES product(product_id)
);")


# Create logistics
logistics <- readr::read_csv("data.upload/logistics_data.csv")
logistics <- na.omit(logistics)

dbExecute(con, "CREATE TABLE IF NOT EXISTS logistics (
    tracking_id INT PRIMARY KEY,
    status VARCHAR(50),
    product_id INT,
    supplier_id INT,
    customer_id INT,
    FOREIGN KEY (supplier_id) REFERENCES supplier(supplier_id),
    FOREIGN KEY (customer_id) REFERENCES supplier(customer_id),
    FOREIGN KEY (product_id) REFERENCES product(product_id)
);")

# Remove duplicate entries, keeping the first occurrence
logistics <- logistics[!duplicated(logistics$tracking_id), ]

# Tracking ID - Numeric
valid_tracking_id <- function(tracking_id) {
  !is.na(tracking_id) && !is.na(as.numeric(tracking_id))
}
tracking_id <- sapply(logistics$tracking_id, valid_tracking_id)

logistics <- logistics[tracking_id, ]

# Status - Non-empty
valid_status <- function(status) {
  !is.na(status) && nchar(status) > 0
}
status <- sapply(logistics$status, valid_status)

logistics <- logistics[status, ]

# Product ID - Numeric
valid_product_id <- function(product_id) {
  !is.na(product_id) && !is.na(as.numeric(product_id))
}
product_id <- sapply(logistics$product_id, valid_product_id)

logistics <- logistics[product_id, ]

# Supplier ID - Numeric
valid_supplier_id <- function(supplier_id) {
  !is.na(supplier_id) && !is.na(as.numeric(supplier_id))
}
supplier_id <- sapply(logistics$supplier_id, valid_supplier_id)

logistics <- logistics[supplier_id, ]

# Customer ID - Numeric
valid_customer_id <- function(customer_id) {
  !is.na(customer_id) && !is.na(as.numeric(customer_id))
}
customer_id <- sapply(logistics$customer_id, valid_customer_id)

logistics <- logistics[customer_id, ]


# Create Orders
orders <- readr::read_csv("data.upload/order_data.csv")
orders <- na.omit(orders)


dbExecute(con, "CREATE TABLE IF NOT EXISTS orders (
    customer_id INT,
    product_id INT, 
    quantity INT,
    date DATE,
    time TIME,
    FOREIGN KEY (customer_id) REFERENCES customer(customer_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);")

# Remove duplicate entries, keeping the first occurrence
orders <- orders[!duplicated(orders[, c("customer_id", "date", "time")]), ]

# Customer ID - Numeric
valid_customer_id <- function(customer_id) {
  !is.na(customer_id) && !is.na(as.numeric(customer_id))
}
customer_id <- sapply(orders$customer_id, valid_customer_id)

orders <- orders[customer_id, ]

# Product ID - Numeric
valid_product_id <- function(product_id) {
  !is.na(product_id) && !is.na(as.numeric(product_id))
}
product_id <- sapply(orders$product_id, valid_product_id)

orders <- orders[product_id, ]

# Quantity - Numeric
valid_quantity <- function(quantity) {
  !is.na(quantity) && !is.na(as.numeric(quantity)) && as.numeric(quantity) >= 0
}
quantity <- sapply(orders$quantity, valid_quantity)

orders <- orders[quantity, ]


# Check referential integrity for product table
invalid_supplier_ids <- !product$supplier_id %in% supplier$supplier_id
if (any(invalid_supplier_ids)) {
  cat("Foreign key violation: supplier_id in product table does not exist in supplier table. Removing invalid rows.\n")
  product <- product[!invalid_supplier_ids, ]
}

# Check referential integrity for reviews table
invalid_product_ids <- !reviews$product_id %in% product$product_id
if (any(invalid_product_ids)) {
  cat("Foreign key violation: product_id in reviews table does not exist in product table. Removing invalid rows.\n")
  reviews <- reviews[!invalid_product_ids, ]
}
invalid_customer_ids <- !reviews$customer_id %in% customer$customer_id
if (any(invalid_customer_ids)) {
  cat("Foreign key violation: customer_id in reviews table does not exist in customer table. Removing invalid rows.\n")
  reviews <- reviews[!invalid_customer_ids, ]
}

# Check referential integrity for product_category table
invalid_product_ids <- !product_category$product_id %in% product$product_id
if (any(invalid_product_ids)) {
  cat("Foreign key violation: product_id in product_category table does not exist in product table. Removing invalid rows.\n")
  product_category <- product_category[!invalid_product_ids, ]
}


# Check referential integrity for logistics table
invalid_supplier_ids <- !logistics$supplier_id %in% supplier$supplier_id
if (any(invalid_supplier_ids)) {
  cat("Foreign key violation: supplier_id in logistics table does not exist in supplier table. Removing invalid rows.\n")
  logistics <- logistics[!invalid_supplier_ids, ]
}
invalid_customer_ids <- !logistics$customer_id %in% customer$customer_id
if (any(invalid_customer_ids)) {
  cat("Foreign key violation: customer_id in logistics table does not exist in customer table. Removing invalid rows.\n")
  logistics <- logistics[!invalid_customer_ids, ]
}
invalid_product_ids <- !logistics$product_id %in% product$product_id
if (any(invalid_product_ids)) {
  cat("Foreign key violation: product_id in logistics table does not exist in product table. Removing invalid rows.\n")
  logistics <- logistics[!invalid_product_ids, ]
}

# Check referential integrity for orders table
invalid_customer_ids <- !orders$customer_id %in% customer$customer_id
if (any(invalid_customer_ids)) {
  cat("Foreign key violation: customer_id in orders table does not exist in customer table. Removing invalid rows.\n")
  orders <- orders[!invalid_customer_ids, ]
}
invalid_product_ids <- !orders$product_id %in% product$product_id
if (any(invalid_product_ids)) {
  cat("Foreign key violation: product_id in orders table does not exist in product table. Removing invalid rows.\n")
  orders <- orders[!invalid_product_ids, ]
}


RSQLite::dbListTables(con)

data_exists <- function(connection, table_name, data_frame) {
  # Construct the query to check for existence of data
  query <- paste0("SELECT COUNT(*) FROM ", table_name)
  result <- dbGetQuery(connection, query)
  return(result[[1]] > 0)
}

# Function to insert data into a table if it doesn't exist
insert_data <- function(connection, table_name, data_frame) {
  # Check if data already exists in the table
  if (data_exists(connection, table_name)) {
    cat("Data already exists in", table_name, "\n")
    return()
  }
  
  # Extract column names
  columns <- names(data_frame)
  
  # Construct the INSERT INTO SQL query
  insert_query <- paste0("INSERT INTO '", table_name, "' (", paste0("'", columns, "'", collapse = ", "), ") VALUES ")
  
  # Loop through each row of the data frame and insert values
  for (i in 1:nrow(data_frame)) {
    values <- paste0("(", paste0("'", gsub("'", "''", unlist(data_frame[i,])), "'", collapse = ","), ")")
    dbExecute(connection, paste0(insert_query, values))
  }
  
  cat("Data inserted into", table_name, "\n")
}


# Corrected insert_data function calls
insert_data(con, "suppliers", supplier)
insert_data(con, "products", product)
insert_data(con, "logistics", logistics)
insert_data(con, "customers", customer)
insert_data(con, "orders", orders)
insert_data(con, "payments_table", payment)

# Close the database connection
RSQLite::dbDisconnect(con)

print("Initial data loaded successfully.")

