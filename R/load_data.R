library(readr)
library(RSQLite)



# Connect to the database
con <- RSQLite::dbConnect(RSQLite::SQLite(), "database/new_database.db")

# Create customers
customers <- readr::read_csv("data.upload/customer_data.csv")
customers <- na.omit(customers)
dbExecute(con, "DROP TABLE IF EXISTS customers")
dbExecute(con, "CREATE TABLE customers (
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

RSQLite::dbWriteTable(con,"customers",customers,append=TRUE)

# Create Suppliers
suppliers <- readr::read_csv("data.upload/supplier_data.csv")
suppliers <- na.omit(suppliers)

dbExecute(con, "DROP TABLE IF EXISTS suppliers")
dbExecute(con, "CREATE TABLE suppliers (
    supplier_id INT PRIMARY KEY,
    supplier_name VARCHAR(255),
    contact_number VARCHAR(20),
    street_number INT,
    street_name VARCHAR(50),
    email VARCHAR(500)
);")

RSQLite::dbWriteTable(con,"suppliers",suppliers,append=TRUE)

# Create Products
products <- readr::read_csv("data.upload/product_data.csv")

dbExecute(con, "DROP TABLE IF EXISTS products")
dbExecute(con, "CREATE TABLE products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(255),
    supplier_id INT,
    quantity INT,
    price DECIMAL(10, 2),
    FOREIGN KEY (supplier_id) REFERENCES supplier(supplier_id)
    );")

RSQLite::dbWriteTable(con,"products",products,append=TRUE)

# Create Orders
orders <- readr::read_csv("data.upload/order_data.csv")
dbExecute(con, "DROP TABLE IF EXISTS orders")
dbExecute(con, "CREATE TABLE orders (
    customer_id INT,
    product_id INT, 
    quantity INT,
    date DATE,
    time TIME,
    FOREIGN KEY (customer_id) REFERENCES customer(customer_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);")

RSQLite::dbWriteTable(con,"orders",orders,append=TRUE)

# Create Reviews
reviews <- readr::read_csv("data.upload/reviews_data.csv")
dbExecute(con, "DROP TABLE IF EXISTS reviews")
dbExecute(con, "CREATE TABLE reviews (
    review_id INT PRIMARY KEY,
    product_id INT,
    customer_id INT,
    score INT,
    FOREIGN KEY (product_id) REFERENCES orders(order_id),
    FOREIGN KEY (customer_id) REFERENCES customer(customer_id)
);")

RSQLite::dbWriteTable(con,"reviews",reviews,append=TRUE)

# Create Categories
product_categories  <- readr::read_csv("data.upload/product_category_data.csv")
dbExecute(con, "DROP TABLE IF EXISTS product_categories ")
dbExecute(con, "CREATE TABLE product_categories  (
    category_id INT PRIMARY KEY,
    product_id INT,
    category_name VARCHAR(255),
    FOREIGN KEY (product_id) REFERENCES product(product_id)
);")

RSQLite::dbWriteTable(con,"product_categories",product_categories,append=TRUE)

# Create Payments
payments <- readr::read_csv("data.upload/payment_data.csv")
dbExecute(con, "DROP TABLE IF EXISTS payments")
dbExecute(con, "CREATE TABLE payments (
    payment_id INT PRIMARY KEY,
    payment_amount DECIMAL(10, 2),
    date DATE, 
    time TIME
);")

RSQLite::dbWriteTable(con,"payments",payments,append=TRUE)

# Create logistics
logistics <- readr::read_csv("data.upload/logistics_data.csv")
dbExecute(con, "DROP TABLE if exists logistics")
dbExecute(con, "CREATE TABLE logistics (
    tracking_id INT PRIMARY KEY,
    status VARCHAR(50),
    product_id INT,
    supplier_id INT,
    customer_id INT,
    FOREIGN KEY (supplier_id) REFERENCES supplier(supplier_id),
    FOREIGN KEY (customer_id) REFERENCES supplier(customer_id),
    FOREIGN KEY (product_id) REFERENCES product(product_id)
);")

RSQLite::dbWriteTable(con,"logistics",logistics,append=TRUE)
RSQLite::dbListTables(con)


# Close the database connection
RSQLite::dbDisconnect(con)

print("Initial data loaded successfully.")
