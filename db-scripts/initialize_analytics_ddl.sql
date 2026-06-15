-- Create a dedicated analytics schema
CREATE SCHEMA IF NOT EXISTS analytics;

-- Build the Dimension Dates Table (Need to populate it first by year)
CREATE TABLE IF NOT EXISTS analytics.dim_dates (
    dim_date INT PRIMARY KEY, -- Format: YYYYMMDD
    calendar_date DATE NOT NULL,
    day_of_week VARCHAR(10) NOT NULL,
    month INT NOT NULL,
    quarter INT NOT NULL,
    year INT NOT NULL);

-- Build the Customers Dimension (keeping all information and changes for each customer)
CREATE TABLE IF NOT EXISTS analytics.dim_customers (
    customer SERIAL PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    customer_name VARCHAR(255),
    customer_address VARCHAR(255),
    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE
    );

-- Build Products Dimension (keeping all information and changes for each product)
CREATE TABLE IF NOT EXISTS analytics.dim_products (
    product SERIAL PRIMARY KEY,
    product_id BIGINT NOT NULL,
    product_name VARCHAR(255),
    barcode VARCHAR(50),
    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE
    );

-- Build the Fact Table (one line item per order)
CREATE TABLE IF NOT EXISTS analytics.customer_order_items (
    customer_order_item BIGSERIAL PRIMARY KEY,
    order_id BIGINT NOT NULL,
    order_item_id BIGINT NOT NULL,
    customer INT NOT NULL REFERENCES analytics.dim_customers(customer),
    product INT NOT NULL REFERENCES analytics.dim_products(product),
    order_date INT NOT NULL REFERENCES analytics.dim_dates(dim_date),
    delivery_date INT REFERENCES analytics.dim_dates(dim_date),
    status VARCHAR(50) NOT NULL,
    quantity INT NOT NULL,
    unity_price NUMERIC(13, 5) NOT NULL,
    total_amount NUMERIC(13, 5) NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    CONSTRAINT unique_order_item UNIQUE (order_id, order_item_id)
    );