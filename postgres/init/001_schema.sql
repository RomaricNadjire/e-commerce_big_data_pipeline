
CREATE TABLE IF NOT EXISTS categories (
    category_id INTEGER PRIMARY KEY,
    category_name VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS products (
    product_id INTEGER PRIMARY KEY,
    category_id INTEGER NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    unit_price NUMERIC(10, 2) NOT NULL,
    FOREIGN KEY (category_id) REFERENCES categories(category_id)
);

CREATE TABLE IF NOT EXISTS customers (
    customer_id INTEGER PRIMARY KEY,
    country VARCHAR(2) NOT NULL,
    segment VARCHAR(20) NOT NULL,
    signup_date DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(10, 2) NOT NULL,
    discount_rate NUMERIC(5, 4) NOT NULL DEFAULT 0,
    amount NUMERIC(10, 2) NOT NULL,
    country VARCHAR(2) NOT NULL,
    status VARCHAR(20) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    payment_method VARCHAR(20) NOT NULL,
    source_file VARCHAR(255),
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_transactions_customer
        FOREIGN KEY (customer_id)
        REFERENCES customers(customer_id),

    CONSTRAINT fk_transactions_product
        FOREIGN KEY (product_id)
        REFERENCES products(product_id),

    CONSTRAINT chk_transaction_quantity
        CHECK (quantity > 0),

    CONSTRAINT chk_transaction_amount
        CHECK (amount >= 0)
);

CREATE TABLE IF NOT EXISTS processed_files (
    dataset VARCHAR(50) NOT NULL,
    filename VARCHAR(255) NOT NULL,
    processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) NOT NULL,
    rows_processed INTEGER NOT NULL,

    PRIMARY KEY (dataset, filename)
);
