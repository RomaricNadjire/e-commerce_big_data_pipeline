CREATE TABLE IF NOT EXISTS gold_transactions_enriched (
    transaction_id      BIGINT        NOT NULL,
    customer_id         INTEGER       NOT NULL,
    product_id          INTEGER       NOT NULL,
    category_id         INTEGER       NOT NULL,
    category_name       VARCHAR(255)  NOT NULL,
    product_name        VARCHAR(255)  NOT NULL,
    customer_segment    VARCHAR(20)   NOT NULL,
    quantity            INTEGER       NOT NULL,
    unit_price          NUMERIC(10,2) NOT NULL,
    discount_rate       NUMERIC(5,4)  NOT NULL,
    amount              NUMERIC(10,2) NOT NULL,
    country             VARCHAR(2)    NOT NULL,
    status              VARCHAR(20)   NOT NULL,
    "timestamp"         TIMESTAMP     NOT NULL,
    payment_method      VARCHAR(20)   NOT NULL,
    transaction_date    DATE          NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_gold_tx_enriched_date
    ON gold_transactions_enriched (transaction_date);
CREATE INDEX IF NOT EXISTS idx_gold_tx_enriched_country
    ON gold_transactions_enriched (country);
CREATE INDEX IF NOT EXISTS idx_gold_tx_enriched_category
    ON gold_transactions_enriched (category_id);


CREATE TABLE IF NOT EXISTS gold_daily_sales (
    transaction_date                DATE          NOT NULL,
    total_transactions              BIGINT        NOT NULL,
    completed_transactions          BIGINT        NOT NULL,
    cancelled_transactions          BIGINT        NOT NULL,
    cancellation_rate_pct           NUMERIC(5,2)  NOT NULL,
    completed_quantity              BIGINT        NOT NULL,
    total_sales_amount              NUMERIC(18,2) NOT NULL,
    avg_completed_transaction_amount NUMERIC(18,2) NOT NULL,
    unique_completed_customers      BIGINT        NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_gold_daily_sales_date
    ON gold_daily_sales (transaction_date);

CREATE TABLE IF NOT EXISTS gold_sales_by_country (
    country                         VARCHAR(2)    NOT NULL,
    total_transactions              BIGINT        NOT NULL,
    completed_transactions          BIGINT        NOT NULL,
    cancelled_transactions          BIGINT        NOT NULL,
    cancellation_rate_pct           NUMERIC(5,2)  NOT NULL,
    completed_quantity              BIGINT        NOT NULL,
    total_sales_amount              NUMERIC(18,2) NOT NULL,
    avg_completed_transaction_amount NUMERIC(18,2) NOT NULL,
    unique_completed_customers      BIGINT        NOT NULL
);

CREATE TABLE IF NOT EXISTS gold_sales_by_category (
    category_id                     INTEGER       NOT NULL,
    category_name                   VARCHAR(255)  NOT NULL,
    total_transactions              BIGINT        NOT NULL,
    completed_transactions          BIGINT        NOT NULL,
    cancelled_transactions          BIGINT        NOT NULL,
    cancellation_rate_pct           NUMERIC(5,2)  NOT NULL,
    completed_quantity              BIGINT        NOT NULL,
    total_sales_amount              NUMERIC(18,2) NOT NULL,
    avg_completed_transaction_amount NUMERIC(18,2) NOT NULL,
    unique_completed_customers      BIGINT        NOT NULL
);

CREATE TABLE IF NOT EXISTS gold_sales_by_product (
    product_id                      INTEGER       NOT NULL,
    product_name                    VARCHAR(255)  NOT NULL,
    total_transactions              BIGINT        NOT NULL,
    completed_transactions          BIGINT        NOT NULL,
    cancelled_transactions          BIGINT        NOT NULL,
    cancellation_rate_pct           NUMERIC(5,2)  NOT NULL,
    completed_quantity              BIGINT        NOT NULL,
    total_sales_amount              NUMERIC(18,2) NOT NULL,
    avg_completed_transaction_amount NUMERIC(18,2) NOT NULL,
    unique_completed_customers      BIGINT        NOT NULL
);

CREATE TABLE IF NOT EXISTS gold_sales_by_segment (
    customer_segment                VARCHAR(20)   NOT NULL,
    total_transactions              BIGINT        NOT NULL,
    completed_transactions          BIGINT        NOT NULL,
    cancelled_transactions          BIGINT        NOT NULL,
    cancellation_rate_pct           NUMERIC(5,2)  NOT NULL,
    completed_quantity              BIGINT        NOT NULL,
    total_sales_amount              NUMERIC(18,2) NOT NULL,
    avg_completed_transaction_amount NUMERIC(18,2) NOT NULL,
    unique_completed_customers      BIGINT        NOT NULL
);