-- DDL for Financial Institution Database supporting S3/Spark Replication
-- This schema includes the necessary metadata columns (last_updated_at)
-- and triggers required for Change Data Capture (CDC) to enable
-- efficient "delete and replace" logic on the S3 Parquet layer.

-- 1. Create a function to automatically update the timestamp column
--    whenever a row is modified.
CREATE OR REPLACE FUNCTION update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
NEW.last_updated_at = now();
RETURN NEW;
END;
$$ language 'plpgsql';

-- 2. CORE TABLES
-- Table: customers
-- This table holds customer demographic information (e.g., name, address).
-- Changes here require soft-delete/replace logic in S3.
CREATE TABLE customers (
customer_id BIGSERIAL PRIMARY KEY,
first_name VARCHAR(100) NOT NULL,
last_name VARCHAR(100) NOT NULL,
email VARCHAR(255) UNIQUE NOT NULL,
phone_number VARCHAR(20),
current_address TEXT,
is_active BOOLEAN NOT NULL DEFAULT TRUE,

-- METADATA FOR REPLICATION / CDC
created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT now(),
last_updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT now()

);

-- Apply the trigger to automatically update the timestamp on modification
CREATE TRIGGER customers_update_timestamp
BEFORE UPDATE ON customers
FOR EACH ROW
EXECUTE PROCEDURE update_timestamp();

-- Table: accounts
-- This table holds transactional accounts (e.g., savings, checking).
-- Changes here (e.g., balance, status) also require soft-delete/replace in S3.
CREATE TABLE accounts (
account_id BIGSERIAL PRIMARY KEY,
customer_id BIGINT NOT NULL REFERENCES customers(customer_id),
account_type VARCHAR(50) NOT NULL, -- 'CHECKING', 'SAVINGS', 'LOAN'
balance NUMERIC(15, 2) NOT NULL DEFAULT 0.00,
status VARCHAR(20) NOT NULL DEFAULT 'OPEN', -- 'OPEN', 'CLOSED', 'FROZEN'

-- METADATA FOR REPLICATION / CDC
created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT now(),
last_updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT now()

);

-- Apply the trigger to automatically update the timestamp on modification
CREATE TRIGGER accounts_update_timestamp
BEFORE UPDATE ON accounts
FOR EACH ROW
EXECUTE PROCEDURE update_timestamp();

-- Table: transactions
-- This is a high-volume, append-only table (Fact data).
-- New data is simply appended to the S3 Parquet dataset daily.
CREATE TABLE transactions (
transaction_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
account_id BIGINT NOT NULL REFERENCES accounts(account_id),
transaction_type VARCHAR(50) NOT NULL, -- 'DEPOSIT', 'WITHDRAWAL', 'TRANSFER'
amount NUMERIC(15, 2) NOT NULL,

-- The actual time the transaction occurred (key for reporting)
transaction_time TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT now(),

-- The time this record was inserted into the source database
ingestion_time TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT now()

);

                   
ALTER TABLE transactions ADD COLUMN created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT now();