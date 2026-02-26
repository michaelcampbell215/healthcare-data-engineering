-- =========================================================================
-- CREATE WAREHOUSE SCHEMA (STAR SCHEMA)
-- Purpose: Optimized tables for BI and Analytics
-- =========================================================================

-- 1. Date Dimension
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INT PRIMARY KEY, -- Format: YYYYMMDD
    full_date DATE NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    day_of_week VARCHAR(20) NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

-- 2. Recipient Dimension (Combined Physicians & Hospitals)
CREATE TABLE IF NOT EXISTS dim_recipient (
    recipient_key INT AUTO_INCREMENT PRIMARY KEY,
    recipient_id VARCHAR(50), -- Natural Key (Profile ID)
    recipient_name VARCHAR(255),
    recipient_type VARCHAR(100),
    specialty VARCHAR(255),
    npi VARCHAR(20),
    ccn VARCHAR(20),
    city VARCHAR(100),
    state VARCHAR(10),
    zip VARCHAR(10),
    population INT,
    lat DECIMAL(10, 6),
    lng DECIMAL(10, 6),
    UNIQUE INDEX idx_recipient_unique (recipient_id)
);

-- 3. Payer Dimension (Manufacturers)
CREATE TABLE IF NOT EXISTS dim_payer (
    payer_key INT AUTO_INCREMENT PRIMARY KEY,
    payer_id VARCHAR(50), -- Natural Key
    payer_name VARCHAR(150),
    UNIQUE INDEX idx_payer_unique (payer_id)
);

-- 4. Product Dimension (Primary Product Only)
CREATE TABLE IF NOT EXISTS dim_product (
    product_key INT AUTO_INCREMENT PRIMARY KEY,
    product_name VARCHAR(255),
    product_category VARCHAR(255),
    product_type VARCHAR(100),
    ndc VARCHAR(20),
    UNIQUE INDEX idx_product_unique (product_name, product_category, product_type, ndc)
);

-- 6. Payment Nature Dimension (Refactoring for Performance)
CREATE TABLE IF NOT EXISTS dim_nature (
    nature_key INT AUTO_INCREMENT PRIMARY KEY,
    payment_nature VARCHAR(255),
    UNIQUE INDEX idx_nature_unique (payment_nature)
);

-- 5. Fact Table (The Center of the Star)
CREATE TABLE IF NOT EXISTS fact_payments (
    fact_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    date_key INT NOT NULL,
    recipient_key INT NOT NULL,
    payer_key INT NOT NULL,
    product_key INT NOT NULL,
    nature_key INT NOT NULL DEFAULT 1, -- New dimension link
    amount_usd DECIMAL(15, 2) NOT NULL,
    number_of_payments INT DEFAULT 1,
    record_id BIGINT, -- Optimized to BIGINT for joining speed (was VARCHAR)
    
    -- Foreign Key Constraints (Optional for performance, good for integrity)
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (recipient_key) REFERENCES dim_recipient(recipient_key),
    FOREIGN KEY (payer_key) REFERENCES dim_payer(payer_key),
    FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
    FOREIGN KEY (nature_key) REFERENCES dim_nature(nature_key)
);
