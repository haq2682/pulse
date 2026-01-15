-- Address Table

CREATE TABLE addresses (
    address_id VARCHAR(50) PRIMARY KEY, -- Unique ID for each address
    city VARCHAR(100) NOT NULL, -- City name, e.g., 'New York'
    state_province VARCHAR(100), -- State or province name, e.g., 'California'
    postal_code VARCHAR(20), -- Zip or postal code, e.g., '10001'
    country VARCHAR(100) NOT NULL -- Full country name, e.g., 'United States'
);

-- 1. Customers Table
CREATE TABLE customers (
    customer_id VARCHAR(50) PRIMARY KEY, -- Unique system-generated ID, e.g., 'CUST_00123'
    gender VARCHAR(10), -- Possible: 'Male', 'Female', 'Other', 'Prefer not to say'
    date_of_birth DATE, -- Format: 'YYYY-MM-DD'
    account_status VARCHAR(20), -- Possible: 'Active', 'Inactive', 'Blocked'
    address_id VARCHAR(50), -- Unique ID for each address
    city VARCHAR(100) NOT NULL, -- City name, e.g., 'New York'
    state_province VARCHAR(100), -- State or province name, e.g., 'California'
    postal_code VARCHAR(20), -- Zip or postal code, e.g., '10001'
    country VARCHAR(100) NOT NULL, -- Full country name, e.g., 'United States'
    account_created_at TIMESTAMP, -- When the account was created
    last_login_date TIMESTAMP,  -- optional
    is_active BOOLEAN DEFAULT TRUE, -- optional
    FOREIGN KEY (address_id) REFERENCES addresses(address_id)
);

-- 14. Suppliers Table
CREATE TABLE suppliers (
    supplier_id VARCHAR(50) PRIMARY KEY,
    supplier_rating DECIMAL(3, 2),
    supplier_status VARCHAR(50) DEFAULT 'active',
    is_preferred BOOLEAN DEFAULT FALSE,
    is_verified BOOLEAN DEFAULT FALSE,
    contract_start_date DATE,
    contract_end_date DATE,
    city VARCHAR(100),
    state VARCHAR(100),
    zip_code VARCHAR(20),
    country VARCHAR(100)
);

-- Categories Table
CREATE TABLE categories (
    category_id VARCHAR(50) PRIMARY KEY, -- Unique category ID
    category_name VARCHAR(100) NOT NULL, -- e.g., 'Running Shoes'
    sub_category VARCHAR(50)
);

-- 3. Products Table
CREATE TABLE products (
    product_id VARCHAR(50) PRIMARY KEY, -- Unique ID, e.g., 'PROD_0001'
    product_name VARCHAR(255) NOT NULL, -- Full product name
    sku VARCHAR(100) UNIQUE, -- Stock Keeping Unit
    category_id VARCHAR(50),
    category VARCHAR(50),
    sub_category VARCHAR(50),
    brand VARCHAR(100),
    supplier_id VARCHAR(50),
    cost_price DECIMAL(10, 2),
    sell_price DECIMAL(10, 2),
    launch_date DATE,
    weight DECIMAL(8, 3),
    dimensions VARCHAR(50),
    color VARCHAR(50),
    size VARCHAR(50),
    material VARCHAR(100),
    FOREIGN KEY (supplier_id) REFERENCES suppliers(supplier_id),
    FOREIGN KEY (category_id) REFERENCES categories(category_id)
);

-- 10. Inventory Table
CREATE TABLE inventory (
    inventory_id VARCHAR(50) PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL,
    supplier_id VARCHAR(50),
    stock_quantity INT NOT NULL,
    reserved_quantity INT DEFAULT 0,
    minimum_stock_level INTEGER DEFAULT 0,
    last_restocked_date TIMESTAMP,
    storage_cost DECIMAL(10, 2),
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    FOREIGN KEY (supplier_id) REFERENCES suppliers(supplier_id)
);

-- 5. Wishlist Table
CREATE TABLE wishlist (
    wishlist_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    added_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    purchased_date TIMESTAMP,
    removed_date TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

-- 6. Shopping Cart Table
CREATE TABLE shopping_cart (
    cart_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    session_id VARCHAR(50),
    product_id VARCHAR(50) NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    added_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    cart_status VARCHAR(20),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

-- 7. Orders Table
CREATE TABLE orders (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    order_status VARCHAR(30),
    subtotal DECIMAL(10, 2),
    tax_amount DECIMAL(10, 2),
    shipping_cost DECIMAL(10, 2),
    total_discount DECIMAL(10, 2),
    total_amount DECIMAL(10, 2),
    currency VARCHAR(10) DEFAULT 'USD',
    order_placed_at TIMESTAMP NOT NULL,
    order_shipped_at TIMESTAMP,
    order_delivered_at TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- 8. Order Items Table
CREATE TABLE order_items (
    order_item_id VARCHAR(50) PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    quantity INT NOT NULL,
    discount_amount DECIMAL(10, 2) DEFAULT 0,
    product_cost DECIMAL(10, 2),
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

-- 9. Payments Table
CREATE TABLE payments (
    payment_id VARCHAR(50) PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL,
    payment_method VARCHAR(50),
    payment_provider VARCHAR(50),
    payment_status VARCHAR(30),
    transaction_id VARCHAR(255),
    processing_fee DECIMAL(10, 2),
    refund_amount DECIMAL(10, 2) DEFAULT 0,
    refund_date TIMESTAMP,
    payment_date TIMESTAMP,
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

-- 11. Reviews Table
CREATE TABLE reviews (
    review_id VARCHAR(50) PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL,
    customer_id VARCHAR(50),
    rating INT CHECK (rating >= 1 AND rating <= 5),
    review_title VARCHAR(255),
    review_desc TEXT,
    review_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- 12. Marketing Campaigns Table
CREATE TABLE marketing_campaigns (
    campaign_id VARCHAR(50) PRIMARY KEY,
    campaign_name VARCHAR(255),
    campaign_type VARCHAR(50),
    start_date DATE,
    end_date DATE,
    budget DECIMAL(10, 2),
    spent_amount DECIMAL(10, 2) DEFAULT 0,
    impressions INT DEFAULT 0,
    clicks INT DEFAULT 0,
    conversions INT DEFAULT 0,
    target_audience TEXT,
    campaign_status VARCHAR(20)
);

-- 13. Customer Sessions Table
CREATE TABLE customer_sessions (
    session_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    session_start TIMESTAMP,
    session_end TIMESTAMP,
    device_type VARCHAR(20),
    referrer_source VARCHAR(100),
    pages_viewed INT,
    products_viewed INT,
    conversion_flag BOOLEAN DEFAULT FALSE,
    cart_abandonment_flag BOOLEAN DEFAULT FALSE,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- User table
CREATE TABLE users (
    user_id VARCHAR(50) PRIMARY KEY,
    username VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL
);

-- Business table
CREATE TABLE businesses (
    business_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    business_name VARCHAR(255) NOT NULL,
    business_region VARCHAR(100),
    business_currency VARCHAR(50),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);
