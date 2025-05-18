-- Create the database tables for WMS MVP

-- Table: marketplaces
CREATE TABLE marketplaces (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: locations
CREATE TABLE locations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    code TEXT NOT NULL UNIQUE,
    address TEXT,
    city TEXT,
    state TEXT,
    country TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: products
CREATE TABLE products (
    msku TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    category TEXT,
    hsn_code TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: sku_mappings
CREATE TABLE sku_mappings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sku TEXT NOT NULL UNIQUE,
    msku TEXT NOT NULL,
    marketplace_id INTEGER NOT NULL,
    asin TEXT,
    fnsku TEXT,
    fsn TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (msku) REFERENCES products (msku),
    FOREIGN KEY (marketplace_id) REFERENCES marketplaces (id)
);

-- Table: inventory
CREATE TABLE inventory (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    msku TEXT NOT NULL,
    quantity INTEGER NOT NULL DEFAULT 0,
    location_id INTEGER NOT NULL,
    disposition TEXT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (msku) REFERENCES products (msku),
    FOREIGN KEY (location_id) REFERENCES locations (id)
);

-- Table: orders
CREATE TABLE orders (
    order_id TEXT PRIMARY KEY,
    marketplace_id INTEGER NOT NULL,
    order_date TIMESTAMP NOT NULL,
    shipment_id TEXT,
    customer_name TEXT,
    customer_state TEXT,
    customer_city TEXT,
    customer_pincode TEXT,
    order_status TEXT,
    tracking_id TEXT,
    invoice_no TEXT,
    invoice_date TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (marketplace_id) REFERENCES marketplaces (id)
);

-- Table: order_items
CREATE TABLE order_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id TEXT NOT NULL,
    msku TEXT NOT NULL,
    sku TEXT,
    quantity INTEGER NOT NULL,
    price REAL NOT NULL,
    discount REAL DEFAULT 0,
    tax_rate REAL DEFAULT 0,
    tax_amount REAL DEFAULT 0,
    shipping_charge REAL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (order_id) REFERENCES orders (order_id),
    FOREIGN KEY (msku) REFERENCES products (msku)
);

-- Table: inventory_transactions
CREATE TABLE inventory_transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    msku TEXT NOT NULL,
    quantity_change INTEGER NOT NULL,
    transaction_type TEXT NOT NULL, -- 'order', 'return', 'adjustment', 'transfer'
    reference_id TEXT, -- order_id or other reference
    location_id INTEGER NOT NULL,
    notes TEXT,
    transaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (msku) REFERENCES products (msku),
    FOREIGN KEY (location_id) REFERENCES locations (id)
);

-- Create indexes for better performance
CREATE INDEX idx_sku_mappings_msku ON sku_mappings (msku);
CREATE INDEX idx_inventory_msku ON inventory (msku);
CREATE INDEX idx_order_items_order_id ON order_items (order_id);
CREATE INDEX idx_order_items_msku ON order_items (msku);
CREATE INDEX idx_orders_marketplace_id ON orders (marketplace_id);
CREATE INDEX idx_orders_order_date ON orders (order_date);
CREATE INDEX idx_inventory_transactions_msku ON inventory_transactions (msku);
CREATE INDEX idx_inventory_transactions_transaction_date ON inventory_transactions (transaction_date);
