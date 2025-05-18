import sqlite3
import os
import logging
import pandas as pd
import requests
from typing import List, Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/setup_database.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("setup_database")

def setup_sqlite_database():
    """
    Set up the SQLite database with the required tables
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Database path
        db_path = os.path.join('data', 'wms_database.db')
        
        # Create database directory if it doesn't exist
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        # Connect to database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Create tables
        
        # Products table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS products (
            msku TEXT PRIMARY KEY,
            name TEXT,
            description TEXT,
            category TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # SKU Mappings table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS sku_mappings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sku TEXT UNIQUE,
            msku TEXT,
            marketplace TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (msku) REFERENCES products (msku)
        )
        ''')
        
        # Inventory table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS inventory (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            msku TEXT,
            quantity INTEGER,
            location TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (msku) REFERENCES products (msku)
        )
        ''')
        
        # Orders table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS orders (
            order_id TEXT PRIMARY KEY,
            marketplace TEXT,
            order_date TIMESTAMP,
            customer_name TEXT,
            customer_state TEXT,
            status TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # Order Items table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS order_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id TEXT,
            msku TEXT,
            sku TEXT,
            quantity INTEGER,
            price REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (order_id) REFERENCES orders (order_id),
            FOREIGN KEY (msku) REFERENCES products (msku)
        )
        ''')
        
        # Marketplaces table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS marketplaces (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # Locations table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS locations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            code TEXT NOT NULL UNIQUE,
            address TEXT,
            city TEXT,
            state TEXT,
            country TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # Inventory Transactions table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS inventory_transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            msku TEXT NOT NULL,
            quantity_change INTEGER NOT NULL,
            transaction_type TEXT NOT NULL,
            reference_id TEXT,
            location_id INTEGER,
            notes TEXT,
            transaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (msku) REFERENCES products (msku)
        )
        ''')
        
        # Insert initial data for marketplaces
        cursor.execute('''
        INSERT OR IGNORE INTO marketplaces (name, description) VALUES 
        ('amazon', 'Amazon India'),
        ('flipkart', 'Flipkart'),
        ('meesho', 'Meesho'),
        ('unknown', 'Unknown Marketplace')
        ''')
        
        # Insert initial data for locations
        cursor.execute('''
        INSERT OR IGNORE INTO locations (name, code, country) VALUES 
        ('Amazon Fulfillment Center Mumbai', 'BOM7', 'IN'),
        ('Flipkart Warehouse Delhi', 'DEL1', 'IN'),
        ('Own Warehouse', 'OWN1', 'IN')
        ''')
        
        # Commit changes
        conn.commit()
        
        # Close connection
        conn.close()
        
        logger.info(f"SQLite database set up successfully at {db_path}")
        return True
    
    except Exception as e:
        logger.error(f"Error setting up SQLite database: {str(e)}")
        return False

def import_sku_mappings_from_url(url: str, db_path: str) -> bool:
    """
    Import SKU mappings from a URL
    
    Args:
        url: URL of the CSV file
        db_path: Path to the SQLite database
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Download the CSV file
        response = requests.get(url)
        response.raise_for_status()
        
        # Create a temporary file
        temp_file = os.path.join('data', 'temp_sku_mappings.csv')
        os.makedirs(os.path.dirname(temp_file), exist_ok=True)
        
        # Save the file
        with open(temp_file, 'wb') as f:
            f.write(response.content)
        
        # Read the CSV file
        df = pd.read_csv(temp_file)
        logger.info(f"Read {len(df)} rows from {url}")
        
        # Connect to the database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Insert the data
        success_count = 0
        fail_count = 0
        
        for _, row in df.iterrows():
            try:
                # Get the values
                sku = str(row['sku'])
                msku = str(row['msku'])
                marketplace = str(row.get('marketplace', 'unknown'))
                
                # Skip empty values
                if pd.isna(sku) or pd.isna(msku) or sku == '' or msku == '':
                    logger.warning(f"Skipping row with empty SKU or MSKU: {row}")
                    fail_count += 1
                    continue
                
                # Insert the product if it doesn't exist
                cursor.execute(
                    "INSERT OR IGNORE INTO products (msku, name) VALUES (?, ?)",
                    (msku, msku)
                )
                
                # Insert the SKU mapping
                cursor.execute(
                    "INSERT OR REPLACE INTO sku_mappings (sku, msku, marketplace) VALUES (?, ?, ?)",
                    (sku, msku, marketplace)
                )
                
                success_count += 1
            except Exception as e:
                logger.error(f"Error processing row {row}: {str(e)}")
                fail_count += 1
        
        # Commit the changes
        conn.commit()
        
        # Close the connection
        conn.close()
        
        # Remove the temporary file
        os.remove(temp_file)
        
        logger.info(f"Imported {success_count} SKU mappings, {fail_count} failed")
        return True
    
    except Exception as e:
        logger.error(f"Error importing SKU mappings from URL: {str(e)}")
        return False

def create_sample_inventory(db_path: str) -> bool:
    """
    Create sample inventory data
    
    Args:
        db_path: Path to the SQLite database
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Connect to the database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get all MSKUs from the products table
        cursor.execute("SELECT msku FROM products LIMIT 100")
        mskus = [row[0] for row in cursor.fetchall()]
        
        if not mskus:
            logger.warning("No MSKUs found in the products table")
            return False
        
        # Create sample inventory data
        inventory_data = []
        
        import random
        for msku in mskus:
            inventory_data.append({
                'msku': msku,
                'quantity': random.randint(1, 20),
                'location': 'OWN1'
            })
        
        # Insert the data
        for item in inventory_data:
            cursor.execute(
                "INSERT OR REPLACE INTO inventory (msku, quantity, location) VALUES (?, ?, ?)",
                (item['msku'], item['quantity'], item['location'])
            )
        
        # Commit the changes
        conn.commit()
        
        # Close the connection
        conn.close()
        
        logger.info(f"Created sample inventory data for {len(inventory_data)} MSKUs")
        return True
    
    except Exception as e:
        logger.error(f"Error creating sample inventory data: {str(e)}")
        return False

if __name__ == "__main__":
    # Set up SQLite database
    if setup_sqlite_database():
        print("SQLite database set up successfully")
        
        # Import SKU mappings from URL
        sku_mappings_url = "https://hebbkx1anhila5yf.public.blob.vercel-storage.com/sku_mapping_upload-L4HFJ5eZL8AwXZYMZxC78pOlp59dKE.csv"
        db_path = os.path.join('data', 'wms_database.db')
        
        if import_sku_mappings_from_url(sku_mappings_url, db_path):
            print("SKU mappings imported successfully")
            
            # Create sample inventory data
            if create_sample_inventory(db_path):
                print("Sample inventory data created successfully")
            else:
                print("Error creating sample inventory data")
        else:
            print("Error importing SKU mappings")
    else:
        print("Error setting up SQLite database")
