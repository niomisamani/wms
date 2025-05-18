import pandas as pd
import requests
import logging
import yaml
import os
import json
from typing import Dict, List, Tuple, Optional, Union, Any
from datetime import datetime
import sqlite3
import dotenv

# Load environment variables
dotenv.load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/database.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("database")

class DatabaseManager:
    """
    Class to handle database operations for the WMS
    Supports Baserow, NoCodeDB, and SQLite
    """
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """
        Initialize the Database Manager with configuration
        
        Args:
            config_path: Path to the configuration file
        """
        # Load configuration
        with open(config_path, 'r') as file:
            self.config = yaml.safe_load(file)
        
        # Get database type from environment variable first, then config
        self.db_type = os.environ.get('DB_TYPE') or self.config['database']['type']
        logger.info(f"Using database type: {self.db_type}")
        
        # Store database path for SQLite (don't connect yet for thread safety)
        if self.db_type == 'sqlite':
            self.db_path = os.path.join('data', 'wms_database.db')
            # Create database directory if it doesn't exist
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            # Create tables if they don't exist
            self._create_sqlite_tables()
        
        # Initialize database connection based on type
        if self.db_type == 'baserow':
            self._init_baserow()
        elif self.db_type == 'nocodedb':
            self._init_nocodedb()
        
        logger.info(f"Database Manager initialized with {self.db_type}")
    
    def _init_baserow(self):
        """Initialize Baserow connection"""
        # Get API URL from config or use default
        self.baserow_api_url = self.config['database'].get('baserow', {}).get('api_url', 'https://api.baserow.io/api/')
        
        # After the line that sets self.baserow_api_url
        logger.info(f"Initializing Baserow connection with API URL: {self.baserow_api_url}")
        
        # Get API key from environment variable
        self.baserow_api_key = os.environ.get('DB_API_KEY')
        
        # After getting the API key
        if self.baserow_api_key:
            logger.info(f"API key found with length: {len(self.baserow_api_key)}")
        else:
            logger.error("Baserow API key not found in environment variables")
        
        if not self.baserow_api_key:
            logger.error("Baserow API key not found in environment variables")
            raise ValueError("Baserow API key not found in environment variables")
        
        logger.info(f"Using Baserow API URL: {self.baserow_api_url}")
        logger.info(f"API Key found: {bool(self.baserow_api_key)}")
        
        # Test connection
        try:
            headers = {
                'Authorization': f'Token {self.baserow_api_key}',
                'Content-Type': 'application/json'
            }
            
            logger.info(f"Making test request to: {self.baserow_api_url}applications/")
            logger.info(f"Headers: Authorization: Token ***{self.baserow_api_key[-4:] if self.baserow_api_key else 'None'} (masked for security)")
            
            response = requests.get(
                f"{self.baserow_api_url}applications/", 
                headers=headers
            )
            
            logger.info(f"Response status code: {response.status_code}")
            logger.info(f"Response content: {response.text[:200]}...")  # Log first 200 chars of response
            
            if response.status_code != 200:
                logger.error(f"Response error: {response.text}")
                raise ConnectionError(f"Failed to connect to Baserow: {response.status_code}")
            
            response.raise_for_status()
            logger.info("Successfully connected to Baserow API")
            
            # Get table IDs from environment variables
            self.table_ids = {
                'marketplaces': os.environ.get('DB_TABLE_ID_MARKETPLACES'),
                'locations': os.environ.get('DB_TABLE_ID_LOCATIONS'),
                'products': os.environ.get('DB_TABLE_ID_PRODUCTS'),
                'sku_mappings': os.environ.get('DB_TABLE_ID_SKU_MAPPINGS'),
                'inventory': os.environ.get('DB_TABLE_ID_INVENTORY'),
                'orders': os.environ.get('DB_TABLE_ID_ORDERS'),
                'order_items': os.environ.get('DB_TABLE_ID_ORDER_ITEMS'),
                'inventory_transactions': os.environ.get('DB_TABLE_ID_INVENTORY_TRANSACTIONS')
            }
            
            # Log table IDs for debugging
            for table, table_id in self.table_ids.items():
                logger.info(f"Table ID for {table}: {table_id}")
            
        except Exception as e:
            logger.error(f"Error connecting to Baserow: {str(e)}")
            # Fall back to SQLite if Baserow connection fails
            logger.info("Falling back to SQLite database")
            self.db_type = 'sqlite'
            self.db_path = os.path.join('data', 'wms_database.db')
            # Create database directory if it doesn't exist
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            # Create tables if they don't exist
            self._create_sqlite_tables()
    
    def _init_nocodedb(self):
        """Initialize NoCodeDB connection"""
        # Implementation for NoCodeDB would go here
        # This is a placeholder as NoCodeDB API details would be needed
        logger.warning("NoCodeDB support is not fully implemented")
    
    def _create_sqlite_tables(self):
        """Create SQLite tables if they don't exist"""
        try:
            # Create a new connection for this operation
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
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
                
                conn.commit()
                
            logger.info("SQLite tables created successfully")
        except Exception as e:
            logger.error(f"Error creating SQLite tables: {str(e)}")
    
    def get_table_data(self, table_id: str) -> pd.DataFrame:
        """
        Get data from a table
        
        Args:
            table_id: Table ID or name
            
        Returns:
            DataFrame: Table data
        """
        if self.db_type == 'baserow':
            return self._get_baserow_table(table_id)
        elif self.db_type == 'sqlite':
            return self._get_sqlite_table(table_id)
        else:
            logger.error(f"get_table_data not implemented for {self.db_type}")
            return pd.DataFrame()
    
    def _get_baserow_table(self, table_id: str) -> pd.DataFrame:
        """Get data from a Baserow table"""
        try:
            headers = {
                'Authorization': f'Token {self.baserow_api_key}',
                'Content-Type': 'application/json'
            }
            response = requests.get(
                f"{self.baserow_api_url}database/rows/table/{table_id}/?user_field_names=true", 
                headers=headers
            )
            response.raise_for_status()
            
            data = response.json()
            df = pd.DataFrame(data.get('results', []))
            
            logger.info(f"Retrieved {len(df)} rows from Baserow table {table_id}")
            return df
        
        except Exception as e:
            logger.error(f"Error getting data from Baserow table {table_id}: {str(e)}")
            return pd.DataFrame()
    
    def _get_sqlite_table(self, table_name: str) -> pd.DataFrame:
        """Get data from a SQLite table"""
        try:
            # Create a new connection for this operation
            with sqlite3.connect(self.db_path) as conn:
                query = f"SELECT * FROM {table_name}"
                df = pd.read_sql_query(query, conn)
                
                logger.info(f"Retrieved {len(df)} rows from SQLite table {table_name}")
                return df
        
        except Exception as e:
            logger.error(f"Error getting data from SQLite table {table_name}: {str(e)}")
            return pd.DataFrame()
    
    def add_records(self, table_id: str, records: List[Dict]) -> bool:
        """
        Add records to a table
        
        Args:
            table_id: Table ID or name
            records: List of record dictionaries
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not records:
            logger.warning("No records to add")
            return False
        
        if self.db_type == 'baserow':
            return self._add_baserow_records(table_id, records)
        elif self.db_type == 'sqlite':
            return self._add_sqlite_records(table_id, records)
        else:
            logger.error(f"add_records not implemented for {self.db_type}")
            return False
    
    def _add_baserow_records(self, table_id: str, records: List[Dict]) -> bool:
        """Add records to a Baserow table"""
        try:
            headers = {
                'Authorization': f'Token {self.baserow_api_key}',
                'Content-Type': 'application/json'
            }
            
            # Baserow API can only handle 100 records at a time
            batch_size = 100
            success = True
            
            for i in range(0, len(records), batch_size):
                batch = records[i:i+batch_size]
                
                payload = {
                    "items": batch
                }
                
                response = requests.post(
                    f"{self.baserow_api_url}database/rows/table/{table_id}/batch/?user_field_names=true", 
                    headers=headers,
                    json=payload
                )
                
                if response.status_code != 200:
                    logger.error(f"Error adding batch to Baserow: {response.text}")
                    success = False
            
            logger.info(f"Added {len(records)} records to Baserow table {table_id}")
            return success
        
        except Exception as e:
            logger.error(f"Error adding records to Baserow table {table_id}: {str(e)}")
            return False
    
    def _add_sqlite_records(self, table_name: str, records: List[Dict]) -> bool:
        """Add records to a SQLite table"""
        try:
            # Get column names from first record
            if not records:
                return False
            
            columns = list(records[0].keys())
            placeholders = ', '.join(['?'] * len(columns))
            columns_str = ', '.join(columns)
            
            # Create a new connection for this operation
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Prepare query
                query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
                
                # Execute for each record
                for record in records:
                    values = [record.get(col) for col in columns]
                    cursor.execute(query, values)
                
                conn.commit()
                
            logger.info(f"Added {len(records)} records to SQLite table {table_name}")
            return True
        
        except Exception as e:
            logger.error(f"Error adding records to SQLite table {table_name}: {str(e)}")
            return False
    
    def update_records(self, table_id: str, records: List[Dict]) -> bool:
        """
        Update records in a table
        
        Args:
            table_id: Table ID or name
            records: List of record dictionaries with ID
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not records:
            logger.warning("No records to update")
            return False
        
        if self.db_type == 'baserow':
            return self._update_baserow_records(table_id, records)
        elif self.db_type == 'sqlite':
            return self._update_sqlite_records(table_id, records)
        else:
            logger.error(f"update_records not implemented for {self.db_type}")
            return False
    
    def _update_baserow_records(self, table_id: str, records: List[Dict]) -> bool:
        """Update records in a Baserow table"""
        try:
            headers = {
                'Authorization': f'Token {self.baserow_api_key}',
                'Content-Type': 'application/json'
            }
            
            success = True
            
            for record in records:
                # Baserow requires ID for updates
                if 'id' not in record:
                    logger.error("Record missing 'id' field for Baserow update")
                    success = False
                    continue
                
                record_id = record.pop('id')
                
                response = requests.patch(
                    f"{self.baserow_api_url}database/rows/table/{table_id}/{record_id}/?user_field_names=true", 
                    headers=headers,
                    json=record
                )
                
                if response.status_code != 200:
                    logger.error(f"Error updating Baserow record: {response.text}")
                    success = False
            
            logger.info(f"Updated {len(records)} records in Baserow table {table_id}")
            return success
        
        except Exception as e:
            logger.error(f"Error updating records in Baserow table {table_id}: {str(e)}")
            return False
    
    def _update_sqlite_records(self, table_name: str, records: List[Dict]) -> bool:
        """Update records in a SQLite table"""
        try:
            success = True
            
            # Create a new connection for this operation
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                for record in records:
                    # SQLite requires a primary key for updates
                    if 'id' not in record and 'msku' not in record and 'order_id' not in record:
                        logger.error("Record missing primary key field for SQLite update")
                        success = False
                        continue
                    
                    # Determine primary key
                    if 'id' in record:
                        pk_name = 'id'
                        pk_value = record.pop('id')
                    elif 'msku' in record and table_name == 'products':
                        pk_name = 'msku'
                        pk_value = record['msku']
                    elif 'order_id' in record and table_name == 'orders':
                        pk_name = 'order_id'
                        pk_value = record['order_id']
                    else:
                        logger.error(f"Could not determine primary key for table {table_name}")
                        success = False
                        continue
                    
                    # Build update query
                    set_clause = ', '.join([f"{k} = ?" for k in record.keys()])
                    query = f"UPDATE {table_name} SET {set_clause} WHERE {pk_name} = ?"
                    
                    # Execute query
                    values = list(record.values()) + [pk_value]
                    cursor.execute(query, values)
                
                conn.commit()
                
            logger.info(f"Updated {len(records)} records in SQLite table {table_name}")
            return success
        
        except Exception as e:
            logger.error(f"Error updating records in SQLite table {table_name}: {str(e)}")
            return False
    
    def delete_records(self, table_id: str, record_ids: List[str]) -> bool:
        """
        Delete records from a table
        
        Args:
            table_id: Table ID or name
            record_ids: List of record IDs to delete
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not record_ids:
            logger.warning("No record IDs to delete")
            return False
        
        if self.db_type == 'baserow':
            return self._delete_baserow_records(table_id, record_ids)
        elif self.db_type == 'sqlite':
            return self._delete_sqlite_records(table_id, record_ids)
        else:
            logger.error(f"delete_records not implemented for {self.db_type}")
            return False
    
    def _delete_baserow_records(self, table_id: str, record_ids: List[str]) -> bool:
        """Delete records from a Baserow table"""
        try:
            headers = {
                'Authorization': f'Token {self.baserow_api_key}',
                'Content-Type': 'application/json'
            }
            
            payload = {
                "items": record_ids
            }
            
            response = requests.post(
                f"{self.baserow_api_url}database/rows/table/{table_id}/batch-delete/", 
                headers=headers,
                json=payload
            )
            
            if response.status_code != 204:
                logger.error(f"Error deleting from Baserow: {response.text}")
                return False
            
            logger.info(f"Deleted {len(record_ids)} records from Baserow table {table_id}")
            return True
        
        except Exception as e:
            logger.error(f"Error deleting records from Baserow table {table_id}: {str(e)}")
            return False
    
    def _delete_sqlite_records(self, table_name: str, record_ids: List[str]) -> bool:
        """Delete records from a SQLite table"""
        try:
            # Create a new connection for this operation
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Determine primary key based on table name
                if table_name == 'products':
                    pk_name = 'msku'
                elif table_name == 'orders':
                    pk_name = 'order_id'
                else:
                    pk_name = 'id'
                
                # Create placeholders for IN clause
                placeholders = ', '.join(['?'] * len(record_ids))
                
                # Execute delete query
                query = f"DELETE FROM {table_name} WHERE {pk_name} IN ({placeholders})"
                cursor.execute(query, record_ids)
                
                conn.commit()
                
            logger.info(f"Deleted {len(record_ids)} records from SQLite table {table_name}")
            return True
        
        except Exception as e:
            logger.error(f"Error deleting records from SQLite table {table_name}: {str(e)}")
            return False
    
    def execute_query(self, query: str, params: Optional[List] = None) -> pd.DataFrame:
        """
        Execute a custom query
        
        Args:
            query: SQL query string
            params: Query parameters
            
        Returns:
            DataFrame: Query results
        """
        if self.db_type == 'sqlite':
            try:
                # Create a new connection for this operation
                with sqlite3.connect(self.db_path) as conn:
                    if params:
                        df = pd.read_sql_query(query, conn, params=params)
                    else:
                        df = pd.read_sql_query(query, conn)
                    
                    logger.info(f"Executed query, returned {len(df)} rows")
                    return df
            
            except Exception as e:
                logger.error(f"Error executing SQLite query: {str(e)}")
                return pd.DataFrame()
        else:
            logger.error(f"execute_query not implemented for {self.db_type}")
            return pd.DataFrame()
