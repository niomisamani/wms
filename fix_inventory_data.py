import pandas as pd
import os
import logging
import sqlite3
from typing import List, Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/fix_inventory_data.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("fix_inventory_data")

def fix_inventory_data():
    """
    Fix inventory data by processing raw files and updating the database
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Connect to SQLite database
        db_path = os.path.join('data', 'wms_database.db')
        conn = sqlite3.connect(db_path)
        
        # Check if inventory table exists
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='inventory'")
        if not cursor.fetchone():
            logger.error("Inventory table does not exist in the database")
            return False
        
        # Process raw files to extract inventory data
        raw_dir = os.path.join('data', 'raw')
        if not os.path.exists(raw_dir):
            logger.error(f"Raw data directory does not exist: {raw_dir}")
            return False
        
        # Get all CSV files in raw directory
        csv_files = [f for f in os.listdir(raw_dir) if f.endswith('.csv')]
        if not csv_files:
            logger.error("No CSV files found in raw directory")
            return False
        
        # Process each file to find inventory data
        inventory_data = []
        
        for file_name in csv_files:
            file_path = os.path.join(raw_dir, file_name)
            logger.info(f"Processing file: {file_path}")
            
            try:
                # Try to read the file
                df = pd.read_csv(file_path)
                
                # Check if this looks like an inventory file
                if 'msku' in df.columns or 'MSKU' in df.columns:
                    # Standardize column names
                    df.columns = df.columns.str.lower()
                    
                    # Check for quantity column
                    quantity_cols = [col for col in df.columns if 'quantity' in col.lower() or 'qty' in col.lower()]
                    if quantity_cols:
                        quantity_col = quantity_cols[0]
                        logger.info(f"Found quantity column: {quantity_col}")
                        
                        # Extract inventory data
                        msku_col = 'msku' if 'msku' in df.columns else df.columns[0]
                        inventory_df = df[[msku_col, quantity_col]].copy()
                        inventory_df.columns = ['msku', 'quantity']
                        
                        # Convert quantity to numeric
                        inventory_df['quantity'] = pd.to_numeric(inventory_df['quantity'], errors='coerce').fillna(0)
                        
                        # Add to inventory data
                        inventory_data.append(inventory_df)
                        logger.info(f"Extracted {len(inventory_df)} inventory records from {file_name}")
            except Exception as e:
                logger.error(f"Error processing file {file_name}: {str(e)}")
        
        if not inventory_data:
            logger.error("No inventory data found in any files")
            return False
        
        # Combine all inventory data
        combined_inventory = pd.concat(inventory_data, ignore_index=True)
        
        # Group by MSKU and sum quantities
        inventory_summary = combined_inventory.groupby('msku')['quantity'].sum().reset_index()
        
        # Clear existing inventory data
        cursor.execute("DELETE FROM inventory")
        
        # Insert new inventory data
        for _, row in inventory_summary.iterrows():
            cursor.execute(
                "INSERT INTO inventory (msku, quantity, location) VALUES (?, ?, ?)",
                (row['msku'], row['quantity'], 'OWN1')
            )
        
        # Commit changes
        conn.commit()
        
        # Close connection
        conn.close()
        
        logger.info(f"Successfully updated inventory data with {len(inventory_summary)} records")
        
        # Save inventory data to CSV for reference
        output_path = os.path.join('data', 'processed', 'fixed_inventory.csv')
        inventory_summary.to_csv(output_path, index=False)
        logger.info(f"Saved fixed inventory data to {output_path}")
        
        return True
    
    except Exception as e:
        logger.error(f"Error fixing inventory data: {str(e)}")
        return False

if __name__ == "__main__":
    if fix_inventory_data():
        print("Successfully fixed inventory data")
    else:
        print("Error fixing inventory data")
