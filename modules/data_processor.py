import pandas as pd
import numpy as np
import os
import logging
import yaml
import re
from typing import Dict, List, Tuple, Optional, Union, Any
from datetime import datetime
import json

from modules.sku_mapper import SKUMapper

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/data_processor.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("data_processor")

class DataProcessor:
    """
    Class to handle loading, processing, and cleaning of sales data
    from various marketplaces
    """
    
    def __init__(self, config_path: str = "config/config.yaml", sku_mapper: Optional[SKUMapper] = None):
        """
        Initialize the Data Processor with configuration
        
        Args:
            config_path: Path to the configuration file
            sku_mapper: SKUMapper instance for mapping SKUs to MSKUs
        """
        # Load configuration
        with open(config_path, 'r') as file:
            self.config = yaml.safe_load(file)
        
        # Set paths
        self.raw_data_path = self.config['paths']['raw_data']
        self.processed_data_path = self.config['paths']['processed_data']
        
        # Create directories if they don't exist
        os.makedirs(self.raw_data_path, exist_ok=True)
        os.makedirs(self.processed_data_path, exist_ok=True)
        
        # Initialize SKU mapper if not provided
        self.sku_mapper = sku_mapper if sku_mapper else SKUMapper(config_path)
        
        # Marketplace specific processors
        self.marketplace_processors = {
            'amazon': self._process_amazon_data,
            'flipkart': self._process_flipkart_data,
            'meesho': self._process_meesho_data
        }
        
        logger.info("Data Processor initialized successfully")
    
    def save_uploaded_file(self, uploaded_file, marketplace: str) -> str:
        """
        Save an uploaded file to the raw data directory
        
        Args:
            uploaded_file: The uploaded file object from Streamlit
            marketplace: The marketplace the file belongs to
            
        Returns:
            str: Path to the saved file
        """
        # Create timestamp for unique filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Get original filename and extension
        filename = uploaded_file.name
        file_ext = os.path.splitext(filename)[1]
        
        # Create new filename with marketplace and timestamp
        new_filename = f"{marketplace}_{timestamp}{file_ext}"
        file_path = os.path.join(self.raw_data_path, new_filename)
        
        # Save file
        with open(file_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        
        logger.info(f"Saved uploaded file to {file_path}")
        return file_path
    
    def detect_marketplace(self, file_path: str) -> str:
        """
        Detect the marketplace based on file content
        
        Args:
            file_path: Path to the file
            
        Returns:
            str: Detected marketplace or 'unknown'
        """
        try:
            # Read first few rows of the file
            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path, nrows=5)
            elif file_path.endswith(('.xlsx', '.xls')):
                df = pd.read_excel(file_path, nrows=5)
            else:
                logger.warning(f"Unsupported file format: {file_path}")
                return 'unknown'
            
            # Check column names to identify marketplace
            columns = set(df.columns.str.lower())
            
            # Amazon specific columns
            if {'fnsku', 'asin', 'msku'}.issubset(columns):
                return 'amazon'
            
            # Flipkart specific columns
            if {'fsn', 'shipment id', 'order id'}.issubset(columns):
                return 'flipkart'
            
            # Meesho specific columns
            if {'sub order no', 'reason for credit entry'}.issubset(columns):
                return 'meesho'
            
            logger.warning(f"Could not detect marketplace from columns: {columns}")
            return 'unknown'
            
        except Exception as e:
            logger.error(f"Error detecting marketplace: {str(e)}")
            return 'unknown'
    
    def process_file(self, file_path: str, marketplace: Optional[str] = None) -> Tuple[pd.DataFrame, str]:
        """
        Process a file based on its marketplace
        
        Args:
            file_path: Path to the file
            marketplace: The marketplace, if known
            
        Returns:
            Tuple[DataFrame, str]: Processed data and marketplace
        """
        try:
            # Detect marketplace if not provided
            if not marketplace or marketplace == 'unknown':
                marketplace = self.detect_marketplace(file_path)
            
            # Load file based on extension
            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path)
            elif file_path.endswith(('.xlsx', '.xls')):
                df = pd.read_excel(file_path)
            else:
                logger.error(f"Unsupported file format: {file_path}")
                return pd.DataFrame(), 'unknown'
            
            # Process based on marketplace
            if marketplace in self.marketplace_processors:
                processed_df = self.marketplace_processors[marketplace](df)
                
                # Save processed file
                processed_file = os.path.join(
                    self.processed_data_path, 
                    f"processed_{marketplace}_{os.path.basename(file_path)}"
                )
                processed_df.to_csv(processed_file, index=False)
                
                logger.info(f"Processed {marketplace} file with {len(processed_df)} rows")
                return processed_df, marketplace
            else:
                logger.warning(f"No processor available for marketplace: {marketplace}")
                return df, marketplace
            
        except Exception as e:
            logger.error(f"Error processing file: {str(e)}")
            return pd.DataFrame(), 'unknown'
    
    def _process_amazon_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Process Amazon sales data
        
        Args:
            df: Raw Amazon data
            
        Returns:
            DataFrame: Processed data with standardized columns
        """
        # Make column names consistent
        df.columns = df.columns.str.strip().str.lower()
        
        # Map SKUs to MSKUs
        if 'msku' in df.columns:
            # Amazon already has MSKU column, validate it
            df['msku_validated'] = df['msku']
        elif 'sku' in df.columns:
            # Map SKU to MSKU
            df['msku_validated'] = df['sku'].apply(
                lambda x: self.sku_mapper.get_msku(str(x)) if pd.notna(x) else None
            )
        elif 'asin' in df.columns:
            # Try to map ASIN to MSKU
            df['msku_validated'] = df['asin'].apply(
                lambda x: self.sku_mapper.get_msku(str(x)) if pd.notna(x) else None
            )
        
        # Standardize date columns
        date_columns = [col for col in df.columns if 'date' in col]
        for col in date_columns:
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce')
            except:
                logger.warning(f"Could not convert {col} to datetime")
        
        # Standardize quantity columns
        if 'quantity' in df.columns:
            df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(0)
        
        # Add marketplace column
        df['marketplace'] = 'amazon'
        
        # Add standardized columns for unified reporting
        df['order_id'] = df.get('reference id', np.nan)
        df['product_name'] = df.get('title', np.nan)
        df['order_date'] = df.get('date', np.nan)
        
        return df
    
    def _process_flipkart_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Process Flipkart sales data
        
        Args:
            df: Raw Flipkart data
            
        Returns:
            DataFrame: Processed data with standardized columns
        """
        # Make column names consistent
        df.columns = df.columns.str.strip().str.lower()
        
        # Map SKUs to MSKUs
        if 'sku' in df.columns:
            # Map SKU to MSKU
            df['msku_validated'] = df['sku'].apply(
                lambda x: self.sku_mapper.get_msku(str(x)) if pd.notna(x) else None
            )
        
        # Standardize date columns
        date_columns = [col for col in df.columns if 'date' in col or col == 'ordered on']
        for col in date_columns:
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce')
            except:
                logger.warning(f"Could not convert {col} to datetime")
        
        # Standardize quantity columns
        if 'quantity' in df.columns:
            df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(0)
        
        # Add marketplace column
        df['marketplace'] = 'flipkart'
        
        # Add standardized columns for unified reporting
        df['order_id'] = df.get('order id', np.nan)
        df['product_name'] = df.get('product', np.nan)
        df['order_date'] = df.get('ordered on', np.nan)
        
        return df
    
    def _process_meesho_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Process Meesho sales data
        
        Args:
            df: Raw Meesho data
            
        Returns:
            DataFrame: Processed data with standardized columns
        """
        # Make column names consistent
        df.columns = df.columns.str.strip().str.lower()
        
        # Map SKUs to MSKUs
        if 'sku' in df.columns:
            # Map SKU to MSKU
            df['msku_validated'] = df['sku'].apply(
                lambda x: self.sku_mapper.get_msku(str(x)) if pd.notna(x) else None
            )
        
        # Standardize date columns
        date_columns = [col for col in df.columns if 'date' in col]
        for col in date_columns:
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce')
            except:
                logger.warning(f"Could not convert {col} to datetime")
        
        # Standardize quantity columns
        if 'quantity' in df.columns:
            df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(0)
        
        # Add marketplace column
        df['marketplace'] = 'meesho'
        
        # Add standardized columns for unified reporting
        df['order_id'] = df.get('sub order no', np.nan)
        df['product_name'] = df.get('product name', np.nan)
        df['order_date'] = df.get('order date', np.nan)
        
        return df
    
    def combine_processed_data(self, files: List[str]) -> pd.DataFrame:
        """
        Combine multiple processed files into a single DataFrame
        
        Args:
            files: List of processed file paths
            
        Returns:
            DataFrame: Combined data
        """
        dfs = []
        
        for file in files:
            try:
                if file.endswith('.csv'):
                    df = pd.read_csv(file)
                elif file.endswith(('.xlsx', '.xls')):
                    df = pd.read_excel(file)
                else:
                    logger.warning(f"Unsupported file format: {file}")
                    continue
                
                dfs.append(df)
            except Exception as e:
                logger.error(f"Error reading file {file}: {str(e)}")
        
        if not dfs:
            logger.warning("No valid files to combine")
            return pd.DataFrame()
        
        # Combine all dataframes
        combined_df = pd.concat(dfs, ignore_index=True)
        
        # Save combined file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        combined_file = os.path.join(
            self.processed_data_path, 
            f"combined_data_{timestamp}.csv"
        )
        combined_df.to_csv(combined_file, index=False)
        
        logger.info(f"Combined {len(dfs)} files with total {len(combined_df)} rows")
        return combined_df
    
    def get_unmapped_skus(self, df: pd.DataFrame) -> List[str]:
        """
        Get list of SKUs that don't have MSKU mappings
        
        Args:
            df: DataFrame with SKU column
            
        Returns:
            List[str]: List of unmapped SKUs
        """
        unmapped = []
        
        if 'sku' in df.columns and 'msku_validated' in df.columns:
            # Get rows where MSKU is None but SKU exists
            mask = df['msku_validated'].isna() & df['sku'].notna()
            unmapped = df.loc[mask, 'sku'].unique().tolist()
            
            logger.info(f"Found {len(unmapped)} unmapped SKUs")
        
        return unmapped
    
    def calculate_inventory_changes(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate inventory changes based on processed data
        
        Args:
            df: Processed sales data
            
        Returns:
            DataFrame: Inventory changes by MSKU
        """
        # Ensure required columns exist
        if 'msku_validated' not in df.columns:
            logger.error("Required column 'msku_validated' missing for inventory calculation")
            # Create a default empty dataframe with the expected columns
            return pd.DataFrame(columns=['msku', 'quantity'])
            
        if 'quantity' not in df.columns:
            logger.error("Required column 'quantity' missing for inventory calculation")
            # Try to find alternative quantity columns
            quantity_cols = [col for col in df.columns if 'quantity' in col.lower()]
            if quantity_cols:
                logger.info(f"Using alternative quantity column: {quantity_cols[0]}")
                df['quantity'] = df[quantity_cols[0]]
            else:
                logger.error("No quantity column found, creating default quantity column with value 1")
                df['quantity'] = 1
        
        # Group by MSKU and sum quantities
        inventory_changes = df.groupby('msku_validated')['quantity'].sum().reset_index()
        
        # Rename columns for clarity
        inventory_changes.columns = ['msku', 'quantity']
        
        logger.info(f"Calculated inventory changes for {len(inventory_changes)} MSKUs")
        return inventory_changes
