import pandas as pd
import re
import logging
import os
import yaml
from typing import Dict, List, Tuple, Optional, Set, Union
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/sku_mapper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("sku_mapper")

class SKUMapper:
    """
    Class to handle mapping of SKUs to Master SKUs (MSKUs)
    Supports single SKUs and combo products
    """
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """
        Initialize the SKU Mapper with configuration
        
        Args:
            config_path: Path to the configuration file
        """
        # Load configuration
        with open(config_path, 'r') as file:
            self.config = yaml.safe_load(file)
        
        # Initialize mappings dictionary
        self.mappings = {}
        self.reverse_mappings = {}  # MSKU to SKU mappings
        
        # Load SKU patterns for different marketplaces
        self.sku_patterns = self.config['sku_mapping']['patterns']
        
        # Set combo separator
        self.combo_separator = self.config['sku_mapping']['combo_separator']
        
        # Load existing mappings if available
        self.mappings_path = self.config['paths']['mappings']
        self._load_mappings()
        
        logger.info("SKU Mapper initialized successfully")
    
    def _load_mappings(self) -> None:
        """Load existing SKU to MSKU mappings from file"""
        mapping_file = os.path.join(self.mappings_path, "sku_mappings.csv")
        
        if os.path.exists(mapping_file):
            try:
                df = pd.read_csv(mapping_file)
                # Create mappings dictionary
                for _, row in df.iterrows():
                    sku = row['SKU']
                    msku = row['MSKU']
                    marketplace = row['Marketplace']
                    
                    self.mappings[sku] = {
                        'msku': msku,
                        'marketplace': marketplace
                    }
                    
                    # Create reverse mappings
                    if msku not in self.reverse_mappings:
                        self.reverse_mappings[msku] = []
                    self.reverse_mappings[msku].append({
                        'sku': sku,
                        'marketplace': marketplace
                    })
                
                logger.info(f"Loaded {len(self.mappings)} SKU mappings from file")
            except Exception as e:
                logger.error(f"Error loading mappings: {str(e)}")
                # Initialize empty mappings
                self.mappings = {}
                self.reverse_mappings = {}
        else:
            logger.warning(f"Mapping file {mapping_file} not found. Starting with empty mappings.")
            # Create directory if it doesn't exist
            os.makedirs(self.mappings_path, exist_ok=True)
    
    def save_mappings(self) -> None:
        """Save current mappings to file"""
        mapping_file = os.path.join(self.mappings_path, "sku_mappings.csv")
        
        # Create dataframe from mappings
        data = []
        for sku, mapping in self.mappings.items():
            data.append({
                'SKU': sku,
                'MSKU': mapping['msku'],
                'Marketplace': mapping['marketplace']
            })
        
        df = pd.DataFrame(data)
        
        try:
            df.to_csv(mapping_file, index=False)
            logger.info(f"Saved {len(self.mappings)} SKU mappings to file")
        except Exception as e:
            logger.error(f"Error saving mappings: {str(e)}")
    
    def add_mapping(self, sku: str, msku: str, marketplace: str) -> bool:
        """
        Add a new SKU to MSKU mapping
        
        Args:
            sku: The SKU to map
            msku: The Master SKU to map to
            marketplace: The marketplace the SKU belongs to
            
        Returns:
            bool: True if mapping was added successfully, False otherwise
        """
        try:
            # Validate SKU format based on marketplace
            if marketplace in self.sku_patterns:
                pattern = self.sku_patterns[marketplace]
                if not re.match(pattern, sku):
                    logger.warning(f"SKU {sku} does not match pattern for {marketplace}")
                    # Continue anyway, just log the warning
            
            # Add mapping
            self.mappings[sku] = {
                'msku': msku,
                'marketplace': marketplace
            }
            
            # Update reverse mappings
            if msku not in self.reverse_mappings:
                self.reverse_mappings[msku] = []
            self.reverse_mappings[msku].append({
                'sku': sku,
                'marketplace': marketplace
            })
            
            # Save mappings to file
            self.save_mappings()
            
            logger.info(f"Added mapping: {sku} -> {msku} ({marketplace})")
            return True
        
        except Exception as e:
            logger.error(f"Error adding mapping: {str(e)}")
            return False
    
    def get_msku(self, sku: str) -> Optional[str]:
        """
        Get the MSKU for a given SKU
        
        Args:
            sku: The SKU to look up
            
        Returns:
            str: The MSKU if found, None otherwise
        """
        if sku in self.mappings:
            return self.mappings[sku]['msku']
        
        # Check if this is a combo product
        if self.combo_separator in sku:
            return self._handle_combo_sku(sku)
        
        logger.warning(f"No mapping found for SKU: {sku}")
        return None
    
    def _handle_combo_sku(self, combo_sku: str) -> Optional[str]:
        """
        Handle combo SKUs by splitting and mapping individual SKUs
        
        Args:
            combo_sku: The combo SKU string (e.g., "SKU1+SKU2+SKU3")
            
        Returns:
            str: Combined MSKUs if all mappings found, None otherwise
        """
        # Split the combo SKU
        skus = combo_sku.split(self.combo_separator)
        
        # Get MSKU for each SKU
        mskus = []
        for sku in skus:
            sku = sku.strip()
            if sku in self.mappings:
                mskus.append(self.mappings[sku]['msku'])
            else:
                logger.warning(f"No mapping found for SKU {sku} in combo {combo_sku}")
                return None
        
        # Combine MSKUs with the same separator
        combined_msku = self.combo_separator.join(mskus)
        logger.info(f"Mapped combo SKU {combo_sku} to {combined_msku}")
        
        return combined_msku
    
    def get_skus_for_msku(self, msku: str) -> List[Dict[str, str]]:
        """
        Get all SKUs mapped to a given MSKU
        
        Args:
            msku: The MSKU to look up
            
        Returns:
            List[Dict]: List of SKUs with their marketplaces
        """
        if msku in self.reverse_mappings:
            return self.reverse_mappings[msku]
        
        logger.warning(f"No SKUs found for MSKU: {msku}")
        return []
    
    def delete_mapping(self, sku: str) -> bool:
        """
        Delete a SKU mapping
        
        Args:
            sku: The SKU to delete
            
        Returns:
            bool: True if mapping was deleted successfully, False otherwise
        """
        if sku in self.mappings:
            msku = self.mappings[sku]['msku']
            
            # Remove from reverse mappings
            if msku in self.reverse_mappings:
                self.reverse_mappings[msku] = [
                    item for item in self.reverse_mappings[msku] 
                    if item['sku'] != sku
                ]
                
                # Remove empty lists
                if not self.reverse_mappings[msku]:
                    del self.reverse_mappings[msku]
            
            # Remove from mappings
            del self.mappings[sku]
            
            # Save mappings to file
            self.save_mappings()
            
            logger.info(f"Deleted mapping for SKU: {sku}")
            return True
        
        logger.warning(f"No mapping found for SKU: {sku}")
        return False
    
    def update_mapping(self, sku: str, msku: str, marketplace: str) -> bool:
        """
        Update an existing SKU mapping
        
        Args:
            sku: The SKU to update
            msku: The new MSKU
            marketplace: The marketplace
            
        Returns:
            bool: True if mapping was updated successfully, False otherwise
        """
        # Delete existing mapping
        if sku in self.mappings:
            self.delete_mapping(sku)
        
        # Add new mapping
        return self.add_mapping(sku, msku, marketplace)
    
    def get_all_mappings(self) -> pd.DataFrame:
        """
        Get all SKU to MSKU mappings as a DataFrame
        
        Returns:
            DataFrame: All mappings
        """
        data = []
        for sku, mapping in self.mappings.items():
            data.append({
                'SKU': sku,
                'MSKU': mapping['msku'],
                'Marketplace': mapping['marketplace']
            })
        
        return pd.DataFrame(data)
    
    def identify_marketplace(self, sku: str) -> Optional[str]:
        """
        Identify the marketplace a SKU belongs to based on its format
        
        Args:
            sku: The SKU to identify
            
        Returns:
            str: The marketplace name if identified, None otherwise
        """
        for marketplace, pattern in self.sku_patterns.items():
            if re.match(pattern, sku):
                return marketplace
        
        return None
    
    def bulk_import_mappings(self, file_path: str) -> Tuple[int, int]:
        """
        Import SKU to MSKU mappings from a CSV file
        
        Args:
            file_path: Path to the CSV file
            
        Returns:
            Tuple[int, int]: (Number of successful imports, Number of failed imports)
        """
        try:
            df = pd.read_csv(file_path)
            
            # Check required columns
            required_cols = ['SKU', 'MSKU', 'Marketplace']
            if not all(col in df.columns for col in required_cols):
                logger.error(f"Import file missing required columns: {required_cols}")
                return 0, 0
            
            success_count = 0
            fail_count = 0
            
            for _, row in df.iterrows():
                sku = str(row['SKU'])
                msku = str(row['MSKU'])
                marketplace = str(row['Marketplace'])
                
                if self.add_mapping(sku, msku, marketplace):
                    success_count += 1
                else:
                    fail_count += 1
            
            logger.info(f"Bulk import: {success_count} successful, {fail_count} failed")
            return success_count, fail_count
        
        except Exception as e:
            logger.error(f"Error during bulk import: {str(e)}")
            return 0, 0
    
    def export_mappings(self, file_path: str) -> bool:
        """
        Export all mappings to a CSV file
        
        Args:
            file_path: Path to save the CSV file
            
        Returns:
            bool: True if export was successful, False otherwise
        """
        try:
            df = self.get_all_mappings()
            df.to_csv(file_path, index=False)
            logger.info(f"Exported {len(df)} mappings to {file_path}")
            return True
        
        except Exception as e:
            logger.error(f"Error exporting mappings: {str(e)}")
            return False
