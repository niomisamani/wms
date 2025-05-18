import streamlit as st
import pandas as pd
import numpy as np
import os
import logging
import yaml
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json
from typing import Dict, List, Tuple, Optional, Union, Any
import time
import requests
from dotenv import load_dotenv
import sqlite3

# Import custom modules
from modules.sku_mapper import SKUMapper
from modules.data_processor import DataProcessor
from modules.database import DatabaseManager
from modules.visualizations import DashboardVisualizer
from modules.ai_query import AIQueryEngine

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/app.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("app")

# Load configuration
with open("config/config.yaml", 'r') as file:
    config = yaml.safe_load(file)

# Initialize session state
if 'initialized' not in st.session_state:
    st.session_state.initialized = False
    st.session_state.current_page = "dashboard"
    st.session_state.processed_data = None
    st.session_state.unmapped_skus = []
    st.session_state.marketplace_options = ["amazon", "flipkart", "meesho", "unknown"]
    st.session_state.uploaded_files = []
    st.session_state.processed_files = []
    st.session_state.combined_data = None
    st.session_state.inventory_data = None
    st.session_state.metrics = None
    st.session_state.query_results = None
    st.session_state.query_sql = ""
    st.session_state.viz_config = None
    st.session_state.import_status = None

# Initialize components
@st.cache_resource
def initialize_components():
    """Initialize all components"""
    try:
        sku_mapper = SKUMapper()
        data_processor = DataProcessor(sku_mapper=sku_mapper)
        db_manager = DatabaseManager()
        visualizer = DashboardVisualizer()
        ai_query = AIQueryEngine()
        
        return sku_mapper, data_processor, db_manager, visualizer, ai_query
    except Exception as e:
        logger.error(f"Error initializing components: {str(e)}")
        st.error(f"Error initializing components: {str(e)}")
        # Return None values to prevent further errors
        return None, None, None, None, None

# Get components
sku_mapper, data_processor, db_manager, visualizer, ai_query = initialize_components()
st.session_state.initialized = True

# App title and navigation
st.title("Warehouse Management System")

# Sidebar navigation
with st.sidebar:
    st.title("Navigation")
    
    # Navigation buttons
    if st.button("Dashboard", use_container_width=True):
        st.session_state.current_page = "dashboard"
    
    if st.button("Data Upload", use_container_width=True):
        st.session_state.current_page = "data_upload"
    
    if st.button("SKU Mapping", use_container_width=True):
        st.session_state.current_page = "sku_mapping"
    
    # if  (use_container_width=True):
    #     st.session_state.current_page = "sku_mapping"
    
    if st.button("Inventory Management", use_container_width=True):
        st.session_state.current_page = "inventory"
    
    if st.button("AI Query", use_container_width=True):
        st.session_state.current_page = "ai_query"
    
    st.divider()
    
    # Display app info
    st.info("WMS MVP v1.0")
    st.caption("Built with Streamlit and Python")

# Dashboard page
def show_dashboard():
    """Show the dashboard page"""
    st.header("Dashboard")
    
    # Check if we have data
    if st.session_state.combined_data is None:
        st.info("No data available. Please upload and process data files first.")
        
        # Show sample data button
        if st.button("Load Sample Data"):
            load_sample_data()
        
        return
    
    # Display key metrics
    if st.session_state.metrics is None:
        # Calculate metrics
        st.session_state.metrics = visualizer.create_dashboard_metrics(st.session_state.combined_data)
    
    # Create metrics row
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Orders", f"{st.session_state.metrics['total_orders']:,}")
    
    with col2:
        st.metric("Units Sold", f"{st.session_state.metrics['total_units_sold']:,}")
    
    with col3:
        st.metric("Total Revenue", f"₹{st.session_state.metrics['total_revenue']:,.2f}")
    
    with col4:
        st.metric("Avg Order Value", f"₹{st.session_state.metrics['avg_order_value']:,.2f}")
    
    # Create second metrics row
    col1, col2 = st.columns(2)
    
    with col1:
        st.metric("Top Selling Product", st.session_state.metrics['top_selling_product'])
    
    with col2:
        st.metric("Top Marketplace", st.session_state.metrics['top_marketplace'])
    
    # Create charts
    st.subheader("Sales Trend")
    sales_trend_chart = visualizer.create_sales_trend_chart(st.session_state.combined_data)
    st.plotly_chart(sales_trend_chart, use_container_width=True)
    
    # Create two columns for charts
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Marketplace Distribution")
        marketplace_chart = visualizer.create_marketplace_distribution_chart(st.session_state.combined_data)
        st.plotly_chart(marketplace_chart, use_container_width=True)
    
    with col2:
        st.subheader("Top Products")
        top_products_chart = visualizer.create_top_products_chart(st.session_state.combined_data)
        st.plotly_chart(top_products_chart, use_container_width=True)
    
    # Inventory levels
    st.subheader("Current Inventory Levels")
    if st.session_state.inventory_data is not None:
        inventory_chart = visualizer.create_inventory_chart(st.session_state.inventory_data)
        st.plotly_chart(inventory_chart, use_container_width=True)
    else:
        st.info("No inventory data available.")

# Data Upload page
def show_data_upload():
    """Show the data upload page"""
    st.header("Data Upload")
    
    # File upload section
    st.subheader("Upload Sales Data")
    
    uploaded_files = st.file_uploader(
        "Upload CSV or Excel files from marketplaces", 
        type=["csv", "xlsx", "xls"],
        accept_multiple_files=True
    )
    
    # Process uploaded files
    if uploaded_files:
        st.session_state.uploaded_files = uploaded_files
        
        # Process button
        if st.button("Process Files"):
            with st.spinner("Processing files..."):
                processed_files = []
                all_data = []
                
                for uploaded_file in uploaded_files:
                    # Save file
                    marketplace = "unknown"  # Will be detected during processing
                    file_path = data_processor.save_uploaded_file(uploaded_file, marketplace)
                    
                    # Process file
                    df, detected_marketplace = data_processor.process_file(file_path)
                    
                    if not df.empty:
                        processed_files.append(file_path)
                        all_data.append(df)
                        
                        # Show success message
                        st.success(f"Processed {uploaded_file.name} ({detected_marketplace})")
                    else:
                        st.error(f"Failed to process {uploaded_file.name}")
                
                # Store processed files
                st.session_state.processed_files = processed_files
                
                # Combine data if we have multiple files
                if len(all_data) > 0:
                    combined_df = pd.concat(all_data, ignore_index=True)
                    st.session_state.combined_data = combined_df
                    
                    # Calculate inventory changes
                    inventory_changes = data_processor.calculate_inventory_changes(combined_df)
                    st.session_state.inventory_data = inventory_changes
                    
                    # Get unmapped SKUs
                    unmapped_skus = data_processor.get_unmapped_skus(combined_df)
                    st.session_state.unmapped_skus = unmapped_skus
                    
                    # Calculate metrics
                    st.session_state.metrics = visualizer.create_dashboard_metrics(combined_df)
                    
                    # Show success message
                    st.success(f"Successfully processed {len(all_data)} files with {len(combined_df)} rows")
                    
                    # Show preview
                    st.subheader("Data Preview")
                    st.dataframe(combined_df.head(10))
                    
                    # Show unmapped SKUs
                    if unmapped_skus:
                        st.warning(f"Found {len(unmapped_skus)} unmapped SKUs. Please map them in the SKU Mapping page.")
    
    # Show processed files
    if st.session_state.processed_files:
        st.subheader("Processed Files")
        for file in st.session_state.processed_files:
            st.text(os.path.basename(file))
    
    # Show combined data preview
    if st.session_state.combined_data is not None:
        st.subheader("Combined Data Preview")
        st.dataframe(st.session_state.combined_data.head(10))
        
        # Download button for combined data
        csv = st.session_state.combined_data.to_csv(index=False)
        st.download_button(
            label="Download Combined Data",
            data=csv,
            file_name="combined_data.csv",
            mime="text/csv"
        )

# SKU Mapping page
def show_sku_mapping():
    """Show the SKU mapping page"""
    st.header("SKU Mapping")
    
    # Tabs for different mapping operations
    tab1, tab2, tab3, tab4 = st.tabs(["Map SKUs", "View Mappings", "Import/Export", "Unmapped SKUs"])
    
    # Tab 1: Map SKUs
    with tab1:
        st.subheader("Map SKU to MSKU")
        
        # Form for mapping
        with st.form("sku_mapping_form"):
            sku = st.text_input("SKU")
            msku = st.text_input("MSKU")
            marketplace = st.selectbox("Marketplace", st.session_state.marketplace_options)
            
            submitted = st.form_submit_button("Add Mapping")
            
            if submitted and sku and msku:
                # Add mapping
                success = sku_mapper.add_mapping(sku, msku, marketplace)
                
                if success:
                    st.success(f"Added mapping: {sku} -> {msku} ({marketplace})")
                else:
                    st.error("Failed to add mapping")
    
    # Tab 2: View Mappings
    with tab2:
        st.subheader("View SKU Mappings")
        
        # Get all mappings
        mappings_df = sku_mapper.get_all_mappings()
        
        if not mappings_df.empty:
            # Add search filter
            search = st.text_input("Search SKU or MSKU")
            
            if search:
                filtered_df = mappings_df[
                    mappings_df['SKU'].str.contains(search, case=False) | 
                    mappings_df['MSKU'].str.contains(search, case=False)
                ]
                st.dataframe(filtered_df)
            else:
                st.dataframe(mappings_df)
            
            # Show total count
            st.info(f"Total mappings: {len(mappings_df)}")
        else:
            st.info("No mappings found")
    
    # Tab 3: Import/Export
    with tab3:
        st.subheader("Import/Export Mappings")
        
        # Import section
        st.markdown("### Import Mappings")
        
        uploaded_file = st.file_uploader(
            "Upload CSV file with mappings (columns: SKU, MSKU, Marketplace)", 
            type=["csv"],
            key="sku_mapping_upload"
        )
        
        if uploaded_file:
            # Display file preview
            try:
                preview_df = pd.read_csv(uploaded_file)
                st.subheader("File Preview")
                st.dataframe(preview_df.head(5))
                
                # Reset file pointer for later use
                uploaded_file.seek(0)
            except Exception as e:
                st.error(f"Error previewing file: {str(e)}")
            
            if st.button("Import Mappings"):
                with st.spinner("Importing mappings..."):
                    try:
                        # Create mappings directory if it doesn't exist
                        os.makedirs(os.path.join("data", "mappings"), exist_ok=True)
                        
                        # Save file
                        file_path = os.path.join("data", "mappings", uploaded_file.name)
                        with open(file_path, "wb") as f:
                            f.write(uploaded_file.getbuffer())
                        
                        # Import mappings
                        success_count, fail_count = sku_mapper.bulk_import_mappings(file_path)
                        
                        # Store import status in session state
                        st.session_state.import_status = {
                            "success": success_count,
                            "fail": fail_count
                        }
                        
                        if success_count > 0:
                            st.success(f"Imported {success_count} mappings successfully")
                        
                        if fail_count > 0:
                            st.warning(f"Failed to import {fail_count} mappings")
                    except Exception as e:
                        st.error(f"Error importing mappings: {str(e)}")
        
        # Show import status if available
        if st.session_state.import_status:
            st.info(f"Last import: {st.session_state.import_status['success']} successful, {st.session_state.import_status['fail']} failed")
        
        # Export section
        st.markdown("### Export Mappings")
        
        if st.button("Export Mappings"):
            # Get all mappings
            mappings_df = sku_mapper.get_all_mappings()
            
            if not mappings_df.empty:
                # Convert to CSV
                csv = mappings_df.to_csv(index=False)
                
                # Download button
                st.download_button(
                    label="Download Mappings CSV",
                    data=csv,
                    file_name="sku_mappings.csv",
                    mime="text/csv"
                )
            else:
                st.info("No mappings to export")
    
    # Tab 4: Unmapped SKUs
    with tab4:
        st.subheader("Unmapped SKUs")
        
        if st.session_state.unmapped_skus:
            # Convert to DataFrame for better display
            unmapped_df = pd.DataFrame(st.session_state.unmapped_skus, columns=["SKU"])
            
            # Add marketplace detection
            unmapped_df["Detected Marketplace"] = unmapped_df["SKU"].apply(
                lambda x: sku_mapper.identify_marketplace(x) or "unknown"
            )
            
            # Add MSKU input column
            unmapped_df["MSKU"] = ""
            
            st.dataframe(unmapped_df)
            
            # Show total count
            st.info(f"Total unmapped SKUs: {len(st.session_state.unmapped_skus)}")
            
            # Bulk mapping form
            st.markdown("### Map Selected SKU")
            
            with st.form("bulk_mapping_form"):
                sku_to_map = st.selectbox("Select SKU to map", st.session_state.unmapped_skus)
                msku = st.text_input("MSKU for selected SKU")
                marketplace = st.selectbox(
                    "Marketplace", 
                    st.session_state.marketplace_options,
                    index=st.session_state.marketplace_options.index(
                        sku_mapper.identify_marketplace(sku_to_map) or "unknown"
                    ) if sku_to_map else 0
                )
                
                submitted = st.form_submit_button("Map Selected SKU")
                
                if submitted and sku_to_map and msku:
                    # Add mapping
                    success = sku_mapper.add_mapping(sku_to_map, msku, marketplace)
                    
                    if success:
                        st.success(f"Added mapping: {sku_to_map} -> {msku} ({marketplace})")
                        # Remove from unmapped list
                        st.session_state.unmapped_skus.remove(sku_to_map)
                        # Force rerun to update the UI
                        st.experimental_rerun()
                    else:
                        st.error("Failed to add mapping")
        else:
            st.info("No unmapped SKUs found")

# Inventory Management page
def show_inventory():
    """Show the inventory management page"""
    st.header("Inventory Management")
    
    # Check if we have inventory data
    if st.session_state.inventory_data is None:
        st.info("No inventory data available. Please upload and process data files first.")
        
        # Try to load inventory data from the database
        try:
            with sqlite3.connect(os.path.join('data', 'wms_database.db')) as conn:
                inventory_df = pd.read_sql_query("SELECT * FROM inventory", conn)
                if not inventory_df.empty:
                    st.session_state.inventory_data = inventory_df
                    st.success("Loaded inventory data from database")
                    st.experimental_rerun()
        except Exception as e:
            logger.error(f"Error loading inventory data from database: {str(e)}")
        
        return
    
    # Display inventory data
    st.subheader("Current Inventory")
    
    # Add search filter
    search = st.text_input("Search MSKU")
    
    if search:
        filtered_df = st.session_state.inventory_data[
            st.session_state.inventory_data['msku'].str.contains(search, case=False)
        ]
        st.dataframe(filtered_df)
    else:
        st.dataframe(st.session_state.inventory_data)
    
    # Show inventory chart
    inventory_chart = visualizer.create_inventory_chart(st.session_state.inventory_data)
    st.plotly_chart(inventory_chart, use_container_width=True)
    
    # Low stock items
    st.subheader("Low Stock Items")
    
    threshold = st.slider("Low Stock Threshold", 1, 50, 10)
    
    # Check if inventory data has the required column
    if st.session_state.inventory_data is not None and 'quantity' in st.session_state.inventory_data.columns:
        low_stock = st.session_state.inventory_data[
            st.session_state.inventory_data['quantity'] <= threshold
        ].sort_values('quantity')
        
        if not low_stock.empty:
            st.dataframe(low_stock)
            
            # Show count
            st.warning(f"Found {len(low_stock)} items with stock level below {threshold}")
        else:
            st.success(f"No items with stock level below {threshold}")
    else:
        st.error("Inventory data is missing the 'quantity' column. Please check your data processing.")
    
    # Manual inventory adjustment
    st.subheader("Manual Inventory Adjustment")
    
    with st.form("inventory_adjustment_form"):
        msku = st.text_input("MSKU")
        quantity_change = st.number_input("Quantity Change", value=0, step=1)
        reason = st.text_input("Reason for Adjustment")
        
        submitted = st.form_submit_button("Adjust Inventory")
        
        if submitted and msku and quantity_change != 0:
            # Check if MSKU exists
            if msku in st.session_state.inventory_data['msku'].values:
                # Update inventory
                idx = st.session_state.inventory_data.index[
                    st.session_state.inventory_data['msku'] == msku
                ].tolist()[0]
                
                st.session_state.inventory_data.at[idx, 'quantity'] += quantity_change
                
                st.success(f"Adjusted inventory for {msku} by {quantity_change}")
            else:
                # Add new inventory item
                new_row = pd.DataFrame({
                    'msku': [msku],
                    'quantity': [quantity_change]
                })
                
                st.session_state.inventory_data = pd.concat(
                    [st.session_state.inventory_data, new_row],
                    ignore_index=True
                )
                
                st.success(f"Added new inventory item {msku} with quantity {quantity_change}")

# AI Query page
def show_ai_query():
    """Show the AI Query page"""
    st.header("AI Query")
    
    # Check if we have data
    if st.session_state.combined_data is None:
        st.info("No data available. Please upload and process data files first.")
        return
    
    # Check if Gemini API key is configured
    gemini_api_key = os.environ.get("GEMINI_API_KEY")
    if gemini_api_key:
        st.success("Google Gemini API is configured and ready to use!")
    else:
        st.warning("Google Gemini API key not found. Using rule-based fallback method.")
        st.info("To use Gemini API, add GEMINI_API_KEY to your .env file.")
    
    # Show example queries
    with st.expander("Example Queries"):
        examples = ai_query.get_example_queries()
        for example in examples:
            st.markdown(f"- {example}")
    
    # Query input
    query_text = st.text_input("Enter your query in natural language")
    
    if query_text:
        with st.spinner("Processing query..."):
            # Process query
            sql_query, results, viz_config = ai_query.process_query(query_text)
            
            # Store results
            st.session_state.query_results = results
            st.session_state.query_sql = sql_query
            st.session_state.viz_config = viz_config
    
    # Display results
    if st.session_state.query_results is not None:
        # Show SQL query
        with st.expander("SQL Query"):
            st.code(st.session_state.query_sql, language="sql")
        
        # Show results
        st.subheader("Query Results")
        
        if not st.session_state.query_results.empty:
            st.dataframe(st.session_state.query_results)
            
            # Show visualization if available
            if st.session_state.viz_config:
                st.subheader("Visualization")
                
                viz_type = st.session_state.viz_config.get('type', 'table')
                
                if viz_type == 'bar':
                    # Create bar chart
                    fig = px.bar(
                        st.session_state.query_results,
                        x=st.session_state.viz_config.get('x'),
                        y=st.session_state.viz_config.get('y'),
                        title="Query Results"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                elif viz_type == 'line':
                    # Create line chart
                    fig = px.line(
                        st.session_state.query_results,
                        x=st.session_state.viz_config.get('x'),
                        y=st.session_state.viz_config.get('y'),
                        title="Query Results",
                        markers=True
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                elif viz_type == 'pie':
                    # Create pie chart
                    fig = px.pie(
                        st.session_state.query_results,
                        names=st.session_state.viz_config.get('names'),
                        values=st.session_state.viz_config.get('values'),
                        title="Query Results"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                elif viz_type == 'choropleth':
                    # Create choropleth map
                    fig = px.choropleth(
                        st.session_state.query_results,
                        locations=st.session_state.viz_config.get('locations'),
                        locationmode='country names',
                        color=st.session_state.viz_config.get('values'),
                        scope='asia',
                        title="Query Results"
                    )
                    st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No results found for your query")

# Load sample data for testing
def load_sample_data():
    """Load sample data for testing"""
    with st.spinner("Loading sample data..."):
        # Create sample data directory if it doesn't exist
        os.makedirs("data/raw", exist_ok=True)
        
        # Create sample data
        # Amazon data
        amazon_data = pd.DataFrame({
            'date': pd.date_range(start='2025-01-01', periods=30),
            'msku': np.random.choice(['CSTE_0322_ST_Axolotl_Blue', 'CSTE_O0390_OT_Toiletry_Bottles', 'CSTE_0321_ST_Axolotl_Pink'], 30),
            'quantity': np.random.randint(-5, 10, 30),
            'marketplace': 'amazon'
        })
        
        # Add order_id
        amazon_data['order_id'] = [f"AMZ{i:06d}" for i in range(1, 31)]
        
        # Add product_name
        product_names = {
            'CSTE_0322_ST_Axolotl_Blue': 'Axolotl Plush Toy Blue',
            'CSTE_O0390_OT_Toiletry_Bottles': 'Travel Toiletry Bottles',
            'CSTE_0321_ST_Axolotl_Pink': 'Axolotl Plush Toy Pink'
        }
        amazon_data['product_name'] = amazon_data['msku'].map(product_names)
        
        # Flipkart data
        flipkart_data = pd.DataFrame({
            'order_date': pd.date_range(start='2025-01-05', periods=20),
            'sku': np.random.choice(['thug_thin', 'CSTE_O0390_OT_Toiletry_Bottles', 'CSTE_0321_ST_Axolotl_Pink'], 20),
            'quantity': np.random.randint(1, 5, 20),
            'marketplace': 'flipkart'
        })
        
        # Add msku_validated
        sku_to_msku = {
            'thug_thin': 'GLGL_0001_GL_Thug_Thin',
            'CSTE_O0390_OT_Toiletry_Bottles': 'CSTE_O0390_OT_Toiletry_Bottles',
            'CSTE_0321_ST_Axolotl_Pink': 'CSTE_0321_ST_Axolotl_Pink'
        }
        flipkart_data['msku_validated'] = flipkart_data['sku'].map(sku_to_msku)
        
        # Add order_id
        flipkart_data['order_id'] = [f"FK{i:06d}" for i in range(1, 21)]
        
        # Add product_name
        product_names.update({
            'GLGL_0001_GL_Thug_Thin': 'Thug Life Sunglasses'
        })
        flipkart_data['product_name'] = flipkart_data['msku_validated'].map(product_names)
        
        # Meesho data
        meesho_data = pd.DataFrame({
            'order_date': pd.date_range(start='2025-01-10', periods=15),
            'sku': np.random.choice(['CSTE_0322_ST_Axolotl_Blue', 'GLGL_0001_GL_Thug_Thin'], 15),
            'quantity': np.random.randint(1, 3, 15),
            'marketplace': 'meesho'
        })
        
        # Add msku_validated
        meesho_data['msku_validated'] = meesho_data['sku']
        
        # Add order_id
        meesho_data['order_id'] = [f"ME{i:06d}" for i in range(1, 16)]
        
        # Add product_name
        meesho_data['product_name'] = meesho_data['msku_validated'].map(product_names)
        
        # Combine data
        combined_data = pd.concat([amazon_data, flipkart_data, meesho_data], ignore_index=True)
        
        # Add customer_state
        states = ['Maharashtra', 'Karnataka', 'Tamil Nadu', 'Delhi', 'Uttar Pradesh', 
                 'Gujarat', 'West Bengal', 'Telangana', 'Kerala', 'Punjab']
        combined_data['customer_state'] = np.random.choice(states, len(combined_data))
        
        # Add price
        combined_data['price'] = np.random.randint(100, 1000, len(combined_data))
        
        # Calculate inventory
        inventory_data = combined_data.groupby('msku_validated')['quantity'].sum().reset_index()
        inventory_data.columns = ['msku', 'quantity']
        
        # Store data
        st.session_state.combined_data = combined_data
        st.session_state.inventory_data = inventory_data
        
        # Calculate metrics
        st.session_state.metrics = visualizer.create_dashboard_metrics(combined_data)
        
        # Show success message
        st.success("Sample data loaded successfully")
        
        # Force rerun to update the UI
        time.sleep(1)
        st.experimental_rerun()

# Function to download a file from a URL
def download_file_from_url(url, local_path):
    """
    Download a file from a URL and save it locally
    
    Args:
        url: URL of the file to download
        local_path: Local path to save the file
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        
        # Save file
        with open(local_path, 'wb') as f:
            f.write(response.content)
        
        logger.info(f"Downloaded file from {url} to {local_path}")
        return True
    except Exception as e:
        logger.error(f"Error downloading file from {url}: {str(e)}")
        return False

# Show the current page
if st.session_state.current_page == "dashboard":
    show_dashboard()
elif st.session_state.current_page == "data_upload":
    show_data_upload()
elif st.session_state.current_page == "sku_mapping":
    show_sku_mapping()
elif st.session_state.current_page == "inventory":
    show_inventory()
elif st.session_state.current_page == "ai_query":
    show_ai_query()
