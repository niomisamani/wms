import pandas as pd
import logging
import yaml
import os
import json
from typing import Dict, List, Tuple, Optional, Union, Any
import sqlite3
import re
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/ai_query.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("ai_query")

class AIQueryEngine:
    """
    Class to handle AI-powered text-to-SQL queries
    Uses a rule-based approach for MVP, can be extended with ML models
    """
    
    def __init__(self, config_path: str = "config/config.yaml", db_path: str = "data/wms_database.db"):
        """
        Initialize the AI Query Engine
        
        Args:
            config_path: Path to the configuration file
            db_path: Path to the SQLite database
        """
        # Load configuration
        with open(config_path, 'r') as file:
            self.config = yaml.safe_load(file)
        
        # Store database path but don't connect yet (for thread safety)
        self.db_path = db_path
        
        # Get database schema - create a temporary connection
        self.tables = self._get_db_schema()
        
        # Define query templates
        self.query_templates = self._define_query_templates()
        
        logger.info("AI Query Engine initialized")
    
    def _get_db_schema(self) -> Dict[str, List[str]]:
        """
        Get database schema (tables and columns)
        
        Returns:
            Dict: Dictionary of tables and their columns
        """
        tables = {}
        
        try:
            # Create a new connection for this operation
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Get list of tables
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                table_names = [row[0] for row in cursor.fetchall()]
                
                # Get columns for each table
                for table in table_names:
                    cursor.execute(f"PRAGMA table_info({table});")
                    columns = [row[1] for row in cursor.fetchall()]
                    tables[table] = columns
            
            logger.info(f"Retrieved schema for {len(tables)} tables")
            return tables
        
        except Exception as e:
            logger.error(f"Error getting database schema: {str(e)}")
            return {}
    
    def _define_query_templates(self) -> Dict[str, str]:
        """
        Define query templates for common questions
        
        Returns:
            Dict: Dictionary of query templates
        """
        return {
            "inventory_levels": "SELECT msku, quantity FROM inventory ORDER BY quantity DESC;",
            "top_products": "SELECT p.msku, p.name, SUM(oi.quantity) as total_sold FROM order_items oi JOIN products p ON oi.msku = p.msku GROUP BY p.msku, p.name ORDER BY total_sold DESC LIMIT {limit};",
            "sales_by_marketplace": "SELECT o.marketplace, SUM(oi.quantity) as total_sold FROM orders o JOIN order_items oi ON o.order_id = oi.order_id GROUP BY o.marketplace ORDER BY total_sold DESC;",
            "sales_by_date": "SELECT DATE(o.order_date) as date, SUM(oi.quantity) as total_sold FROM orders o JOIN order_items oi ON o.order_id = oi.order_id GROUP BY DATE(o.order_date) ORDER BY date;",
            "sales_by_state": "SELECT o.customer_state, SUM(oi.quantity) as total_sold FROM orders o JOIN order_items oi ON o.order_id = oi.order_id GROUP BY o.customer_state ORDER BY total_sold DESC;",
            "low_stock_items": "SELECT msku, quantity FROM inventory WHERE quantity < {threshold} ORDER BY quantity ASC;",
            "sku_mappings": "SELECT sku, msku, marketplace FROM sku_mappings WHERE msku = '{msku}' OR sku = '{sku}';"
        }
    
    def process_query(self, query_text: str) -> Tuple[str, pd.DataFrame, Optional[Dict]]:
        """
        Process a natural language query and convert to SQL
        
        Args:
            query_text: Natural language query
            
        Returns:
            Tuple[str, DataFrame, Dict]: SQL query, results, and visualization config
        """
        try:
            # Normalize query text
            query_text = query_text.lower().strip()
            
            # Log the query for debugging
            logger.info(f"Processing query: {query_text}")
            
            # Match query to template
            sql_query, viz_config = self._match_query_template(query_text)
            
            if not sql_query:
                # Try rule-based parsing
                sql_query, viz_config = self._rule_based_parsing(query_text)
            
            if not sql_query:
                logger.warning(f"Could not parse query: {query_text}")
                return "", pd.DataFrame(), None
            
            # Log the generated SQL
            logger.info(f"Generated SQL: {sql_query}")
            
            # Execute query with a new connection (thread-safe)
            try:
                # Create a new connection for this query
                with sqlite3.connect(self.db_path) as conn:
                    results = pd.read_sql_query(sql_query, conn)
                    logger.info(f"Query returned {len(results)} rows")
            except Exception as e:
                logger.error(f"Error executing SQL query: {str(e)}")
                # Try a simpler query for debugging
                if "count total orders by marketplace" in query_text:
                    debug_sql = "SELECT marketplace, COUNT(*) as order_count FROM orders GROUP BY marketplace"
                    logger.info(f"Trying simplified query: {debug_sql}")
                    try:
                        with sqlite3.connect(self.db_path) as conn:
                            results = pd.read_sql_query(debug_sql, conn)
                            logger.info(f"Debug query returned {len(results)} rows")
                            return debug_sql, results, {'type': 'bar', 'x': 'marketplace', 'y': 'order_count'}
                    except Exception as e2:
                        logger.error(f"Error executing debug query: {str(e2)}")
                        # Check if the orders table exists
                        try:
                            with sqlite3.connect(self.db_path) as conn:
                                tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table'", conn)
                                logger.info(f"Available tables: {tables['name'].tolist()}")
                        except:
                            pass
                return sql_query, pd.DataFrame(), viz_config
            
            return sql_query, results, viz_config
        
        except Exception as e:
            logger.error(f"Error processing query: {str(e)}")
            return "", pd.DataFrame(), None
    
    def _match_query_template(self, query_text: str) -> Tuple[str, Optional[Dict]]:
        """
        Match query to a template
        
        Args:
            query_text: Natural language query
            
        Returns:
            Tuple[str, Dict]: SQL query and visualization config
        """
        # Check for inventory levels query
        if any(keyword in query_text for keyword in ['inventory', 'stock levels', 'current stock']):
            return self.query_templates['inventory_levels'], {'type': 'bar', 'x': 'quantity', 'y': 'msku'}
        
        # Check for top products query
        if any(keyword in query_text for keyword in ['top products', 'best selling', 'most sold']):
            limit = 10  # Default limit
            # Try to extract a number
            match = re.search(r'top (\d+)', query_text)
            if match:
                limit = int(match.group(1))
            
            query = self.query_templates['top_products'].format(limit=limit)
            return query, {'type': 'bar', 'x': 'total_sold', 'y': 'msku'}
        
        # Check for sales by marketplace query
        if any(keyword in query_text for keyword in ['sales by marketplace', 'marketplace distribution', 'platform sales']):
            return self.query_templates['sales_by_marketplace'], {'type': 'pie', 'values': 'total_sold', 'names': 'marketplace'}
        
        # Check for sales by date query
        if any(keyword in query_text for keyword in ['sales by date', 'daily sales', 'sales trend']):
            return self.query_templates['sales_by_date'], {'type': 'line', 'x': 'date', 'y': 'total_sold'}
        
        # Check for sales by state query
        if any(keyword in query_text for keyword in ['sales by state', 'state distribution', 'geographic sales']):
            return self.query_templates['sales_by_state'], {'type': 'choropleth', 'locations': 'customer_state', 'values': 'total_sold'}
        
        # Check for low stock items query
        if any(keyword in query_text for keyword in ['low stock', 'reorder', 'out of stock']):
            threshold = 10  # Default threshold
            # Try to extract a number
            match = re.search(r'less than (\d+)', query_text)
            if match:
                threshold = int(match.group(1))
            
            query = self.query_templates['low_stock_items'].format(threshold=threshold)
            return query, {'type': 'bar', 'x': 'quantity', 'y': 'msku'}
        
        # Check for SKU mappings query
        if any(keyword in query_text for keyword in ['sku mapping', 'msku', 'sku to msku']):
            msku = ""
            sku = ""
            
            # Try to extract MSKU or SKU
            msku_match = re.search(r'msku[:\s]+([A-Za-z0-9_]+)', query_text)
            if msku_match:
                msku = msku_match.group(1)
            
            sku_match = re.search(r'sku[:\s]+([A-Za-z0-9_]+)', query_text)
            if sku_match:
                sku = sku_match.group(1)
            
            query = self.query_templates['sku_mappings'].format(msku=msku, sku=sku)
            return query, {'type': 'table'}
        
        return "", None
    
    def _rule_based_parsing(self, query_text: str) -> Tuple[str, Optional[Dict]]:
        """
        Parse query using rule-based approach
        
        Args:
            query_text: Natural language query
            
        Returns:
            Tuple[str, Dict]: SQL query and visualization config
        """
        # Extract entities from query
        entities = self._extract_entities(query_text)
        
        # Determine query type
        query_type = self._determine_query_type(query_text)
        
        # Build SQL query based on type and entities
        if query_type == 'select':
            return self._build_select_query(entities), self._determine_viz_config(entities)
        elif query_type == 'aggregate':
            return self._build_aggregate_query(entities), self._determine_viz_config(entities)
        elif query_type == 'filter':
            return self._build_filter_query(entities), self._determine_viz_config(entities)
        else:
            return "", None
    
    def _extract_entities(self, query_text: str) -> Dict[str, Any]:
        """
        Extract entities from query text
        
        Args:
            query_text: Natural language query
            
        Returns:
            Dict: Extracted entities
        """
        entities = {
            'tables': [],
            'columns': [],
            'filters': [],
            'aggregations': [],
            'limit': None,
            'order_by': None,
            'order_direction': 'DESC'
        }
        
        # Extract tables
        for table in self.tables.keys():
            if table in query_text or table.rstrip('s') in query_text:
                entities['tables'].append(table)
        
        # Extract columns
        for table, columns in self.tables.items():
            for column in columns:
                if column in query_text:
                    entities['columns'].append({'table': table, 'column': column})
        
        # Extract aggregations
        if any(agg in query_text for agg in ['count', 'sum', 'average', 'total', 'mean']):
            if 'count' in query_text:
                entities['aggregations'].append({'function': 'COUNT', 'column': '*'})
            if any(agg in query_text for agg in ['sum', 'total']):
                # Find column to sum
                for col in ['quantity', 'price', 'revenue']:
                    if col in query_text:
                        entities['aggregations'].append({'function': 'SUM', 'column': col})
            if any(agg in query_text for agg in ['average', 'mean']):
                # Find column to average
                for col in ['quantity', 'price', 'revenue']:
                    if col in query_text:
                        entities['aggregations'].append({'function': 'AVG', 'column': col})
        
        # Extract filters
        # This is a simplified approach, a real implementation would be more complex
        filter_keywords = ['where', 'with', 'has', 'contains', 'greater than', 'less than', 'equal to']
        for keyword in filter_keywords:
            if keyword in query_text:
                # Extract filter condition
                parts = query_text.split(keyword)
                if len(parts) > 1:
                    filter_text = parts[1].strip()
                    entities['filters'].append(filter_text)
        
        # Extract limit
        limit_match = re.search(r'(top|first|limit) (\d+)', query_text)
        if limit_match:
            entities['limit'] = int(limit_match.group(2))
        
        # Extract order by
        order_keywords = ['order by', 'sort by', 'ranked by']
        for keyword in order_keywords:
            if keyword in query_text:
                parts = query_text.split(keyword)
                if len(parts) > 1:
                    order_text = parts[1].strip()
                    # Check for direction
                    if 'ascending' in order_text or 'asc' in order_text:
                        entities['order_direction'] = 'ASC'
                    # Find column to order by
                    for col in ['quantity', 'price', 'revenue', 'date']:
                        if col in order_text:
                            entities['order_by'] = col
        
        return entities
    
    def _determine_query_type(self, query_text: str) -> str:
        """
        Determine the type of query
        
        Args:
            query_text: Natural language query
            
        Returns:
            str: Query type (select, aggregate, filter)
        """
        # Check for aggregation keywords
        if any(keyword in query_text for keyword in ['count', 'sum', 'average', 'total', 'group by']):
            return 'aggregate'
        
        # Check for filter keywords
        if any(keyword in query_text for keyword in ['where', 'with', 'has', 'contains', 'greater than', 'less than']):
            return 'filter'
        
        # Default to select
        return 'select'
    
    def _build_select_query(self, entities: Dict[str, Any]) -> str:
        """
        Build a SELECT query
        
        Args:
            entities: Extracted entities
            
        Returns:
            str: SQL query
        """
        # Determine tables
        if not entities['tables']:
            return ""
        
        # Determine columns
        columns = []
        if entities['columns']:
            for col in entities['columns']:
                columns.append(f"{col['table']}.{col['column']}")
        else:
            # Default to all columns from first table
            columns = [f"{entities['tables'][0]}.*"]
        
        # Build query
        query = f"SELECT {', '.join(columns)} FROM {entities['tables'][0]}"
        
        # Add joins if multiple tables
        if len(entities['tables']) > 1:
            # This is a simplified approach, assumes foreign keys follow convention
            for i in range(1, len(entities['tables'])):
                table1 = entities['tables'][0]
                table2 = entities['tables'][i]
                # Try to find common column
                common_col = None
                for col in self.tables[table1]:
                    if col in self.tables[table2]:
                        common_col = col
                        break
                
                if common_col:
                    query += f" JOIN {table2} ON {table1}.{common_col} = {table2}.{common_col}"
                else:
                    # Try foreign key convention
                    fk = f"{table1.rstrip('s')}_id"
                    if fk in self.tables[table2]:
                        query += f" JOIN {table2} ON {table1}.id = {table2}.{fk}"
                    else:
                        fk = f"{table2.rstrip('s')}_id"
                        if fk in self.tables[table1]:
                            query += f" JOIN {table2} ON {table1}.{fk} = {table2}.id"
        
        # Add filters
        if entities['filters']:
            # This is a very simplified approach
            query += " WHERE " + " AND ".join(entities['filters'])
        
        # Add order by
        if entities['order_by']:
            query += f" ORDER BY {entities['order_by']} {entities['order_direction']}"
        
        # Add limit
        if entities['limit']:
            query += f" LIMIT {entities['limit']}"
        
        return query
    
    def _build_aggregate_query(self, entities: Dict[str, Any]) -> str:
        """
        Build an aggregate query
        
        Args:
            entities: Extracted entities
            
        Returns:
            str: SQL query
        """
        # Determine tables
        if not entities['tables']:
            return ""
        
        # Determine columns and aggregations
        select_parts = []
        group_by_cols = []
        
        # Add non-aggregated columns (these will be in GROUP BY)
        for col in entities['columns']:
            if not any(agg['column'] == col['column'] for agg in entities['aggregations']):
                select_parts.append(f"{col['table']}.{col['column']}")
                group_by_cols.append(f"{col['table']}.{col['column']}")
        
        # Add aggregations
        for agg in entities['aggregations']:
            if agg['column'] == '*':
                select_parts.append(f"{agg['function']}(*) as count")
            else:
                select_parts.append(f"{agg['function']}({agg['column']}) as {agg['function'].lower()}_{agg['column']}")
        
        # Build query
        if select_parts:
            query = f"SELECT {', '.join(select_parts)} FROM {entities['tables'][0]}"
        else:
            # Default to COUNT(*)
            query = f"SELECT COUNT(*) as count FROM {entities['tables'][0]}"
        
        # Add joins if multiple tables
        if len(entities['tables']) > 1:
            # Same join logic as in _build_select_query
            for i in range(1, len(entities['tables'])):
                table1 = entities['tables'][0]
                table2 = entities['tables'][i]
                # Try to find common column
                common_col = None
                for col in self.tables[table1]:
                    if col in self.tables[table2]:
                        common_col = col
                        break
                
                if common_col:
                    query += f" JOIN {table2} ON {table1}.{common_col} = {table2}.{common_col}"
                else:
                    # Try foreign key convention
                    fk = f"{table1.rstrip('s')}_id"
                    if fk in self.tables[table2]:
                        query += f" JOIN {table2} ON {table1}.id = {table2}.{fk}"
                    else:
                        fk = f"{table2.rstrip('s')}_id"
                        if fk in self.tables[table1]:
                            query += f" JOIN {table2} ON {table1}.{fk} = {table2}.id"
        
        # Add filters
        if entities['filters']:
            query += " WHERE " + " AND ".join(entities['filters'])
        
        # Add group by
        if group_by_cols:
            query += f" GROUP BY {', '.join(group_by_cols)}"
        
        # Add order by
        if entities['order_by']:
            query += f" ORDER BY {entities['order_by']} {entities['order_direction']}"
        elif entities['aggregations']:
            # Default to ordering by the first aggregation
            agg = entities['aggregations'][0]
            if agg['column'] == '*':
                query += f" ORDER BY count DESC"
            else:
                query += f" ORDER BY {agg['function'].lower()}_{agg['column']} DESC"
        
        # Add limit
        if entities['limit']:
            query += f" LIMIT {entities['limit']}"
        
        return query
    
    def _build_filter_query(self, entities: Dict[str, Any]) -> str:
        """
        Build a filter query
        
        Args:
            entities: Extracted entities
            
        Returns:
            str: SQL query
        """
        # This is essentially a select query with filters
        return self._build_select_query(entities)
    
    def _determine_viz_config(self, entities: Dict[str, Any]) -> Optional[Dict]:
        """
        Determine visualization configuration based on query entities
        
        Args:
            entities: Extracted entities
            
        Returns:
            Dict: Visualization configuration
        """
        # Default to table visualization
        viz_config = {'type': 'table'}
        
        # Check for aggregations that might be suitable for charts
        if entities['aggregations']:
            # If grouping by a single column with a count/sum, use a bar chart
            if len(entities['columns']) == 1 and len(entities['aggregations']) == 1:
                agg = entities['aggregations'][0]
                col = entities['columns'][0]
                
                if agg['function'] in ['COUNT', 'SUM']:
                    viz_config = {
                        'type': 'bar',
                        'x': col['column'],
                        'y': f"{agg['function'].lower()}_{agg['column']}" if agg['column'] != '*' else 'count'
                    }
            
            # If grouping by date with a sum/count, use a line chart
            date_cols = ['date', 'order_date', 'created_at', 'updated_at']
            if any(col['column'] in date_cols for col in entities['columns']):
                date_col = next(col['column'] for col in entities['columns'] if col['column'] in date_cols)
                agg = entities['aggregations'][0]
                
                viz_config = {
                    'type': 'line',
                    'x': date_col,
                    'y': f"{agg['function'].lower()}_{agg['column']}" if agg['column'] != '*' else 'count'
                }
            
            # If grouping by marketplace or category, use a pie chart
            category_cols = ['marketplace', 'category', 'customer_state']
            if any(col['column'] in category_cols for col in entities['columns']):
                category_col = next(col['column'] for col in entities['columns'] if col['column'] in category_cols)
                agg = entities['aggregations'][0]
                
                viz_config = {
                    'type': 'pie',
                    'names': category_col,
                    'values': f"{agg['function'].lower()}_{agg['column']}" if agg['column'] != '*' else 'count'
                }
        
        return viz_config
    
    def get_example_queries(self) -> List[str]:
        """
        Get a list of example queries
        
        Returns:
            List[str]: Example queries
        """
        return [
            "Show me the current inventory levels",
            "What are the top 10 selling products?",
            "Show sales distribution by marketplace",
            "What is the daily sales trend?",
            "Show me sales by state",
            "Which products have less than 5 items in stock?",
            "Show me the SKU mappings for MSKU: CSTE_0322_ST_Axolotl_Blue",
            "Count total orders by marketplace",
            "What is the average order quantity by product?",
            "Show me products ordered in the last 7 days"
        ]
