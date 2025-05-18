import logging
import json
import os
import requests
from typing import Dict, List, Tuple, Optional, Union, Any
import pandas as pd
import dotenv

# Load environment variables
dotenv.load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/gemini_query.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("gemini_query")

class GeminiQueryEngine:
    """
    Class to handle AI-powered text-to-SQL queries using Google's Gemini API
    """
    
    def __init__(self, db_schema: Dict[str, List[str]]):
        """
        Initialize the Gemini Query Engine
        
        Args:
            db_schema: Dictionary of tables and their columns
        """
        # Get API key from environment variables
        self.api_key = os.environ.get("GEMINI_API_KEY")
        if not self.api_key:
            logger.error("Gemini API key not found in environment variables")
            raise ValueError("Gemini API key not found in environment variables")
        
        # Set API endpoint
        self.api_url = "https://generativelanguage.googleapis.com/v1beta/models/1.5-flash:generateContent"
        
        # Store database schema
        self.db_schema = db_schema
        
        logger.info("Gemini Query Engine initialized")
    
    def generate_sql(self, query_text: str) -> Tuple[str, Optional[Dict]]:
        """
        Generate SQL from natural language query using Gemini API
        
        Args:
            query_text: Natural language query
            
        Returns:
            Tuple[str, Dict]: SQL query and visualization config
        """
        try:
            # Format schema for prompt
            schema_text = self._format_schema_for_prompt()
            
            # Create prompt for Gemini
            prompt = f"""
            You are a SQL expert. Convert the following natural language query to a valid SQLite SQL query.
            
            Database Schema:
            {schema_text}
            
            Natural Language Query: {query_text}
            
            Return your response in the following JSON format:
            {{
                "sql_query": "The SQL query",
                "visualization": {{
                    "type": "bar|line|pie|table",
                    "x_column": "column name for x-axis if applicable",
                    "y_column": "column name for y-axis if applicable",
                    "values_column": "column name for values if applicable",
                    "names_column": "column name for names if applicable"
                }}
            }}
            
            Choose the most appropriate visualization type based on the query:
            - bar: for comparing categories
            - line: for time series data
            - pie: for showing proportions
            - table: for raw data or complex results
            
            Only include the JSON in your response, nothing else.
            """
            
            # Prepare request payload
            payload = {
                "contents": [{
                    "parts": [{
                        "text": prompt
                    }]
                }],
                "generationConfig": {
                    "temperature": 0.2,
                    "topP": 0.8,
                    "topK": 40,
                    "maxOutputTokens": 1024
                }
            }
            
            # Add API key to URL
            url_with_key = f"{self.api_url}?key={self.api_key}"
            
            # Make request to Gemini API
            headers = {
                "Content-Type": "application/json"
            }
            
            response = requests.post(url_with_key, json=payload, headers=headers)
            response.raise_for_status()
            
            # Parse response
            response_data = response.json()
            
            # Extract text from response
            if 'candidates' in response_data and len(response_data['candidates']) > 0:
                if 'content' in response_data['candidates'][0]:
                    content = response_data['candidates'][0]['content']
                    if 'parts' in content and len(content['parts']) > 0:
                        result_text = content['parts'][0]['text']
                        
                        # Parse JSON from response
                        try:
                            result_json = json.loads(result_text)
                            sql_query = result_json.get('sql_query', '')
                            viz_config = result_json.get('visualization', {})
                            
                            logger.info(f"Generated SQL query: {sql_query}")
                            return sql_query, viz_config
                        except json.JSONDecodeError:
                            # If JSON parsing fails, try to extract SQL directly
                            logger.warning("Failed to parse JSON from Gemini response")
                            sql_match = result_text.split("```sql")[1].split("```")[0].strip() if "```sql" in result_text else ""
                            if sql_match:
                                logger.info(f"Extracted SQL query from markdown: {sql_match}")
                                return sql_match, {"type": "table"}
            
            logger.error("Failed to extract SQL from Gemini response")
            return "", None
            
        except Exception as e:
            logger.error(f"Error generating SQL with Gemini: {str(e)}")
            return "", None
    
    def _format_schema_for_prompt(self) -> str:
        """
        Format database schema for inclusion in the prompt
        
        Returns:
            str: Formatted schema text
        """
        schema_lines = []
        
        for table, columns in self.db_schema.items():
            schema_lines.append(f"Table: {table}")
            schema_lines.append("Columns:")
            for column in columns:
                schema_lines.append(f"  - {column}")
            schema_lines.append("")
        
        return "\n".join(schema_lines)
    
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
