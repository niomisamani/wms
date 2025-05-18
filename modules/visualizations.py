import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import logging
from typing import Dict, List, Tuple, Optional, Union, Any
import numpy as np
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/visualizations.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("visualizations")

class DashboardVisualizer:
    """
    Class to create visualizations for the WMS dashboard
    """
    
    def __init__(self):
        """Initialize the Dashboard Visualizer"""
        logger.info("Dashboard Visualizer initialized")
    
    def create_inventory_chart(self, df: pd.DataFrame) -> go.Figure:
        """
        Create inventory level chart
        
        Args:
            df: DataFrame with inventory data
            
        Returns:
            Figure: Plotly figure object
        """
        try:
            if df.empty or 'msku' not in df.columns or 'quantity' not in df.columns:
                logger.warning("Invalid data for inventory chart")
                # Return empty figure with message
                fig = go.Figure()
                fig.add_annotation(
                    text="No inventory data available",
                    xref="paper", yref="paper",
                    x=0.5, y=0.5, showarrow=False
                )
                return fig
            
            # Sort by quantity for better visualization
            df_sorted = df.sort_values('quantity', ascending=False)
            
            # Limit to top 20 items for readability
            if len(df_sorted) > 20:
                df_sorted = df_sorted.head(20)
            
            # Create horizontal bar chart
            fig = px.bar(
                df_sorted,
                y='msku',
                x='quantity',
                orientation='h',
                title='Current Inventory Levels',
                labels={'msku': 'Master SKU', 'quantity': 'Quantity'},
                color='quantity',
                color_continuous_scale='Viridis'
            )
            
            # Update layout
            fig.update_layout(
                height=600,
                xaxis_title="Quantity",
                yaxis_title="Master SKU",
                yaxis={'categoryorder': 'total ascending'},
                coloraxis_showscale=False
            )
            
            logger.info("Created inventory chart")
            return fig
        
        except Exception as e:
            logger.error(f"Error creating inventory chart: {str(e)}")
            # Return empty figure with error message
            fig = go.Figure()
            fig.add_annotation(
                text="Error creating inventory chart",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False
            )
            return fig
    
    def create_sales_trend_chart(self, df: pd.DataFrame) -> go.Figure:
        """
        Create sales trend chart
        
        Args:
            df: DataFrame with sales data
            
        Returns:
            Figure: Plotly figure object
        """
        try:
            if df.empty or 'order_date' not in df.columns or 'quantity' not in df.columns:
                logger.warning("Invalid data for sales trend chart")
                # Return empty figure with message
                fig = go.Figure()
                fig.add_annotation(
                    text="No sales data available",
                    xref="paper", yref="paper",
                    x=0.5, y=0.5, showarrow=False
                )
                return fig
            
            # Ensure order_date is datetime
            df['order_date'] = pd.to_datetime(df['order_date'])
            
            # Group by date and sum quantities
            daily_sales = df.groupby(df['order_date'].dt.date)['quantity'].sum().reset_index()
            
            # Create line chart
            fig = px.line(
                daily_sales,
                x='order_date',
                y='quantity',
                title='Daily Sales Trend',
                labels={'order_date': 'Date', 'quantity': 'Units Sold'},
                markers=True
            )
            
            # Add moving average
            if len(daily_sales) > 3:
                daily_sales['ma_3day'] = daily_sales['quantity'].rolling(window=3).mean()
                fig.add_scatter(
                    x=daily_sales['order_date'],
                    y=daily_sales['ma_3day'],
                    mode='lines',
                    name='3-Day Moving Average',
                    line=dict(color='red', dash='dash')
                )
            
            # Update layout
            fig.update_layout(
                height=400,
                xaxis_title="Date",
                yaxis_title="Units Sold",
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="right",
                    x=1
                )
            )
            
            logger.info("Created sales trend chart")
            return fig
        
        except Exception as e:
            logger.error(f"Error creating sales trend chart: {str(e)}")
            # Return empty figure with error message
            fig = go.Figure()
            fig.add_annotation(
                text="Error creating sales trend chart",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False
            )
            return fig
    
    def create_marketplace_distribution_chart(self, df: pd.DataFrame) -> go.Figure:
        """
        Create marketplace distribution chart
        
        Args:
            df: DataFrame with sales data
            
        Returns:
            Figure: Plotly figure object
        """
        try:
            if df.empty or 'marketplace' not in df.columns or 'quantity' not in df.columns:
                logger.warning("Invalid data for marketplace distribution chart")
                # Return empty figure with message
                fig = go.Figure()
                fig.add_annotation(
                    text="No marketplace data available",
                    xref="paper", yref="paper",
                    x=0.5, y=0.5, showarrow=False
                )
                return fig
            
            # Group by marketplace and sum quantities
            marketplace_sales = df.groupby('marketplace')['quantity'].sum().reset_index()
            
            # Create pie chart
            fig = px.pie(
                marketplace_sales,
                values='quantity',
                names='marketplace',
                title='Sales Distribution by Marketplace',
                color='marketplace',
                color_discrete_sequence=px.colors.qualitative.Pastel
            )
            
            # Update layout
            fig.update_layout(
                height=400,
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=-0.1,
                    xanchor="center",
                    x=0.5
                )
            )
            
            # Update traces
            fig.update_traces(
                textposition='inside',
                textinfo='percent+label',
                hole=0.4,
                marker=dict(line=dict(color='#FFFFFF', width=2))
            )
            
            logger.info("Created marketplace distribution chart")
            return fig
        
        except Exception as e:
            logger.error(f"Error creating marketplace distribution chart: {str(e)}")
            # Return empty figure with error message
            fig = go.Figure()
            fig.add_annotation(
                text="Error creating marketplace distribution chart",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False
            )
            return fig
    
    def create_top_products_chart(self, df: pd.DataFrame) -> go.Figure:
        """
        Create top products chart
        
        Args:
            df: DataFrame with sales data
            
        Returns:
            Figure: Plotly figure object
        """
        try:
            if df.empty or 'msku_validated' not in df.columns or 'quantity' not in df.columns:
                logger.warning("Invalid data for top products chart")
                # Return empty figure with message
                fig = go.Figure()
                fig.add_annotation(
                    text="No product data available",
                    xref="paper", yref="paper",
                    x=0.5, y=0.5, showarrow=False
                )
                return fig
            
            # Group by product and sum quantities
            product_sales = df.groupby('msku_validated')['quantity'].sum().reset_index()
            
            # Sort and get top 10
            top_products = product_sales.sort_values('quantity', ascending=False).head(10)
            
            # Create bar chart
            fig = px.bar(
                top_products,
                y='msku_validated',
                x='quantity',
                orientation='h',
                title='Top 10 Products by Sales Volume',
                labels={'msku_validated': 'Master SKU', 'quantity': 'Units Sold'},
                color='quantity',
                color_continuous_scale='Viridis'
            )
            
            # Update layout
            fig.update_layout(
                height=500,
                xaxis_title="Units Sold",
                yaxis_title="Master SKU",
                yaxis={'categoryorder': 'total ascending'},
                coloraxis_showscale=False
            )
            
            logger.info("Created top products chart")
            return fig
        
        except Exception as e:
            logger.error(f"Error creating top products chart: {str(e)}")
            # Return empty figure with error message
            fig = go.Figure()
            fig.add_annotation(
                text="Error creating top products chart",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False
            )
            return fig
    
    def create_geographic_distribution_chart(self, df: pd.DataFrame) -> go.Figure:
        """
        Create geographic distribution chart
        
        Args:
            df: DataFrame with sales data including state information
            
        Returns:
            Figure: Plotly figure object
        """
        try:
            # Check if we have state data
            state_col = None
            for col in ['customer_state', 'state', 'Customer State']:
                if col in df.columns:
                    state_col = col
                    break
            
            if df.empty or not state_col or 'quantity' not in df.columns:
                logger.warning("Invalid data for geographic distribution chart")
                # Return empty figure with message
                fig = go.Figure()
                fig.add_annotation(
                    text="No geographic data available",
                    xref="paper", yref="paper",
                    x=0.5, y=0.5, showarrow=False
                )
                return fig
            
            # Group by state and sum quantities
            state_sales = df.groupby(state_col)['quantity'].sum().reset_index()
            
            # Create choropleth map for India
            fig = px.choropleth(
                state_sales,
                locations=state_col,
                locationmode='country names',
                color='quantity',
                scope='asia',
                title='Sales Distribution by State',
                labels={'quantity': 'Units Sold'},
                color_continuous_scale='Viridis'
            )
            
            # Update layout
            fig.update_layout(
                height=600,
                geo=dict(
                    visible=True,
                    showframe=True,
                    showcoastlines=True,
                    projection_type='mercator',
                    center=dict(lat=20, lon=78),  # Center on India
                    lataxis=dict(range=[5, 35]),  # Latitude range for India
                    lonaxis=dict(range=[65, 95])  # Longitude range for India
                )
            )
            
            logger.info("Created geographic distribution chart")
            return fig
        
        except Exception as e:
            logger.error(f"Error creating geographic distribution chart: {str(e)}")
            # Return empty figure with error message
            fig = go.Figure()
            fig.add_annotation(
                text="Error creating geographic distribution chart",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False
            )
            return fig
    
    def create_dashboard_metrics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Calculate key metrics for dashboard
        
        Args:
            df: DataFrame with sales data
            
        Returns:
            Dict: Dictionary of metrics
        """
        metrics = {
            'total_orders': 0,
            'total_units_sold': 0,
            'total_revenue': 0,
            'avg_order_value': 0,
            'top_selling_product': 'N/A',
            'top_marketplace': 'N/A'
        }
        
        try:
            if df.empty:
                logger.warning("Empty DataFrame for metrics calculation")
                return metrics
            
            # Calculate total orders (unique order IDs)
            order_id_col = None
            for col in ['order_id', 'Order Id', 'Sub Order No']:
                if col in df.columns:
                    order_id_col = col
                    break
            
            if order_id_col:
                metrics['total_orders'] = df[order_id_col].nunique()
            
            # Calculate total units sold
            if 'quantity' in df.columns:
                metrics['total_units_sold'] = df['quantity'].sum()
            
            # Calculate total revenue
            price_col = None
            for col in ['price', 'Price inc. FKMP Contribution & Subsidy', 'Supplier Discounted Price (Incl GST and Commision)']:
                if col in df.columns:
                    price_col = col
                    break
            
            if price_col and 'quantity' in df.columns:
                # Calculate revenue as price * quantity
                df['revenue'] = df[price_col] * df['quantity']
                metrics['total_revenue'] = df['revenue'].sum()
                
                # Calculate average order value
                if order_id_col:
                    order_revenue = df.groupby(order_id_col)['revenue'].sum()
                    metrics['avg_order_value'] = order_revenue.mean()
            
            # Find top selling product
            if 'msku_validated' in df.columns and 'quantity' in df.columns:
                product_sales = df.groupby('msku_validated')['quantity'].sum()
                if not product_sales.empty:
                    metrics['top_selling_product'] = product_sales.idxmax()
            
            # Find top marketplace
            if 'marketplace' in df.columns and 'quantity' in df.columns:
                marketplace_sales = df.groupby('marketplace')['quantity'].sum()
                if not marketplace_sales.empty:
                    metrics['top_marketplace'] = marketplace_sales.idxmax()
            
            logger.info("Calculated dashboard metrics")
            return metrics
        
        except Exception as e:
            logger.error(f"Error calculating dashboard metrics: {str(e)}")
            return metrics
