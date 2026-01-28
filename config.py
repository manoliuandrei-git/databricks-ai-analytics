"""
Configuration management for Databricks and Anthropic connections.
Handles environment variables and creates reusable connection objects.
"""

import os
import streamlit as st
from databricks import sql
from anthropic import Anthropic


class Config:
    """
    Centralized configuration for the application.
    Loads from Streamlit secrets in production or environment variables locally.
    """
    
    def __init__(self):
        # Try Streamlit secrets first (for Streamlit Cloud), fall back to env vars
        self.databricks_hostname = self._get_secret('DATABRICKS_SERVER_HOSTNAME')
        self.databricks_http_path = self._get_secret('DATABRICKS_HTTP_PATH')
        self.databricks_token = self._get_secret('DATABRICKS_ACCESS_TOKEN')
        self.anthropic_api_key = self._get_secret('ANTHROPIC_API_KEY')
        
        # Database configuration
        self.catalog = self._get_secret('DATABRICKS_CATALOG', 'workspace')
        self.schema = self._get_secret('DATABRICKS_SCHEMA', 'claude')
        
        # Claude model configuration
        self.claude_model = "claude-sonnet-4-20250514"
        self.max_tokens = 4096
    
    def _get_secret(self, key, default=None):
        """
        Get secret from Streamlit secrets or environment variables.
        Streamlit secrets take precedence (for cloud deployment).
        """
        # Try Streamlit secrets first
        if hasattr(st, 'secrets') and key in st.secrets:
            return st.secrets[key]
        
        # Fall back to environment variables
        value = os.environ.get(key, default)
        
        if value is None and default is None:
            raise ValueError(f"Missing required configuration: {key}")
        
        return value
    
    def get_databricks_connection(self):
        """
        Create and return a Databricks SQL connection.
        This connection can execute queries against your Databricks tables.
        """
        try:
            connection = sql.connect(
                server_hostname=self.databricks_hostname,
                http_path=self.databricks_http_path,
                access_token=self.databricks_token
            )
            return connection
        except Exception as e:
            st.error(f"Failed to connect to Databricks: {str(e)}")
            raise
    
    def get_anthropic_client(self):
        """
        Create and return an Anthropic API client.
        This client is used to make requests to Claude.
        """
        try:
            client = Anthropic(api_key=self.anthropic_api_key)
            return client
        except Exception as e:
            st.error(f"Failed to initialize Anthropic client: {str(e)}")
            raise
    
    def get_full_table_name(self, table_name):
        """
        Returns fully qualified table name: catalog.schema.table
        Example: workspace.claude.customers
        """
        return f"{self.catalog}.{self.schema}.{table_name}"


# Create a singleton instance that can be imported
config = Config()
