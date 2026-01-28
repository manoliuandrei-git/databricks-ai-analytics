"""
Database operations for executing SQL queries against Databricks.
Returns results as pandas DataFrames for easy display and manipulation.
"""

import pandas as pd
import streamlit as st
from config import config


class DatabaseManager:
    """
    Handles all interactions with Databricks SQL warehouse.
    Executes queries and returns results as DataFrames.
    """
    
    def __init__(self):
        self.config = config
    
    def execute_query(self, sql_query):
        """
        Execute a SQL query and return results as a pandas DataFrame.
        
        Args:
            sql_query (str): The SQL query to execute
            
        Returns:
            pandas.DataFrame: Query results, or empty DataFrame if query fails
        """
        connection = None
        cursor = None
        
        try:
            # Create fresh connection
            connection = self.config.get_databricks_connection()
            cursor = connection.cursor()
            
            # Execute query
            cursor.execute(sql_query)
            
            # Fetch results
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            rows = cursor.fetchall()
            
            # Convert to DataFrame
            if rows:
                df = pd.DataFrame(rows, columns=columns)
                return df
            else:
                # Return empty DataFrame with column names
                return pd.DataFrame(columns=columns)
                
        except Exception as e:
            st.error(f"Query execution failed: {str(e)}")
            st.code(sql_query, language="sql")
            return pd.DataFrame()  # Return empty DataFrame on error
            
        finally:
            # Always close connections
            if cursor:
                cursor.close()
            if connection:
                connection.close()
    
    def get_table_schema(self, table_name):
        """
        Get the schema information for a table.
        Returns column names and types.
        
        Args:
            table_name (str): Simple table name (e.g., 'customers')
            
        Returns:
            pandas.DataFrame: Schema information
        """
        full_table_name = self.config.get_full_table_name(table_name)
        query = f"DESCRIBE {full_table_name}"
        return self.execute_query(query)
    
    def get_sample_data(self, table_name, limit=5):
        """
        Get sample rows from a table.
        
        Args:
            table_name (str): Simple table name
            limit (int): Number of rows to return
            
        Returns:
            pandas.DataFrame: Sample data
        """
        full_table_name = self.config.get_full_table_name(table_name)
        query = f"SELECT * FROM {full_table_name} LIMIT {limit}"
        return self.execute_query(query)
    
    def get_database_context(self):
        """
        Build a comprehensive description of the database schema.
        This will be used to give Claude context about available tables.
        
        Returns:
            str: Formatted description of all tables and columns
        """
        tables = ['customers', 'products', 'sales']
        context_parts = ["Available database tables:\n"]
        
        for table in tables:
            schema_df = self.get_table_schema(table)
            if not schema_df.empty:
                full_name = self.config.get_full_table_name(table)
                context_parts.append(f"\n{full_name}:")
                
                for _, row in schema_df.iterrows():
                    col_name = row['col_name']
                    data_type = row['data_type']
                    context_parts.append(f"  - {col_name} ({data_type})")
        
        return "\n".join(context_parts)


# Create singleton instance
db = DatabaseManager()
