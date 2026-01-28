"""
Business insights retrieval and management.
Accesses historical insights stored in the business_insights table.
"""

import pandas as pd
import streamlit as st
from datetime import datetime
from database import db
from config import config


class InsightsManager:
    """
    Manages retrieval and display of business insights.
    Reads from the business_insights table created by your Databricks notebooks.
    """
    
    def __init__(self):
        self.insights_table = config.get_full_table_name('business_insights')
    
    def get_all_insights(self, limit=50):
        """
        Retrieve all insights ordered by most recent.
        
        Args:
            limit (int): Maximum number of insights to return
            
        Returns:
            pandas.DataFrame: Insights data
        """
        query = f"""
        SELECT 
            insight_id,
            insight_type,
            metric_name,
            metric_value,
            ai_interpretation,
            generated_at
        FROM {self.insights_table}
        ORDER BY generated_at DESC
        LIMIT {limit}
        """
        return db.execute_query(query)
    
    def get_insights_by_type(self, insight_type):
        """
        Get insights filtered by type (e.g., 'Product Performance', 'Sales Patterns').
        
        Args:
            insight_type (str): The type of insight to retrieve
            
        Returns:
            pandas.DataFrame: Filtered insights
        """
        query = f"""
        SELECT 
            insight_id,
            metric_name,
            metric_value,
            ai_interpretation,
            generated_at
        FROM {self.insights_table}
        WHERE insight_type = '{insight_type}'
        ORDER BY generated_at DESC
        """
        return db.execute_query(query)
    
    def get_latest_insights_by_type(self):
        """
        Get the most recent insight for each insight type.
        Useful for dashboard summary view.
        
        Returns:
            pandas.DataFrame: Latest insight per type
        """
        query = f"""
        WITH ranked_insights AS (
            SELECT 
                insight_id,
                insight_type,
                metric_name,
                metric_value,
                ai_interpretation,
                generated_at,
                ROW_NUMBER() OVER (PARTITION BY insight_type ORDER BY generated_at DESC) as rn
            FROM {self.insights_table}
        )
        SELECT 
            insight_id,
            insight_type,
            metric_name,
            metric_value,
            ai_interpretation,
            generated_at
        FROM ranked_insights
        WHERE rn = 1
        ORDER BY insight_type
        """
        return db.execute_query(query)
    
    def get_insight_types(self):
        """
        Get list of all unique insight types in the database.
        
        Returns:
            list: List of insight type strings
        """
        query = f"""
        SELECT DISTINCT insight_type
        FROM {self.insights_table}
        ORDER BY insight_type
        """
        df = db.execute_query(query)
        if not df.empty:
            return df['insight_type'].tolist()
        return []
    
    def search_insights(self, search_term):
        """
        Search insights by keyword in metric_name or ai_interpretation.
        
        Args:
            search_term (str): Term to search for
            
        Returns:
            pandas.DataFrame: Matching insights
        """
        query = f"""
        SELECT 
            insight_id,
            insight_type,
            metric_name,
            metric_value,
            ai_interpretation,
            generated_at
        FROM {self.insights_table}
        WHERE LOWER(metric_name) LIKE LOWER('%{search_term}%')
           OR LOWER(ai_interpretation) LIKE LOWER('%{search_term}%')
        ORDER BY generated_at DESC
        LIMIT 20
        """
        return db.execute_query(query)
    
    def get_insights_summary_stats(self):
        """
        Get summary statistics about insights.
        
        Returns:
            dict: Summary statistics
        """
        query = f"""
        SELECT 
            COUNT(*) as total_insights,
            COUNT(DISTINCT insight_type) as unique_types,
            MIN(generated_at) as earliest_insight,
            MAX(generated_at) as latest_insight
        FROM {self.insights_table}
        """
        df = db.execute_query(query)
        
        if not df.empty:
            return {
                'total_insights': int(df['total_insights'].iloc[0]),
                'unique_types': int(df['unique_types'].iloc[0]),
                'earliest_insight': df['earliest_insight'].iloc[0],
                'latest_insight': df['latest_insight'].iloc[0]
            }
        return None


# Create singleton instance
insights_manager = InsightsManager()
