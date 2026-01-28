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
            run_id,
            generated_at,
            metric_name,
            metric_category,
            metric_description,
            metric_value,
            metric_type,
            source
        FROM {self.insights_table}
        ORDER BY generated_at DESC
        LIMIT {limit}
        """
        return db.execute_query(query)
    
    def get_insights_by_type(self, insight_type):
        """
        Get insights filtered by source or metric_category.
        
        Args:
            insight_type (str): The type of insight to retrieve
            
        Returns:
            pandas.DataFrame: Filtered insights
        """
        query = f"""
        SELECT 
            run_id,
            generated_at,
            metric_name,
            metric_category,
            metric_description,
            metric_value,
            metric_type,
            source
        FROM {self.insights_table}
        WHERE source = '{insight_type}' OR metric_category = '{insight_type}'
        ORDER BY generated_at DESC
        """
        return db.execute_query(query)
    
    def get_latest_insights_by_type(self):
        """
        Get the most recent insight for each source.
        
        Returns:
            pandas.DataFrame: Latest insight per source
        """
        query = f"""
        WITH ranked_insights AS (
            SELECT 
                run_id,
                generated_at,
                metric_name,
                metric_category,
                metric_description,
                metric_value,
                metric_type,
                source,
                ROW_NUMBER() OVER (PARTITION BY source ORDER BY generated_at DESC) as rn
            FROM {self.insights_table}
        )
        SELECT 
            run_id,
            generated_at,
            metric_name,
            metric_category,
            metric_description,
            metric_value,
            metric_type,
            source
        FROM ranked_insights
        WHERE rn = 1
        ORDER BY source
        """
        return db.execute_query(query)
    
    def get_insight_types(self):
        """
        Get list of all unique insight sources in the database.
        
        Returns:
            list: List of insight source strings
        """
        query = f"""
        SELECT DISTINCT source
        FROM {self.insights_table}
        ORDER BY source
        """
        df = db.execute_query(query)
        if not df.empty:
            return df['source'].tolist()
        return []
    
    def search_insights(self, search_term):
        """
        Search insights by keyword in metric_name or metric_description.
        
        Args:
            search_term (str): Term to search for
            
        Returns:
            pandas.DataFrame: Matching insights
        """
        query = f"""
        SELECT 
            run_id,
            generated_at,
            metric_name,
            metric_category,
            metric_description,
            metric_value,
            metric_type,
            source
        FROM {self.insights_table}
        WHERE LOWER(metric_name) LIKE LOWER('%{search_term}%')
           OR LOWER(metric_description) LIKE LOWER('%{search_term}%')
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
            COUNT(DISTINCT source) as unique_types,
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