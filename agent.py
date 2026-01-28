"""
AI Agent with conversation memory for natural language data queries.
Translates user questions into SQL and maintains conversation context.
"""

import streamlit as st
from datetime import datetime
from config import config
from database import db


class ConversationManager:
    """
    Manages multi-turn conversations with Claude for data analytics.
    Maintains conversation history and generates SQL queries from natural language.
    """
    
    def __init__(self):
        self.client = config.get_anthropic_client()
        self.model = config.claude_model
        
        # Initialize conversation history in session state if not exists
        if 'conversation_history' not in st.session_state:
            st.session_state.conversation_history = []
        
        # Cache database context (only build once per session)
        if 'database_context' not in st.session_state:
            st.session_state.database_context = db.get_database_context()
    
    def get_conversation_history(self):
        """Return current conversation history from session state."""
        return st.session_state.conversation_history
    
    def add_to_history(self, role, content):
        """Add a message to conversation history."""
        st.session_state.conversation_history.append({
            "role": role,
            "content": content
        })
    
    def clear_history(self):
        """Clear conversation history (for new conversation)."""
        st.session_state.conversation_history = []
    
    def build_system_prompt(self):
        """
        Build the system prompt that tells Claude how to behave.
        Includes database schema and instructions for SQL generation.
        """
        system_prompt = f"""You are a data analytics assistant with access to a retail analytics database.

{st.session_state.database_context}

Your task is to help users query and analyze this data by generating SQL queries.

IMPORTANT RULES:
1. Generate ONLY valid SQL queries for Databricks SQL
2. Use fully qualified table names: workspace.claude.tablename
3. When results are empty (0 rows), explain what might be missing rather than suggesting to check the database
4. For follow-up questions like "filter those" or "show their average", use context from previous queries
5. Always use proper SQL syntax with appropriate JOINs when querying multiple tables
6. Return ONLY the SQL query, no explanations or markdown formatting
7. If the question cannot be answered with the available data, explain why

Current date: {datetime.now().strftime('%Y-%m-%d')}
"""
        return system_prompt
    
    def generate_sql(self, user_question):
        """
        Generate SQL query from natural language question.
        Includes conversation history for context-aware responses.
        
        Args:
            user_question (str): The user's natural language question
            
        Returns:
            tuple: (sql_query, ai_response_text)
        """
        # Build messages with conversation history
        messages = self.get_conversation_history().copy()
        messages.append({
            "role": "user",
            "content": user_question
        })
        
        try:
            # Call Claude API
            response = self.client.messages.create(
                model=self.model,
                max_tokens=config.max_tokens,
                system=self.build_system_prompt(),
                messages=messages
            )
            
            # Extract response
            ai_response = response.content[0].text
            
            # Add to conversation history
            self.add_to_history("user", user_question)
            self.add_to_history("assistant", ai_response)
            
            # Check if response contains SQL
            if "SELECT" in ai_response.upper():
                # Extract SQL (remove markdown if present)
                sql_query = ai_response.replace("```sql", "").replace("```", "").strip()
                return sql_query, ai_response
            else:
                # Response is explanation, not SQL
                return None, ai_response
                
        except Exception as e:
            error_msg = f"Error generating SQL: {str(e)}"
            st.error(error_msg)
            return None, error_msg
    
    def ask_question(self, user_question, max_retries=1):
        """
        Complete workflow: take user question, generate SQL, execute query, return results.
        If SQL fails, sends error back to Claude for one automatic fix attempt.
        
        Args:
            user_question (str): Natural language question
            max_retries (int): Number of times to retry fixing failed SQL (default 1)
            
        Returns:
            dict: Contains 'success', 'sql', 'data', 'message', 'retry_attempted'
        """
        # Generate SQL from question
        sql_query, ai_response = self.generate_sql(user_question)
        
        # If no SQL generated, return the explanation
        if sql_query is None:
            return {
                'success': False,
                'sql': None,
                'data': None,
                'message': ai_response,
                'retry_attempted': False
            }
        
        # Execute the SQL query
        df = db.execute_query(sql_query)
        
        # Check if query failed (execute_query returns empty DataFrame on error)
        # We need to detect actual errors vs legitimate empty results
        # If the query failed, db.execute_query will have shown an error in Streamlit
        # We'll attempt retry if DataFrame is empty AND there was likely an error
        
        # First attempt succeeded
        if df is not None and len(df.columns) > 0:
            if not df.empty:
                return {
                    'success': True,
                    'sql': sql_query,
                    'data': df,
                    'message': f"Found {len(df)} results",
                    'retry_attempted': False
                }
            else:
                # Empty but valid result
                return {
                    'success': True,
                    'sql': sql_query,
                    'data': df,
                    'message': "Query executed successfully but returned no results",
                    'retry_attempted': False
                }
        
        # Query failed - attempt to fix if retries available
        if max_retries > 0:
            st.warning("SQL query failed. Asking Claude to fix it...")
            
            # Ask Claude to fix the SQL with error context
            fixed_sql, fixed_response = self.fix_failed_sql(sql_query, user_question)
            
            if fixed_sql:
                # Try executing the fixed SQL
                df_fixed = db.execute_query(fixed_sql)
                
                if df_fixed is not None and len(df_fixed.columns) > 0:
                    if not df_fixed.empty:
                        st.success("Claude fixed the query successfully!")
                        return {
                            'success': True,
                            'sql': fixed_sql,
                            'data': df_fixed,
                            'message': f"Found {len(df_fixed)} results (after automatic fix)",
                            'retry_attempted': True
                        }
                    else:
                        return {
                            'success': True,
                            'sql': fixed_sql,
                            'data': df_fixed,
                            'message': "Fixed query executed but returned no results",
                            'retry_attempted': True
                        }
        
        # All attempts failed
        return {
            'success': False,
            'sql': sql_query,
            'data': None,
            'message': "Query failed and could not be automatically fixed",
            'retry_attempted': max_retries > 0
        }

    def fix_failed_sql(self, failed_sql, original_question):
        """
        Ask Claude to fix a failed SQL query.
        Provides the error context and asks for a corrected version.
        
        Args:
            failed_sql (str): The SQL query that failed
            original_question (str): The original user question
            
        Returns:
            tuple: (fixed_sql, ai_response)
        """
        fix_prompt = f"""The following SQL query failed to execute:
```sql
{failed_sql}
```

Original question: "{original_question}"

Common issues in Databricks SQL:
- DATE_TRUNC() uses different syntax than PostgreSQL
- Some aggregate functions have different parameter orders
- String functions may have different names

Please provide a CORRECTED SQL query that will work in Databricks SQL.
Return ONLY the corrected SQL query, no explanations."""

        try:
            # Call Claude with fix request
            response = self.client.messages.create(
                model=self.model,
                max_tokens=config.max_tokens,
                system=self.build_system_prompt(),
                messages=[{
                    "role": "user",
                    "content": fix_prompt
                }]
            )
            
            ai_response = response.content[0].text
            
            # Extract SQL
            if "SELECT" in ai_response.upper():
                fixed_sql = ai_response.replace("```sql", "").replace("```", "").strip()
                return fixed_sql, ai_response
            else:
                return None, ai_response
                
        except Exception as e:
            st.error(f"Error asking Claude to fix SQL: {str(e)}")
            return None, str(e)


# Create singleton instance
agent = ConversationManager()
