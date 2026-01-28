"""
Main Streamlit application for AI-powered data analytics.
Provides chat interface and insights dashboard.
"""

import streamlit as st
import pandas as pd
from datetime import datetime
from agent import agent
from insights import insights_manager
from database import db


# Page configuration
st.set_page_config(
    page_title="AI Data Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        margin-bottom: 1rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    .chat-message {
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    .user-message {
        background-color: #e3f2fd;
    }
    .assistant-message {
        background-color: #f5f5f5;
    }
    </style>
""", unsafe_allow_html=True)


def main():
    """Main application entry point."""
    
    # Sidebar navigation
    st.sidebar.title("Navigation")
    page = st.sidebar.radio(
        "Choose a page:",
        ["Chat Interface", "Insights Dashboard", "Database Explorer"]
    )
    
    # Route to selected page
    if page == "Chat Interface":
        chat_interface()
    elif page == "Insights Dashboard":
        insights_dashboard()
    elif page == "Database Explorer":
        database_explorer()


def chat_interface():
    """
    Interactive chat interface for asking questions about data.
    Uses the AI agent to convert natural language to SQL queries.
    """
    st.markdown('<div class="main-header">💬 Chat with Your Data</div>', unsafe_allow_html=True)
    st.write("Ask questions about your retail data in natural language.")
    
    # Sidebar controls
    with st.sidebar:
        st.subheader("Conversation Controls")
        if st.button("Clear Conversation", type="secondary"):
            agent.clear_history()
            st.success("Conversation cleared!")
            st.rerun()
        
        st.divider()
        
        # Show conversation stats
        history = agent.get_conversation_history()
        st.metric("Messages in conversation", len(history))
    
    # Display conversation history
    history = agent.get_conversation_history()
    
    if history:
        st.subheader("Conversation History")
        for msg in history:
            if msg["role"] == "user":
                st.markdown(f'<div class="chat-message user-message"><strong>You:</strong> {msg["content"]}</div>', 
                          unsafe_allow_html=True)
            else:
                st.markdown(f'<div class="chat-message assistant-message"><strong>Assistant:</strong> {msg["content"]}</div>', 
                          unsafe_allow_html=True)
        
        st.divider()
    
    # Input area
    st.subheader("Ask a Question")
    
    # Example questions
    with st.expander("Example questions you can ask"):
        st.write("""
        - Show me the top 5 customers by total spending
        - Which products have the highest ratings?
        - What were our sales by month in 2024?
        - Show me customers from California
        - What's the average order value by payment method?
        - Which brands are most popular?
        """)
    
    # Question input
    user_question = st.text_input(
        "Your question:",
        placeholder="e.g., Show me the top 10 products by revenue",
        label_visibility="collapsed"
    )
    
    col1, col2 = st.columns([1, 5])
    with col1:
        ask_button = st.button("Ask", type="primary", use_container_width=True)
    
    # Process question
    if ask_button and user_question:
        with st.spinner("Thinking..."):
            result = agent.ask_question(user_question)
        
        # Display results
        if result['success']:
            st.success(result['message'])
            
            if result['retry_attempted']:
                st.info("Note: The initial query had an error and was automatically corrected by Claude.")
            
            # Show SQL query
            with st.expander("View Generated SQL"):
                st.code(result['sql'], language="sql")
            
            # Show data
            if result['data'] is not None and not result['data'].empty:
                st.subheader("Results")
                st.dataframe(result['data'], use_container_width=True)
                
                # Download button
                csv = result['data'].to_csv(index=False)
                st.download_button(
                    label="Download as CSV",
                    data=csv,
                    file_name=f"query_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
            else:
                st.info("The query executed successfully but returned no results.")
        else:
            st.error(result['message'])
            if result['sql']:
                with st.expander("View Failed SQL"):
                    st.code(result['sql'], language="sql")


def insights_dashboard():
    """
    Dashboard displaying historical business insights.
    Shows insights generated by your Databricks notebooks.
    """
    st.markdown('<div class="main-header">📈 Business Insights Dashboard</div>', unsafe_allow_html=True)
    
    # Get summary statistics
    stats = insights_manager.get_insights_summary_stats()
    
    if stats:
        # Display summary metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Insights", stats['total_insights'])
        with col2:
            st.metric("Insight Types", stats['unique_types'])
        with col3:
            st.metric("First Insight", stats['earliest_insight'].strftime('%Y-%m-%d') if stats['earliest_insight'] else "N/A")
        with col4:
            st.metric("Latest Insight", stats['latest_insight'].strftime('%Y-%m-%d') if stats['latest_insight'] else "N/A")
        
        st.divider()
    
    # Sidebar filters
    with st.sidebar:
        st.subheader("Filters")
        
        # Get available insight types
        insight_types = insights_manager.get_insight_types()
        
        if insight_types:
            selected_type = st.selectbox(
                "Filter by Type:",
                ["All Types"] + insight_types
            )
        else:
            selected_type = "All Types"
            st.warning("No insight types found")
        
        # Search box
        search_term = st.text_input("Search insights:", placeholder="Enter keyword...")
    
    # Main content area
    tab1, tab2, tab3 = st.tabs(["Latest Insights", "All Insights", "By Type"])
    
    with tab1:
        st.subheader("Most Recent Insight per Category")
        latest_insights = insights_manager.get_latest_insights_by_type()
        
        if not latest_insights.empty:
            for _, row in latest_insights.iterrows():
                with st.container():
                    st.markdown(f"### {row['insight_type']}")
                    col1, col2 = st.columns([3, 1])
                    with col1:
                        st.markdown(f"**{row['metric_name']}:** {row['metric_value']}")
                        st.write(row['ai_interpretation'])
                    with col2:
                        st.caption(f"Generated: {row['generated_at']}")
                    st.divider()
        else:
            st.info("No insights found. Run your Databricks insights generation notebook to create insights.")
    
    with tab2:
        st.subheader("All Insights (Most Recent First)")
        
        if search_term:
            all_insights = insights_manager.search_insights(search_term)
            st.caption(f"Search results for: {search_term}")
        else:
            all_insights = insights_manager.get_all_insights()
        
        if not all_insights.empty:
            st.dataframe(
                all_insights,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "generated_at": st.column_config.DatetimeColumn(
                        "Generated At",
                        format="YYYY-MM-DD HH:mm:ss"
                    )
                }
            )
        else:
            st.info("No insights found.")
    
    with tab3:
        st.subheader(f"Insights: {selected_type}")
        
        if selected_type != "All Types":
            type_insights = insights_manager.get_insights_by_type(selected_type)
            
            if not type_insights.empty:
                for _, row in type_insights.iterrows():
                    with st.container():
                        col1, col2 = st.columns([3, 1])
                        with col1:
                            st.markdown(f"**{row['metric_name']}:** {row['metric_value']}")
                            st.write(row['ai_interpretation'])
                        with col2:
                            st.caption(f"Generated: {row['generated_at']}")
                        st.divider()
            else:
                st.info(f"No insights found for type: {selected_type}")
        else:
            st.info("Select an insight type from the sidebar to view insights.")


def database_explorer():
    """
    Simple database explorer to view table schemas and sample data.
    Helpful for understanding what data is available.
    """
    st.markdown('<div class="main-header">🗄️ Database Explorer</div>', unsafe_allow_html=True)
    st.write("Explore your database tables and schemas.")
    
    tables = ['customers', 'products', 'sales']
    
    selected_table = st.selectbox("Select a table:", tables)
    
    if selected_table:
        st.subheader(f"Table: {selected_table}")
        
        # Show schema
        with st.expander("View Schema", expanded=True):
            schema_df = db.get_table_schema(selected_table)
            if not schema_df.empty:
                st.dataframe(schema_df, use_container_width=True, hide_index=True)
        
        # Show sample data
        st.subheader("Sample Data")
        sample_size = st.slider("Number of rows:", min_value=5, max_value=50, value=10)
        sample_data = db.get_sample_data(selected_table, limit=sample_size)
        
        if not sample_data.empty:
            st.dataframe(sample_data, use_container_width=True, hide_index=True)
            
            # Show row count
            st.caption(f"Showing {len(sample_data)} rows")
        else:
            st.warning("No data found in this table.")


if __name__ == "__main__":
    main()
