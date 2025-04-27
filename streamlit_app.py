import streamlit as st
import pandas as pd
import json
import tempfile
import os
import time
import matplotlib.pyplot as plt
from db_connect import db_connect
from db_queries import DBQueries
from log_parser import LogParser
from index_recommender import IndexRecommender

# Set page config
st.set_page_config(
    page_title="MongoDB Index Recommender",
    page_icon="📊",
    layout="wide"
)

# App title
st.title("MongoDB Automated Index Recommendation System")

# Initialize session state
if 'connected' not in st.session_state:
    st.session_state.connected = False
if 'client' not in st.session_state:
    st.session_state.client = None
if 'db_queries' not in st.session_state:
    st.session_state.db_queries = None
if 'log_parser' not in st.session_state:
    st.session_state.log_parser = None
if 'recommender' not in st.session_state:
    st.session_state.recommender = None
if 'parsed_queries' not in st.session_state:
    st.session_state.parsed_queries = []
if 'recommendations' not in st.session_state:
    st.session_state.recommendations = []
if 'selected_db' not in st.session_state:
    st.session_state.selected_db = None

# Sidebar navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["Connect to MongoDB", "Upload & Parse Logs", 
                               "View Recommendations", "Apply Indexes"])

# MongoDB Connection page
if page == "Connect to MongoDB":
    st.header("Connect to MongoDB")
    
    # Connection form
    with st.form("mongodb_connection_form"):
        mongo_uri = st.text_input("MongoDB Connection String", "mongodb://localhost:27017")
        submit_button = st.form_submit_button("Connect")
        
        if submit_button:
            with st.spinner("Connecting to MongoDB..."):
                try:
                    # Connect to MongoDB
                    os.environ["MONGO_URL"] = mongo_uri
                    client = db_connect()
                    
                    # Store the client in session state
                    st.session_state.client = client
                    st.session_state.connected = True
                    
                    st.success("Successfully connected to MongoDB!")
                    
                    # List available databases
                    databases = client.list_database_names()
                    st.session_state.databases = databases
                    
                    # Select a database
                    st.subheader("Select a Database")
                    selected_db = st.selectbox("Select a database", databases)
                    
                    if selected_db:
                        st.session_state.selected_db = selected_db
                        st.session_state.db_queries = DBQueries(selected_db)
                        
                        # List collections
                        collections = st.session_state.db_queries.list_collections()
                        st.write(f"Collections in {selected_db}:", collections)
                    
                except Exception as e:
                    st.error(f"Error connecting to MongoDB: {e}")
    
    # Show connection status
    if st.session_state.connected:
        st.success("✅ Connected to MongoDB")
        
        if st.session_state.selected_db:
            st.write(f"Current Database: **{st.session_state.selected_db}**")
            
            # Change database option
            new_db = st.selectbox("Change Database", st.session_state.databases, 
                                index=st.session_state.databases.index(st.session_state.selected_db))
            
            if new_db != st.session_state.selected_db:
                st.session_state.selected_db = new_db
                st.session_state.db_queries = DBQueries(new_db)
                st.rerun()
    else:
        st.warning("⚠️ Not connected to MongoDB")

# Upload & Parse Logs page
elif page == "Upload & Parse Logs":
    st.header("Upload & Parse MongoDB Logs")
    
    # Check if connected to MongoDB
    if not st.session_state.connected:
        st.warning("Please connect to MongoDB first")
        st.stop()
    
    # File upload
    uploaded_file = st.file_uploader("Upload MongoDB log file", type=["log", "txt"])
    
    if uploaded_file is not None:
        # Create a temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix='.log') as tmp_file:
            tmp_file.write(uploaded_file.getvalue())
            log_file_path = tmp_file.name
        
        # Initialize log parser
        log_parser = LogParser()
        
        # Parse button
        if st.button("Parse Log File"):
            with st.spinner("Parsing logs..."):
                # Parse the log file
                parsed_queries = log_parser.parse_log_file(log_file_path)
                
                # Store in session state
                st.session_state.log_parser = log_parser
                st.session_state.parsed_queries = parsed_queries
                
                st.success(f"Successfully parsed {len(parsed_queries)} queries")
                
                # Show stats
                st.subheader("Log Statistics")
                col1, col2 = st.columns(2)
                
                with col1:
                    st.metric("Total Queries", len(parsed_queries))
                
                with col2:
                    slow_queries = log_parser.get_slow_queries()
                    st.metric("Slow Queries (>100ms)", len(slow_queries))
                
                # Display sample of parsed queries
                if parsed_queries:
                    st.subheader("Sample of Parsed Queries")
                    
                    if len(parsed_queries) > 0:
                        df = pd.DataFrame([
                            {
                                'timestamp': q.get('timestamp'),
                                'database': q.get('database'),
                                'collection': q.get('collection'),
                                'execution_time_ms': q.get('execution_time_ms'),
                                'query': str(q.get('query', ''))[:50] + '...' if q.get('query') else ''
                            }
                            for q in parsed_queries[:10]
                        ])
                        
                        st.dataframe(df)
        
        # If logs are already parsed, show stats
        if st.session_state.log_parser and st.session_state.parsed_queries:
            st.subheader("Log Statistics")
            col1, col2 = st.columns(2)
            
            with col1:
                st.metric("Total Queries", len(st.session_state.parsed_queries))
            
            with col2:
                slow_queries = st.session_state.log_parser.get_slow_queries()
                st.metric("Slow Queries (>100ms)", len(slow_queries))
                
            # Display slow queries
            if slow_queries:
                st.subheader("Slow Queries")
                
                df_slow = pd.DataFrame([
                    {
                        'timestamp': q.get('timestamp'),
                        'database': q.get('database'),
                        'collection': q.get('collection'),
                        'execution_time_ms': q.get('execution_time_ms'),
                        'query': str(q.get('query', ''))[:50] + '...' if q.get('query') else ''
                    }
                    for q in slow_queries[:10]
                ])
                
                st.dataframe(df_slow)
        
        # Clean up the temporary file
        try:
            os.unlink(log_file_path)
        except:
            pass

# View Recommendations page
elif page == "View Recommendations":
    st.header("Index Recommendations")
    
    # Check if connected to MongoDB and logs are parsed
    if not st.session_state.connected:
        st.warning("Please connect to MongoDB first")
        st.stop()
    
    if not st.session_state.log_parser:
        st.warning("Please upload and parse logs first")
        st.stop()
    
    # Initialize recommender if needed
    if not st.session_state.recommender:
        st.session_state.recommender = IndexRecommender(
            st.session_state.db_queries, 
            st.session_state.log_parser
        )
    
    # Display query patterns for debugging
    if st.checkbox("Show Query Patterns (Debug)"):
        st.subheader("Query Patterns")
        st.json(st.session_state.log_parser.query_patterns)
    
    # Generate recommendations button
    if st.button("Generate Index Recommendations"):
        with st.spinner("Analyzing query patterns and generating recommendations..."):
            # Generate recommendations
            recommendations = st.session_state.recommender.recommend_indexes_from_logs()
            
            # Store in session state
            st.session_state.recommendations = recommendations
            
            st.success(f"Generated {len(recommendations)} index recommendations")
    
    # Display recommendations
    if st.session_state.recommendations:
        st.subheader("Recommended Indexes")
        
        for i, rec in enumerate(st.session_state.recommendations):
            with st.expander(f"Recommendation #{i+1}: Index on {rec['collection']} ({len(rec['fields'])} fields)"):
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown(f"**Collection:** {rec['collection']}")
                    st.markdown(f"**Fields:** {', '.join(rec['fields'])}")
                    st.markdown(f"**Average Query Time:** {rec['avg_execution_time_ms']:.2f} ms")
                
                with col2:
                    st.markdown(f"**Query Pattern:** {rec['query_pattern']}")
                    st.markdown(f"**Execution Count:** {rec['execution_count']}")
                    st.markdown(f"**Potential Impact:** {rec['potential_impact']:.2f}")
        
        # Export recommendations
        if st.button("Export Recommendations as JSON"):
            with tempfile.NamedTemporaryFile(delete=False, suffix='.json') as tmp_file:
                export_data = {
                    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "recommendations": st.session_state.recommendations
                }
                
                tmp_file.write(json.dumps(export_data, indent=2).encode())
                export_path = tmp_file.name
            
            with open(export_path, "rb") as file:
                st.download_button(
                    label="Download JSON",
                    data=file,
                    file_name="index_recommendations.json",
                    mime="application/json"
                )
    else:
        st.info("No recommendations yet. Click 'Generate Index Recommendations' to start.")

# Apply Indexes page
elif page == "Apply Indexes":
    st.header("Apply Recommended Indexes")
    
    # Check prerequisites
    if not st.session_state.connected:
        st.warning("Please connect to MongoDB first")
        st.stop()
    
    if not st.session_state.recommender or not st.session_state.recommendations:
        st.warning("Please generate index recommendations first")
        st.stop()
    
    # Display recommendations with apply buttons
    st.subheader("Select Indexes to Apply")
    
    applied_indexes = []
    
    for i, rec in enumerate(st.session_state.recommendations):
        col1, col2 = st.columns([3, 1])
        
        with col1:
            st.markdown(f"**Index on `{rec['collection']}` fields:** `{', '.join(rec['fields'])}`")
        
        with col2:
            # Apply button
            if st.button(f"Apply #{i+1}", key=f"apply_btn_{i}"):
                with st.spinner(f"Creating index on {rec['collection']}..."):
                    # Generate index spec
                    index_spec = st.session_state.recommender.generate_index_spec(rec)
                    
                    try:
                        # Create the index
                        index_name = st.session_state.db_queries.create_index(
                            rec['collection'], index_spec
                        )
                        
                        applied_indexes.append({
                            "collection": rec['collection'],
                            "fields": rec['fields'],
                            "index_name": index_name
                        })
                        
                        st.success(f"Index created: {index_name}")
                    except Exception as e:
                        st.error(f"Error creating index: {e}")
    
    # Show applied indexes
    if applied_indexes:
        st.subheader("Applied Indexes")
        
        for idx in applied_indexes:
            st.markdown(f"- Index `{idx['index_name']}` on collection `{idx['collection']}` (fields: {', '.join(idx['fields'])})")

# Handle app termination
def on_shutdown():
    if 'client' in st.session_state and st.session_state.client:
        st.session_state.client.close()

import atexit
atexit.register(on_shutdown)