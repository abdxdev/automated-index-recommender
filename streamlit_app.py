import streamlit as st
import pandas as pd
import json
import tempfile
import os
import time
import matplotlib.pyplot as plt
import importlib
import sys

# Force reload of modules to fix caching issues
if "db_connect" in sys.modules:
    importlib.reload(sys.modules["db_connect"])
if "db_queries" in sys.modules:
    importlib.reload(sys.modules["db_queries"])
if "index_recommender" in sys.modules:
    importlib.reload(sys.modules["index_recommender"])

from db_connect import db_connect
from db_queries import DBQueries
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
if 'recommender' not in st.session_state:
    st.session_state.recommender = None
if 'query_results' not in st.session_state:
    st.session_state.query_results = []
if 'recommendations' not in st.session_state:
    st.session_state.recommendations = []
if 'selected_db' not in st.session_state:
    st.session_state.selected_db = None

# Sidebar navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["Connect to MongoDB", "Analyze Queries", 
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
                    
                    # Default to sample_mflix database if available
                    if "sample_mflix" in databases:
                        selected_db = "sample_mflix"
                    else:
                        selected_db = databases[0] if databases else None
                    
                    if selected_db:
                        st.session_state.selected_db = selected_db
                        st.session_state.db_queries = DBQueries(selected_db)
                        
                        # Initialize recommender right away
                        st.session_state.recommender = IndexRecommender(st.session_state.db_queries)
                        
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
                st.session_state.recommender = IndexRecommender(st.session_state.db_queries)
                st.session_state.query_results = []
                st.session_state.recommendations = []
                st.rerun()
    else:
        st.warning("⚠️ Not connected to MongoDB")

# Analyze Queries page
elif page == "Analyze Queries":
    st.header("Analyze MongoDB Queries")
    
    # Check if connected to MongoDB
    if not st.session_state.connected:
        st.warning("Please connect to MongoDB first")
        st.stop()
    
    # Ensure recommender is initialized
    if not st.session_state.recommender:
        try:
            st.session_state.recommender = IndexRecommender(st.session_state.db_queries)
            st.info("Initialized new index recommender")
        except Exception as e:
            st.error(f"Failed to initialize recommender: {e}")
            st.stop()
    
    # Custom queries JSON file uploader
    uploaded_queries = st.file_uploader("Upload custom queries JSON file (optional)", type=["json"])
    
    if uploaded_queries:
        # Create a temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix='.json') as tmp_file:
            tmp_file.write(uploaded_queries.getvalue())
            queries_file_path = tmp_file.name
    else:
        # Make sure the file exists in the current directory
        if os.path.exists("queries.json"):
            queries_file_path = "queries.json"
        else:
            full_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "queries.json")
            if os.path.exists(full_path):
                queries_file_path = full_path
            else:
                st.error("queries.json file not found in the current directory")
                st.stop()
    
    # Run query analysis
    if st.button("Run Query Analysis"):
        with st.spinner("Running test queries and analyzing performance..."):
            try:
                # Check if the run_test_queries method exists
                if not hasattr(st.session_state.recommender, 'run_test_queries'):
                    st.error("The recommender does not have a run_test_queries method. Reinitializing...")
                    # Force reload the module
                    if "index_recommender" in sys.modules:
                        importlib.reload(sys.modules["index_recommender"])
                    # Create a new recommender instance
                    st.session_state.recommender = IndexRecommender(st.session_state.db_queries)
                    if not hasattr(st.session_state.recommender, 'run_test_queries'):
                        st.error("Still cannot find run_test_queries method after reload.")
                        st.stop()
                
                # Run queries and analyze results
                query_results = st.session_state.recommender.run_test_queries(queries_file_path)
                
                # Store results in session state
                st.session_state.query_results = query_results
                
                st.success(f"Successfully analyzed {len(query_results)} queries")
                
                # Show stats
                st.subheader("Query Statistics")
                col1, col2 = st.columns(2)
                
                with col1:
                    st.metric("Total Queries", len(query_results))
                
                with col2:
                    slow_queries = [q for q in query_results if q.get("execution_time_ms", 0) > 100]
                    st.metric("Slow Queries (>100ms)", len(slow_queries))
                
                # Display sample of test queries
                if query_results:
                    st.subheader("Sample of Test Queries")
                    
                    df = pd.DataFrame([
                        {
                            'collection': q.get('collection'),
                            'query_name': q.get('query_name', ''),
                            'execution_time_ms': round(q.get('execution_time_ms', 0), 2),
                            'is_indexed': q.get('is_indexed', False),
                            'results': q.get('result_count', 0),
                            'query': str(q.get('query', ''))[:50] + '...' if len(str(q.get('query', ''))) > 50 else str(q.get('query', ''))
                        }
                        for q in query_results[:10]
                    ])
                    
                    st.dataframe(df)
            except Exception as e:
                st.error(f"Error analyzing queries: {e}")
    
    # Clean up uploaded file if needed
    if uploaded_queries and 'queries_file_path' in locals():
        try:
            os.unlink(queries_file_path)
        except:
            pass
    
    # If queries are already analyzed, show stats
    if st.session_state.query_results:
        st.subheader("Query Statistics")
        col1, col2 = st.columns(2)
        
        with col1:
            st.metric("Total Queries", len(st.session_state.query_results))
        
        with col2:
            slow_queries = [q for q in st.session_state.query_results if q.get("execution_time_ms", 0) > 100]
            st.metric("Slow Queries (>100ms)", len(slow_queries))
            
        # Display slow queries
        if slow_queries:
            st.subheader("Slow Queries")
            
            df_slow = pd.DataFrame([
                {
                    'collection': q.get('collection'),
                    'query_name': q.get('query_name', ''),
                    'execution_time_ms': round(q.get('execution_time_ms', 0), 2),
                    'is_indexed': q.get('is_indexed', False),
                    'results': q.get('result_count', 0),
                    'query': str(q.get('query', ''))[:50] + '...' if len(str(q.get('query', ''))) > 50 else str(q.get('query', ''))
                }
                for q in slow_queries[:10]
            ])
            
            st.dataframe(df_slow)

# View Recommendations page
elif page == "View Recommendations":
    st.header("Index Recommendations")
    
    # Check if connected to MongoDB
    if not st.session_state.connected:
        st.warning("Please connect to MongoDB first")
        st.stop()
    
    # Initialize recommender if needed
    if st.session_state.db_queries and not st.session_state.recommender:
        st.session_state.recommender = IndexRecommender(st.session_state.db_queries)
    
    # Generate recommendations button
    if st.button("Generate Index Recommendations"):
        with st.spinner("Analyzing query patterns and generating recommendations..."):
            # Generate recommendations
            recommender = st.session_state.recommender
            recommendations = recommender.recommend_indexes()
            
            # Store in session state
            st.session_state.recommendations = recommendations
            
            if recommendations:
                st.success(f"Generated {len(recommendations)} index recommendations")
            else:
                st.info("No index recommendations found. All queries may already be optimized with existing indexes.")
    
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
                    # Save previous execution time
                    previous_time = rec.get("avg_execution_time_ms", 0)
                    
                    # Generate index spec
                    index_spec = st.session_state.recommender.generate_index_spec(rec)
                    
                    try:
                        # Create the index
                        index_name = st.session_state.db_queries.create_index(
                            rec['collection'], index_spec
                        )
                        
                        # Test the performance with the new index
                        fields = rec.get("fields", [])
                        sample_query = {fields[0]: {"$exists": True}} if fields else {"_id": {"$exists": True}}
                        
                        # Execute the query with the new index
                        results, current_time, explain = st.session_state.db_queries.execute_query(
                            rec['collection'], sample_query
                        )
                        
                        # Calculate improvement
                        time_diff = previous_time - current_time
                        improvement_pct = (time_diff / previous_time) * 100 if previous_time > 0 else 0
                        
                        applied_indexes.append({
                            "collection": rec['collection'],
                            "fields": rec['fields'],
                            "index_name": index_name,
                            "previous_time": previous_time,
                            "current_time": current_time,
                            "improvement_pct": improvement_pct
                        })
                        
                        # Display performance improvement
                        st.success(f"Index created: {index_name}")
                        
                        # Show performance metrics in an expandable section
                        with st.expander("Performance Improvement Details"):
                            st.markdown(f"**Previous execution time:** {previous_time:.2f} ms")
                            st.markdown(f"**Current execution time:** {current_time:.2f} ms")
                            st.markdown(f"**Improvement:** {improvement_pct:.2f}%")
                            
                            # Add visual indicator
                            if improvement_pct > 50:
                                st.success(f"Significant improvement! ✅")
                            elif improvement_pct > 20:
                                st.info(f"Moderate improvement! ℹ️")
                            elif improvement_pct > 0:
                                st.warning(f"Minimal improvement. ⚠️")
                            else:
                                st.error(f"No improvement or performance decreased. ❌")
                    
                    except Exception as e:
                        st.error(f"Error creating index: {e}")
    
    # Show applied indexes
    if applied_indexes:
        st.subheader("Applied Indexes")
        
        # Create a table of applied indexes with performance metrics
        performance_data = []
        for idx in applied_indexes:
            performance_data.append({
                "Collection": idx['collection'],
                "Fields": ", ".join(idx['fields']),
                "Previous Time (ms)": round(idx['previous_time'], 2),
                "Current Time (ms)": round(idx['current_time'], 2),
                "Improvement (%)": round(idx['improvement_pct'], 2)
            })
        
        if performance_data:
            st.table(pd.DataFrame(performance_data))
            
            # Create a simple bar chart to visualize improvements
            if len(performance_data) > 0:
                fig, ax = plt.subplots(figsize=(10, 5))
                
                collections = [data["Collection"] + "\n" + data["Fields"] for data in performance_data]
                prev_times = [data["Previous Time (ms)"] for data in performance_data]
                curr_times = [data["Current Time (ms)"] for data in performance_data]
                
                x = range(len(collections))
                bar_width = 0.35
                
                ax.bar([i - bar_width/2 for i in x], prev_times, bar_width, label='Before Index', color='coral')
                ax.bar([i + bar_width/2 for i in x], curr_times, bar_width, label='After Index', color='skyblue')
                
                ax.set_xlabel('Collection and Fields')
                ax.set_ylabel('Execution Time (ms)')
                ax.set_title('Query Performance Before and After Indexing')
                ax.set_xticks(x)
                ax.set_xticklabels(collections, rotation=45, ha='right')
                ax.legend()
                
                plt.tight_layout()
                st.pyplot(fig)
        
        # Option to re-analyze after applying indexes
        if st.button("Re-analyze Queries"):
            with st.spinner("Re-running queries to verify performance improvements..."):
                # Re-run test queries
                recommender = st.session_state.recommender
                query_results = recommender.run_test_queries()
                st.session_state.query_results = query_results
                
                # Generate new recommendations
                recommendations = recommender.recommend_indexes()
                st.session_state.recommendations = recommendations
                
                st.success("Re-analysis complete!")
                st.rerun()

# Handle app termination
def on_shutdown():
    if 'client' in st.session_state and st.session_state.client:
        st.session_state.client.close()

import atexit
atexit.register(on_shutdown)