import os
import sys
import json
import time
import tempfile
import importlib

import pandas as pd
import streamlit as st

from db_queries import DBQueries
from db_connect import db_connect
from index_recommender import IndexRecommender

st.set_page_config(page_title="MongoDB Index Recommender", page_icon="📊", layout="wide")

st.title("MongoDB Automated Index Recommendation System")

if "connected" not in st.session_state:
    st.session_state.connected = False
if "client" not in st.session_state:
    st.session_state.client = None
if "db_queries" not in st.session_state:
    st.session_state.db_queries = None
if "recommender" not in st.session_state:
    st.session_state.recommender = None
if "query_results" not in st.session_state:
    st.session_state.query_results = []
if "recommendations" not in st.session_state:
    st.session_state.recommendations = []
if "selected_db" not in st.session_state:
    st.session_state.selected_db = None

st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["Connect to MongoDB", "Analyze Queries", "View Recommendations", "Apply Indexes"])

if page == "Connect to MongoDB":
    st.header("Connect to MongoDB")

    with st.form("mongodb_connection_form"):
        mongo_uri = st.text_input("MongoDB Connection String", "mongodb://localhost:27017")
        submit_button = st.form_submit_button("Connect")

        if submit_button:
            with st.spinner("Connecting to MongoDB..."):
                try:
                    os.environ["MONGO_URL"] = mongo_uri
                    client = db_connect()

                    st.session_state.client = client
                    st.session_state.connected = True

                    st.success("Successfully connected to MongoDB!")

                    databases = client.list_database_names()
                    st.session_state.databases = databases

                    if "sample_mflix" in databases:
                        selected_db = "sample_mflix"
                    else:
                        selected_db = databases[0] if databases else None

                    if selected_db:
                        st.session_state.selected_db = selected_db
                        st.session_state.db_queries = DBQueries(selected_db)

                        st.session_state.recommender = IndexRecommender(st.session_state.db_queries)

                        collections = st.session_state.db_queries.list_collections()
                        st.write(f"Collections in {selected_db}:", collections)

                except Exception as e:
                    st.error(f"Error connecting to MongoDB: {e}")

    if st.session_state.connected:
        st.success("✅ Connected to MongoDB")

        if st.session_state.selected_db:
            st.write(f"Current Database: **{st.session_state.selected_db}**")

            new_db = st.selectbox("Change Database", st.session_state.databases, index=st.session_state.databases.index(st.session_state.selected_db))

            if new_db != st.session_state.selected_db:
                st.session_state.selected_db = new_db
                st.session_state.db_queries = DBQueries(new_db)
                st.session_state.recommender = IndexRecommender(st.session_state.db_queries)
                st.session_state.query_results = []
                st.session_state.recommendations = []
                st.rerun()
    else:
        st.warning("⚠️ Not connected to MongoDB")

elif page == "Analyze Queries":
    st.header("Analyze MongoDB Queries")

    if not st.session_state.connected:
        st.warning("Please connect to MongoDB first")
        st.stop()

    if not st.session_state.recommender:
        try:
            st.session_state.recommender = IndexRecommender(st.session_state.db_queries)
            st.info("Initialized new index recommender")
        except Exception as e:
            st.error(f"Failed to initialize recommender: {e}")
            st.stop()

    uploaded_queries = st.file_uploader("Upload custom queries JSON file (optional)", type=["json"])

    if uploaded_queries:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as tmp_file:
            tmp_file.write(uploaded_queries.getvalue())
            queries_file_path = tmp_file.name
    else:
        if os.path.exists("queries.json"):
            queries_file_path = "queries.json"
        else:
            full_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "queries.json")
            if os.path.exists(full_path):
                queries_file_path = full_path
            else:
                st.error("queries.json file not found in the current directory")
                st.stop()

    if st.button("Run Query Analysis"):
        with st.spinner("Running test queries and analyzing performance..."):
            try:
                if not hasattr(st.session_state.recommender, "run_test_queries"):
                    st.error("The recommender does not have a run_test_queries method. Reinitializing...")
                    if "index_recommender" in sys.modules:
                        importlib.reload(sys.modules["index_recommender"])
                    st.session_state.recommender = IndexRecommender(st.session_state.db_queries)
                    if not hasattr(st.session_state.recommender, "run_test_queries"):
                        st.error("Still cannot find run_test_queries method after reload.")
                        st.stop()

                query_results = st.session_state.recommender.run_test_queries(queries_file_path)

                st.session_state.query_results = query_results

                st.success(f"Successfully analyzed {len(query_results)} queries")
            except Exception as e:
                st.error(f"Error analyzing queries: {e}")

    if uploaded_queries and "queries_file_path" in locals():
        try:
            os.unlink(queries_file_path)
        except:
            pass

    if st.session_state.query_results:
        st.subheader("Query Statistics")
        col1, col2 = st.columns(2)

        with col1:
            st.metric("Total Queries", len(st.session_state.query_results))

        with col2:
            slow_queries = [q for q in st.session_state.query_results if q.get("execution_time_ms", 0) > 100]
            st.metric("Slow Queries (>100ms)", len(slow_queries))

        st.subheader("Query Analysis Results")

        show_only_slow = st.checkbox("Show only slow queries (>100ms)", value=False)

        if show_only_slow:
            display_queries = slow_queries
        else:
            display_queries = st.session_state.query_results

        if not display_queries:
            st.info("No queries to display with current filter. Try changing the filter option.")
        else:
            df_queries = pd.DataFrame([{"Collection": q.get("collection"), "Query Name": q.get("query_name", ""), "Time (ms)": round(q.get("execution_time_ms", 0), 2), "Indexed?": "✅" if q.get("is_indexed", False) else "❌", "Results": q.get("result_count", 0), "Query": str(q.get("query", ""))[:50] + "..." if len(str(q.get("query", ""))) > 50 else str(q.get("query", ""))} for q in display_queries])

            df_queries = df_queries.sort_values(by="Time (ms)", ascending=False)

            st.dataframe(df_queries)

elif page == "View Recommendations":
    st.header("Index Recommendations")

    if not st.session_state.connected:
        st.warning("Please connect to MongoDB first")
        st.stop()

    if st.session_state.db_queries and not st.session_state.recommender:
        st.session_state.recommender = IndexRecommender(st.session_state.db_queries)

    if st.button("Generate Recommendations"):
        with st.spinner("Analyzing queries..."):
            recommender = st.session_state.recommender
            recommendations = recommender.recommend_indexes()

            st.session_state.recommendations = recommendations

            if recommendations:
                st.success(f"Generated {len(recommendations)} recommendations")
            else:
                st.info("No recommendations found")

    if st.session_state.recommendations:
        recommendation_data = []
        for i, rec in enumerate(st.session_state.recommendations):
            recommendation_data.append({"#": i + 1, "Collection": rec["collection"], "Fields": ", ".join(rec["fields"]), "Query Time (ms)": round(rec["avg_execution_time_ms"], 2), "Count": rec["execution_count"]})

        st.table(pd.DataFrame(recommendation_data))

        selected_index = st.selectbox("View details for recommendation:", range(1, len(st.session_state.recommendations) + 1), format_func=lambda x: f"#{x}")

        if selected_index:
            rec = st.session_state.recommendations[selected_index - 1]
            st.write(f"Collection: {rec['collection']}")
            st.write(f"Fields to index: {', '.join(rec['fields'])}")
            st.write(f"Query pattern: {rec['query_pattern']}")
            st.write(f"Average execution time: {rec['avg_execution_time_ms']:.2f} ms")
            st.write(f"Execution count: {rec['execution_count']}")

        if st.button("Export as JSON"):
            with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as tmp_file:
                export_data = {"timestamp": time.strftime("%Y-%m-%d %H:%M:%S"), "recommendations": st.session_state.recommendations}

                tmp_file.write(json.dumps(export_data, indent=2).encode())
                export_path = tmp_file.name

            with open(export_path, "rb") as file:
                st.download_button(label="Download", data=file, file_name="index_recommendations.json", mime="application/json")
    else:
        st.info("No recommendations yet")

elif page == "Apply Indexes":
    st.header("Apply Recommended Indexes")

    if not st.session_state.connected:
        st.warning("Please connect to MongoDB first")
        st.stop()

    if not st.session_state.recommender or not st.session_state.recommendations:
        st.warning("Please generate index recommendations first")
        st.stop()

    st.subheader("Select Indexes to Apply")

    if "applied_indexes" not in st.session_state:
        st.session_state.applied_indexes = []

    for i, rec in enumerate(st.session_state.recommendations):
        col1, col2 = st.columns([3, 1])

        with col1:
            st.markdown(f"**Index on `{rec['collection']}` fields:** `{', '.join(rec['fields'])}`")
            st.caption(f"Current query time: {rec['avg_execution_time_ms']:.2f} ms")

        with col2:
            if st.button(f"Apply #{i+1}", key=f"apply_btn_{i}"):
                with st.spinner(f"Creating index on {rec['collection']}..."):
                    previous_time = rec.get("avg_execution_time_ms", 0)

                    index_spec = st.session_state.recommender.generate_index_spec(rec)

                    try:
                        index_name = st.session_state.db_queries.create_index(rec["collection"], index_spec)

                        fields = rec.get("fields", [])
                        sample_query = {fields[0]: {"$exists": True}} if fields else {"_id": {"$exists": True}}

                        results, current_time, explain = st.session_state.db_queries.execute_query(rec["collection"], sample_query)

                        time_diff = previous_time - current_time
                        improvement_pct = (time_diff / previous_time) * 100 if previous_time > 0 else 0

                        st.session_state.applied_indexes.append({"collection": rec["collection"], "fields": rec["fields"], "index_name": index_name, "previous_time": previous_time, "current_time": current_time, "improvement_pct": improvement_pct, "timestamp": time.strftime("%H:%M:%S"), "is_indexed": "COLLSCAN" not in str(explain)})

                        st.success(f"Index '{index_name}' created successfully!")

                        performance_col1, performance_col2, performance_col3 = st.columns([1, 1, 1])

                        with performance_col1:
                            st.metric("Before", f"{previous_time:.2f} ms")

                        with performance_col2:
                            st.metric("After", f"{current_time:.2f} ms")

                        with performance_col3:
                            st.metric("Improvement", f"{improvement_pct:.1f}%", delta=f"{improvement_pct:.1f}%", delta_color="normal")

                        if "COLLSCAN" not in str(explain):
                            st.success("✅ Index is being used for this query!")
                        else:
                            st.warning("⚠️ Index is not being used for this query.")

                    except Exception as e:
                        st.error(f"Error creating index: {e}")

    if st.session_state.applied_indexes:
        st.subheader("Performance Improvements")

        performance_data = []
        for idx in st.session_state.applied_indexes:
            performance_data.append({"Collection": idx["collection"], "Fields": ", ".join(idx["fields"]), "Previous (ms)": round(idx["previous_time"], 2), "Current (ms)": round(idx["current_time"], 2), "Improvement": f"{round(idx['improvement_pct'], 1)}%", "Index Used": "Yes" if idx.get("is_indexed", False) else "No"})

        if performance_data:
            st.dataframe(pd.DataFrame(performance_data))

            st.subheader("Performance Visualization")

            improved_indexes = [idx for idx in st.session_state.applied_indexes if idx["improvement_pct"] > 0]

            if improved_indexes:
                import matplotlib.pyplot as plt

                fig1, ax1 = plt.subplots(figsize=(10, 5))

                collections = [f"{idx['collection']}\n({', '.join(idx['fields'][:2])}{'...' if len(idx['fields']) > 2 else ''})" for idx in improved_indexes]
                prev_times = [idx["previous_time"] for idx in improved_indexes]
                curr_times = [idx["current_time"] for idx in improved_indexes]

                x = range(len(collections))
                bar_width = 0.35

                ax1.bar([i - bar_width / 2 for i in x], prev_times, bar_width, label="Before Index", color="#FF7F50")
                ax1.bar([i + bar_width / 2 for i in x], curr_times, bar_width, label="After Index", color="#4682B4")

                for i, v in enumerate(prev_times):
                    ax1.text(i - bar_width / 2, v + 1, f"{v:.1f}", ha="center", fontsize=9)

                for i, v in enumerate(curr_times):
                    ax1.text(i + bar_width / 2, v + 1, f"{v:.1f}", ha="center", fontsize=9)

                ax1.set_xlabel("Collection and Fields")
                ax1.set_ylabel("Query Time (ms)")
                ax1.set_title("Before vs After Indexing")
                ax1.set_xticks(x)
                ax1.set_xticklabels(collections, rotation=45, ha="right")
                ax1.legend()

                plt.tight_layout()
                st.pyplot(fig1)

                fig2, ax2 = plt.subplots(figsize=(10, 4))

                improvements = [idx["improvement_pct"] for idx in improved_indexes]

                for i, v in enumerate(improvements):
                    ax2.text(i, v + 1, f"{v:.1f}%", ha="center")

                ax2.set_xlabel("Collection and Fields")
                ax2.set_ylabel("Improvement (%)")
                ax2.set_title("Percentage Improvement After Indexing")
                ax2.set_xticklabels(collections, rotation=45, ha="right")

                plt.tight_layout()
                st.pyplot(fig2)
            else:
                st.info("No performance improvements detected for the applied indexes.")

        if st.button("Re-analyze Queries"):
            with st.spinner("Re-running queries to verify performance improvements..."):
                recommender = st.session_state.recommender
                query_results = recommender.run_test_queries()
                st.session_state.query_results = query_results

                recommendations = recommender.recommend_indexes()
                st.session_state.recommendations = recommendations

                st.success("Re-analysis complete!")
                st.rerun()

        if st.button("Clear Applied Indexes Data"):
            st.session_state.applied_indexes = []
            st.info("Applied indexes data cleared.")
            st.rerun()


def on_shutdown():
    if "client" in st.session_state and st.session_state.client:
        st.session_state.client.close()


import atexit

atexit.register(on_shutdown)
