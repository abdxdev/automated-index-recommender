import time
from pymongo import MongoClient
from db_connect import db_connect

class DBQueries:
    def __init__(self, database_name):
        self.client = db_connect()
        self.db = self.client[database_name]
        
    def list_collections(self):
        return self.db.list_collection_names()
    
    def get_collection_stats(self, collection_name):
        return self.db.command("collStats", collection_name)
    
    def get_collection_indexes(self, collection_name):
        return list(self.db[collection_name].list_indexes())
    
    def execute_query(self, collection_name, query, projection=None, sort=None, limit=None):
        collection = self.db[collection_name]
        start_time = time.time()
        
        # Get explain plan
        explain_plan = collection.find(query, projection).explain()
        
        # Execute query
        cursor = collection.find(query, projection)
        if sort:
            cursor = cursor.sort(sort)
        if limit:
            cursor = cursor.limit(limit)
        
        results = list(cursor)
        end_time = time.time()
        execution_time_ms = (end_time - start_time) * 1000
        
        return results, execution_time_ms, explain_plan
    
    def create_index(self, collection_name, index_spec, index_name=None, unique=False):
        collection = self.db[collection_name]
        return collection.create_index(index_spec, name=index_name, unique=unique)
    
    def drop_index(self, collection_name, index_name):
        collection = self.db[collection_name]
        collection.drop_index(index_name)
    
    def analyze_query_performance(self, collection_name, query, projection=None, sort=None, limit=None):
        collection = self.db[collection_name]
        
        # Get explain plan
        cursor = collection.find(query, projection)
        if sort:
            cursor = cursor.sort(sort)
        if limit:
            cursor = cursor.limit(limit)
            
        explain_plan = cursor.explain()
        
        # Execute query with timing
        _, execution_time_ms, _ = self.execute_query(
            collection_name, query, projection, sort, limit
        )
        
        # Extract key metrics
        index_used = "COLLSCAN" not in str(explain_plan)
        query_planner = explain_plan.get("queryPlanner", {})
        winning_plan = query_planner.get("winningPlan", {})
        
        metrics = {
            "execution_time_ms": execution_time_ms,
            "index_used": index_used,
            "winning_plan": winning_plan,
            "full_explain_plan": explain_plan
        }
        
        return metrics
    
    def compare_performance(self, collection_name, query, index_spec):
        # Get performance before index
        before_metrics = self.analyze_query_performance(collection_name, query)
        before_time = before_metrics["execution_time_ms"]
        before_index_used = before_metrics["index_used"]
        
        # Create the index
        index_name = self.create_index(collection_name, index_spec)
        
        # Get performance after index
        after_metrics = self.analyze_query_performance(collection_name, query)
        after_time = after_metrics["execution_time_ms"]
        after_index_used = after_metrics["index_used"]
        
        # Calculate improvement
        time_diff = before_time - after_time
        time_improvement_pct = (time_diff / before_time) * 100 if before_time > 0 else 0
        
        return {
            "before": {
                "execution_time_ms": before_time,
                "index_used": before_index_used
            },
            "after": {
                "execution_time_ms": after_time,
                "index_used": after_index_used,
                "index_created": index_name
            },
            "improvement": {
                "time_diff_ms": time_diff,
                "time_improvement_pct": time_improvement_pct
            }
        }

    def close(self):
        if self.client:
            self.client.close()