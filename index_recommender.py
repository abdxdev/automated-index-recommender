import json
import re

class IndexRecommender:
    def __init__(self, db_queries):
        self.db_queries = db_queries
        self.query_results = []
        self.recommendations = []
        
    def run_test_queries(self, queries_file_path="queries.json"):
        """Run test queries from JSON file and collect performance data"""
        self.query_results = []
        
        try:
            with open(queries_file_path, 'r') as f:
                query_data = json.load(f)
                
            # Iterate through collections and their queries
            for collection_data in query_data.get("queries", []):
                collection_name = collection_data.get("collection")
                
                # Check if collection exists in the database
                all_collections = self.db_queries.list_collections()
                if collection_name not in all_collections:
                    print(f"Collection {collection_name} not found in database. Skipping...")
                    continue
                    
                # Run each query for this collection
                for query_item in collection_data.get("queries", []):
                    query = query_item.get("query", {})
                    projection = query_item.get("projection")
                    
                    # Convert sort format from JSON to MongoDB format
                    sort = None
                    if query_item.get("sort"):
                        try:
                            # Handle sort as a list of [field, direction] lists
                            sort = []
                            for sort_item in query_item.get("sort"):
                                if isinstance(sort_item, list) and len(sort_item) == 2:
                                    sort.append((sort_item[0], sort_item[1]))
                        except Exception as e:
                            print(f"Error processing sort parameter: {e}")
                        
                    limit = query_item.get("limit", 100)
                    
                    try:
                        # Execute query and record results
                        results, execution_time_ms, explain = self.db_queries.execute_query(
                            collection_name, query, projection, sort, limit
                        )
                        
                        # Check if index was used
                        is_indexed = "COLLSCAN" not in str(explain)
                        
                        # Record query result
                        result = {
                            "collection": collection_name,
                            "query": query,
                            "query_name": query_item.get("name", ""),
                            "query_shape": str(query),
                            "execution_time_ms": execution_time_ms,
                            "is_indexed": is_indexed,
                            "result_count": len(results)
                        }
                        
                        self.query_results.append(result)
                        print(f"Executed {query_item.get('name')} on {collection_name}: {execution_time_ms:.2f}ms, indexed: {is_indexed}")
                        
                    except Exception as e:
                        print(f"Error executing query {query_item.get('name')} on {collection_name}: {e}")
                        
            return self.query_results
                
        except Exception as e:
            print(f"Error loading or processing queries file: {e}")
            return []
    
    def analyze_query(self, query):
        index_fields = []
        sort_fields = []
        
        # Analyze query operators
        for field, value in self._extract_query_fields(query):
            if field not in index_fields:
                index_fields.append(field)
                
        # Include sort fields
        if "sort" in query:
            for field, direction in query["sort"].items():
                sort_dir = 1 if direction > 0 else -1
                sort_fields.append((field, sort_dir))
                
        return {
            "query": query,
            "recommended_fields": index_fields,
            "sort_fields": sort_fields
        }
    
    def _extract_query_fields(self, query, prefix=""):
        fields = []
        
        for key, value in query.items():
            # Skip operators
            if key.startswith("$"):
                continue
                
            current_key = f"{prefix}.{key}" if prefix else key
            
            if isinstance(value, dict):
                # Handle query operators or nested documents
                if any(op.startswith("$") for op in value.keys()):
                    fields.append((current_key, value))
                else:
                    fields.extend(self._extract_query_fields(value, current_key))
            else:
                fields.append((current_key, value))
                
        return fields
    
    def extract_fields_from_query_shape(self, query_shape):
        fields = []
        
        # Handle empty query
        if not query_shape or query_shape in ['{}', '{}']: 
            return fields
            
        try:
            # Extract field names with regex
            field_matches = re.findall(r'["\']?([\w\.]+)["\']?\s*:', query_shape)
            
            # Extract fields from special operators
            nested_matches = re.findall(r'\$near[\s\S]*?["\']?([\w\.]+)["\']?\s*:', query_shape)
            
            all_matches = field_matches + nested_matches
            
            for field in all_matches:
                # Filter out operators and duplicates
                if field not in fields and not field.startswith("$") and field not in ["type", "coordinates", "geometry"]:
                    fields.append(field)
            
            return fields
        except Exception as e:
            print(f"Error extracting fields: {e}")
            return []
    
    def recommend_indexes(self):
        if not self.query_results:
            print("No query results found. Running test queries...")
            self.run_test_queries()
            
        # Process query results
        query_patterns = {}
        for result in self.query_results:
            query_pattern = str(result.get("query_shape", {}))
            collection = result.get("collection", "")
            
            if (query_pattern, collection) in query_patterns:
                query_patterns[(query_pattern, collection)]["count"] += 1
                query_patterns[(query_pattern, collection)]["total_time"] += result.get("execution_time_ms", 0)
            else:
                query_patterns[(query_pattern, collection)] = {
                    "count": 1,
                    "total_time": result.get("execution_time_ms", 0),
                    "collection": collection,
                    "is_indexed": result.get("is_indexed", False)
                }
        
        # Generate candidates for indexing
        candidates = []
        for (pattern, collection), stats in query_patterns.items():
            # Skip already indexed patterns
            if stats.get("is_indexed", False):
                continue
                
            avg_time = stats["total_time"] / stats["count"] if stats["count"] > 0 else 0
            
            if (avg_time > 50 and stats["count"] >= 1) or avg_time > 100:
                candidates.append({
                    "pattern": pattern,
                    "avg_execution_time_ms": avg_time,
                    "execution_count": stats["count"],
                    "collection": stats["collection"]
                })
        
        # Process candidates into recommendations
        recommendations = []
        for candidate in candidates:
            collection = candidate.get("collection")
            if not collection:
                continue
                
            query_pattern = candidate.get("pattern")
            fields = self.extract_fields_from_query_shape(query_pattern)
            
            if fields:
                recommendation = {
                    "collection": collection,
                    "fields": fields,
                    "query_pattern": query_pattern,
                    "avg_execution_time_ms": candidate.get("avg_execution_time_ms"),
                    "execution_count": candidate.get("execution_count"),
                    "potential_impact": candidate.get("avg_execution_time_ms") * candidate.get("execution_count")
                }
                recommendations.append(recommendation)
                
        # Sort by potential impact
        recommendations.sort(key=lambda x: x.get("potential_impact", 0), reverse=True)
        self.recommendations = recommendations
        return recommendations
    
    def generate_index_spec(self, recommendation):
        index_spec = []
        
        for field in recommendation.get("fields", []):
            # Default to ascending index
            index_spec.append((field, 1))
            
        return index_spec
    
    def validate_recommendation(self, collection, recommendation):
        # Check existing indexes
        existing_indexes = self.db_queries.get_collection_indexes(collection)
        similar_index = None
        
        recommended_fields = recommendation.get("fields", [])
        
        for index in existing_indexes:
            index_keys = index.get("key", {})
            index_fields = list(index_keys.keys())
            
            # Check if existing index covers recommended fields
            if all(field in index_fields for field in recommended_fields):
                similar_index = index.get("name")
                break
        
        # Return validation result
        if similar_index:
            return {
                "recommendation": recommendation,
                "is_valid": False,
                "reason": f"Similar index already exists: {similar_index}",
                "similar_index": similar_index
            }
            
        return {
            "recommendation": recommendation,
            "is_valid": True
        }
    
    def apply_recommendation(self, collection, recommendation):
        # Generate sample query
        fields = recommendation.get("fields", [])
        if not fields:
            return {"success": False, "reason": "No fields to index"}
        
        # Create simple query using first field
        sample_query = {fields[0]: {"$exists": True}}
        
        # Generate index spec
        index_spec = self.generate_index_spec(recommendation)
        
        # Compare performance
        comparison = self.db_queries.compare_performance(collection, sample_query, index_spec)
        
        return {
            "recommendation": recommendation,
            "index_spec": index_spec,
            "performance_comparison": comparison,
            "success": True
        }
    
    def get_top_recommendations(self, limit=5):
        if not self.recommendations:
            self.recommend_indexes()
            
        return self.recommendations[:limit]
    
    def estimate_storage_impact(self, collection, index_spec):
        # Get collection stats
        stats = self.db_queries.get_collection_stats(collection)
        
        # Estimate index size
        avg_doc_size = stats.get("avgObjSize", 0)
        num_docs = stats.get("count", 0)
        
        # Rough estimation: 12 bytes per indexed field per document
        num_fields = len(index_spec)
        estimated_bytes_per_doc = 12 * num_fields
        estimated_index_size = estimated_bytes_per_doc * num_docs
        
        return {
            "collection": collection,
            "document_count": num_docs,
            "estimated_index_size_bytes": estimated_index_size,
            "estimated_index_size_mb": estimated_index_size / (1024 * 1024)
        }
    
    def export_recommendations(self, file_path):
        export_data = {
            "timestamp": "",
            "recommendations": self.recommendations
        }
        
        with open(file_path, 'w') as f:
            json.dump(export_data, f, indent=2)