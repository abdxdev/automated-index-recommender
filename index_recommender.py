import json


class IndexRecommender:
    def __init__(self, db_queries):
        self.db_queries = db_queries
        self.query_results = []
        self.recommendations = []

    def run_test_queries(self, queries_file_path="queries.json"):
        """Run test queries from JSON file and collect performance data"""
        self.query_results = []

        with open(queries_file_path, "r") as f:
            query_data = json.load(f)

        for collection_data in query_data.get("queries", []):
            collection_name = collection_data.get("collection")

            all_collections = self.db_queries.list_collections()
            if collection_name not in all_collections:
                print(f"Collection {collection_name} not found in database. Skipping...")
                continue

            for query_item in collection_data.get("queries", []):
                query = query_item.get("query", {})
                projection = query_item.get("projection")

                sort = None
                if query_item.get("sort"):
                    sort = []
                    for sort_item in query_item.get("sort"):
                        if isinstance(sort_item, list) and len(sort_item) == 2:
                            sort.append((sort_item[0], sort_item[1]))

                limit = query_item.get("limit", 100)

                try:
                    results, execution_time_ms, explain = self.db_queries.execute_query(collection_name, query, projection, sort, limit)

                    is_indexed = "COLLSCAN" not in str(explain)

                    result = {"collection": collection_name, "query": query, "query_name": query_item.get("name", ""), "query_shape": str(query), "execution_time_ms": execution_time_ms, "is_indexed": is_indexed, "result_count": len(results)}

                    self.query_results.append(result)
                    print(f"Executed {query_item.get('name')} on {collection_name}: {execution_time_ms:.2f}ms, indexed: {is_indexed}")

                except Exception as e:
                    print(f"Error executing query {query_item.get('name')} on {collection_name}: {e}")

            return self.query_results

    def _extract_query_fields(self, query, prefix=""):
        fields = []

        for key, value in query.items():
            if key.startswith("$"):
                continue

            current_key = f"{prefix}.{key}" if prefix else key

            if isinstance(value, dict):
                if any(op.startswith("$") for op in value.keys()):
                    fields.append((current_key, value))
                else:
                    fields.extend(self._extract_query_fields(value, current_key))
            else:
                fields.append((current_key, value))

        return fields

    def extract_fields_from_query_shape(self, query_shape):
        """Extract field names from a query using pure JSON parsing without regex"""
        fields = []

        if not query_shape or query_shape in ["{}", "{}"]:
            return fields

        try:
            if isinstance(query_shape, str):
                try:
                    cleaned_shape = query_shape.replace("'", '"')
                    query_dict = json.loads(cleaned_shape)
                except json.JSONDecodeError:
                    first_field = query_shape.split(":")[0].strip().strip("{").strip("\"'")
                    if first_field and not first_field.startswith("$"):
                        return [first_field]
                    return []
            else:
                query_dict = query_shape

            for field in query_dict:
                if field and not field.startswith("$") and not field.isdigit() and field not in ["type", "coordinates", "geometry"]:
                    fields.append(field)

            return fields

        except Exception as e:
            print(f"Error extracting fields: {e}")
            return []

    def recommend_indexes(self):
        if not self.query_results:
            print("No query results found. Running test queries...")
            self.run_test_queries()

        query_patterns = {}
        for result in self.query_results:
            query_pattern = str(result.get("query_shape", {}))
            collection = result.get("collection", "")

            if (query_pattern, collection) in query_patterns:
                query_patterns[(query_pattern, collection)]["count"] += 1
                query_patterns[(query_pattern, collection)]["total_time"] += result.get("execution_time_ms", 0)
            else:
                query_patterns[(query_pattern, collection)] = {"count": 1, "total_time": result.get("execution_time_ms", 0), "collection": collection, "is_indexed": result.get("is_indexed", False)}

        candidates = []
        for (pattern, collection), stats in query_patterns.items():
            if stats.get("is_indexed", False):
                continue

            avg_time = stats["total_time"] / stats["count"] if stats["count"] > 0 else 0

            if (avg_time > 50 and stats["count"] >= 1) or avg_time > 100:
                candidates.append({"pattern": pattern, "avg_execution_time_ms": avg_time, "execution_count": stats["count"], "collection": stats["collection"]})

        recommendations = []
        for candidate in candidates:
            collection = candidate.get("collection")
            if not collection:
                continue

            query_pattern = candidate.get("pattern")
            fields = self.extract_fields_from_query_shape(query_pattern)

            if fields:
                recommendation = {"collection": collection, "fields": fields, "query_pattern": query_pattern, "avg_execution_time_ms": candidate.get("avg_execution_time_ms"), "execution_count": candidate.get("execution_count"), "potential_impact": candidate.get("avg_execution_time_ms") * candidate.get("execution_count")}
                recommendations.append(recommendation)

        recommendations.sort(key=lambda x: x.get("potential_impact", 0), reverse=True)
        self.recommendations = recommendations
        return recommendations

    def generate_index_spec(self, recommendation):
        index_spec = []

        for field in recommendation.get("fields", []):
            index_spec.append((field, 1))

        return index_spec
