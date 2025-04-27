import re

class LogParser:
    def __init__(self):
        self.slow_queries = []
        self.query_patterns = {}
        
    def parse_log_file(self, log_file_path):
        try:
            parsed_queries = []
            with open(log_file_path, 'r') as file:
                for line in file:
                    if "COMMAND" in line and "command:" in line:
                        query_info = self._extract_query_info(line)
                        
                        if query_info and query_info.get("execution_time_ms") is not None:
                            parsed_queries.append(query_info)
                            
                            if query_info.get("execution_time_ms", 0) > 100:
                                self.slow_queries.append(query_info)
                                
                            query_pattern = str(query_info.get("query_shape", {}))
                            if query_pattern in self.query_patterns:
                                self.query_patterns[query_pattern]["count"] += 1
                                self.query_patterns[query_pattern]["total_time"] += query_info.get("execution_time_ms", 0)
                            else:
                                self.query_patterns[query_pattern] = {
                                    "count": 1,
                                    "total_time": query_info.get("execution_time_ms", 0),
                                    "collection": query_info.get("collection", "")
                                }
            
            return parsed_queries
        except Exception as e:
            print(f"Error parsing log file: {e}")
            return []
    
    def _extract_query_info(self, log_line):
        try:
            timestamp_match = re.search(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})', log_line)
            timestamp = timestamp_match.group(1) if timestamp_match else None
            
            collection_match = re.search(r'command: (\w+)\.(\w+)', log_line)
            db_name = collection_match.group(1) if collection_match else None
            collection = collection_match.group(2) if collection_match else None
            
            query = None
            operation_type = None
            
            if "find" in log_line:
                operation_type = "find"
                query_match = re.search(r'filter: ({.*?})', log_line)
                query = query_match.group(1) if query_match else None
            elif "insert" in log_line:
                operation_type = "insert"
            elif "update" in log_line:
                operation_type = "update"
                query_match = re.search(r'q: ({.*?})', log_line)
                query = query_match.group(1) if query_match else None
            
            time_match = re.search(r'durationMillis: (\d+)', log_line)
            execution_time_ms = int(time_match.group(1)) if time_match else 0
            
            is_collscan = "COLLSCAN" in log_line
            
            query_shape = self._extract_query_shape(query) if query else "{}"
            
            return {
                "timestamp": timestamp,
                "database": db_name,
                "collection": collection,
                "operation_type": operation_type,
                "query": query,
                "query_shape": query_shape,
                "execution_time_ms": execution_time_ms,
                "is_collscan": is_collscan,
                "raw_log": log_line
            }
        except Exception as e:
            print(f"Error extracting query info: {e}")
            return {}
    
    def _extract_query_shape(self, query_str):
        try:
            if not query_str:
                return {}
                
            shape = query_str
            
            shape = re.sub(r'ObjectId\([\'"]([^\'"]+)[\'"]\)', 'ObjectId("<id>")', shape)
            shape = re.sub(r':\s*\d+(\.\d+)?', ': <number>', shape)
            shape = re.sub(r':\s*"[^"]*"', ': "<string>"', shape)
            shape = re.sub(r":\s*'[^']*'", ": '<string>'", shape)
            shape = re.sub(r'coordinates:\s*\[\s*-?\d+\.?\d*\s*,\s*-?\d+\.?\d*\s*\]', 'coordinates: [<number>, <number>]', shape)
            
            return shape
        except Exception as e:
            print(f"Error extracting query shape: {e}")
            return query_str
    
    def get_slow_queries(self):
        return self.slow_queries
    
    def get_frequent_queries(self, min_count=5):
        return {k: v for k, v in self.query_patterns.items() if v["count"] >= min_count}
    
    def get_query_candidates_for_indexing(self):
        candidates = []
        for pattern, stats in self.query_patterns.items():
            avg_time = stats["total_time"] / stats["count"] if stats["count"] > 0 else 0
            
            if (avg_time > 50 and stats["count"] >= 3) or avg_time > 200:
                candidates.append({
                    "pattern": pattern,
                    "avg_execution_time_ms": avg_time,
                    "execution_count": stats["count"],
                    "collection": stats["collection"]
                })
        
        candidates.sort(key=lambda x: x["execution_count"] * x["avg_execution_time_ms"], reverse=True)
        return candidates