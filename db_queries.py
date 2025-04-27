import time
from db_connect import db_connect


class DBQueries:
    def __init__(self, database_name):
        self.client = db_connect()
        self.db = self.client[database_name]

    def list_collections(self):
        return self.db.list_collection_names()

    def execute_query(self, collection_name, query, projection=None, sort=None, limit=None):
        collection = self.db[collection_name]
        start_time = time.time()

        explain_plan = collection.find(query, projection).explain()

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

    def close(self):
        if self.client:
            self.client.close()
