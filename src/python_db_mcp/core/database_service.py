import time
from typing import Optional, List, Dict, Any
from ..adapters.base import DbAdapter, DbConfig, QueryResult, SchemaInfo, TableInfo
from ..utils.safety import validate_query
from ..utils.schema import SchemaEnhancer

class DatabaseService:
    def __init__(self, adapter: DbAdapter, config: DbConfig):
        self.adapter = adapter
        self.config = config
        self.schema_enhancer = SchemaEnhancer()
        self._schema_cache: Optional[SchemaInfo] = None
        self._schema_cache_time = 0
        self._cache_ttl = 300 # 5 minutes

    async def execute_query(self, query: str, params: Optional[list] = None) -> QueryResult:
        validate_query(query, self.config)
        return await self.adapter.execute_query(query, params)

    async def get_schema(self, force_refresh: bool = False) -> SchemaInfo:
        now = time.time()
        if not force_refresh and self._schema_cache and (now - self._schema_cache_time < self._cache_ttl):
            return self._schema_cache

        schema = await self.adapter.get_schema()
        
        # Enhance schema
        if schema.relationships is None:
            schema.relationships = []
            
        schema.relationships = self.schema_enhancer.enhance_relationships(
            schema.tables, schema.relationships
        )

        self._schema_cache = schema
        self._schema_cache_time = now
        return schema

    async def get_table_info(self, table_name: str) -> TableInfo:
        schema = await self.get_schema()
        for table in schema.tables:
            if table.name.lower() == table_name.lower():
                return table
        raise ValueError(f"Table {table_name} not found")

    async def get_sample_data(self, table_name: str, limit: int = 5) -> List[Dict[str, Any]]:
        # Validate table existence first
        await self.get_table_info(table_name)
        query = f"SELECT * FROM {table_name} LIMIT {limit}"
        result = await self.execute_query(query)
        return result.rows

    async def get_enum_values(self, table_name: str, column_name: str) -> List[str]:
        # Validate table exists
        await self.get_table_info(table_name)
        # Simple distinct query - in production, check if column exists first
        query = f"SELECT DISTINCT {column_name} FROM {table_name} LIMIT 100"
        result = await self.execute_query(query)
        return [str(row.get(column_name) or row.get(column_name.lower())) for row in result.rows]

    def clear_cache(self):
        self._schema_cache = None
        self._schema_cache_time = 0
