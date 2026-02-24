import asyncio
from mcp.server.fastmcp import FastMCP
from ..core.database_service import DatabaseService
from ..adapters.base import DbConfig
from ..adapters.sqlite import SQLiteAdapter
from ..adapters.mysql import MySQLAdapter
from ..adapters.postgres import PostgresAdapter
from ..adapters.redis import RedisAdapter

def create_adapter(config: DbConfig):
    if config.type == 'sqlite':
        return SQLiteAdapter(config)
    elif config.type == 'mysql':
        return MySQLAdapter(config)
    elif config.type == 'postgres':
        return PostgresAdapter(config)
    elif config.type == 'redis':
        return RedisAdapter(config)
    else:
        raise ValueError(f"Unsupported database type: {config.type}")

class DatabaseMCPServer:
    def __init__(self, config: DbConfig):
        self.config = config
        self.adapter = create_adapter(config)
        self.service = DatabaseService(self.adapter, config)
        self.mcp = FastMCP("universal-db-mcp")

        # Register tools
        self.mcp.tool(name="execute_query", description="Execute SQL query")(self.execute_query)
        self.mcp.tool(name="get_schema", description="Get database schema")(self.get_schema)
        self.mcp.tool(name="get_table_info", description="Get table info")(self.get_table_info)
        self.mcp.tool(name="get_sample_data", description="Get sample data from a table")(self.get_sample_data)
        self.mcp.tool(name="get_enum_values", description="Get distinct values for a column")(self.get_enum_values)
        self.mcp.tool(name="clear_cache", description="Clear schema cache")(self.clear_cache)

    async def connect(self):
        await self.adapter.connect()

    async def execute_query(self, query: str) -> str:
        """Execute a SQL query."""
        try:
            result = await self.service.execute_query(query)
            return result.model_dump_json(exclude_none=True)
        except Exception as e:
            return f"Error: {str(e)}"

    async def get_schema(self) -> str:
        """Get the database schema."""
        try:
            schema = await self.service.get_schema()
            return schema.model_dump_json(exclude_none=True)
        except Exception as e:
            return f"Error: {str(e)}"

    async def get_table_info(self, table_name: str) -> str:
        """Get detailed information about a specific table."""
        try:
            table = await self.service.get_table_info(table_name)
            return table.model_dump_json(exclude_none=True)
        except Exception as e:
            return f"Error: {str(e)}"

    async def get_sample_data(self, table_name: str, limit: int = 5) -> str:
        """Get sample data from a table."""
        try:
            data = await self.service.get_sample_data(table_name, limit)
            import json
            return json.dumps(data, default=str)
        except Exception as e:
            return f"Error: {str(e)}"

    async def get_enum_values(self, table_name: str, column_name: str) -> str:
        """Get all distinct values for a column."""
        try:
            values = await self.service.get_enum_values(table_name, column_name)
            import json
            return json.dumps(values, default=str)
        except Exception as e:
            return f"Error: {str(e)}"

    async def clear_cache(self) -> str:
        """Clear the internal schema cache."""
        self.service.clear_cache()
        return "Cache cleared"

    def run(self):
        # Initialize connection before running
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.connect())
        self.mcp.run()
