import redis.asyncio as redis
import time
from typing import List, Optional, Union, Any, Dict
from ..adapters.base import DbAdapter, DbConfig, QueryResult, SchemaInfo, TableInfo, ColumnInfo, IndexInfo, ForeignKeyInfo, RelationshipInfo
from ..utils.safety import is_write_operation

class RedisAdapter(DbAdapter):
    def __init__(self, config: DbConfig):
        self.config = config
        self.client = None

    async def connect(self) -> None:
        try:
            self.client = redis.Redis(
                host=self.config.host,
                port=self.config.port,
                password=self.config.password,
                db=int(self.config.database) if self.config.database else 0,
                decode_responses=True
            )
            await self.client.ping()
        except Exception as e:
            raise ConnectionError(f"Redis connection failed: {str(e)}")

    async def disconnect(self) -> None:
        if self.client:
            await self.client.aclose()
            self.client = None

    async def execute_query(self, query: str, params: Optional[Union[List[Any], Dict[str, Any]]] = None) -> QueryResult:
        if not self.client:
            raise ConnectionError("Database not connected")

        start_time = time.time()
        
        try:
            # Parse command
            parts = query.strip().split()
            if not parts:
                raise ValueError("Empty query")
            
            command = parts[0].lower()
            args = parts[1:]
            
            # Allow params to extend args
            if params and isinstance(params, list):
                args.extend([str(p) for p in params])

            # Execute via execute_command
            result = await self.client.execute_command(command, *args)
            
            execution_time = (time.time() - start_time) * 1000
            
            # Format result
            rows = self._format_result(command, result)
            
            return QueryResult(
                rows=rows,
                execution_time=execution_time,
                metadata={'raw_result': str(result)}
            )
        except Exception as e:
            raise RuntimeError(f"Redis command failed: {str(e)}")

    def _format_result(self, command: str, result: Any) -> List[Dict[str, Any]]:
        if result is None:
            return [{'result': None}]
        
        if isinstance(result, list):
            if command == 'hgetall':
                # Convert list to dict for hgetall
                obj = {}
                for i in range(0, len(result), 2):
                    if i + 1 < len(result):
                        obj[result[i]] = result[i+1]
                return [obj]
            return [{'index': i, 'value': v} for i, v in enumerate(result)]
        
        if isinstance(result, dict):
            return [result]
            
        return [{'result': result}]

    async def get_schema(self) -> SchemaInfo:
        if not self.client:
            raise ConnectionError("Database not connected")

        info = await self.client.info()
        version = info.get('redis_version', 'unknown')
        
        # Sample keys to guess schema
        keys = await self.client.keys('*')
        # Limit sample
        sample_keys = keys[:100]
        
        type_map = {}
        for key in sample_keys:
            key_type = await self.client.type(key)
            if key_type not in type_map:
                type_map[key_type] = []
            type_map[key_type].append(key)

        tables = []
        # Create virtual tables for each type
        for key_type, key_list in type_map.items():
            columns = [
                ColumnInfo(name='key', type='string', nullable=False),
                ColumnInfo(name='type', type='string', nullable=False)
            ]
            
            if key_type == 'string':
                columns.append(ColumnInfo(name='value', type='string', nullable=True))
            elif key_type == 'list':
                columns.append(ColumnInfo(name='length', type='number', nullable=False))
            elif key_type == 'hash':
                columns.append(ColumnInfo(name='field_count', type='number', nullable=False))
            
            tables.append(TableInfo(
                name=f"keys_{key_type}",
                columns=columns,
                primaryKeys=['key'],
                estimatedRows=len(key_list)
            ))

        # Add overview table
        tables.append(TableInfo(
            name='_overview',
            columns=[
                ColumnInfo(name='metric', type='string', nullable=False),
                ColumnInfo(name='value', type='string', nullable=False)
            ],
            estimatedRows=len(info)
        ))

        return SchemaInfo(
            databaseType="redis",
            databaseName=f"db{self.config.database or 0}",
            tables=tables,
            version=version
        )

    def is_write_operation(self, query: str) -> bool:
        write_commands = {
            'SET', 'SETEX', 'SETNX', 'MSET', 'DEL', 'UNLINK', 'FLUSHDB', 'FLUSHALL',
            'LPUSH', 'RPUSH', 'LPOP', 'RPOP', 'SADD', 'SREM', 'HSET', 'HDEL', 'INCR', 'DECR'
        }
        cmd = query.strip().split()[0].upper()
        return cmd in write_commands
