import aiosqlite
import time
from typing import List, Optional, Union, Any, Dict
from ..adapters.base import DbAdapter, DbConfig, QueryResult, SchemaInfo, TableInfo, ColumnInfo, IndexInfo, ForeignKeyInfo, RelationshipInfo
from ..utils.safety import is_write_operation

class SQLiteAdapter(DbAdapter):
    def __init__(self, config: DbConfig):
        self.config = config
        self.connection: Optional[aiosqlite.Connection] = None

    async def connect(self) -> None:
        try:
            if not self.config.file_path:
                raise ValueError("SQLite requires 'file_path' in configuration")
            
            # Open connection
            # aiosqlite doesn't support 'readonly' mode in connect directly in older versions easily
            # but we enforce safety via is_write_operation checks
            self.connection = await aiosqlite.connect(self.config.file_path)
            self.connection.row_factory = aiosqlite.Row
            
            # Enable foreign keys
            await self.connection.execute("PRAGMA foreign_keys = ON")
        except Exception as e:
            raise ConnectionError(f"SQLite connection failed: {str(e)}")

    async def disconnect(self) -> None:
        if self.connection:
            await self.connection.close()
            self.connection = None

    async def execute_query(self, query: str, params: Optional[Union[List[Any], Dict[str, Any]]] = None) -> QueryResult:
        if not self.connection:
            raise ConnectionError("Database not connected")

        start_time = time.time()
        
        try:
            cursor = await self.connection.execute(query, params or [])
            
            if is_write_operation(query):
                await self.connection.commit()
                affected_rows = cursor.rowcount
                rows = []
            else:
                rows = await cursor.fetchall()
                # Convert Row objects to dicts
                rows = [dict(row) for row in rows]
                affected_rows = None

            execution_time = (time.time() - start_time) * 1000 # ms

            return QueryResult(
                rows=rows,
                affected_rows=affected_rows,
                execution_time=execution_time
            )
        except Exception as e:
            raise RuntimeError(f"Query execution failed: {str(e)}")

    async def get_schema(self) -> SchemaInfo:
        if not self.connection:
            raise ConnectionError("Database not connected")

        # Get version
        cursor = await self.connection.execute("SELECT sqlite_version()")
        version_row = await cursor.fetchone()
        version = version_row[0] if version_row else "unknown"

        # Get tables
        cursor = await self.connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
        tables_rows = await cursor.fetchall()
        table_names = [row[0] for row in tables_rows]

        tables: List[TableInfo] = []
        relationships: List[RelationshipInfo] = []

        for table_name in table_names:
            table_info = await self._get_table_info(table_name)
            tables.append(table_info)
            
            # Collect relationships
            for fk in table_info.foreign_keys:
                relationships.append(RelationshipInfo(
                    fromTable=table_name,
                    fromColumns=fk.columns,
                    toTable=fk.referenced_table,
                    toColumns=fk.referenced_columns,
                    type="many-to-one",
                    constraintName=fk.name
                ))

        return SchemaInfo(
            databaseType="sqlite",
            databaseName=self.config.file_path.split('/')[-1] if self.config.file_path else "unknown",
            tables=tables,
            version=version,
            relationships=relationships
        )

    async def _get_table_info(self, table_name: str) -> TableInfo:
        # Columns
        cursor = await self.connection.execute(f"PRAGMA table_info({table_name})")
        columns_data = await cursor.fetchall()
        
        columns = []
        primary_keys = []
        
        # PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
        for col in columns_data:
            columns.append(ColumnInfo(
                name=col['name'],
                type=col['type'],
                nullable=not col['notnull'],
                defaultValue=col['dflt_value']
            ))
            if col['pk'] > 0:
                primary_keys.append((col['name'], col['pk']))
        
        # Sort primary keys by index
        primary_keys.sort(key=lambda x: x[1])
        primary_keys = [pk[0] for pk in primary_keys]

        # Indexes
        cursor = await self.connection.execute(f"PRAGMA index_list({table_name})")
        indexes_data = await cursor.fetchall()
        indexes = []
        
        for idx in indexes_data:
            if idx['origin'] == 'pk': continue
            
            idx_name = idx['name']
            cursor_info = await self.connection.execute(f"PRAGMA index_info({idx_name})")
            idx_columns = await cursor_info.fetchall()
            
            indexes.append(IndexInfo(
                name=idx_name,
                columns=[c['name'] for c in idx_columns],
                unique=bool(idx['unique'])
            ))

        # Foreign Keys
        cursor = await self.connection.execute(f"PRAGMA foreign_key_list({table_name})")
        fks_data = await cursor.fetchall()
        
        # Group by id
        fks_map = {}
        for fk in fks_data:
            fk_id = fk['id']
            if fk_id not in fks_map:
                fks_map[fk_id] = {
                    'table': fk['table'],
                    'from': [],
                    'to': [],
                    'on_update': fk['on_update'],
                    'on_delete': fk['on_delete']
                }
            fks_map[fk_id]['from'].append(fk['from'])
            fks_map[fk_id]['to'].append(fk['to'])

        foreign_keys = []
        for fk_id, fk_info in fks_map.items():
            foreign_keys.append(ForeignKeyInfo(
                name=f"fk_{table_name}_{fk_id}",
                columns=fk_info['from'],
                referencedTable=fk_info['table'],
                referencedColumns=fk_info['to'],
                onUpdate=fk_info['on_update'] if fk_info['on_update'] != 'NO ACTION' else None,
                onDelete=fk_info['on_delete'] if fk_info['on_delete'] != 'NO ACTION' else None
            ))

        # Estimated Rows
        cursor = await self.connection.execute(f"SELECT COUNT(*) as count FROM {table_name}")
        row_count = (await cursor.fetchone())['count']

        return TableInfo(
            name=table_name,
            columns=columns,
            primaryKeys=primary_keys,
            indexes=indexes,
            foreignKeys=foreign_keys,
            estimatedRows=row_count
        )

    def is_write_operation(self, query: str) -> bool:
        return is_write_operation(query)
