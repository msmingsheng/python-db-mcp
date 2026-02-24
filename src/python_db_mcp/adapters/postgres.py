import asyncpg
import time
from typing import List, Optional, Union, Any, Dict
from ..adapters.base import DbAdapter, DbConfig, QueryResult, SchemaInfo, TableInfo, ColumnInfo, IndexInfo, ForeignKeyInfo, RelationshipInfo
from ..utils.safety import is_write_operation

class PostgresAdapter(DbAdapter):
    def __init__(self, config: DbConfig):
        self.config = config
        self.pool = None

    async def connect(self) -> None:
        try:
            # Create a pool
            self.pool = await asyncpg.create_pool(
                host=self.config.host,
                port=self.config.port,
                user=self.config.user,
                password=self.config.password,
                database=self.config.database
            )
        except Exception as e:
            raise ConnectionError(f"PostgreSQL connection failed: {str(e)}")

    async def disconnect(self) -> None:
        if self.pool:
            await self.pool.close()
            self.pool = None

    async def execute_query(self, query: str, params: Optional[Union[List[Any], Dict[str, Any]]] = None) -> QueryResult:
        if not self.pool:
            raise ConnectionError("Database not connected")

        start_time = time.time()
        
        try:
            async with self.pool.acquire() as connection:
                # asyncpg uses $1, $2 for params, but users might provide ? or named params
                # For simplicity here, we assume user provides correct placeholders or no params
                # A more robust solution would convert ? to $n
                
                # Handling simple list params
                if params and isinstance(params, list):
                    # Simple regex replace ? with $1, $2...
                    # Note: This is naive and breaks if ? is in strings
                    # A proper SQL parser/tokenizer is better but out of scope for "minimal dependencies"
                    # We will rely on users providing $n syntax for Postgres as per asyncpg
                    # OR we can attempt a simple split/join if we really want to support '?'
                    
                    # For now, pass directly
                    result = await connection.fetch(query, *params)
                else:
                    result = await connection.fetch(query)
                
                # Check for affected rows if it's not a SELECT
                status = await connection.execute(query, *params) if params else await connection.execute(query)
                
                # Extract affected rows from status string (e.g. "INSERT 0 1", "UPDATE 5")
                affected_rows = 0
                parts = status.split(' ')
                if len(parts) > 0 and parts[-1].isdigit():
                    affected_rows = int(parts[-1])

                rows = [dict(r) for r in result]
                execution_time = (time.time() - start_time) * 1000

                return QueryResult(
                    rows=rows,
                    affected_rows=affected_rows,
                    execution_time=execution_time
                )
        except Exception as e:
            raise RuntimeError(f"Query execution failed: {str(e)}")

    async def get_schema(self) -> SchemaInfo:
        if not self.pool:
            raise ConnectionError("Database not connected")

        async with self.pool.acquire() as conn:
            version = await conn.fetchval("SELECT version()")
            db_name = await conn.fetchval("SELECT current_database()")

            # Batch fetch columns
            all_columns = await conn.fetch("""
                SELECT 
                    c.table_name, c.column_name, c.data_type, c.is_nullable, 
                    c.column_default, c.character_maximum_length,
                    pg_catalog.col_description(c.table_name::regclass::oid, c.ordinal_position) as column_comment
                FROM information_schema.columns c
                WHERE c.table_schema = 'public'
                ORDER BY c.table_name, c.ordinal_position
            """)

            # Batch fetch primary keys
            all_pks = await conn.fetch("""
                SELECT t.relname as table_name, a.attname as column_name
                FROM pg_index i
                JOIN pg_class t ON t.oid = i.indrelid
                JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(i.indkey)
                JOIN pg_namespace n ON n.oid = t.relnamespace
                WHERE i.indisprimary AND n.nspname = 'public'
            """)

            # Batch fetch indexes
            all_indexes = await conn.fetch("""
                SELECT 
                    t.relname as table_name, i.relname as index_name, 
                    a.attname as column_name, ix.indisunique as is_unique
                FROM pg_class t
                JOIN pg_index ix ON t.oid = ix.indrelid
                JOIN pg_class i ON i.oid = ix.indexrelid
                JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
                JOIN pg_namespace n ON n.oid = t.relnamespace
                WHERE t.relkind = 'r' AND n.nspname = 'public' AND NOT ix.indisprimary
            """)

            # Batch fetch stats
            all_stats = await conn.fetch("""
                SELECT 
                    c.relname as table_name, 
                    c.reltuples::bigint as estimated_rows,
                    obj_description(c.oid) as table_comment
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relkind = 'r' AND n.nspname = 'public'
            """)

            # Batch fetch foreign keys
            all_fks = await conn.fetch("""
                SELECT
                    tc.table_name, kcu.column_name,
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name,
                    rc.constraint_name,
                    rc.update_rule, rc.delete_rule
                FROM information_schema.table_constraints AS tc
                JOIN information_schema.key_column_usage AS kcu
                  ON tc.constraint_name = kcu.constraint_name
                  AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage AS ccu
                  ON ccu.constraint_name = tc.constraint_name
                  AND ccu.table_schema = tc.table_schema
                JOIN information_schema.referential_constraints AS rc
                  ON rc.constraint_name = tc.constraint_name
                WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema='public'
            """)

        return self._assemble_schema(db_name, version, all_columns, all_pks, all_indexes, all_stats, all_fks)

    def _assemble_schema(self, db_name, version, all_columns, all_pks, all_indexes, all_stats, all_fks) -> SchemaInfo:
        tables_map = {}
        relationships = []

        # Initialize tables
        for stat in all_stats:
            tables_map[stat['table_name']] = {
                'name': stat['table_name'],
                'comment': stat['table_comment'],
                'columns': [],
                'primaryKeys': [],
                'indexes': {},
                'foreignKeys': {},
                'estimatedRows': stat['estimated_rows']
            }

        # Process columns
        for col in all_columns:
            t_name = col['table_name']
            if t_name not in tables_map: continue
            
            data_type = col['data_type']
            if col['character_maximum_length']:
                data_type += f"({col['character_maximum_length']})"

            tables_map[t_name]['columns'].append(ColumnInfo(
                name=col['column_name'],
                type=data_type,
                nullable=col['is_nullable'] == 'YES',
                defaultValue=str(col['column_default']) if col['column_default'] else None,
                comment=col['column_comment']
            ))

        # Process PKs
        for pk in all_pks:
            t_name = pk['table_name']
            if t_name in tables_map:
                tables_map[t_name]['primaryKeys'].append(pk['column_name'])

        # Process Indexes
        for idx in all_indexes:
            t_name = idx['table_name']
            if t_name not in tables_map: continue
            
            idx_name = idx['index_name']
            if idx_name not in tables_map[t_name]['indexes']:
                tables_map[t_name]['indexes'][idx_name] = {
                    'columns': [],
                    'unique': idx['is_unique']
                }
            tables_map[t_name]['indexes'][idx_name]['columns'].append(idx['column_name'])

        # Process FKs
        for fk in all_fks:
            t_name = fk['table_name']
            if t_name not in tables_map: continue
            
            c_name = fk['constraint_name']
            if c_name not in tables_map[t_name]['foreignKeys']:
                tables_map[t_name]['foreignKeys'][c_name] = {
                    'columns': [],
                    'referencedTable': fk['foreign_table_name'],
                    'referencedColumns': [],
                    'onDelete': fk['delete_rule'],
                    'onUpdate': fk['update_rule']
                }
            tables_map[t_name]['foreignKeys'][c_name]['columns'].append(fk['column_name'])
            tables_map[t_name]['foreignKeys'][c_name]['referencedColumns'].append(fk['foreign_column_name'])

        final_tables = []
        for t_name, t_data in tables_map.items():
            indexes_list = [
                IndexInfo(name=k, columns=v['columns'], unique=v['unique'])
                for k, v in t_data['indexes'].items()
            ]
            fks_list = []
            for k, v in t_data['foreignKeys'].items():
                fks_list.append(ForeignKeyInfo(
                    name=k,
                    columns=v['columns'],
                    referencedTable=v['referencedTable'],
                    referencedColumns=v['referencedColumns'],
                    onUpdate=v['onUpdate'],
                    onDelete=v['onDelete']
                ))
                relationships.append(RelationshipInfo(
                    fromTable=t_name,
                    fromColumns=v['columns'],
                    toTable=v['referencedTable'],
                    toColumns=v['referencedColumns'],
                    type='many-to-one',
                    constraintName=k
                ))

            final_tables.append(TableInfo(
                name=t_name,
                comment=t_data['comment'],
                columns=t_data['columns'],
                primaryKeys=t_data['primaryKeys'],
                indexes=indexes_list,
                foreignKeys=fks_list,
                estimatedRows=t_data['estimatedRows']
            ))

        return SchemaInfo(
            databaseType="postgres",
            databaseName=db_name,
            tables=final_tables,
            version=version,
            relationships=relationships
        )

    def is_write_operation(self, query: str) -> bool:
        return is_write_operation(query)
