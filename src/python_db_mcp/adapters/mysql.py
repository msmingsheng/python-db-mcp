import asyncmy
import time
from typing import List, Optional, Union, Any, Dict
from ..adapters.base import DbAdapter, DbConfig, QueryResult, SchemaInfo, TableInfo, ColumnInfo, IndexInfo, ForeignKeyInfo, RelationshipInfo
from ..utils.safety import is_write_operation

class MySQLAdapter(DbAdapter):
    def __init__(self, config: DbConfig):
        self.config = config
        self.connection = None

    async def connect(self) -> None:
        try:
            self.connection = await asyncmy.connect(
                host=self.config.host,
                port=self.config.port,
                user=self.config.user,
                password=self.config.password,
                database=self.config.database,
                cursorclass=asyncmy.cursors.DictCursor
            )
        except Exception as e:
            raise ConnectionError(f"MySQL connection failed: {str(e)}")

    async def disconnect(self) -> None:
        if self.connection:
            self.connection.close()
            self.connection = None

    async def execute_query(self, query: str, params: Optional[Union[List[Any], Dict[str, Any]]] = None) -> QueryResult:
        if not self.connection:
            raise ConnectionError("Database not connected")

        start_time = time.time()
        
        try:
            async with self.connection.cursor() as cursor:
                await cursor.execute(query, params)
                
                if is_write_operation(query):
                    # For write operations, asyncmy doesn't return rows usually
                    # but commit might be needed depending on autocommit settings
                    # asyncmy autocommit is False by default
                    await self.connection.commit()
                    rows = []
                    affected_rows = cursor.rowcount
                else:
                    rows = await cursor.fetchall()
                    affected_rows = None

            execution_time = (time.time() - start_time) * 1000

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
        async with self.connection.cursor() as cursor:
            await cursor.execute("SELECT VERSION() as version")
            version_row = await cursor.fetchone()
            version = version_row['version']

            # Get database name
            await cursor.execute("SELECT DATABASE() as db")
            db_row = await cursor.fetchone()
            db_name = db_row['db'] or self.config.database or "unknown"

            # Batch fetch columns
            await cursor.execute("""
                SELECT 
                    TABLE_NAME, COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, 
                    COLUMN_DEFAULT, COLUMN_KEY, COLUMN_COMMENT
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_SCHEMA = %s 
                ORDER BY TABLE_NAME, ORDINAL_POSITION
            """, (db_name,))
            all_columns = await cursor.fetchall()

            # Batch fetch statistics (indexes)
            await cursor.execute("""
                SELECT 
                    TABLE_NAME, INDEX_NAME, COLUMN_NAME, NON_UNIQUE, SEQ_IN_INDEX
                FROM INFORMATION_SCHEMA.STATISTICS
                WHERE TABLE_SCHEMA = %s
                ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX
            """, (db_name,))
            all_indexes = await cursor.fetchall()

            # Batch fetch table stats
            await cursor.execute("""
                SELECT TABLE_NAME, TABLE_ROWS, TABLE_COMMENT 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'
            """, (db_name,))
            all_stats = await cursor.fetchall()

            # Batch fetch foreign keys
            await cursor.execute("""
                SELECT 
                    kcu.TABLE_NAME, kcu.CONSTRAINT_NAME, kcu.COLUMN_NAME, 
                    kcu.REFERENCED_TABLE_NAME, kcu.REFERENCED_COLUMN_NAME,
                    rc.DELETE_RULE, rc.UPDATE_RULE
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
                    ON kcu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
                    AND kcu.TABLE_SCHEMA = rc.CONSTRAINT_SCHEMA
                WHERE kcu.TABLE_SCHEMA = %s AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
            """, (db_name,))
            all_fks = await cursor.fetchall()

        return self._assemble_schema(db_name, version, all_columns, all_indexes, all_stats, all_fks)

    def _assemble_schema(self, db_name, version, all_columns, all_indexes, all_stats, all_fks) -> SchemaInfo:
        tables_map = {}
        relationships = []

        # Initialize tables
        for stat in all_stats:
            tables_map[stat['TABLE_NAME']] = {
                'name': stat['TABLE_NAME'],
                'comment': stat['TABLE_COMMENT'],
                'columns': [],
                'primaryKeys': [],
                'indexes': {},
                'foreignKeys': {},
                'estimatedRows': stat['TABLE_ROWS']
            }

        # Process columns
        for col in all_columns:
            table_name = col['TABLE_NAME']
            if table_name not in tables_map: continue
            
            tables_map[table_name]['columns'].append(ColumnInfo(
                name=col['COLUMN_NAME'],
                type=col['COLUMN_TYPE'],
                nullable=col['IS_NULLABLE'] == 'YES',
                defaultValue=str(col['COLUMN_DEFAULT']) if col['COLUMN_DEFAULT'] is not None else None,
                comment=col['COLUMN_COMMENT']
            ))
            
            if col['COLUMN_KEY'] == 'PRI':
                tables_map[table_name]['primaryKeys'].append(col['COLUMN_NAME'])

        # Process indexes
        for idx in all_indexes:
            table_name = idx['TABLE_NAME']
            if table_name not in tables_map: continue
            if idx['INDEX_NAME'] == 'PRIMARY': continue

            idx_name = idx['INDEX_NAME']
            if idx_name not in tables_map[table_name]['indexes']:
                tables_map[table_name]['indexes'][idx_name] = {
                    'columns': [],
                    'unique': not idx['NON_UNIQUE']
                }
            tables_map[table_name]['indexes'][idx_name]['columns'].append(idx['COLUMN_NAME'])

        # Process FKs
        for fk in all_fks:
            table_name = fk['TABLE_NAME']
            if table_name not in tables_map: continue
            
            constraint_name = fk['CONSTRAINT_NAME']
            if constraint_name not in tables_map[table_name]['foreignKeys']:
                tables_map[table_name]['foreignKeys'][constraint_name] = {
                    'columns': [],
                    'referencedTable': fk['REFERENCED_TABLE_NAME'],
                    'referencedColumns': [],
                    'onDelete': fk['DELETE_RULE'],
                    'onUpdate': fk['UPDATE_RULE']
                }
            
            tables_map[table_name]['foreignKeys'][constraint_name]['columns'].append(fk['COLUMN_NAME'])
            tables_map[table_name]['foreignKeys'][constraint_name]['referencedColumns'].append(fk['REFERENCED_COLUMN_NAME'])

        # Finalize tables list
        final_tables = []
        for t_name, t_data in tables_map.items():
            # Convert indexes dict to list
            indexes_list = [
                IndexInfo(name=k, columns=v['columns'], unique=v['unique'])
                for k, v in t_data['indexes'].items()
            ]
            
            # Convert FKs dict to list
            fks_list = []
            for k, v in t_data['foreignKeys'].items():
                fks_list.append(ForeignKeyInfo(
                    name=k,
                    columns=v['columns'],
                    referencedTable=v['referencedTable'],
                    referencedColumns=v['referencedColumns'],
                    onDelete=v['onDelete'],
                    onUpdate=v['onUpdate']
                ))
                
                # Add to relationships
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
            databaseType="mysql",
            databaseName=db_name,
            tables=final_tables,
            version=version,
            relationships=relationships
        )

    def is_write_operation(self, query: str) -> bool:
        return is_write_operation(query)
