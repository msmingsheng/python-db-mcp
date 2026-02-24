from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Literal, Union
from pydantic import BaseModel, Field

class DbConfig(BaseModel):
    type: Literal['mysql', 'postgres', 'redis', 'sqlite']
    host: Optional[str] = None
    port: Optional[int] = None
    user: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None
    file_path: Optional[str] = Field(None, alias="filePath")
    permission_mode: Literal['safe', 'readwrite', 'full', 'custom'] = Field('safe', alias="permissionMode")
    permissions: Optional[List[str]] = None

class ColumnInfo(BaseModel):
    name: str
    type: str
    nullable: bool
    default_value: Optional[str] = Field(None, alias="defaultValue")
    comment: Optional[str] = None

class IndexInfo(BaseModel):
    name: str
    columns: List[str]
    unique: bool

class ForeignKeyInfo(BaseModel):
    name: str
    columns: List[str]
    referenced_table: str = Field(..., alias="referencedTable")
    referenced_columns: List[str] = Field(..., alias="referencedColumns")
    on_delete: Optional[str] = Field(None, alias="onDelete")
    on_update: Optional[str] = Field(None, alias="onUpdate")

class TableInfo(BaseModel):
    name: str
    comment: Optional[str] = None
    columns: List[ColumnInfo]
    primary_keys: List[str] = Field(default_factory=list, alias="primaryKeys")
    indexes: List[IndexInfo] = Field(default_factory=list)
    foreign_keys: List[ForeignKeyInfo] = Field(default_factory=list, alias="foreignKeys")
    estimated_rows: Optional[int] = Field(None, alias="estimatedRows")

class RelationshipInfo(BaseModel):
    from_table: str = Field(..., alias="fromTable")
    from_columns: List[str] = Field(..., alias="fromColumns")
    to_table: str = Field(..., alias="toTable")
    to_columns: List[str] = Field(..., alias="toColumns")
    type: Literal['one-to-one', 'one-to-many', 'many-to-one']
    constraint_name: Optional[str] = Field(None, alias="constraintName")
    source: Literal['foreign_key', 'inferred'] = 'foreign_key'
    confidence: Optional[float] = 1.0

class SchemaInfo(BaseModel):
    database_type: str = Field(..., alias="databaseType")
    database_name: str = Field(..., alias="databaseName")
    tables: List[TableInfo]
    version: Optional[str] = None
    relationships: Optional[List[RelationshipInfo]] = None

class QueryResult(BaseModel):
    rows: List[Dict[str, Any]]
    affected_rows: Optional[int] = Field(None, alias="affectedRows")
    execution_time: Optional[float] = Field(None, alias="executionTime")
    metadata: Optional[Dict[str, Any]] = None

class DbAdapter(ABC):
    @abstractmethod
    async def connect(self) -> None:
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        pass

    @abstractmethod
    async def execute_query(self, query: str, params: Optional[Union[List[Any], Dict[str, Any]]] = None) -> QueryResult:
        pass

    @abstractmethod
    async def get_schema(self) -> SchemaInfo:
        pass

    @abstractmethod
    def is_write_operation(self, query: str) -> bool:
        pass
