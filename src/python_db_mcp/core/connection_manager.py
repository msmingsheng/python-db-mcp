import time
from typing import Dict
from nanoid import generate
from .database_service import DatabaseService
from ..adapters.base import DbConfig, DbAdapter
from ..server.mcp_server import create_adapter

class ConnectionManager:
    def __init__(self, session_timeout: int = 3600):
        self.sessions: Dict[str, DatabaseService] = {}
        self.adapters: Dict[str, DbAdapter] = {}
        self.last_accessed: Dict[str, float] = {}
        self.session_timeout = session_timeout

    async def connect(self, config: DbConfig) -> str:
        adapter = create_adapter(config)
        await adapter.connect()
        
        session_id = generate()
        service = DatabaseService(adapter, config)
        
        self.sessions[session_id] = service
        self.adapters[session_id] = adapter
        self.last_accessed[session_id] = time.time()
        
        return session_id

    async def disconnect(self, session_id: str):
        if session_id in self.adapters:
            await self.adapters[session_id].disconnect()
            del self.adapters[session_id]
            del self.sessions[session_id]
            del self.last_accessed[session_id]

    def get_service(self, session_id: str) -> DatabaseService:
        if session_id not in self.sessions:
            raise ValueError("Session not found")
        
        self.last_accessed[session_id] = time.time()
        return self.sessions[session_id]

    async def disconnect_all(self):
        for session_id in list(self.sessions.keys()):
            await self.disconnect(session_id)
