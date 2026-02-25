from fastapi import FastAPI, HTTPException, Header, Body, Depends, Request
from typing import Optional, List, Dict, Any
from pydantic import BaseModel
from contextlib import asynccontextmanager
from sse_starlette.sse import EventSourceResponse
import uvicorn
import os
import json
import asyncio

from ..core.connection_manager import ConnectionManager
from ..adapters.base import DbConfig

class ConnectRequest(DbConfig):
    pass

class QueryRequest(BaseModel):
    sessionId: str
    query: str
    params: Optional[list] = None

class DisconnectRequest(BaseModel):
    sessionId: str

connection_manager = ConnectionManager()

@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await connection_manager.disconnect_all()

app = FastAPI(title="Universal DB MCP API", lifespan=lifespan)

API_KEYS = os.getenv("API_KEYS", "").split(",") if os.getenv("API_KEYS") else []

async def verify_api_key(x_api_key: str = Header(None)):
    if not API_KEYS:
        return
    if x_api_key not in API_KEYS:
        raise HTTPException(status_code=403, detail="Invalid API Key")

@app.get("/api/health")
async def health():
    return {"status": "ok"}

@app.post("/api/connect", dependencies=[Depends(verify_api_key)])
async def connect(config: ConnectRequest):
    try:
        session_id = await connection_manager.connect(config)
        return {"success": True, "data": {"sessionId": session_id}}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.post("/api/query", dependencies=[Depends(verify_api_key)])
async def query(request: QueryRequest):
    try:
        service = connection_manager.get_service(request.sessionId)
        result = await service.execute_query(request.query, request.params)
        return {"success": True, "data": result.dict(by_alias=True)}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.post("/api/disconnect", dependencies=[Depends(verify_api_key)])
async def disconnect(request: DisconnectRequest):
    try:
        await connection_manager.disconnect(request.sessionId)
        return {"success": True}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/tables", dependencies=[Depends(verify_api_key)])
async def list_tables(sessionId: str):
    try:
        service = connection_manager.get_service(sessionId)
        tables = await service.list_tables()
        return {"success": True, "data": {"tables": tables}}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/schema", dependencies=[Depends(verify_api_key)])
async def get_schema(sessionId: str):
    try:
        service = connection_manager.get_service(sessionId)
        schema = await service.get_schema()
        return {"success": True, "data": schema.dict(by_alias=True)}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/schema/{table}", dependencies=[Depends(verify_api_key)])
async def get_table_info(table: str, sessionId: str):
    try:
        service = connection_manager.get_service(sessionId)
        table_info = await service.get_table_info(table)
        return {"success": True, "data": table_info.dict(by_alias=True)}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/enum-values", dependencies=[Depends(verify_api_key)])
async def get_enum_values(sessionId: str, table: str, column: str, limit: int = 100):
    try:
        service = connection_manager.get_service(sessionId)
        values = await service.get_enum_values(table, column)
        return {"success": True, "data": {"values": values}}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/sample-data", dependencies=[Depends(verify_api_key)])
async def get_sample_data(sessionId: str, table: str, limit: int = 5):
    try:
        service = connection_manager.get_service(sessionId)
        rows = await service.get_sample_data(table, limit)
        return {"success": True, "data": {"rows": rows}}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/sse")
async def handle_sse(request: Request):
    """
    Handle SSE connection for MCP protocol.
    """
    # Simplified placeholder for SSE logic
    async def event_generator():
        yield {"event": "message", "data": json.dumps({"jsonrpc": "2.0", "method": "connection_established"})}
        while True:
            await asyncio.sleep(15)
            yield {"event": "ping", "data": ""}
            
    return EventSourceResponse(event_generator())

@app.post("/sse/message")
async def handle_sse_message(request: Request):
    # Placeholder for handling incoming JSON-RPC messages
    return {"status": "ok"}

def start_http_server(host: str, port: int):
    """
    Start the FastAPI server using uvicorn.
    """
    print(f"🌐 Starting HTTP API server on {host}:{port}")
    uvicorn.run(app, host=host, port=port)
