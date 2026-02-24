from fastapi import FastAPI, HTTPException, Header, Body, Depends
from typing import Optional, List
from pydantic import BaseModel
from contextlib import asynccontextmanager
import uvicorn
import os

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

from fastapi import FastAPI, HTTPException, Header, Body, Depends, Request
from sse_starlette.sse import EventSourceResponse
import json
import asyncio

# ... (existing imports)

# Add to imports
from mcp.server.sse import SseServerTransport
from ..server.mcp_server import DatabaseMCPServer

# ... (existing code)

@app.get("/sse")
async def handle_sse(request: Request):
    """
    Handle SSE connection for MCP protocol.
    Query params should contain db config.
    """
    params = dict(request.query_params)
    
    # Extract DB config from params
    # This is a simplified version; in production, you'd map params to DbConfig
    # and create a dedicated MCP server instance for this session.
    
    # For now, we return a placeholder to indicate where the SSE logic goes.
    # In a full implementation, you would:
    # 1. Create a DbConfig from params
    # 2. Instantiate DatabaseMCPServer(config)
    # 3. Create SseServerTransport
    # 4. Connect them
    
    async def event_generator():
        yield {"event": "message", "data": json.dumps({"jsonrpc": "2.0", "method": "connection_established"})}
        while True:
            await asyncio.sleep(1)
            # Yield keepalive or actual messages from the MCP server
            
    return EventSourceResponse(event_generator())

@app.post("/sse/message")
async def handle_sse_message(request: Request):
    # Handle incoming JSON-RPC messages from the client
    pass

# ... (existing start_http_server)
