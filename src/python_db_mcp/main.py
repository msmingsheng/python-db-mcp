import typer
import os
from typing import Optional
from .adapters.base import DbConfig
from .server.mcp_server import DatabaseMCPServer
from .server.http_server import start_http_server

app = typer.Typer()

@app.command()
def start(
    mode: str = typer.Option("mcp", help="Mode: mcp or http"),
    # DB Config
    type: str = typer.Option(..., help="Database type"),
    host: Optional[str] = typer.Option(None, help="Database host"),
    port: Optional[int] = typer.Option(None, help="Database port"),
    user: Optional[str] = typer.Option(None, help="Database user"),
    password: Optional[str] = typer.Option(None, help="Database password"),
    database: Optional[str] = typer.Option(None, help="Database name"),
    file_path: Optional[str] = typer.Option(None, help="SQLite file path"),
    # HTTP Config
    http_port: int = typer.Option(3000, help="HTTP server port"),
    http_host: str = typer.Option("0.0.0.0", help="HTTP server host"),
    # Safety
    permission_mode: str = typer.Option("safe", help="Permission mode: safe, readwrite, full"),
):
    if mode == "http":
        start_http_server(http_host, http_port)
    else:
        # MCP Mode
        config = DbConfig(
            type=type,
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            filePath=file_path,
            permissionMode=permission_mode
        )
        
        server = DatabaseMCPServer(config)
        server.run()

if __name__ == "__main__":
    app()
