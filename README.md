# Universal Database MCP Server (Python)

A Python implementation of the Universal Database MCP Server.

## Features

*   **Databases:** MySQL, PostgreSQL, Redis, SQLite.
*   **Modes:** MCP (stdio) and HTTP API.
*   **Security:** Read-only mode by default.
*   **Performance:** Batch schema fetching.

## Installation

```bash
pip install .
```

## Usage

### MCP Mode (for Claude Desktop)

```bash
python-db-mcp --type mysql --host localhost --port 3306 --user root --password secret --database mydb
```

### HTTP Mode

```bash
python-db-mcp --mode http --http-port 3000
```

## Configuration

See `src/python_db_mcp/main.py` for all available flags.
