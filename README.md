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
python-db-mcp --mode http --port 3000
```

### SQLite Example (Quick Start)

1. Create a mock database:
```python
import sqlite3
conn = sqlite3.connect('test.db')
conn.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
conn.execute('INSERT INTO users (name) VALUES ("Alice"), ("Bob")')
conn.commit()
```

2. Start the server:
```bash
python-db-mcp start --type sqlite --file-path test.db
```

## Docker Usage

### Build the Image
```bash
docker build -t python-db-mcp ./python-db-mcp
```

### Run with SQLite
To use SQLite with Docker, you need to mount the directory containing your database file:
```bash
docker run -p 3000:3000 \
  -v $(pwd):/data \
  python-db-mcp --type sqlite --file-path /data/test.db
```

### Run with Remote DB (MySQL/Postgres)
```bash
docker run -p 3000:3000 \
  python-db-mcp --type mysql --host host.docker.internal --user root --database mydb
```

## OpenAI Integration

See `example.py` for a complete example of using this server with the OpenAI SDK as a tool.

1. Start server in HTTP mode:
```bash
python-db-mcp start --mode http --port 3000
```

2. Run example:
```bash
export OPENAI_API_KEY=your_key
python example.py
```

## Configuration

See `src/python_db_mcp/main.py` for all available flags.
