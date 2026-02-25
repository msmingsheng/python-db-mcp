import os
import json
import httpx
from openai import OpenAI

# Configuration
# 1. Start your server first: 
#    python-db-mcp start --mode http --host 127.0.0.1 --port 3000
# 2. Set your OpenAI API Key
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "your-key-here")
MCP_SERVER_URL = "http://127.0.0.1:3000"

client = OpenAI(api_key=OPENAI_API_KEY)

def call_mcp_api(endpoint: str, method: str = "GET", data: dict = None):
    """Helper to communicate with the Universal DB MCP HTTP API"""
    url = f"{MCP_SERVER_URL}/api/{endpoint}"
    with httpx.Client() as http_client:
        if method == "POST":
            response = http_client.post(url, json=data)
        else:
            response = http_client.get(url, params=data)
        return response.json()

# Define tools for OpenAI based on MCP capabilities
tools = [
    {
        "type": "function",
        "function": {
            "name": "connect_database",
            "description": "Connect to a specific database",
            "parameters": {
                "type": "object",
                "properties": {
                    "type": {"type": "string", "enum": ["sqlite", "mysql", "postgres", "redis"]},
                    "filePath": {"type": "string", "description": "Path to SQLite file (if type is sqlite)"},
                    "host": {"type": "string", "description": "Database host (if not sqlite)"},
                    "database": {"type": "string", "description": "Database name"}
                },
                "required": ["type"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "query_database",
            "description": "Execute a SQL query on the connected database",
            "parameters": {
                "type": "object",
                "properties": {
                    "sessionId": {"type": "string", "description": "The active session ID from connect_database"},
                    "query": {"type": "string", "description": "The SQL query to execute"}
                },
                "required": ["sessionId", "query"]
            }
        }
    }
]

def run_conversation():
    messages = [
        {"role": "system", "content": "You are a helpful assistant that can access databases to answer questions."},
        {"role": "user", "content": "Connect to 'test.db' (sqlite) and show me all users."}
    ]

    print(f"User: {messages[1]['content']}")
    
    # 1. First Response (Likely calling connect_database)
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=messages,
        tools=tools,
    )
    
    response_message = response.choices[0].message
    messages.append(response_message)

    if response_message.tool_calls:
        for tool_call in response_message.tool_calls:
            function_name = tool_call.function.name
            args = json.loads(tool_call.function.arguments)
            
            if function_name == "connect_database":
                print(f"Action: Connecting to {args.get('filePath') or args.get('host')}...")
                result = call_mcp_api("connect", "POST", args)
                # result['data']['sessionId'] contains the ID we need
            
            elif function_name == "query_database":
                print(f"Action: Querying DB with {args.get('query')}...")
                result = call_mcp_api("query", "POST", args)

            # Send tool result back to LLM
            messages.append({
                "tool_call_id": tool_call.id,
                "role": "tool",
                "name": function_name,
                "content": json.dumps(result),
            })

        # 2. Final Response
        second_response = client.chat.completions.create(
            model="gpt-4o",
            messages=messages,
        )
        print(f"Assistant: {second_response.choices[0].message.content}")

if __name__ == "__main__":
    run_conversation()
