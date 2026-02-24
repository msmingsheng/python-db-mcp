FROM python:3.9-slim

WORKDIR /app

# Install system dependencies if needed (e.g. for postgres/mysql drivers)
RUN apt-get update && apt-get install -y 
    build-essential 
    libpq-dev 
    && rm -rf /var/lib/apt/lists/*

# Install uv
RUN pip install uv

# Copy project files
COPY pyproject.toml .
COPY src src
COPY README.md .

# Install dependencies
RUN uv pip install --system .

# Expose port for HTTP mode
EXPOSE 3000

# Default command (can be overridden)
CMD ["python-db-mcp", "--mode", "http"]
