FROM python:3.10-slim

WORKDIR /app

# Install system dependencies for database drivers
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install uv for fast package management
RUN pip install uv

# Copy project files
COPY pyproject.toml .
COPY src src
COPY README.md .

# Install the package and its dependencies
RUN uv pip install --system .

# Expose the default HTTP port
EXPOSE 3000

# Use ENTRYPOINT so arguments can be passed directly to the container
ENTRYPOINT ["python-db-mcp", "start"]

# Default to HTTP mode if no arguments are provided
CMD ["--mode", "http"]
