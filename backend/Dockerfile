FROM python:3.11-slim

WORKDIR /app

# Install curl for health checks, libpq for PostgreSQL, and uv for faster package installation
RUN apt-get update && apt-get install -y curl libpq-dev && rm -rf /var/lib/apt/lists/*
RUN pip install uv

# Copy dependency files
COPY pyproject.toml .

# Install dependencies
RUN uv pip install --system -e .

# Copy source code
COPY src/ src/

# Expose port
EXPOSE 8000

# Run the application
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]
