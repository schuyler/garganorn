# Use Python 3.11 slim image as base
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY pyproject.toml .
COPY garganorn/ ./garganorn/

# Install Python dependencies
RUN pip install --no-cache-dir .

# Create directory for database volume mount
RUN mkdir -p /app/db

# Set environment variables
ENV PYTHONPATH="/app"
ENV FLASK_APP="garganorn"
ENV FLASK_ENV="production"

# Expose port 8000 for the API
EXPOSE 8000

# Health check to ensure the service is running
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Run the application
CMD ["python", "-m", "garganorn"]
