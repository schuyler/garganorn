# Docker Usage Guide for Garganorn

## Building the Docker Image

To build the Garganorn Docker image:

```bash
docker build -t garganorn .
```

## Running with Docker

### Prerequisites

Before running the Docker container, you need to have DuckDB databases available. You can create them using the provided scripts:

```bash
# For Overture Maps data
./scripts/import-overture-extract.sh -122.5137 37.7099 -122.3785 37.8101

# Or for Foursquare OSP data
./scripts/import-fsq-extract.sh -122.5137 37.7099 -122.3785 37.8101
```

### Running with Docker Compose (Recommended)

The easiest way to run Garganorn is using Docker Compose:

```bash
# Start the service
docker-compose up -d

# View logs
docker-compose logs -f

# Stop the service
docker-compose down
```

### Running with Docker CLI

You can also run the container directly with Docker:

```bash
docker run -d \
  --name garganorn \
  -p 8000:8000 \
  -v $(pwd)/db:/app/db:ro \
  garganorn
```

### Debugging

If you're experiencing issues (like 500 errors), here are some debugging approaches:

**Check logs:**
```bash
# View container logs
docker logs -f garganorn
# or with docker-compose
docker-compose logs -f garganorn
```

**Run interactively for debugging:**
```bash
# Stop the current container
docker stop garganorn && docker rm garganorn

# Run interactively to see output in real-time
docker run -it \
  --name garganorn \
  -p 8000:8000 \
  -v $(pwd)/db:/app/db \
  garganorn
```

**Execute shell in running container:**
```bash
# Get a shell inside the container
docker exec -it garganorn /bin/bash

# Then you can:
# - Check if database files exist: ls -la /app/db/
# - Test database connectivity: python -c "import duckdb; print(duckdb.connect('/app/db/overture-maps.duckdb').execute('SELECT 1').fetchone())"
# - Check Python environment: python -c "import garganorn; print('OK')"
```

**Run with debug mode:**
```bash
docker run -it \
  --name garganorn \
  -p 8000:8000 \
  -v $(pwd)/db:/app/db \
  -e FLASK_DEBUG=true \
  garganorn
```

### Environment Variables

- `FLASK_ENV`: Set to `production` for production use (default: `production`)
- `FLASK_DEBUG`: Set to `false` to disable debug mode (default: `false`)

### Volume Mounts

The container expects DuckDB database files to be mounted at `/app/db`. The following database files should be present:

- `overture-maps.duckdb` (for Overture Maps data)
- `fsq-osp.duckdb` (for Foursquare OSP data)

The application opens databases in read-only mode and uses temporary directories for DuckDB's working files. DuckDB temporary files are managed by the application in the container's writable filesystem.

### Health Checks

The container includes a health check that verifies the Flask server is responding properly at the `/health` endpoint. You can check the health status with:

```bash
docker ps
# or
docker-compose ps
```

You can also manually check the health endpoint:

```bash
curl http://localhost:8000/health
# Returns: {"status": "ok", "service": "garganorn"}
```
