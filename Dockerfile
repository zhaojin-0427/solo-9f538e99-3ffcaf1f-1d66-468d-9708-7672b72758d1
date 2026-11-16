FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    OSM_DATA_DIR=/data

WORKDIR /app

# Install dependencies first to leverage Docker layer caching.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Application code.
COPY app ./app
COPY wsgi.py ./wsgi.py

# Persistent SQLite directory (mounted as a volume in docker-compose).
RUN mkdir -p /data && \
    adduser --disabled-password --gecos "" --uid 10001 appuser && \
    chown -R appuser:appuser /data /app
USER appuser
VOLUME ["/data"]

EXPOSE 8000

# Two sync workers sharing one SQLite database: writes are serialized by
# BEGIN IMMEDIATE while reads stay lock-free in WAL mode; threads allow an
# export to proceed beside an upload inside the same worker. Each worker
# registers itself on startup so crash recovery never takes over a sibling's
# in-flight batch. `--preload` is deliberately not used: every worker must
# have its own process identity and database connection.
CMD ["gunicorn", "--bind", "0.0.0.0:8000", "--workers", "2", "--threads", "4", "--timeout", "120", "--access-logfile", "-", "--error-logfile", "-", "wsgi:app"]