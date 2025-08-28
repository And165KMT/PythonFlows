# PythonFlows backend + static frontend
# Lightweight base with prebuilt wheels support
FROM python:3.11-slim

# Ensure logs are unbuffered
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# Install minimal runtime deps (scikit-learn uses OpenMP)
RUN apt-get update \
    && apt-get install -y --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps first for better layer caching
COPY backend/requirements.txt /app/backend/requirements.txt
RUN python -m pip install -r /app/backend/requirements.txt

# Copy source
COPY backend /app/backend
COPY frontend /app/frontend

# Data volume for persisted flows
RUN mkdir -p /data/flows
ENV PYFLOWS_DATA_DIR=/data/flows

# Default port (can be overridden by cloud platforms via PORT)
ENV PORT=8000
EXPOSE 8000

# Start server (bind to all interfaces); PORT is honored if provided by the platform
CMD ["sh","-c","python -m uvicorn backend.main:app --host 0.0.0.0 --port ${PORT} --loop asyncio"]
