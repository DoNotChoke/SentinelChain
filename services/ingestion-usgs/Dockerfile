# ingestion-usgs image.
# Build context is the repo root so libs/common can be copied in:
#   docker build -f services/ingestion-usgs/Dockerfile -t sentinelchain/ingestion-usgs .
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Shared library first (better layer caching).
COPY libs/common /app/libs/common
COPY services/ingestion-usgs /app/services/ingestion-usgs

RUN pip install --upgrade pip \
    && pip install /app/libs/common \
    && pip install /app/services/ingestion-usgs

# Health + metrics ports (PLAN §23).
EXPOSE 8080 9100

CMD ["python", "-m", "ingestion_usgs.main"]
