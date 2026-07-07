# supply-chain-simulator image.
# Build context is the repo root so libs/common can be copied in:
#   docker build -f services/supply-chain-simulator/Dockerfile -t sentinelchain/supply-chain-simulator .
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Shared library first (better layer caching).
COPY libs/common /app/libs/common
COPY services/supply-chain-simulator /app/services/supply-chain-simulator

RUN pip install --upgrade pip \
    && pip install /app/libs/common \
    && pip install /app/services/supply-chain-simulator

# Health + metrics ports (PLAN §23).
EXPOSE 8080 9100

CMD ["python", "-m", "supply_chain_simulator.main"]
