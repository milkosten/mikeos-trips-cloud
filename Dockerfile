# mikeos-trips-cloud on Hetzner 242 (moved off Railway 2026-11-14). Railway used nixpacks; this is for 242.
FROM python:3.12-slim
ARG GIT_REVISION=unknown
ENV TRIPS_REVISION=$GIT_REVISION
LABEL org.opencontainers.image.revision=$GIT_REVISION
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY server/ ./server/
COPY migrations/ ./migrations/
EXPOSE 8000
CMD ["uvicorn", "server.http_server:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
