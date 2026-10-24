# Multi-stage: build the dashboard, then run backend + static dashboard in one container.
FROM node:22-slim AS web
WORKDIR /app/frontend
COPY frontend/package.json frontend/package-lock.json* ./
RUN npm ci --no-audit --no-fund || npm install --no-audit --no-fund
COPY frontend/ ./
RUN npm run build

FROM python:3.12-slim
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
COPY backend/pyproject.toml backend/
COPY backend/astrq backend/astrq
RUN pip install --no-cache-dir ./backend
COPY config/ config/
COPY --from=web /app/frontend/dist frontend/dist
# Listen on all interfaces inside the container; the port comes from $PORT (Render) or 8000.
ENV ASTRQ_CONFIG=/app/config/settings.yaml \
    ASTRQ_API__HOST=0.0.0.0
EXPOSE 8000
HEALTHCHECK CMD python -c "import os,urllib.request; urllib.request.urlopen(f'http://127.0.0.1:{os.environ.get(\"PORT\",\"8000\")}/health')"
CMD ["astrq", "serve", "--plain-logs"]
