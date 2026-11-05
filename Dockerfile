# Multi-stage: build the dashboard, build the Rust backend, run both from a slim image.
FROM node:22-slim AS web
WORKDIR /app/frontend
COPY frontend/package.json frontend/package-lock.json* ./
RUN npm ci --no-audit --no-fund || npm install --no-audit --no-fund
COPY frontend/ ./
RUN npm run build

FROM rust:1-slim-bookworm AS rust
WORKDIR /app/backend-rs
# Build dependencies first (cached layer) with a placeholder crate.
COPY backend-rs/Cargo.toml backend-rs/Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs && echo '' > src/lib.rs \
    && cargo build --release --locked && rm -rf src target/release/deps/*astrq* target/release/.fingerprint/astrq-* target/release/astrq*
COPY backend-rs/src ./src
RUN touch src/main.rs src/lib.rs && cargo build --release --locked

FROM debian:bookworm-slim
WORKDIR /app
COPY --from=rust /app/backend-rs/target/release/astrq /usr/local/bin/astrq
COPY config/ config/
COPY --from=web /app/frontend/dist frontend/dist
# Listen on all interfaces inside the container; the port comes from $PORT (Render) or 8000.
ENV ASTRQ_CONFIG=/app/config/settings.yaml \
    ASTRQ_API__HOST=0.0.0.0
EXPOSE 8000
USER nobody
CMD ["astrq", "serve", "--plain-logs"]
