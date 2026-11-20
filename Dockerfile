# Runtime image: the Rust backend + the dashboard (built by astrq-platform-ui).
#
# The dashboard is not built here: CI downloads the release asset pinned in `ui.version`
# into ./ui-dist before `docker build` (see .github/workflows/image.yml, `make ui-dist`).
# An empty ./ui-dist gives an API-only image (Swagger UI at /docs still works).

FROM rust:1-slim-bookworm AS rust
WORKDIR /app
# Build dependencies first (cached layer) with a placeholder crate.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs && echo '' > src/lib.rs \
    && cargo build --release --locked && rm -rf src target/release/deps/*astrq* target/release/.fingerprint/astrq-* target/release/astrq*
COPY src ./src
RUN touch src/main.rs src/lib.rs && cargo build --release --locked

FROM debian:bookworm-slim
WORKDIR /app
COPY --from=rust /app/target/release/astrq /usr/local/bin/astrq
COPY config/ config/
COPY ui-dist/ frontend/dist/
# Listen on all interfaces inside the container; the port comes from $PORT (Render) or 8000.
ENV ASTRQ_CONFIG=/app/config/settings.yaml \
    ASTRQ_API__HOST=0.0.0.0
EXPOSE 8000
USER nobody
CMD ["astrq", "serve", "--plain-logs"]
