FROM rust:1.98-slim-bookworm AS build
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY web ./web
RUN cargo build --release --locked --jobs 1

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates curl libgcc-s1 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --home-dir /app appuser
WORKDIR /app
COPY --from=build /app/target/release/btc-execution-lab /app/bin/btc-execution-lab
USER appuser
EXPOSE 4330
CMD ["/app/bin/btc-execution-lab"]
