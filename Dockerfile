FROM rust:1.67 as builder
WORKDIR /app
COPY . .
RUN cargo clean
RUN cargo install --path .

FROM debian:bookworm-slim
RUN apt-get update && \
    apt-get install -y \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd -r user && useradd -r -g user user

COPY --from=builder /usr/local/cargo/bin/kafka-queue-over-http-rust /usr/local/bin/kafka-queue-over-http-rust
COPY ./scripts/start.sh ./start.sh
COPY ./scripts/wait-for-it.sh ./wait-for-it.sh
RUN chmod +x /wait-for-it.sh \
    && chmod +x /start.sh \
    && chown user:user /start.sh \
    && chown user:user /wait-for-it.sh

USER user:user