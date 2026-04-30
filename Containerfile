# PCW — multi-stage Podman/OCI build
# Stage 1: builder
FROM docker.io/rust:1.82-bookworm AS builder

WORKDIR /build

# Cache dependency compilation separately from source
COPY Cargo.toml Cargo.lock ./
COPY crates/core/Cargo.toml          crates/core/Cargo.toml
COPY crates/deltashots/Cargo.toml    crates/deltashots/Cargo.toml
COPY crates/infra/Cargo.toml         crates/infra/Cargo.toml
COPY crates/agents/Cargo.toml        crates/agents/Cargo.toml
COPY crates/artifacts/Cargo.toml     crates/artifacts/Cargo.toml
COPY crates/workflows/Cargo.toml     crates/workflows/Cargo.toml
COPY crates/runtime/Cargo.toml       crates/runtime/Cargo.toml
COPY crates/intelligence/Cargo.toml  crates/intelligence/Cargo.toml
COPY crates/storm/Cargo.toml         crates/storm/Cargo.toml
COPY crates/audit/Cargo.toml         crates/audit/Cargo.toml
COPY crates/timeline/Cargo.toml      crates/timeline/Cargo.toml
COPY crates/api/Cargo.toml           crates/api/Cargo.toml

# Stub sources so cargo can fetch and compile deps
RUN find crates -name Cargo.toml -exec bash -c \
    'dir=$(dirname "$1"); mkdir -p "$dir/src"; echo "pub fn _dummy() {}" > "$dir/src/lib.rs"' \
    _ {} \;
RUN mkdir -p crates/api/src/bin && \
    echo "fn main() {}" > crates/api/src/main.rs && \
    echo "fn main() {}" > crates/api/src/bin/storm_worker.rs
RUN cargo build --release 2>&1 | tail -5

# Remove stub artifacts so real sources compile fresh
RUN find target/release/.fingerprint -name "*pcw*" -o -name "*api*" \
    -o -name "*runtime*" -o -name "*storm*" | xargs rm -rf 2>/dev/null; true

# Copy real sources and build
COPY crates ./crates
RUN cargo build --release --bin pcw --bin pcw-storm-worker

# ── Stage 2: minimal runtime image ───────────────────────────────────────────
FROM docker.io/debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /build/target/release/pcw              ./pcw
COPY --from=builder /build/target/release/pcw-storm-worker ./pcw-storm-worker

# Non-root user for rootless Podman
RUN groupadd -r pcw && useradd -r -g pcw -s /sbin/nologin pcw
USER pcw

EXPOSE 8000

ENTRYPOINT ["./pcw"]
