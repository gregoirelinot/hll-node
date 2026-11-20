# syntax=docker/dockerfile:1.7
# Multi-arch friendly (build on r7g/m7g with --platform linux/arm64 or r7i with linux/amd64).
FROM rust:1.91-bookworm AS build
WORKDIR /src
# Cache dependencies separately from sources.
COPY Cargo.toml Cargo.lock build.rs ./
COPY proto ./proto
RUN mkdir src && echo 'fn main(){}' > src/main.rs && echo '' > src/lib.rs \
 && cargo build --release --locked --bin hll-node || true
COPY src ./src
RUN touch src/main.rs src/lib.rs && cargo build --release --locked --bin hll-node

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /src/target/release/hll-node /usr/local/bin/hll-node
EXPOSE 7280/udp 8080/tcp 9090/tcp
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/hll-node"]
