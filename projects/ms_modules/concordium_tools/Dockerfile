# # ---------------------------------------------------------
# # projects/concordium_tools/Dockerfile
# # One-time heavy build for cargo-concordium + concordium-client
# # ---------------------------------------------------------
# FROM rust:1.80-bookworm AS build

# ARG CONCORDIUM_CLIENT_VERSION="9.1.4-0-rc"

# # Compile cargo-concordium from source
# RUN set -eux; \
#     rustup target add wasm32-unknown-unknown; \
#     cargo install --locked cargo-concordium

# # Download concordium-client binary
# RUN set -eux; \
#     curl -L "https://github.com/Concordium/concordium-client/releases/download/${CONCORDIUM_CLIENT_VERSION}/concordium-client-linux" \
#       -o /usr/local/bin/concordium-client; \
#     chmod +x /usr/local/bin/concordium-client

# # ---------------------------------------------------------
# # Slim runtime image
# # ---------------------------------------------------------
# FROM debian:bookworm-slim

# # Copy built binaries from the builder stage
# COPY --from=build /usr/local/cargo/bin/cargo-concordium /usr/local/bin/cargo-concordium
# COPY --from=build /usr/local/bin/concordium-client /usr/local/bin/concordium-client

# # Create a 'cargo' shim so subprocess(["cargo","concordium",...]) works
# RUN printf '%s\n' '#!/bin/sh' 'exec /usr/local/bin/cargo-concordium "$@"' > /usr/local/bin/cargo && \
#     chmod +x /usr/local/bin/cargo && \
#     apt-get update && apt-get install -y --no-install-recommends ca-certificates && \
#     rm -rf /var/lib/apt/lists/*

# ENTRYPOINT ["/usr/local/bin/cargo-concordium"]

#### Run this locally to build and push the image:
#### may need to do docker login before...
# docker buildx build \
#   --platform linux/amd64 \
#   -t sderuiter/concordium-tools:8.0.0 \
#   -f projects/concordium_tools/Dockerfile \
#   --push .