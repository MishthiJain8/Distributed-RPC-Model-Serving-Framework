FROM ubuntu:22.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    git \
    pkg-config \
    libssl-dev \
    zlib1g-dev \
    libprotobuf-dev \
    protobuf-compiler \
    protobuf-compiler-grpc \
    libgrpc++-dev \
    libgrpc++1 \
    && rm -rf /var/lib/apt/lists/*

# Ensure grpc_cpp_plugin is discoverable
ENV PATH="/usr/bin:/usr/local/bin:${PATH}"

WORKDIR /workspace

COPY . /workspace

# Debug check + build
RUN which grpc_cpp_plugin && \
    grpc_cpp_plugin --version || true

RUN cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && \
    cmake --build build -- -j$(nproc)

FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    libgrpc++1 \
    libprotobuf23 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /workspace/build/orchestrator /app/orchestrator
COPY --from=builder /workspace/build/worker /app/worker
COPY --from=builder /workspace/build/client /app/client

EXPOSE 50051 50052 50053 50054

ENTRYPOINT ["/app/orchestrator"]