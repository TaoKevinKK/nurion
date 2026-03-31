# Nurion Engine Base Image
# Includes: Python 3.12, JVM 17, FFmpeg, Ray, anvil-py
# Based on Ubuntu 24.04
# 
# This is a base image - engine code is NOT included.
# Mount or copy the engine code at build/runtime.
#
# Build from nurion root directory:
#   docker build -f engine/Dockerfile -t nurion-engine-base .

FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    # Basic tools
    curl wget software-properties-common gnupg tini gosu libnss-wrapper git ca-certificates \
    # JVM (OpenJDK 17)
    openjdk-17-jdk maven \
    # Python 3.12
    python3.12 python3.12-dev python3.12-venv python3-pip \
    # FFmpeg for video processing
    ffmpeg \
    # Build dependencies for Rust packages
    build-essential pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Setup Python alternatives and remove externally-managed marker
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.12 1 && \
    update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.12 1 && \
    rm -f /usr/lib/python3.12/EXTERNALLY-MANAGED

# Install uv (fast Python package manager)
RUN curl -LsSf https://astral.sh/uv/install.sh | sh

# Setup environment
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="/root/.local/bin:${JAVA_HOME}/bin:${PATH}"

WORKDIR /app

# Copy only what's needed for building (anvil-rs and java from lib/)
COPY lib/anvil-rs /app/build/anvil-rs
COPY lib/raydp/java /app/build/java

# Install Rust and protoc, build anvil-rs, then cleanup completely (all in one layer)
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y && \
    export PATH="/root/.cargo/bin:$PATH" && \
    cargo install maturin && \
    apt-get update && apt-get install -y protobuf-compiler && \
    cd /app/build/anvil-rs && \
    maturin build --release && \
    uv pip install --system --no-cache target/wheels/*.whl && \
    # Cleanup Rust, protoc and source completely
    rm -rf /app/build/anvil-rs && \
    rm -rf /root/.cargo && \
    rm -rf /root/.rustup && \
    apt-get remove -y protobuf-compiler && apt-get autoremove -y

# Build RayDP JARs, then cleanup Maven artifacts (all in one layer)
RUN cd /app/build/java && \
    mvn clean package -DskipTests -q && \
    mkdir -p /app/lib/raydp/jars && \
    cp raydp-main/target/raydp-*.jar /app/lib/raydp/jars/ && \
    cp shims/common/target/raydp-shims-*.jar /app/lib/raydp/jars/ && \
    cp shims/spark340/target/raydp-shims-*.jar /app/lib/raydp/jars/ && \
    cp shims/spark350/target/raydp-shims-*.jar /app/lib/raydp/jars/ && \
    # Cleanup Maven build and source
    rm -rf /app/build && \
    rm -rf /root/.m2

# Install pyspark
RUN uv pip install --system --no-cache "pyspark==3.5.6"

# Download extra JARs for Spark S3/Lance support
ENV M=https://repo1.maven.org/maven2
RUN PYSPARK_JARS=$(python -c "import pyspark; print(pyspark.__path__[0])")/jars && \
    rm -f ${PYSPARK_JARS}/arrow-*.jar && \
    wget -q ${M}/org/apache/spark/spark-hadoop-cloud_2.12/3.5.6/spark-hadoop-cloud_2.12-3.5.6.jar \
         ${M}/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
         ${M}/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar \
         ${M}/org/lance/lance-spark-bundle-3.5_2.12/0.1.3-beta.7/lance-spark-bundle-3.5_2.12-0.1.3-beta.7.jar \
         -P ${PYSPARK_JARS}/

# Install Python runtime dependencies (minimal set for Ray + data processing)
RUN uv pip install --system --no-cache \
    "ray[all]>=2.52.0" \
    pyarrow>=18.1.0 \
    pandas>=2.0.0 \
    click>=8.1.7 \
    "fsspec[s3]>=2024.6.0" \
    pylance>=0.38.0 \
    s3fs>=2024.6.0 \
    grpcio>=1.68.0

# Verify installations and remove build dependencies
RUN python -c "import ray; print(f'Ray: {ray.__version__}')" && \
    python -c "import anvil_py; print('anvil_py: OK')" && \
    python -c "import pyarrow; print(f'PyArrow: {pyarrow.__version__}')" && \
    ffmpeg -version | head -1 && \
    java -version 2>&1 | head -1

# Default working directory
WORKDIR /app

# No entrypoint - this is a base image
CMD ["python"]
