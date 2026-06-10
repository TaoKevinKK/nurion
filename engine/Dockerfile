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

# Mirror the repo's lib/ layout in the image so RayDP's protobuf-maven-plugin
# can resolve its proto source: raydp-main/pom.xml points protoSourceRoot at
# ${project.basedir}/../../../anvil-rs/proto, i.e. the sibling lib/anvil-rs/proto.
# Copying to /app/build/lib/{anvil-rs,raydp/java} keeps that relative path valid.
COPY lib/anvil-rs /app/build/lib/anvil-rs
COPY lib/raydp/java /app/build/lib/raydp/java

# Build the anvil-rs Python wheel and the RayDP JARs in a single layer. Both
# consume anvil-rs/proto (anvil-rs via prost, RayDP's gRPC stubs via grpc-java),
# and the RayDP maven build must run while that proto is still present, so the
# two cannot be split without either breaking proto resolution or bloating the
# image. Clean every build dependency (Rust, protoc, .m2, sources) at the end.
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y && \
    export PATH="/root/.cargo/bin:$PATH" && \
    cargo install maturin && \
    apt-get update && apt-get install -y protobuf-compiler && \
    # anvil-rs Python wheel
    cd /app/build/lib/anvil-rs && \
    maturin build --release && \
    uv pip install --system --no-cache target/wheels/*.whl && \
    # RayDP JARs (Spark 4.1 / Scala 2.13); gRPC stubs generated from anvil proto
    cd /app/build/lib/raydp/java && \
    mvn -P scala-2.13 clean package -DskipTests -q && \
    mkdir -p /app/lib/raydp/jars && \
    cp raydp-main/target/raydp_2.13-*.jar /app/lib/raydp/jars/ && \
    cp shims/common/target/raydp-shims-*_2.13-*.jar /app/lib/raydp/jars/ && \
    cp shims/spark410/target/raydp-shims-spark410_2.13-*.jar /app/lib/raydp/jars/ && \
    # Cleanup all build deps + sources in this same layer
    rm -rf /app/build /root/.m2 /root/.cargo /root/.rustup && \
    apt-get remove -y protobuf-compiler && apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

# Install pyspark (Spark 4.1.x, Scala 2.13)
RUN uv pip install --system --no-cache "pyspark>=4.1,<5"

# Download extra JARs for Spark S3/Lance support (Scala 2.13 / Spark 4.1 variants)
ENV M=https://repo1.maven.org/maven2
RUN PYSPARK_JARS=$(python -c "import pyspark; print(pyspark.__path__[0])")/jars && \
    rm -f ${PYSPARK_JARS}/arrow-*.jar && \
    wget -q ${M}/org/apache/spark/spark-hadoop-cloud_2.13/4.1.1/spark-hadoop-cloud_2.13-4.1.1.jar \
         ${M}/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
         ${M}/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar \
         ${M}/org/lance/lance-spark-bundle-4.1_2.13/0.5.1/lance-spark-bundle-4.1_2.13-0.5.1.jar \
         -P ${PYSPARK_JARS}/

# Install Python runtime dependencies (minimal set for Ray + data processing)
RUN uv pip install --system --no-cache \
    "ray[all]>=2.52.0" \
    pyarrow>=18.1.0 \
    pandas>=2.0.0 \
    click>=8.1.7 \
    "fsspec[s3]>=2024.6.0" \
    pylance==7.0.0 \
    s3fs>=2024.6.0 \
    grpcio>=1.68.0

# Install the raydp Python package (raydp.init_spark) so this base is a complete
# Spark-on-Ray image, not just the jars. The Scala-2.13 jars built above
# (/app/lib/raydp/jars) are staged into the package — raydp/utils.py resolves
# jars at <pkg>/jars/ — and raydp's runtime deps (ray, pyspark, pandas) are
# installed above. The source is copied from the build context, so there is no
# Maven rebuild and downstream consumers (e.g. data-api query_processor) need no
# nurion git access to get `import raydp`.
COPY lib/raydp/__init__.py lib/raydp/context.py lib/raydp/utils.py /tmp/raydp-src/
COPY lib/raydp/spark /tmp/raydp-src/spark
RUN SP="$(python -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])')" && \
    mkdir -p "$SP/raydp/jars" && \
    cp /tmp/raydp-src/*.py "$SP/raydp/" && \
    cp -r /tmp/raydp-src/spark "$SP/raydp/spark" && \
    cp /app/lib/raydp/jars/*.jar "$SP/raydp/jars/" && \
    rm -rf /tmp/raydp-src && \
    python -c "import raydp; print('raydp:', raydp.__file__)"

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
