# Build stage
FROM maven:3.9-eclipse-temurin-17 AS build

WORKDIR /app

# Copy pom.xml first for dependency caching
COPY pom.xml .

# Download dependencies (cached unless pom.xml changes)
RUN mvn dependency:go-offline -B

# Copy source code
COPY src ./src

# Build the fat JAR
RUN mvn clean package -DskipTests -B

# Runtime stage - using slim instead of alpine for RocksDB glibc compatibility
FROM eclipse-temurin:17-jre-jammy

WORKDIR /app

# Create non-root user for security
RUN groupadd -g 1000 streams && \
    useradd -u 1000 -g streams -s /bin/bash streams

# Create state directory for RocksDB
RUN mkdir -p /var/kafka-streams && \
    chown -R streams:streams /var/kafka-streams

# Copy the fat JAR from build stage
COPY --from=build /app/target/kafka-streams-topology-1.0.0.jar /app/app.jar

# Switch to non-root user
USER streams

# Environment variables (override in docker-compose or k8s)
ENV KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
    APPLICATION_ID=ev-opportunity-streams \
    STATE_DIR=/var/kafka-streams \
    COMMIT_INTERVAL_MS=500 \
    CACHE_MAX_BYTES=10485760 \
    NUM_STREAM_THREADS=2 \
    REPLICATION_FACTOR=1 \
    JAVA_OPTS="-Xmx2G -XX:+UseG1GC -XX:MaxGCPauseMillis=20"

# Health check - verify process is running
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD pgrep -f "app.jar" || exit 1

# Run the application
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar /app/app.jar"]
