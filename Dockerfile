# SynapseDB CLI Docker Image
FROM eclipse-temurin:17-jre

# Metadata
LABEL maintainer="Amit Tiwari"
LABEL description="SynapseDB - In-Memory Document Database with Lucene-powered Search"
LABEL version="0.1.0"

# Install required tools
RUN apk add --no-cache bash ncurses

# Create app directory
WORKDIR /app

# Copy the CLI JAR
COPY synapsedb-cli/target/synapsedb-cli-0.1.0-SNAPSHOT-jar-with-dependencies.jar /app/synapsedb-cli.jar

# Copy sample data
COPY synapsedb-cli/sample-data /app/sample-data

# Create directories for user data
RUN mkdir -p /data

# Set environment variables
ENV JAVA_OPTS="-Xmx2g -XX:+UseG1GC"

# Expose port (for future REST API)
EXPOSE 9200

# Create entrypoint script
RUN echo '#!/bin/bash' > /app/entrypoint.sh && \
    echo 'java $JAVA_OPTS -jar /app/synapsedb-cli.jar "$@"' >> /app/entrypoint.sh && \
    chmod +x /app/entrypoint.sh

# Set entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]

# Default command (can be overridden)
CMD []

