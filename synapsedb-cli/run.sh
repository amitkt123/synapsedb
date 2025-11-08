#!/bin/bash

# SynapseDB CLI Launch Script
# This script builds and launches the SynapseDB CLI

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}╔═══════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║              SynapseDB CLI Launcher                      ║${NC}"
echo -e "${BLUE}╚═══════════════════════════════════════════════════════════╝${NC}"
echo ""

# Check if Java is installed
if ! command -v java &> /dev/null; then
    echo -e "${RED}Error: Java is not installed or not in PATH${NC}"
    echo "Please install Java 11 or higher"
    exit 1
fi

# Check Java version
JAVA_VERSION=$(java -version 2>&1 | head -1 | cut -d'"' -f2 | sed 's/\..*//')
if [ "$JAVA_VERSION" -lt 11 ]; then
    echo -e "${RED}Error: Java 11 or higher is required (found Java $JAVA_VERSION)${NC}"
    exit 1
fi

# Determine the script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$SCRIPT_DIR/.."

# Check if JAR exists
JAR_PATH="$SCRIPT_DIR/target/synapsedb-cli-0.1.0-SNAPSHOT-jar-with-dependencies.jar"

if [ ! -f "$JAR_PATH" ]; then
    echo -e "${BLUE}Building SynapseDB CLI...${NC}"
    cd "$PROJECT_ROOT"

    if ! mvn clean package -pl synapsedb-cli -am -DskipTests; then
        echo -e "${RED}Build failed!${NC}"
        exit 1
    fi

    echo -e "${GREEN}✓ Build successful${NC}"
    echo ""
fi

# Check if JAR exists after build
if [ ! -f "$JAR_PATH" ]; then
    echo -e "${RED}Error: JAR file not found at $JAR_PATH${NC}"
    exit 1
fi

# Launch the CLI with any provided arguments
echo -e "${GREEN}Launching SynapseDB CLI...${NC}"
echo ""

java -jar "$JAR_PATH" "$@"

