#!/bin/bash

# SynapseDB Distribution Builder
# Creates distributable packages for sharing

set -e

echo "╔═══════════════════════════════════════════════════════════╗"
echo "║           SynapseDB Distribution Builder                 ║"
echo "╚═══════════════════════════════════════════════════════════╝"
echo ""

VERSION="0.1.0"
BUILD_DIR="dist"
PROJECT_DIR="/Users/amittiwari/Desktop/synapsedb"

cd "$PROJECT_DIR"

# Clean previous builds
echo "🧹 Cleaning previous builds..."
rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"

# Build the project
echo "🔨 Building SynapseDB..."
mvn clean package -DskipTests

if [ $? -ne 0 ]; then
    echo "❌ Build failed!"
    exit 1
fi

echo "✅ Build successful!"
echo ""

# Create standalone distribution
echo "📦 Creating standalone distribution..."
STANDALONE_DIR="$BUILD_DIR/synapsedb-standalone-$VERSION"
mkdir -p "$STANDALONE_DIR"
mkdir -p "$STANDALONE_DIR/lib"
mkdir -p "$STANDALONE_DIR/sample-data"
mkdir -p "$STANDALONE_DIR/bin"

# Copy JAR
cp synapsedb-cli/target/synapsedb-cli-$VERSION-SNAPSHOT-jar-with-dependencies.jar \
   "$STANDALONE_DIR/lib/synapsedb-cli.jar"

# Copy sample data
cp -r synapsedb-cli/sample-data/* "$STANDALONE_DIR/sample-data/"

# Create launch scripts
cat > "$STANDALONE_DIR/bin/synapsedb.sh" << 'EOF'
#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
JAR_PATH="$SCRIPT_DIR/../lib/synapsedb-cli.jar"
java -jar "$JAR_PATH" "$@"
EOF
chmod +x "$STANDALONE_DIR/bin/synapsedb.sh"

cat > "$STANDALONE_DIR/bin/synapsedb.bat" << 'EOF'
@echo off
set SCRIPT_DIR=%~dp0
set JAR_PATH=%SCRIPT_DIR%..\lib\synapsedb-cli.jar
java -jar "%JAR_PATH%" %*
EOF

# Copy documentation
cp README.md "$STANDALONE_DIR/" 2>/dev/null || true
cp synapsedb-cli/README.md "$STANDALONE_DIR/CLI-README.md" 2>/dev/null || true
cp synapsedb-cli/USAGE.md "$STANDALONE_DIR/" 2>/dev/null || true
cp synapsedb-cli/QUICKSTART.md "$STANDALONE_DIR/" 2>/dev/null || true

# Create README for distribution
cat > "$STANDALONE_DIR/README.txt" << 'EOF'
╔════��══════════════════════════════════════════════════════╗
║              SynapseDB Standalone Distribution            ║
║                    Version 0.1.0                          ║
╚═══════════════════════════════════════════════════════════╝

QUICK START
-----------

Windows:
  bin\synapsedb.bat

Mac/Linux:
  bin/synapsedb.sh

REQUIREMENTS
------------
- Java 11 or higher

USAGE
-----
Once started, type 'help' to see all commands.

Try this:
  use books
  load sample-data/books.json
  count
  find
  index title author description
  search title java

See USAGE.md and QUICKSTART.md for detailed instructions.

EOF

# Create tarball
cd "$BUILD_DIR"
tar -czf "synapsedb-standalone-$VERSION.tar.gz" "synapsedb-standalone-$VERSION"
zip -r "synapsedb-standalone-$VERSION.zip" "synapsedb-standalone-$VERSION" > /dev/null
cd ..

echo "✅ Standalone distribution created:"
echo "   📦 $BUILD_DIR/synapsedb-standalone-$VERSION.tar.gz"
echo "   📦 $BUILD_DIR/synapsedb-standalone-$VERSION.zip"
echo ""

# Build Docker image
echo "🐳 Building Docker image..."
if command -v docker &> /dev/null; then
    docker build -t synapsedb:$VERSION -t synapsedb:latest .

    if [ $? -eq 0 ]; then
        echo "✅ Docker image built: synapsedb:$VERSION"

        # Save Docker image
        echo "💾 Saving Docker image..."
        docker save synapsedb:$VERSION | gzip > "$BUILD_DIR/synapsedb-docker-$VERSION.tar.gz"
        echo "✅ Docker image saved: $BUILD_DIR/synapsedb-docker-$VERSION.tar.gz"
    else
        echo "⚠️  Docker build failed"
    fi
else
    echo "⚠️  Docker not found, skipping Docker build"
fi

echo ""
echo "╔═══════════════════════════════════════════════════════════╗"
echo "║                  Build Complete!                          ║"
echo "╚═══════════════════════════════════════════════════════════╝"
echo ""
echo "📦 Distribution files created in: $BUILD_DIR/"
echo ""
echo "To share with others:"
echo "  1. Send them the .tar.gz or .zip file"
echo "  2. Or push Docker image to registry"
echo "  3. Or share the load-docker.sh script"
echo ""

