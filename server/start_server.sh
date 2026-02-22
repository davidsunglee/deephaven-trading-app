#!/bin/bash
# Deephaven Trading Server — Launch Script

echo "=================================================="
echo "  Deephaven Trading Server"
echo "=================================================="

# Check prerequisites
if ! command -v python3 &> /dev/null; then
    echo "ERROR: Python 3 is required."
    exit 1
fi

if ! command -v java &> /dev/null; then
    echo "ERROR: Java 11+ is required. Set JAVA_HOME."
    exit 1
fi

echo "Python: $(python3 --version)"
echo "Java:   $(java -version 2>&1 | head -1)"
echo ""
echo "Server will be available at: http://localhost:10000"
echo "Press Ctrl+C to stop"
echo "=================================================="

python3 -i app.py
