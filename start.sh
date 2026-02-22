#!/bin/bash

# Real-Time Trading App — Deephaven.io
echo "=================================================="
echo "  Real-Time Trading App with Deephaven.io"
echo "=================================================="

# Check prerequisites
if ! command -v python3 &> /dev/null; then
    echo "ERROR: Python 3 is required. Install it first."
    exit 1
fi

if [ -z "$JAVA_HOME" ]; then
    echo "ERROR: JAVA_HOME is not set. Deephaven requires Java 11+."
    exit 1
fi

echo "Python:    $(python3 --version)"
echo "Java:      $JAVA_HOME"
echo ""
echo "Dashboard: http://localhost:10000"
echo "Press Ctrl+C to stop"
echo "=================================================="

# Run in interactive mode so the server stays alive
python3 -i app.py
