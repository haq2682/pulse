#!/bin/bash
set -e

echo "=== Manual Setup Script ==="
echo "Note: This should NOT be run during Docker build"
echo ""

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Setting up permissions..."
chmod +x "$SCRIPT_DIR"/*.sh
echo "✓ Permissions set"

echo ""
echo "Setting up spaCy model..."
python -m spacy download en_core_web_md
echo "✓ spaCy model installed"

echo ""
echo "✅ Setup complete!"
echo ""
echo "To start all services, run:"
echo "  docker compose up -d"