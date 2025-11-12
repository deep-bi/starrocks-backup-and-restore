#!/bin/bash
set -e

echo "Building starrocks-br executable..."

if ! command -v pyinstaller &> /dev/null; then
    echo "Installing PyInstaller..."
    pip install pyinstaller
fi

echo "Creating executable..."
pyinstaller --onefile --name starrocks-br --paths src entry_point.py

echo ""
echo "✓ Build complete!"
echo "✓ Executable location: $(pwd)/dist/starrocks-br"
echo ""
echo "You can now distribute this executable to your client."
echo "They can run it directly without Python or any dependencies:"
echo "  ./dist/starrocks-br --help"
