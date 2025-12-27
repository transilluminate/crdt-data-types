#!/bin/bash

# CRDT Rogue - Demo Server Launcher

echo "=================================================="
echo "   CRDT ROGUE - Multiplayer Demo Server"
echo "=================================================="
echo ""
echo "Building server..."

# Build the project
cargo build -p demo-server --release

if [ $? -eq 0 ]; then
    echo ""
    echo "Server built successfully!"
    echo "Starting game server on http://localhost:3000"
    echo "Press Ctrl+C to stop."
    echo ""
    
    # Run the server
    cargo run -p demo-server --release
else
    echo "Build failed. Please check the errors above."
    exit 1
fi
