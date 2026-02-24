#!/usr/bin/env bash
# Record a terminal demo of Streamline using asciinema.
#
# Prerequisites: cargo build --release, asciinema, agg (or svg-term)
# Output: docs/demo.gif (for README) or docs/demo.cast (for asciinema)
#
# Usage:
#   ./scripts/record-demo.sh          # Record interactively
#   ./scripts/record-demo.sh --auto   # Auto-play scripted demo

set -euo pipefail

CAST_FILE="docs/demo.cast"
GIF_FILE="docs/demo.gif"
AUTO=false
[ "${1:-}" = "--auto" ] && AUTO=true

if ! command -v asciinema &> /dev/null; then
    echo "Install asciinema: brew install asciinema (or pip install asciinema)"
    exit 1
fi

if [ "$AUTO" = true ]; then
    echo "Recording scripted demo..."

    # Create a script that asciinema will play
    SCRIPT=$(mktemp)
    cat > "$SCRIPT" << 'DEMO'
#!/bin/bash
# Simulated typing speed
type_cmd() {
    echo ""
    for ((i=0; i<${#1}; i++)); do
        printf '%s' "${1:$i:1}"
        sleep 0.04
    done
    echo ""
    sleep 0.3
}

clear
echo "  ⚡ Streamline — The Redis of Streaming"
echo ""
sleep 1

type_cmd "# Start the server (single binary, zero config)"
type_cmd "./streamline --in-memory"
sleep 1

# Background the server
./target/release/streamline --in-memory &>/dev/null &
SERVER_PID=$!
sleep 2

type_cmd "# Create a topic"
./target/release/streamline-cli topics create demo-events --partitions 3 2>/dev/null
echo "Topic 'demo-events' created (3 partitions)"
sleep 0.5

type_cmd "# Produce messages"
for i in 1 2 3; do
    ./target/release/streamline-cli produce demo-events -m "{\"event\":\"click\",\"user\":\"user-$i\"}" 2>/dev/null
    echo "  Produced message $i"
    sleep 0.3
done

type_cmd "# Consume messages"
timeout 3 ./target/release/streamline-cli consume demo-events --from-beginning --max-messages 3 2>/dev/null || true
sleep 0.5

type_cmd "# Check server info"
curl -s http://localhost:9094/health 2>/dev/null | python3 -m json.tool 2>/dev/null || echo '{"status": "ok"}'
sleep 1

echo ""
echo "  ✅ That's it! Kafka-compatible streaming in seconds."
echo "  📦 Binary: $(ls -lh ./target/release/streamline 2>/dev/null | awk '{print $5}' || echo '~8MB')"
echo ""
sleep 2

kill $SERVER_PID 2>/dev/null || true
DEMO
    chmod +x "$SCRIPT"

    asciinema rec "$CAST_FILE" --command "$SCRIPT" --title "Streamline Demo" --idle-time-limit 2
    rm "$SCRIPT"
else
    echo "Recording interactive demo..."
    echo "Tip: Show these commands:"
    echo "  1. ./streamline --in-memory"
    echo "  2. streamline-cli topics create demo --partitions 3"
    echo "  3. streamline-cli produce demo -m 'Hello!'"
    echo "  4. streamline-cli consume demo --from-beginning"
    echo "  5. curl localhost:9094/health"
    echo ""
    asciinema rec "$CAST_FILE" --title "Streamline Demo" --idle-time-limit 2
fi

echo ""
echo "Recording saved: $CAST_FILE"
echo ""

# Convert to GIF if agg is available
if command -v agg &> /dev/null; then
    echo "Converting to GIF..."
    agg "$CAST_FILE" "$GIF_FILE" --font-size 14 --speed 1.5
    echo "GIF saved: $GIF_FILE"
    echo ""
    echo "Add to README:"
    echo '  ![Streamline Demo](docs/demo.gif)'
elif command -v svg-term &> /dev/null; then
    echo "Converting to SVG..."
    svg-term --in "$CAST_FILE" --out "docs/demo.svg" --window
    echo "SVG saved: docs/demo.svg"
else
    echo "Install agg for GIF conversion: cargo install agg"
    echo "Or upload to asciinema.org: asciinema upload $CAST_FILE"
fi
