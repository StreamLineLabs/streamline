#!/usr/bin/env bash
#
# Streamline Docker quickstart
# One command to start the container and seed demo data.
#
# Usage: ./scripts/quickstart-docker.sh
#

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-$PROJECT_ROOT/docker-compose.yml}"
CONTAINER_NAME="${CONTAINER_NAME:-streamline}"

if ! command -v docker >/dev/null 2>&1; then
    echo "Docker is required. Install Docker Desktop or docker engine first."
    exit 1
fi

DOCKER_COMPOSE="docker compose"
if ! docker compose version >/dev/null 2>&1; then
    if command -v docker-compose >/dev/null 2>&1; then
        DOCKER_COMPOSE="docker-compose"
    else
        echo "docker compose is required. Install Docker Compose v2."
        exit 1
    fi
fi

echo "Starting Streamline via docker compose..."
$DOCKER_COMPOSE -f "$COMPOSE_FILE" up -d

echo -n "Waiting for server"
for _ in {1..30}; do
    if curl -fsS "http://localhost:9094/health" > /dev/null 2>&1; then
        echo " ✓"
        break
    fi
    sleep 0.5
    echo -n "."
done
echo

if ! curl -fsS "http://localhost:9094/health" > /dev/null 2>&1; then
    echo "Server did not become ready. Check docker logs:"
    echo "  docker logs ${CONTAINER_NAME} --tail 200"
    exit 1
fi

echo "Seeding demo data..."
docker exec "$CONTAINER_NAME" streamline-cli quickstart \
    --no-interactive \
    --no-start-server \
    --server-addr "localhost:9092" \
    --http-addr "localhost:9094" \
    --data-dir "/data"

echo
echo "Next steps:"
echo "  docker exec ${CONTAINER_NAME} streamline-cli topics list"
echo "  docker exec ${CONTAINER_NAME} streamline-cli consume orders --from-beginning"
echo "  docker exec ${CONTAINER_NAME} streamline-cli top"
echo
echo "To stop:"
echo "  ${DOCKER_COMPOSE} -f \"$COMPOSE_FILE\" down"
