#!/usr/bin/env sh
set -e

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"

echo "Building and starting services..."
docker compose -f "$ROOT_DIR/infra/compose/docker-compose.yml" up -d --build

echo "Services are running:"
docker compose -f "$ROOT_DIR/infra/compose/docker-compose.yml" ps
