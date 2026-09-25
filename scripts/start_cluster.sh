#!/usr/bin/env bash
set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
cd "$DIR"

VENV_BIN="$DIR/.venv/bin"
export PYTHONPATH="$DIR"

echo "Starting Vault Cluster (3 Storage Nodes + Gateway on port 8000)..."
exec "$VENV_BIN/python" -m vault.server cluster --nodes 3 --port 8000 --dir ./vault_data
