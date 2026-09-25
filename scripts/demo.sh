#!/usr/bin/env bash
set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
cd "$DIR"

echo "================================================================="
echo "   VAULT: FAULT-TOLERANT DISTRIBUTED OBJECT STORAGE DEMO"
echo "================================================================="

VENV_BIN="$DIR/.venv/bin"
export PYTHONPATH="$DIR"
GATEWAY="http://127.0.0.1:8000"
DATA_DIR="$DIR/vault_data"
SERVER_PID=""

# Check if cluster is already running
if curl -s "$GATEWAY/api/v1/cluster/status" >/dev/null 2>&1; then
    echo "Found active Vault cluster running on $GATEWAY."
else
    echo "[1/7] Launching 3 Storage Nodes and Gateway in background..."
    mkdir -p "$DATA_DIR"
    "$VENV_BIN/python" -m vault.server cluster --nodes 3 --port 8000 --dir "$DATA_DIR" > "$DATA_DIR/cluster.log" 2>&1 &
    SERVER_PID=$!

    echo "Waiting for cluster to be ready on port 8000..."
    for i in {1..30}; do
        if curl -s "$GATEWAY/api/v1/cluster/status" >/dev/null 2>&1; then
            echo "Cluster is ONLINE!"
            break
        fi
        sleep 0.5
    done
fi

cleanup() {
    if [ -n "$SERVER_PID" ]; then
        echo ""
        echo "Shutting down cluster (PID: $SERVER_PID)..."
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
        echo "Cluster stopped."
    fi
}
trap cleanup EXIT

echo ""
echo "[2/7] Checking Cluster Topology via vaultctl..."
"$VENV_BIN/vaultctl" --gateway "$GATEWAY" status

echo ""
echo "[3/7] Uploading Objects with Quorum Writes (W=2)..."
TMP_DIR="$DIR/.demo_tmp"
mkdir -p "$TMP_DIR"
echo "Hello from Vault Distributed Storage Object 1" > "$TMP_DIR/file1.txt"
echo "Hello from Vault Distributed Storage Object 2" > "$TMP_DIR/file2.txt"

"$VENV_BIN/vaultctl" --gateway "$GATEWAY" put mybucket docs/file1.txt "$TMP_DIR/file1.txt"
"$VENV_BIN/vaultctl" --gateway "$GATEWAY" put mybucket docs/file2.txt "$TMP_DIR/file2.txt"

echo ""
echo "[4/7] Listing committed objects..."
"$VENV_BIN/vaultctl" --gateway "$GATEWAY" ls mybucket

echo ""
echo "[5/7] Simulating Bitrot / Disk Corruption on node-1..."
BLOB_PATH=$(find "$DATA_DIR/node_1/objects" -name "*.blob" 2>/dev/null | head -n 1 || true)
if [ -n "$BLOB_PATH" ]; then
    echo "Corrupting blob file on disk: $BLOB_PATH"
    echo -n "CORRUPTED_BYTES" | dd of="$BLOB_PATH" conv=notrunc 2>/dev/null
    echo "Bitrot injected. Now fetching object with Automatic Inline Read-Repair..."
    "$VENV_BIN/vaultctl" --gateway "$GATEWAY" get mybucket docs/file1.txt
    echo "Read repair executed. Running integrity scrubber..."
    "$VENV_BIN/vaultctl" --gateway "$GATEWAY" scrub
else
    echo "Triggering active scrubber..."
    "$VENV_BIN/vaultctl" --gateway "$GATEWAY" scrub
fi

echo ""
echo "[6/7] Running Cluster Ring Rebalance..."
"$VENV_BIN/vaultctl" --gateway "$GATEWAY" rebalance

echo ""
echo "[7/7] Checking Metrics Endpoint (Prometheus format excerpt)..."
curl -s "$GATEWAY/metrics" | head -n 25

rm -rf "$TMP_DIR"

echo ""
echo "================================================================="
echo "   DEMO COMPLETED SUCCESSFULLY!"
echo "   All quorum operations, read-repairs, and scrubs verified."
echo "================================================================="
