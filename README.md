# Resilio: Fault-Tolerant Distributed Object Storage System
*Resilio — evokes resilience and recovery*

Resilio is a distributed, fault-tolerant object storage system designed to store, replicate, retrieve, verify, and automatically repair objects across multiple independent storage nodes. It remains available and protects data integrity under node failures, network partitions, and on-disk bitrot corruption.

---

## 1. System Architecture

```
                                  +---------------------------------------+
                                  |                Clients                |
                                  |   (vaultctl CLI, REST API, Web UI)    |
                                  +-------------------+-------------------+
                                                      | HTTP / REST
                                                      v
                        +===========================================================+
                        |                Resilio Gateway Coordinator                |
                        |  - Request Router & Streaming Pipeline                    |
                        |  - Quorum Enforcer (N=3, W=2, R=2 -> Strong Consistency)  |
                        |  - Multipart Upload Engine                                |
                        |  - Failure Detector & Liveness Heartbeats                 |
                        |  - Inline Read-Repair & Active Scrubber Worker            |
                        |  - Rate-Limited Rebalancing Engine                        |
                        +=============================+=============================+
                                                      |
                        +-----------------------------+-----------------------------+
                        |                                                           |
                        v                                                           v
        +-------------------------------+                           +-------------------------------+
        |  Consistent Hash Ring Engine  |                           |  Transactional Metadata Store |
        |  - 128 Virtual Nodes / Node   |                           |  - SQLite WAL Mode & OCC      |
        |  - Failure-Domain Placement   |                           |  - Object Versions & Replicas |
        +-------------------------------+                           |  - Tombstones & Multipart     |
                                                                    +-------------------------------+
                                                      |
                        +-----------------------------+-----------------------------+
                        |                             |                             |
                        v                             v                             v
        +-------------------------------+ +-------------------------------+ +-------------------------------+
        |    Storage Node 1 (:9001)     | |    Storage Node 2 (:9002)     | |    Storage Node 3 (:9003)     |
        |  - Local Object Store         | |  - Local Object Store         | |  - Local Object Store         |
        |  - Chunked Streaming I/O      | |  - Chunked Streaming I/O      | |  - Chunked Streaming I/O      |
        |  - Atomic Staging & Rename    | |  - Atomic Staging & Rename    | |  - Atomic Staging & Rename    |
        |  - SHA-256 On-Disk Verifier   | |  - SHA-256 On-Disk Verifier   | |  - SHA-256 On-Disk Verifier   |
        +-------------------------------+ +-------------------------------+ +-------------------------------+
```

---

## 2. Core Capabilities

### A. Object Operations & Concurrency
- **Streaming Uploads & Atomic Staging**: Payload bytes are streamed directly to isolated staging directories (`.staging/<uuid>.tmp`) on candidate storage nodes. SHA-256 checksums are calculated on the fly. Once the write quorum is satisfied, staging files are atomically renamed to their final paths.
- **Multipart Uploads**: Handles large files via chunked uploads (`/api/v1/multipart/init`, `part`, `complete`, `abort`). Out-of-order part uploads are assembled into a contiguous object manifest.
- **Optimistic Concurrency Control (OCC)**: Every object update generates a unique version ID and monotonically increments revisions. Prevents lost updates using `If-Match: <etag>` and `If-None-Match: *` headers.
- **S3-Compatible Object Listing**: Supports prefix matching, delimiter-based directory simulation, and marker pagination.

### B. Replication & Durability Policy
- **Configurable Quorum**:
  - $N$ (Replication Factor, default: `3`)
  - $W$ (Write Quorum, default: `2`)
  - $R$ (Read Quorum, default: `2`)
- **Consistency Guarantees**:
  - When $R + W > N$, the system guarantees **Strong Consistency** (any read quorum intersects with the write quorum that acknowledged the latest write).
  - Clients receive HTTP 201 only when at least $W$ independent nodes acknowledge on-disk persistence.
  - If fewer than $W$ nodes succeed, the write fails and staged temporary files are rolled back.
- **Failure Domain Diversity**: Replicas are placed across distinct racks and failure zones using consistent hashing.

### C. Failure Handling & Network Partitions
- **Node Health State Machine**:
  - Heartbeat monitor runs every 2s checking `/node/heartbeat`.
  - `HEALTHY`: Responding normally.
  - `SUSPECT`: Missed $\ge 2$ consecutive heartbeats.
  - `DEAD`: Missed $\ge 5$ consecutive heartbeats ($> 10\text{s}$).
- **Partition Behavior**:
  - If 1 node fails in a 3-node cluster ($W=2, R=2$), reads and writes continue uninterrupted.
  - If 2 nodes fail, the write quorum ($W=2$) cannot be met; write requests are rejected cleanly with `503 Quorum Not Satisfied`, preventing split-brain or phantom commits.

### D. Data Integrity & Self-Healing
- **SHA-256 Verification**: Every object has an incremental SHA-256 digest computed at upload time, verified on-disk, and stored in metadata.
- **Inline Read Repair**: During reads, the coordinator queries candidate replicas. If a replica returns corrupted bytes or is missing, the coordinator serves the verified healthy replica to the client while immediately repairing the corrupted node in the background.
- **Active Background Scrubber**: Periodically iterates over all cataloged objects, sends verification requests to storage nodes, identifies bitrot, and restores degraded replicas from healthy donor copies.
- **Unrecoverable Object Alerting**: If all replicas for an object are damaged ($0$ healthy copies), the system flags the object as `CRITICAL_UNRECOVERABLE` and increments telemetry counters.

### E. Rate-Limited Rebalancing
- **Consistent Hashing**: Uses MD5/SHA-256 consistent hash ring with 128 virtual nodes per physical storage node to ensure uniform key distribution.
- **Migration Engine**: When nodes are added or removed, the rebalancer computes replica placement deltas and migrates objects to new target nodes.
- **Token-Bucket Throttling**: Limits migration bandwidth (configurable byte-per-second ceiling) and task concurrency to protect foreground client operations.

### F. Observability & Control Plane
- **Interactive Web Dashboard**: Accessible at `http://localhost:8000/dashboard` featuring dark-mode aesthetics, real-time node health badges, storage utilization, ring topology, event logs, object explorer, and chaos testing controls.
- **Prometheus Metrics**: Exported at `/metrics` (request counts, latencies, active rebalances, corruptions detected, repairs performed, node counts).
- **JSON Telemetry**: Available at `/api/v1/metrics` and `/api/v1/cluster/status`.
- **Administrative CLI (`vaultctl`)**: Full command-line interface for cluster administration, uploads, downloads, scrubs, and rebalances.

---

## 3. Durability & Availability Trade-offs

| Parameter Setup | Consistency Guarantee | Availability on 1 Node Failure | Availability on 2 Node Failures | Trade-Off Description |
|---|---|---|---|---|
| **$N=3, W=2, R=2$ (Default)** | **Strong** ($R+W=4 > 3$) | **Full Read & Write** | Read/Write Unavailable (Quorum Lost) | Balanced majority quorum. Protects against bitrot and stale reads while tolerating 1 node crash. |
| **$N=3, W=3, R=1$** | **Strong** ($R+W=4 > 3$) | Read Available, Write Unavailable | Write Unavailable | Extremely fast reads, but any node crash blocks writes. High durability requirement. |
| **$N=3, W=1, R=1$** | **Eventual** ($R+W=2 \le 3$) | Full Read & Write | Full Read & Write | High availability (AP), but susceptible to stale reads and conflicting concurrent writes. |

---

## 4. Quickstart Guide

### 1. Prerequisites
- Python 3.9+

### 2. Installation
```bash
# Clone and create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -e .
```

### 3. Launching the Cluster
```bash
# Run local 3-node storage cluster with gateway coordinator on port 8000
./scripts/start_cluster.sh
```
Or launch using `vault.server`:
```bash
python -m vault.server cluster --nodes 3 --port 8000 --dir ./vault_data
```

### 4. Open the Web Dashboard
Navigate to:
```
http://localhost:8000/dashboard
```

---

## 5. CLI (`vaultctl`) Usage

### Cluster Status
```bash
vaultctl --gateway http://127.0.0.1:8000 status
```

### Uploading an Object
```bash
vaultctl --gateway http://127.0.0.1:8000 put photos profile.jpg ./my_photo.jpg
```

### Downloading an Object
```bash
vaultctl --gateway http://127.0.0.1:8000 get photos profile.jpg -o ./downloaded.jpg
```

### Listing Objects
```bash
vaultctl --gateway http://127.0.0.1:8000 ls photos --prefix profile
```

### Triggering Active Integrity Scrubber
```bash
vaultctl --gateway http://127.0.0.1:8000 scrub
```

### Triggering Cluster Rebalance
```bash
vaultctl --gateway http://127.0.0.1:8000 rebalance
```

---

## 6. Verification & Automated Test Suite

The test suite covers concurrent operations, node failures, interrupted transfers, corrupted data, stale replicas, read-repairs, and rebalancing:

```bash
# Run all unit and integration tests
PYTHONPATH=. .venv/bin/pytest -v
```

### Test Coverage Summary:
- `tests/test_hashing.py`: Consistent hash ring token distribution, virtual nodes (128 vnodes), ring walks, node joins and removals.
- `tests/test_metadata.py`: Transactional SQLite WAL catalog, optimistic concurrency control, ETag matching, version lineage, soft deletes.
- `tests/test_storage_node.py`: Chunked streaming I/O, atomic staging and commit, SHA-256 verification, HTTP Range requests.
- `tests/test_quorum_engine.py`: Quorum writes ($W=2$) and reads ($R=2$), rollback on quorum failure, OCC versioning.
- `tests/test_concurrent_ops.py`: Concurrent writes to distinct keys and concurrent updates to the same key without lost updates.
- `tests/test_multipart.py`: Large object chunking, out-of-order parts, assembled manifest verification, abort cleanup.
- `tests/test_failure_handling.py`: Heartbeat failure detection, node status degradation (`HEALTHY` -> `SUSPECT` -> `DEAD`), single node failure resilience, quorum loss write rejection.
- `tests/test_integrity_repair.py`: On-disk bitrot corruption injection, inline read-repair restoration, active background scrubber, unrecoverable data alert.
- `tests/test_rebalance.py`: Dynamic addition of a 4th node, ring delta calculation, token-bucket migration, replica pruning.
- `tests/test_gateway_api.py`: Full REST API integration with FastAPI and `httpx`.

---

## 7. Interactive End-to-End Demo Script

To execute a complete automated demonstration of the entire system (cluster startup, quorum uploads, listing, bitrot injection, inline read-repair, active scrubber verification, rebalancing, and Prometheus telemetry):

```bash
./scripts/demo.sh
```

---

## 8. Operational Limitations & Architecture Trade-offs

1. **Coordinator-Driven Metadata**: Metadata is centralized in a transactional SQLite engine with Write-Ahead Logging (WAL) and optimistic locking. While SQLite WAL delivers high throughput and transactional ACID guarantees for single-coordinator deployments, multi-region active-active coordinators would require a distributed consensus log (such as Raft or etcd).
2. **Network Partitions**: The coordinator uses a heartbeat threshold ($\ge 5$ misses) to classify nodes as `DEAD`. In asymmetric network partitions where a node can reach the coordinator but not peer nodes, client retries fallback to alternative nodes in the ring.
3. **Bandwidth Throttling**: The rebalancer uses token-bucket rate limiting to prevent disk I/O saturation. High transfer rates on resource-constrained disks may temporarily increase foreground request latency.
