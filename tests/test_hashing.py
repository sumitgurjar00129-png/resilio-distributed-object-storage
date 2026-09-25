"""
Unit tests for Consistent Hashing Ring with Virtual Nodes.
"""

import pytest
from vault.hashing import ConsistentHashRing


def test_ring_add_and_remove_nodes():
    ring = ConsistentHashRing(vnodes=64)
    ring.add_node("node-1")
    ring.add_node("node-2")
    ring.add_node("node-3")

    assert len(ring.nodes) == 3
    assert len(ring.ring) == 3 * 64

    # Lookup primary node
    node = ring.get_primary_node("my-test-key")
    assert node in ("node-1", "node-2", "node-3")

    # Remove node
    ring.remove_node("node-1")
    assert len(ring.nodes) == 2
    assert len(ring.ring) == 2 * 64
    assert "node-1" not in ring.nodes


def test_ring_get_distinct_nodes():
    ring = ConsistentHashRing(vnodes=64)
    nodes = ["node-1", "node-2", "node-3", "node-4"]
    for n in nodes:
        ring.add_node(n)

    # Request 3 distinct nodes for a key
    selected = ring.get_nodes("documents/invoice.pdf", count=3)
    assert len(selected) == 3
    assert len(set(selected)) == 3  # All distinct

    # Exclude dead node
    selected_ex = ring.get_nodes("documents/invoice.pdf", count=3, exclude_nodes={"node-1"})
    assert "node-1" not in selected_ex
    assert len(selected_ex) == 3


def test_ring_distribution_uniformity():
    ring = ConsistentHashRing(vnodes=128)
    nodes = [f"node-{i}" for i in range(1, 6)]
    for n in nodes:
        ring.add_node(n)

    counts = {n: 0 for n in nodes}
    num_keys = 2000
    for i in range(num_keys):
        n = ring.get_primary_node(f"key-{i}")
        counts[n] += 1

    # In uniform distribution with 128 vnodes across 5 nodes, each node gets ~400 keys (within reasonable bound)
    for n, cnt in counts.items():
        assert cnt > 200, f"Node {n} received too few keys: {cnt}"
        assert cnt < 600, f"Node {n} received too many keys: {cnt}"
