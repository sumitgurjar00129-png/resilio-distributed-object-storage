"""
Consistent Hashing Ring with Virtual Nodes.
Provides deterministic, uniform key placement across storage nodes with minimal redistribution.
"""

from __future__ import annotations
import bisect
import hashlib
from typing import List, Dict, Set, Optional


class ConsistentHashRing:
    def __init__(self, vnodes: int = 128) -> None:
        self.vnodes = vnodes
        self.ring: List[int] = []  # Sorted hash tokens
        self.token_to_node: Dict[int, str] = {}  # token -> physical node_id
        self.nodes: Set[str] = set()

    def _hash(self, key: str) -> int:
        """64-bit integer hash from MD5."""
        digest = hashlib.md5(key.encode("utf-8")).digest()
        # Take first 8 bytes for a 64-bit integer
        return int.from_bytes(digest[:8], byteorder="big", signed=False)

    def add_node(self, node_id: str, weight: int = 1) -> None:
        """Add a physical node with virtual nodes proportional to its weight."""
        if node_id in self.nodes:
            return
        self.nodes.add(node_id)
        num_vnodes = self.vnodes * max(1, weight)
        for i in range(num_vnodes):
            vnode_key = f"{node_id}#vnode-{i}"
            token = self._hash(vnode_key)
            self.token_to_node[token] = node_id
            bisect.insort(self.ring, token)

    def remove_node(self, node_id: str) -> None:
        """Remove a physical node and all its virtual tokens from the ring."""
        if node_id not in self.nodes:
            return
        self.nodes.remove(node_id)
        new_ring: List[int] = []
        for token in self.ring:
            if self.token_to_node[token] == node_id:
                del self.token_to_node[token]
            else:
                new_ring.append(token)
        self.ring = new_ring

    def get_nodes(self, key: str, count: int = 1, exclude_nodes: Optional[Set[str]] = None) -> List[str]:
        """
        Get up to `count` distinct physical nodes for a given key, walking clockwise.
        Optionally excludes specific nodes (e.g. dead or suspect nodes).
        """
        if not self.ring:
            return []
        
        excluded = exclude_nodes or set()
        token = self._hash(key)
        idx = bisect.bisect_right(self.ring, token)
        
        selected: List[str] = []
        seen: Set[str] = set()
        ring_len = len(self.ring)

        # Walk ring clockwise
        for step in range(ring_len):
            curr_idx = (idx + step) % ring_len
            curr_token = self.ring[curr_idx]
            physical_node = self.token_to_node[curr_token]
            
            if physical_node not in seen and physical_node not in excluded:
                seen.add(physical_node)
                selected.append(physical_node)
                if len(selected) >= count:
                    break

        return selected

    def get_primary_node(self, key: str, exclude_nodes: Optional[Set[str]] = None) -> Optional[str]:
        nodes = self.get_nodes(key, count=1, exclude_nodes=exclude_nodes)
        return nodes[0] if nodes else None

    def clone(self) -> ConsistentHashRing:
        """Create a deep copy of the current ring state."""
        new_ring = ConsistentHashRing(vnodes=self.vnodes)
        new_ring.ring = list(self.ring)
        new_ring.token_to_node = dict(self.token_to_node)
        new_ring.nodes = set(self.nodes)
        return new_ring
