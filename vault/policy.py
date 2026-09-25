"""
Vault Quorum Durability & Placement Policy.
Enforces N, W, R quorum consistency constraints and computes replica placement sets.
"""

from __future__ import annotations
from typing import List, Dict, Set, Optional, Tuple
from vault.config import QuorumConfig, StorageNodeConfig
from vault.hashing import ConsistentHashRing


class QuorumPolicy:
    def __init__(self, config: QuorumConfig) -> None:
        self.config = config
        self.config.validate_durability()

    @property
    def N(self) -> int:
        return self.config.replication_factor

    @property
    def W(self) -> int:
        return self.config.write_quorum

    @property
    def R(self) -> int:
        return self.config.read_quorum

    @property
    def is_strongly_consistent(self) -> bool:
        """Strong consistency requires R + W > N."""
        return (self.R + self.W) > self.N

    def check_write_success(self, successful_writes: int) -> bool:
        """Determines if a write operation achieved its required write quorum."""
        return successful_writes >= self.W

    def check_read_success(self, successful_reads: int) -> bool:
        """Determines if a read operation satisfied the read quorum."""
        return successful_reads >= self.R

    def compute_placement(
        self,
        ring: ConsistentHashRing,
        key: str,
        node_metadata: Dict[str, StorageNodeConfig],
        exclude_nodes: Optional[Set[str]] = None
    ) -> List[str]:
        """
        Computes the target replica nodes for an object key.
        Places replicas across distinct failure domains (racks/zones) where possible.
        """
        candidate_nodes = ring.get_nodes(key, count=len(ring.nodes), exclude_nodes=exclude_nodes)
        if len(candidate_nodes) <= self.N:
            return candidate_nodes[:self.N]

        # Prioritize failure domain / rack diversity
        chosen: List[str] = []
        seen_racks: Set[str] = set()
        leftovers: List[str] = []

        for node_id in candidate_nodes:
            meta = node_metadata.get(node_id)
            rack = meta.rack if meta else "default"
            if rack not in seen_racks and len(chosen) < self.N:
                seen_racks.add(rack)
                chosen.append(node_id)
            else:
                leftovers.append(node_id)

        # Fill remaining slots up to N if rack diversity was exhausted
        while len(chosen) < self.N and leftovers:
            chosen.append(leftovers.pop(0))

        return chosen
