"""
Vault Metrics & Observability Module.
Tracks request counts, latencies, node statuses, data corruptions, repairs, and rebalancing progress.
Exposes both Prometheus text format and structured JSON.
"""

from __future__ import annotations
import time
from typing import Dict, Any, List
from collections import defaultdict


class VaultMetrics:
    def __init__(self) -> None:
        self.counters: Dict[str, int] = defaultdict(int)
        self.gauges: Dict[str, float] = defaultdict(float)
        self.latencies: Dict[str, List[float]] = defaultdict(list)
        self.events: List[Dict[str, Any]] = []

    def inc(self, metric: str, amount: int = 1) -> None:
        self.counters[metric] += amount

    def set_gauge(self, metric: str, value: float) -> None:
        self.gauges[metric] = value

    def observe_latency(self, metric: str, duration_sec: float) -> None:
        self.latencies[metric].append(duration_sec)
        # Keep recent 500 samples
        if len(self.latencies[metric]) > 500:
            self.latencies[metric].pop(0)

    def log_event(self, event_type: str, details: Dict[str, Any]) -> None:
        entry = {
            "timestamp": time.time(),
            "type": event_type,
            "details": details
        }
        self.events.append(entry)
        if len(self.events) > 200:
            self.events.pop(0)

    def to_json(self) -> Dict[str, Any]:
        avg_latencies = {}
        for k, v in self.latencies.items():
            if v:
                avg_latencies[k] = round(sum(v) / len(v), 5)
        return {
            "counters": dict(self.counters),
            "gauges": dict(self.gauges),
            "average_latency_sec": avg_latencies,
            "recent_events": self.events[-20:]
        }

    def to_prometheus(self) -> str:
        lines: List[str] = []
        for name, count in self.counters.items():
            clean_name = f"vault_{name}".replace("-", "_").replace(".", "_")
            lines.append(f"# TYPE {clean_name} counter")
            lines.append(f"{clean_name} {count}")
            
        for name, val in self.gauges.items():
            clean_name = f"vault_{name}".replace("-", "_").replace(".", "_")
            lines.append(f"# TYPE {clean_name} gauge")
            lines.append(f"{clean_name} {val}")

        for name, vals in self.latencies.items():
            if vals:
                clean_name = f"vault_{name}_latency_seconds".replace("-", "_").replace(".", "_")
                lines.append(f"# TYPE {clean_name}_avg gauge")
                lines.append(f"{clean_name}_avg {round(sum(vals)/len(vals), 5)}")

        return "\n".join(lines) + "\n"


metrics = VaultMetrics()
