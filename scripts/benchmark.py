"""
Vault Benchmark Suite.
Measures write/read throughput, IOPS, and latency percentiles under concurrent workloads.
"""

from __future__ import annotations
import argparse
import asyncio
import os
import sys
import time
from typing import List
import httpx
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

console = Console()


async def run_benchmark(
    gateway_url: str,
    num_requests: int = 100,
    concurrency: int = 10,
    object_size_kb: int = 64
) -> None:
    data_payload = os.urandom(object_size_kb * 1024)
    bucket = "benchmarks"

    console.print(Panel(
        f"[bold cyan]Vault Performance Benchmark[/bold cyan]\n"
        f"Gateway: {gateway_url}\n"
        f"Total Requests: {num_requests}\n"
        f"Concurrency: {concurrency}\n"
        f"Object Size: {object_size_kb} KB ({len(data_payload):,} bytes)",
        border_style="cyan"
    ))

    limits = httpx.Limits(max_connections=concurrency * 2, max_keepalive_connections=concurrency)
    timeout = httpx.Timeout(30.0)

    async with httpx.AsyncClient(base_url=gateway_url, limits=limits, timeout=timeout) as client:
        # --- 1. WRITE BENCHMARK ---
        console.print("[bold yellow]Executing Quorum Write Benchmark (W=2)...[/bold yellow]")
        write_latencies: List[float] = []
        sem = asyncio.Semaphore(concurrency)

        async def _single_write(idx: int):
            async with sem:
                t0 = time.time()
                resp = await client.put(
                    f"/api/v1/objects/{bucket}/bench_obj_{idx}.bin",
                    content=data_payload,
                    headers={"Content-Type": "application/octet-stream"}
                )
                dur = time.time() - t0
                if resp.status_code in (200, 201):
                    write_latencies.append(dur)

        t_write_start = time.time()
        await asyncio.gather(*[_single_write(i) for i in range(num_requests)])
        t_write_total = time.time() - t_write_start

        # --- 2. READ BENCHMARK ---
        console.print("[bold green]Executing Quorum Read Benchmark (R=2)...[/bold green]")
        read_latencies: List[float] = []

        async def _single_read(idx: int):
            async with sem:
                t0 = time.time()
                resp = await client.get(f"/api/v1/objects/{bucket}/bench_obj_{idx}.bin")
                dur = time.time() - t0
                if resp.status_code == 200:
                    read_latencies.append(dur)

        t_read_start = time.time()
        await asyncio.gather(*[_single_read(i) for i in range(num_requests)])
        t_read_total = time.time() - t_read_start

    # Output stats
    def calc_percentiles(lats: List[float]) -> dict:
        if not lats:
            return {"p50": 0, "p90": 0, "p95": 0, "p99": 0}
        s = sorted(lats)
        n = len(s)
        return {
            "p50": round(s[int(n * 0.50)] * 1000, 2),
            "p90": round(s[min(int(n * 0.90), n - 1)] * 1000, 2),
            "p95": round(s[min(int(n * 0.95), n - 1)] * 1000, 2),
            "p99": round(s[min(int(n * 0.99), n - 1)] * 1000, 2)
        }

    w_p = calc_percentiles(write_latencies)
    r_p = calc_percentiles(read_latencies)

    total_mb = (num_requests * object_size_kb) / 1024.0
    write_mb_s = round(total_mb / t_write_total, 2) if t_write_total > 0 else 0
    read_mb_s = round(total_mb / t_read_total, 2) if t_read_total > 0 else 0

    table = Table(title="Benchmark Results", show_header=True, header_style="bold magenta")
    table.add_column("Operation")
    table.add_column("Completed", justify="right")
    table.add_column("Throughput (MB/s)", justify="right")
    table.add_column("IOPS (req/s)", justify="right")
    table.add_column("p50 (ms)", justify="right")
    table.add_column("p90 (ms)", justify="right")
    table.add_column("p95 (ms)", justify="right")
    table.add_column("p99 (ms)", justify="right")

    table.add_row(
        "[cyan]PUT (W=2)[/cyan]",
        f"{len(write_latencies)} / {num_requests}",
        str(write_mb_s),
        str(round(len(write_latencies) / t_write_total, 1)),
        str(w_p["p50"]),
        str(w_p["p90"]),
        str(w_p["p95"]),
        str(w_p["p99"])
    )
    table.add_row(
        "[green]GET (R=2)[/green]",
        f"{len(read_latencies)} / {num_requests}",
        str(read_mb_s),
        str(round(len(read_latencies) / t_read_total, 1)),
        str(r_p["p50"]),
        str(r_p["p90"]),
        str(r_p["p95"]),
        str(r_p["p99"])
    )

    console.print(table)


def main():
    parser = argparse.ArgumentParser(description="Vault Storage Benchmark")
    parser.add_argument("--gateway", default="http://127.0.0.1:8000", help="Gateway URL")
    parser.add_argument("--requests", type=int, default=50, help="Number of requests")
    parser.add_argument("--concurrency", type=int, default=8, help="Concurrent client workers")
    parser.add_argument("--size-kb", type=int, default=32, help="Payload size in KB")
    args = parser.parse_args()

    asyncio.run(run_benchmark(
        gateway_url=args.gateway,
        num_requests=args.requests,
        concurrency=args.concurrency,
        object_size_kb=args.size_kb
    ))


if __name__ == "__main__":
    main()
