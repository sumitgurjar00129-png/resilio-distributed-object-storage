"""
Vault Command Line Interface (vaultctl).
Provides comprehensive administrative and object operations: cluster management,
uploads, downloads, scrubs, rebalances, and chaos testing.
"""

from __future__ import annotations
import argparse
import asyncio
import os
import sys
from pathlib import Path
from typing import Optional
import httpx
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

console = Console()
DEFAULT_GATEWAY_URL = "http://127.0.0.1:8000"


def get_client(gateway_url: str = DEFAULT_GATEWAY_URL) -> httpx.Client:
    return httpx.Client(base_url=gateway_url, timeout=30.0)


def cmd_status(args: argparse.Namespace) -> None:
    client = get_client(args.gateway)
    try:
        resp = client.get("/api/v1/cluster/status")
        resp.raise_for_status()
        data = resp.json()

        q = data["quorum"]
        consistency = "[green]Strong (R+W > N)[/green]" if q["strongly_consistent"] else "[yellow]Weak[/yellow]"
        console.print(Panel(
            f"[bold]Cluster:[/bold] {data['cluster_name']}\n"
            f"[bold]Quorum:[/bold] N={q['N']} | W={q['W']} | R={q['R']} ({consistency})\n"
            f"[bold]Status:[/bold] {'[green]AVAILABLE[/green]' if data['is_available'] else '[red]QUORUM LOST[/red]'}\n"
            f"[bold]Healthy Nodes:[/bold] {data['nodes_healthy']} / {data['nodes_total']}",
            title="Vault Cluster Topology",
            border_style="cyan"
        ))

        table = Table(title="Storage Nodes", show_header=True, header_style="bold magenta")
        table.add_column("Node ID", style="bold")
        table.add_column("Endpoint")
        table.add_column("Rack / Zone")
        table.add_column("Status")
        table.add_column("Failures")

        for nid, info in data["nodes"].items():
            status_style = "green" if info["status"] == "HEALTHY" else ("yellow" if info["status"] == "SUSPECT" else "red")
            table.add_row(
                nid,
                info["url"],
                f"{info['rack']} / {info['zone']}",
                f"[{status_style}]{info['status']}[/{status_style}]",
                str(info["consecutive_failures"])
            )
        console.print(table)

    except Exception as e:
        console.print(f"[bold red]Failed to get cluster status:[/bold red] {e}")
        sys.exit(1)


def cmd_put(args: argparse.Namespace) -> None:
    client = get_client(args.gateway)
    filepath = Path(args.file)
    if not filepath.exists() or not filepath.is_file():
        console.print(f"[bold red]File not found:[/bold red] {filepath}")
        sys.exit(1)

    content = filepath.read_bytes()
    try:
        with console.status(f"Writing {args.bucket}/{args.key} with Quorum write..."):
            resp = client.put(
                f"/api/v1/objects/{args.bucket}/{args.key}",
                content=content,
                headers={"Content-Type": "application/octet-stream"}
            )
            resp.raise_for_status()
            data = resp.json()

        console.print(f"[bold green]Successfully committed object:[/bold green] {args.bucket}/{args.key}")
        console.print(f"  [dim]Version ID:[/dim] {data['version_id']}")
        console.print(f"  [dim]ETag:[/dim] {data['etag']}")
        console.print(f"  [dim]Size:[/dim] {data['size_bytes']} bytes")
        console.print(f"  [dim]SHA-256:[/dim] {data['sha256']}")
        console.print(f"  [dim]Replicas:[/dim] [cyan]{', '.join(data['replica_nodes'])}[/cyan]")
    except Exception as e:
        console.print(f"[bold red]Upload failed:[/bold red] {e}")
        sys.exit(1)


def cmd_get(args: argparse.Namespace) -> None:
    client = get_client(args.gateway)
    try:
        with console.status(f"Fetching verified object {args.bucket}/{args.key}..."):
            resp = client.get(f"/api/v1/objects/{args.bucket}/{args.key}")
            resp.raise_for_status()

        sha = resp.headers.get("X-Checksum-SHA256", "unverified")
        ver = resp.headers.get("X-Version-Id", "")

        if args.output:
            out_path = Path(args.output)
            out_path.write_bytes(resp.content)
            console.print(f"[bold green]Saved to:[/bold green] {out_path} ({len(resp.content)} bytes, SHA: {sha[:16]}...)")
        else:
            try:
                text = resp.text
                console.print(f"[bold cyan]--- Object Content ({args.bucket}/{args.key}) ---[/bold cyan]")
                console.print(text)
            except Exception:
                console.print(f"[bold green]Retrieved binary object ({len(resp.content)} bytes)[/bold green]")
    except Exception as e:
        console.print(f"[bold red]Download failed:[/bold red] {e}")
        sys.exit(1)


def cmd_ls(args: argparse.Namespace) -> None:
    client = get_client(args.gateway)
    try:
        params = {"prefix": args.prefix or ""}
        resp = client.get(f"/api/v1/objects/{args.bucket}", params=params)
        resp.raise_for_status()
        data = resp.json()

        table = Table(title=f"Objects in bucket '{args.bucket}'", show_header=True, header_style="bold cyan")
        table.add_column("Key", style="bold")
        table.add_column("Size (Bytes)", justify="right")
        table.add_column("ETag")
        table.add_column("Replicas")

        for obj in data["objects"]:
            table.add_row(
                obj["key"],
                str(obj["size_bytes"]),
                obj["etag"],
                ", ".join(obj["replica_nodes"])
            )
        console.print(table)
    except Exception as e:
        console.print(f"[bold red]List failed:[/bold red] {e}")
        sys.exit(1)


def cmd_rm(args: argparse.Namespace) -> None:
    client = get_client(args.gateway)
    try:
        resp = client.delete(f"/api/v1/objects/{args.bucket}/{args.key}")
        resp.raise_for_status()
        console.print(f"[bold green]Object deleted successfully:[/bold green] {args.bucket}/{args.key}")
    except Exception as e:
        console.print(f"[bold red]Delete failed:[/bold red] {e}")
        sys.exit(1)


def cmd_scrub(args: argparse.Namespace) -> None:
    client = get_client(args.gateway)
    try:
        with console.status("Running active cluster-wide integrity scrubber..."):
            resp = client.post("/api/v1/cluster/scrub")
            resp.raise_for_status()
            res = resp.json()

        table = Table(title="Scrubber Integrity Audit Report", show_header=True, header_style="bold green")
        table.add_column("Metric", style="bold")
        table.add_column("Value")

        table.add_row("Scanned Objects", str(res["objects_scanned"]))
        table.add_row("Replicas Checked", str(res["replicas_checked"]))
        table.add_row("Healthy Replicas", f"[green]{res['healthy_replicas']}[/green]")
        table.add_row("Corrupted Replicas", f"[red]{res['corrupted_replicas']}[/red]")
        table.add_row("Missing Replicas", f"[yellow]{res['missing_replicas']}[/yellow]")
        table.add_row("Replicas Repaired", f"[cyan]{res['repaired_replicas']}[/cyan]")
        table.add_row("Unrecoverable Objects", f"[bold red]{res['unrecoverable_count']}[/bold red]")

        console.print(table)
    except Exception as e:
        console.print(f"[bold red]Scrub failed:[/bold red] {e}")
        sys.exit(1)


def cmd_rebalance(args: argparse.Namespace) -> None:
    client = get_client(args.gateway)
    try:
        with console.status("Triggering ring rebalance..."):
            resp = client.post("/api/v1/cluster/rebalance")
            resp.raise_for_status()
            res = resp.json()

        console.print(Panel(
            f"[bold]State:[/bold] {res['state']}\n"
            f"[bold]Migrations Planned:[/bold] {res['migrations_planned']}\n"
            f"[bold]Migrations Completed:[/bold] {res['migrations_completed']}\n"
            f"[bold]Bytes Transferred:[/bold] {res['bytes_transferred']}\n"
            f"[bold]Elapsed:[/bold] {res['elapsed_seconds']}s",
            title="Rebalance Execution Summary",
            border_style="cyan"
        ))
    except Exception as e:
        console.print(f"[bold red]Rebalance failed:[/bold red] {e}")
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(prog="vaultctl", description="Vault Distributed Object Storage CLI")
    parser.add_argument("--gateway", default=DEFAULT_GATEWAY_URL, help="Gateway URL (default: http://127.0.0.1:8000)")
    
    subparsers = parser.add_subparsers(dest="command")

    # status
    subparsers.add_parser("status", help="Display cluster topology and node health")

    # put
    p_put = subparsers.add_parser("put", help="Upload an object")
    p_put.add_argument("bucket", help="Bucket name")
    p_put.add_argument("key", help="Object key")
    p_put.add_argument("file", help="Path to local file")

    # get
    p_get = subparsers.add_parser("get", help="Download an object")
    p_get.add_argument("bucket", help="Bucket name")
    p_get.add_argument("key", help="Object key")
    p_get.add_argument("-o", "--output", help="Output path (prints to stdout if omitted)")

    # ls
    p_ls = subparsers.add_parser("ls", help="List objects")
    p_ls.add_argument("bucket", help="Bucket name")
    p_ls.add_argument("--prefix", default="", help="Prefix filter")

    # rm
    p_rm = subparsers.add_parser("rm", help="Delete an object")
    p_rm.add_argument("bucket", help="Bucket name")
    p_rm.add_argument("key", help="Object key")

    # scrub
    subparsers.add_parser("scrub", help="Trigger active integrity scrubber")

    # rebalance
    subparsers.add_parser("rebalance", help="Trigger ring rebalance")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    dispatch = {
        "status": cmd_status,
        "put": cmd_put,
        "get": cmd_get,
        "ls": cmd_ls,
        "rm": cmd_rm,
        "scrub": cmd_scrub,
        "rebalance": cmd_rebalance
    }
    dispatch[args.command](args)


if __name__ == "__main__":
    main()
