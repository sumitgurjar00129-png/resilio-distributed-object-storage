"""
Vault Server Orchestrator.
Allows launching standalone storage nodes, the gateway coordinator, or an all-in-one local cluster.
"""

from __future__ import annotations
import argparse
import asyncio
import logging
import signal
import sys
from pathlib import Path
from typing import List
import uvicorn

from vault.config import VaultConfig, StorageNodeConfig
from vault.node import StorageNodeServer
from vault.gateway import create_gateway_app

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("vault.server")


async def run_local_cluster(num_nodes: int = 3, base_dir: Optional[Path] = None, gateway_port: int = 8000) -> None:
    """Spins up an in-process local cluster with `num_nodes` storage nodes and the gateway."""
    data_dir = (base_dir or Path("./vault_data")).resolve()
    config = VaultConfig.default_cluster(base_dir=data_dir, num_nodes=num_nodes)
    config.gateway_port = gateway_port

    logger.info("Initializing %d Storage Nodes...", num_nodes)
    storage_nodes: List[StorageNodeServer] = []
    for node_cfg in config.nodes:
        node_server = StorageNodeServer(
            node_id=node_cfg.node_id,
            host=node_cfg.host,
            port=node_cfg.port,
            data_dir=node_cfg.data_dir,
            rack=node_cfg.rack,
            zone=node_cfg.zone
        )
        await node_server.start()
        storage_nodes.append(node_server)

    logger.info("Storage nodes started. Initializing Gateway Coordinator on port %d...", gateway_port)
    app = create_gateway_app(config)

    uvi_config = uvicorn.Config(
        app=app,
        host=config.gateway_host,
        port=config.gateway_port,
        log_level="info",
        access_log=False
    )
    uvi_server = uvicorn.Server(uvi_config)

    # Setup graceful shutdown
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _sig_handler():
        logger.info("Received termination signal, shutting down cluster...")
        stop_event.set()
        uvi_server.should_exit = True

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _sig_handler)
        except NotImplementedError:
            pass

    try:
        await uvi_server.serve()
    finally:
        logger.info("Stopping storage nodes...")
        for node in storage_nodes:
            await node.stop()
        logger.info("Cluster shutdown complete.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Vault Distributed Object Storage Server")
    sub = parser.add_subparsers(dest="mode")

    p_cluster = sub.add_parser("cluster", help="Run in-process multi-node local cluster")
    p_cluster.add_argument("--nodes", type=int, default=3, help="Number of storage nodes (default: 3)")
    p_cluster.add_argument("--port", type=int, default=8000, help="Gateway port (default: 8000)")
    p_cluster.add_argument("--dir", type=str, default="./vault_data", help="Base storage directory")

    p_node = sub.add_parser("node", help="Run a standalone storage node")
    p_node.add_argument("--id", required=True, help="Node ID (e.g. node-1)")
    p_node.add_argument("--port", type=int, required=True, help="Port to listen on")
    p_node.add_argument("--host", default="127.0.0.1", help="Host address")
    p_node.add_argument("--dir", required=True, help="Data directory path")
    p_node.add_argument("--rack", default="rack-1", help="Rack identifier")

    args = parser.parse_args()
    if not args.mode or args.mode == "cluster":
        num_nodes = getattr(args, "nodes", 3)
        port = getattr(args, "port", 8000)
        base_dir = Path(getattr(args, "dir", "./vault_data"))
        asyncio.run(run_local_cluster(num_nodes=num_nodes, base_dir=base_dir, gateway_port=port))
    elif args.mode == "node":
        async def _run_node():
            node_server = StorageNodeServer(
                node_id=args.id,
                host=args.host,
                port=args.port,
                data_dir=Path(args.dir),
                rack=args.rack
            )
            await node_server.start()
            while True:
                await asyncio.sleep(3600)
        asyncio.run(_run_node())


if __name__ == "__main__":
    main()
