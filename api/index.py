from pathlib import Path
from vault.config import VaultConfig
from vault.gateway import create_gateway_app

config = VaultConfig.default_cluster(
    base_dir=Path("/tmp/vault_data"),
    num_nodes=3,
)

app = create_gateway_app(config)
