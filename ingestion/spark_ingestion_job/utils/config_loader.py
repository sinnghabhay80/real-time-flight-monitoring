import yaml
from pathlib import Path
from typing import Dict


def load_config(config_path: str = "config/config.yaml") -> Dict:
    """
    Loads YAML configuration from the given path.
    """
    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(f"Config file not found at {config_path}")

    with open(config_file, "r") as f:
        return yaml.safe_load(f)