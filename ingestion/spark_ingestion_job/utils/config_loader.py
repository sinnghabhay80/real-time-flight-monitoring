import yaml
import os
import re
from pathlib import Path
from typing import Dict
from dotenv import load_dotenv

# Load .env file once at module level
load_dotenv()

env_var_pattern = re.compile(r"\$\{([A-Z0-9_]+)\}")

def _interpolate_env_vars(value: str) -> str:
    match = env_var_pattern.findall(value)
    for var in match:
        env_value = os.environ.get(var)
        if env_value is None:
            raise EnvironmentError(f"Environment variable {var} is not set.")
        value = value.replace(f"${{{var}}}", env_value)
    return value


def _expand_env_in_dict(config_dict):
    for key, value in config_dict.items():
        if isinstance(value, dict):
            _expand_env_in_dict(value)
        elif isinstance(value, str) and "${" in value:
            config_dict[key] = _interpolate_env_vars(value)
    return config_dict


def load_config(config_path: str = "config/config.yaml") -> Dict:
    """
        Loads YAML configuration from the given path.
    """
    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(f"Config file not found at {config_path}")

    with open(config_file, "r") as f:
        raw_config = yaml.safe_load(f)

    return _expand_env_in_dict(raw_config)
