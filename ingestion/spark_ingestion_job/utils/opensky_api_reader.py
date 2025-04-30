import requests
import yaml
import logging
from pathlib import Path
from typing import List, Dict, Optional
from tenacity import retry, wait_fixed, stop_after_attempt, retry_if_exception_type

# Configuring structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("OpenSkyAPIClient")


class OpenSkyAPIClient:
    def __init__(self, config_path: str = "../config/api_keys.yaml"):
        """
        Initializes the OpenSky API client and attempts to load credentials.
        """
        self.base_url = "https://opensky-network.org/api/states/all"
        self.username, self.password = self._load_credentials(config_path)

    def _load_credentials(self, path: str) -> (Optional[str], Optional[str]):
        """
        Load API credentials from a YAML config file.
        """
        try:
            config_file = Path(path)
            if config_file.exists():
                with open(config_file, "r") as f:
                    cfg = yaml.safe_load(f)
                    username = cfg["opensky"].get("username")
                    password = cfg["opensky"].get("password")
                    if username and password:
                        logger.info("Loaded OpenSky API credentials.")
                    else:
                        logger.warning("Username/password missing in YAML, using unauthenticated mode.")
                    return username, password
            else:
                logger.warning("API key config not found. Using unauthenticated mode.")
                return None, None
        except Exception as e:
            logger.error(f"Error reading credentials: {e}")
            return None, None

    @retry(wait=wait_fixed(10), stop=stop_after_attempt(3), retry=retry_if_exception_type(requests.RequestException))
    def fetch_states(self) -> Optional[List[Dict]]:
        """
        Fetch live aircraft state data from OpenSky API.
        Retries on failure with exponential backoff.
        """
        try:
            response = requests.get(
                self.base_url,
                auth=(self.username, self.password) if self.username and self.password else None,
                timeout=10
            )
            if response.status_code == 200:
                data = response.json()
                num_records = len(data.get("states", []))
                logger.info(f"Fetched {num_records} aircraft states successfully.")
                return data.get("states", [])
            elif response.status_code == 429:
                logger.warning("Rate limit exceeded. Consider using authenticated access.")
                return None
            else:
                logger.error(f"API error: {response.status_code} - {response.text}")
                return None
        except requests.RequestException as e:
            logger.exception("API request failed due to network error.")
            raise
