import requests
import logging
from typing import List, Dict, Optional
from tenacity import retry, wait_fixed, stop_after_attempt, retry_if_exception_type
from utils.config_loader import load_config

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("OpenSkyAPIClient")


class OpenSkyAPIClient:
    def __init__(self, config_path: str = "/home/abhays/real-time-flight-monitoring/config/config.yaml"):
        """
        Initializes the OpenSky API client with credentials from YAML config.
        """
        self.config = load_config(config_path)
        self.base_url = "https://opensky-network.org/api/states/all"
        self.username = self.config["opensky"].get("username")
        self.password = self.config["opensky"].get("password")

    @retry(wait=wait_fixed(10), stop=stop_after_attempt(3),
           retry=retry_if_exception_type(requests.RequestException))
    def fetch_states(self) -> Optional[List[Dict]]:
        """
        Fetch live aircraft state data from OpenSky API.
        Retries on failure with exponential backoff.
        """
        try:
            response = requests.get(
                self.base_url,
                # auth=(self.username, self.password) if self.username and self.password else None,
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