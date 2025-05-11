import subprocess
import os
import logging
from utils.process_life_check import process_alive_or_not
from utils.config_loader import load_config

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("BronzeDAG")


def check_or_start_bronze():
    config = load_config()
    bronze_script_path = config["paths"]["bronze_level_script"]
    script_name = os.path.basename(bronze_script_path)
    script_path = os.path.realpath(bronze_script_path)

    process_life = process_alive_or_not(script_name)

    if not process_life:
        logger.info("Starting bronze polling script...")
        subprocess.Popen(["python", script_path])

    else: logger.info("Bronze polling script already running!")