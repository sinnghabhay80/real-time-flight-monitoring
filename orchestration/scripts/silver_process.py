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
    silver_script_path = config["paths"]["silver_level_script"]
    script_name = os.path.basename(silver_script_path)
    script_path = os.path.realpath(silver_script_path)

    process_life = process_alive_or_not(script_name)

    if not process_life:
        logger.info("Starting silver streaming script...")
        subprocess.Popen(["python", script_path])

    else: logger.info("Silver streaming script already running!")