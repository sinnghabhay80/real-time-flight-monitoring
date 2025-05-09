import psutil
import logging

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger(__name__)


def process_alive_or_not(script_name: str) -> bool:
    for process in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            if script_name in process.cmdline:
                return

        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

    return False