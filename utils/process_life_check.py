import psutil
import logging

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger(__name__)


def process_alive_or_not(script_path: str) -> bool:
    current_processes = list(psutil.process_iter(['pid', 'name', 'cmdline']))
    for process in current_processes:
        try:
            cmdline = process.info.get('cmdline')
            if cmdline and script_path in cmdline:
                return True

        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

    return False