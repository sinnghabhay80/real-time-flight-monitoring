from pyspark.sql.streaming import StreamingQueryListener
import logging

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("StreamingQueryListener")

class MyQueryListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        logger.info(f"Query started: {event.id}")

    def onQueryProgress(self, event):
        progress = event.progress
        logging.info(f"Query Progress ID: {progress.id}")
        logging.info(f"Batch ID: {progress.batchId}")
        logging.info(f"Num Input Rows: {progress.numInputRows}")
        logging.info(f"Processed Rows per Second: {progress.processedRowsPerSecond}")
        if progress.numInputRows == 0:
            logging.info("No new data processed. Query is waiting for new files.")

    def onQueryTerminated(self, event):
        logging.info(f"Query terminated: {event.id}")