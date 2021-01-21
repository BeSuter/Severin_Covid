import time
import sys
import multiprocessing
import logging

import db_saver


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)

logger.addHandler(handler)


def db_job(topic):
    while True:
        start_time = time.time()
        db_saver.periodic_db_save([topic])
        end_time = time.time()
        remaining_time = 1200 - (end_time - start_time)
        if remaining_time > 0:
            logger.info(f"Sleeping for {remaining_time/60.} min")
            time.sleep(remaining_time)


if __name__ == "__main__":

    with multiprocessing.Pool(1) as p:
        p.map(db_job, ["covid"])
