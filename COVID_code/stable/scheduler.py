import sentry_sdk
import time
import multiprocessing

import db_saver

sentry_sdk.init(
    "https://18cbe5d7f9a94285964756bd86fade44@o455312.ingest.sentry.io/5458536",
    traces_sample_rate=1.0,
)


def db_job(topic):
    while True:
        start_time = time.time()
        db_saver.periodic_db_save([topic])
        end_time = time.time()
        remaining_time = 85 - (end_time - start_time)
        if remaining_time > 0:
            time.sleep(remaining_time)


if __name__ == "__main__":

    with multiprocessing.Pool(3) as p:
        p.map(db_job, ["crypto", "snp500", "election"])
