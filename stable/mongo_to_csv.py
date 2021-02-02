import sys
import util
import logging
import pandas as pd

from datetime import datetime

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)

logger.addHandler(handler)


def csv_exporter(topic, query={}, no_id=True):
    date = datetime.utcnow().strftime("%Y_%m_%d")
    tweet_collection = util.get_db_collection(f"{topic}_tweets")
    cursor = tweet_collection.find(query)
    df = pd.DataFrame(list(cursor))

    if no_id:
        del df['_id']
    df.to_csv(f"./TweetData_date={date}_noID={no_id}.csv")


if __name__ == "__main__":
    logger.info("Writing covid_tweets to .csv file")
    csv_exporter("covid")