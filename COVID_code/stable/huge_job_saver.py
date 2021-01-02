import datetime
import os
import sys
import time
import logging
import pandas as pd

import news_articles
import util


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)

logger.addHandler(handler)


def tweet_handler(batch):
    all_tweets = [tweet for snapshot in batch for tweet in snapshot]
    df = pd.DataFrame(all_tweets)
    df.sort_values("id")
    df.drop_duplicates("id", inplace=True)
    return df


def process_new_tweets(topic):
    logger.info(f"Processing tweets for {topic}")
    df_dict = {}

    snap_dir = f"./snapshots/{topic}_archive/"
    snapshots = util.absolute_filepaths(snap_dir)[:20]
    if len(snapshots) == 0:
        return df_dict

    snapshots_ordered = sorted(snapshots, key=lambda f: os.path.getmtime(f))
    batch = [util.load_pickle(file) for file in snapshots_ordered]
    tweet_df = tweet_handler(batch)
    df_dict[topic] = dict(df=tweet_df, snapshots=snapshots_ordered)
    return df_dict


def save_new_tweets_to_db(tweet_data):
    logger.info("Saving tweets")
    for topic, tweets in tweet_data.items():
        tweet_collection = util.get_db_collection(f"{topic}_tweets")
        res = tweet_collection.insert_many(tweets["df"].to_dict("records"))
        tweets["df"]["object_id"] = res.inserted_ids
    return tweet_data


def process_and_save_articles(tweet_data):
    logger.info("Processing articles")
    for topic, tweet_data in tweet_data.items():
        db_article_collection = util.get_db_collection(f"{topic}_articles")
        tweet_df = tweet_data["df"][
            tweet_data["df"].astype(str)["collected_urls"] != "[]"
        ]
        article_list = news_articles.get_valid_articles(tweet_df, db_article_collection)

        logger.info("Saving articles")
        for url, article in article_list.items():
            now = datetime.datetime.utcnow()

            db_article_collection.update_one(
                {"url": url},
                {
                    "$set": {
                        "last_update": now,
                    },
                    "$addToSet": {
                        "tweet_ids": {"$each": article["id"]},
                        "object_ids": {"$each": article["object_id"]},
                    },
                    "$setOnInsert": {
                        "insertion_date": now,
                        "title": article.get("title"),
                        "authors": article.get("authors"),
                        "publish_date": article.get("publish_date"),
                        "text": article.get("text"),
                        "final_format": article.get("final_format"),
                    },
                },
                upsert=True,
            )


def cleanup_snapshot_files(tweet_data):
    logger.info("Cleanup")
    for tweet_data in tweet_data.values():
        for fname in tweet_data["snapshots"]:
            if os.path.isfile(fname):  # this makes the code more robust
                os.remove(fname)


@util.timing
def periodic_db_save(topic_list=None):
    """
    Function that should be called regularly to store the tweets to the database and to download referenced articles

    :param topic_list: list of topics that tweets are collected for and that should be saved to the database
    :return: None
    """
    if topic_list is None:
        topic_list = ["crypto", "snp500", "election"]

    for topic in topic_list:
        tweets_left = True
        while tweets_left:
            tweet_data = process_new_tweets(topic)
            tweet_data = save_new_tweets_to_db(tweet_data)

            process_and_save_articles(tweet_data)

            cleanup_snapshot_files(tweet_data)

            snap_dir = f"./snapshots/{topic}_archive/"
            snapshots = util.absolute_filepaths(snap_dir)
            tweets_left = not len(snapshots) == 0
            logger.info(f"Number of files left={len(snapshots)}")
            time.sleep(2)


if __name__ == "__main__":
    periodic_db_save()
