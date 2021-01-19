import logging
import uuid
import pickle
import os
import sys
import pytz

import pandas as pd
from tweepy import OAuthHandler
from tweepy import Cursor
from tweepy import API
from datetime import datetime
from datetime import timedelta

import util

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)

logger.addHandler(handler)


def cred_to_auth(cred):
    auth = OAuthHandler(cred["consumer_key"], cred["consumer_secret"])
    auth.set_access_token(cred["access_token"], cred["access_token_secret"])
    return auth


def tweet_handler(batch):
    all_tweets = [tweet for snapshot in batch for tweet in snapshot]
    df = pd.DataFrame(all_tweets)
    df.sort_values("id")
    df.drop_duplicates("id", inplace=True)
    return df


def process_new_tweets(topic_list):
    df_dict = {}
    for crawl_type in topic_list:
        logger.info(f"Processing all new tweets in {crawl_type}")
        snap_dir = f"./snapshots/{crawl_type}/"
        # Ensure a manageable amount of tweets
        snapshots = util.absolute_filepaths(snap_dir)[:20]
        if len(snapshots) == 0:
            continue

        snapshots_ordered = sorted(snapshots, key=lambda f: os.path.getmtime(f))
        batch = [util.load_pickle(file) for file in snapshots_ordered]
        tweet_df = tweet_handler(batch)
        df_dict[crawl_type] = dict(df=tweet_df, snapshots=snapshots_ordered)
    return df_dict


def save_new_tweets_to_db(tweet_data, api):
    for topic, tweets in tweet_data.items():
        logger.info(f"Saving all new tweets for {topic}")
        remaining_tweets = []
        for tweet in tweets["df"].to_dict("records"):

            # Ugly way of testing if is retweet... make better
            logger.info("\nNew Tweet: ")
            try:
                tweet["retweeted_status"]["id"]
                logger.info(f"Retweeted_Statud Id is {tweet['retweeted_status']['id']}")
                retweeted_status = True
            except TypeError:
                retweeted_status = False
            if tweet["in_reply_to_status_id_str"] or \
                tweet["is_quote_status"] or \
                retweeted_status:
                logger.info("Passing")
                logger.info(f"in_reply_to_status_id_str was {tweet['in_reply_to_status_id_str']}")
                logger.info(f"is_quote_status was {tweet['is_quote_status']}")
                logger.info(f"retweeted_status was {retweeted_status}")
                pass
            else:
                logger.info(f"Found a tweet")
                time_of_creation = tweet["created_at"]
                datetime_creation = datetime.strptime(time_of_creation,
                                                      '%a %b %d %H:%M:%S +0000 %Y').replace(tzinfo=pytz.UTC)
                time_delta = datetime.now().replace(tzinfo=pytz.UTC) - datetime_creation
                if time_delta.total_seconds() >= timedelta(days=0.5).total_seconds():
                    logger.info(f"Looking at tweet_id={tweet['id']}: ")
                    all_replies = []
                    for replie in Cursor(api.search,
                                        q="to: " + tweet["user"]["screen_name"],
                                        since_id=tweet["id"],
                                        timeout=999999).items(1000):
                        if hasattr(replie, "in_reply_to_status_id"):
                            if (replie.in_reply_to_status_id == tweet["id"]):
                                all_replies.append(replie.to_dict("records"))
                    logger.info(f"We found {len(all_replies)}/{tweet['reply_count']} replies")
                    first_100_retweets = api.retweets(tweet["id"])
                    logger.info(f"We found {len(first_100_retweets)}/{tweet['retweet_count']} retweets")

                    all_collected_info = {"original_tweet": tweet,
                                          "replies": all_replies,
                                          "retweets": first_100_retweets,
                                          "insertion_date": datetime.utcnow()}
                    tweet_collection = util.get_db_collection(f"{topic}_tweets_test")
                    tweet_collection.insert_one(all_collected_info)
                else:
                    logger.info("Tweet did not full fill time condition")
                    remaining_tweets.append(tweet)

        logger.info("[crawler.StdOutListener] :: Saving remaining tweets")
        outpath = f"./snapshots/{topic}/"
        os.makedirs(outpath, exist_ok=True)
        dumpfile = f"{outpath}/remaining_tweets_{uuid.uuid4().hex}.p"
        with open(dumpfile, "wb") as output_file:
            pickle.dump(
                remaining_tweets,
                output_file,
            )
        remaining_tweets.clear()


def cleanup_snapshot_files(tweet_data):
    logger.info("Cleanup")
    for tweet_data in tweet_data.values():
        for fname in tweet_data["snapshots"]:
            if os.path.isfile(fname):
                os.remove(fname)


@util.timing
def periodic_db_save(topic_list=None):
    """
    Function that should be called regularly to store the tweets to the database and to download referenced articles

    :param topic_list: list of topics that tweets are collected for and that should be saved to the database
    :return: None
    """
    cred_dict = util.get_config("./config_api.ini")
    auth = [cred_to_auth(cred) for cred in cred_dict.values()][3]
    api = API(auth)

    if topic_list is None:
        topic_list = ["covid"]
    logger.info(f"Starting periodic DB saver for {topic_list}")
    tweet_data = process_new_tweets(topic_list)
    save_new_tweets_to_db(tweet_data, api)

    #cleanup_snapshot_files(tweet_data)


if __name__ == "__main__":
    periodic_db_save()
