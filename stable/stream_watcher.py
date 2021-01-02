import csv
import os
import pickle
import sys
import uuid
import logging

from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener

from flashtext import KeywordProcessor

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


class StdOutListener(StreamListener):
    def __init__(self, crawl_type="", keywords=[]):
        super(StdOutListener, self).__init__()
        self.crawl_type = crawl_type
        self.keywords = keywords
        self.tweet_dump = []
        self.keyword_processor = KeywordProcessor()
        self.keyword_processor.add_keywords_from_list(self.keywords)

    def extract_valid_urls(self, tweet, source="original"):
        url_list = []
        filter_keywords = [r"https://twitter.com", "status"]
        for url in tweet.entities["urls"]:
            # do not include links to other tweets
            if all([kw in url["expanded_url"] for kw in filter_keywords]):
                continue
            url_list.append(
                {"tweet_id": tweet.id, "source": source, "url": url["expanded_url"]}
            )
        return url_list

    def get_urls(self, tweet_data):
        url_list = []
        if tweet_data.is_quote_status and hasattr(tweet_data, "quoted_status"):
            url_list += self.extract_valid_urls(tweet_data, source="original")
            url_list += self.extract_valid_urls(
                tweet_data.quoted_status, source="quoted_status"
            )

        elif hasattr(tweet_data, "retweeted_status"):
            url_list += self.extract_valid_urls(
                tweet_data.retweeted_status, source="retweeted_status"
            )
        else:
            url_list += self.extract_valid_urls(tweet_data, source="original")

        return url_list

    def get_tweet_text(self, tweet_data):
        if tweet_data.truncated:
            return tweet_data.extended_tweet["full_text"]
        return tweet_data.text

    def get_full_text(self, tweet_data):
        text = self.get_tweet_text(tweet_data)
        if tweet_data.is_quote_status and hasattr(tweet_data, "quote_status"):
            text += self.get_tweet_text(tweet_data.quoted_status)

        elif hasattr(tweet_data, "retweeted_status"):
            text += self.get_tweet_text(tweet_data.retweeted_status)
        return text

    def extract_keywords_from_tweet(self, tweet_data):
        kw_list = []
        kw_list += self.keyword_processor.extract_keywords(tweet_data.text)
        for el in tweet_data.entities.get("media",[]):
            kw_list += self.keyword_processor.extract_keywords(el["display_url"])
            kw_list += self.keyword_processor.extract_keywords(el["expanded_url"])
        for el in tweet_data.entities.get("urls",[]):
            kw_list += self.keyword_processor.extract_keywords(el["display_url"])
            kw_list += self.keyword_processor.extract_keywords(el["expanded_url"])
        for el in tweet_data.entities.get("user_mentions",[]):
            kw_list += self.keyword_processor.extract_keywords(el["screen_name"])
        for el in tweet_data.entities.get("hashtags", []):
            kw_list += self.keyword_processor.extract_keywords(el["text"])
        return kw_list

    def get_keywords(self, text, tweet_data):
        flash_kw = []
        flash_kw += self.keyword_processor.extract_keywords(text)
        flash_kw += self.extract_keywords_from_tweet(tweet_data)
        if tweet_data.is_quote_status and hasattr(tweet_data, "quote_status"):
            flash_kw += self.extract_keywords_from_tweet(tweet_data.quoted_status)
        elif hasattr(tweet_data, "retweeted_status"):
            flash_kw += self.extract_keywords_from_tweet(tweet_data.retweeted_status)
        return list(set(flash_kw))

    def on_status(self, data):
        logger.info(f"[crawler.StdOutListener] :: Status ID: {data.id}")

        text = self.get_full_text(data)
        tweet_data = data._json
        tweet_data["collected_text"] = text
        keywords = self.get_keywords(text, data)
        tweet_data["keywords"] = keywords
        if len(keywords) != 0:
            self.tweet_dump.append(tweet_data)

        if len(self.tweet_dump) >= 10:
            self.dump_tweets()

        return True

    def on_error(self, status_code):
        if status_code in [420, 429]:
            logger.info("[crawler.StdOutListener] :: Rate limit reached")
        elif status_code >= 500:
            logger.info("[crawler.StdOutListener] :: Twitter internal error")
        elif status_code in [304]:
            logger.info("[crawler.StdOutListener] :: No new data received")
        else:
            logger.info(f"[crawler.StdOutListener] :: Status_code: {status_code}")

    def dump_tweets(self):
        logger.info("[crawler.StdOutListener] :: Dumping tweets")
        outpath = f"./snapshots/{self.crawl_type}/"
        os.makedirs(outpath, exist_ok=True)
        dumpfile = f"{outpath}/tweets_{uuid.uuid4().hex}.p"
        with open(dumpfile, "wb") as output_file:
            pickle.dump(
                self.tweet_dump,
                output_file,
            )
        self.tweet_dump.clear()


def prepare_keywords(crawl_type, top_limit):
    csv_path = f"./keywords/{crawl_type}.csv"

    reader = csv.DictReader(open(csv_path, "r"), delimiter=",")
    keywords = []  # maximum 400 for standard users
    if crawl_type in ["snp500", "crypto"]:
        for i, x in enumerate(reader):
            if top_limit > 0 and i >= top_limit:
                break
            name = x["name"]
            symbol = "$" + x["symbol"]
            keywords += [name, symbol]
    else:
        for i, x in enumerate(reader):
            if top_limit > 0 and i >= top_limit:
                break
            keywords.append(x["name"])
    if len(keywords) % 2 != 0:
        keywords = keywords[:-1]
    return keywords[:400]


def crawl(auth, crawl_type, top_limit=-1):
    logger.info("[crawler] :: Crawling started")
    keywords = prepare_keywords(crawl_type, top_limit)

    listener = StdOutListener(crawl_type=crawl_type, keywords=keywords)

    while True:
        try:
            stream = Stream(auth, listener, tweet_mode="extended")
            stream.filter(languages=["en", "de"], track=keywords, is_async=False)

        except KeyboardInterrupt:
            logger.critical("[crawler] :: Keyboard interrupt - shutting down")
            exit(0)

        except Exception as e:
            logger.critical(f"[crawler] :: Error occured - {e}")
            logger.info("[crawler] :: Continuing...")
            capture_exception(e)
            continue


if __name__ == "__main__":
    args = sys.argv[1:]
    crawl_type = args[0]
    credential_num = int(args[1])
    logger.info(f"[crawler] :: Crawling {crawl_type} with Credential{credential_num}")

    assert crawl_type in ["crypto", "snp500", "election", "covid"], "not supported crawl_type"

    cred_dict = util.get_config("./config_api.ini")
    auths = [cred_to_auth(cred) for cred in cred_dict.values()]
    print(auths)

    crawl(auths[credential_num], crawl_type, top_limit=600)
