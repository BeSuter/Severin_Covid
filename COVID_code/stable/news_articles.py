from newspaper import Article, ArticleException
import ssl


def load_article_content(article):
    article_features = {}
    try:
        article.download()
        article.parse()

        article_features["title"] = article.title
        article_features["authors"] = article.authors
        article_features["publish_date"] = article.publish_date
        article_features["text"] = article.text
        article_features["final_format"] = True

    except (ArticleException, ValueError, ssl.SSLError):
        # Added ValueError due to Invalid IPv6 URL --> Find a better way
        #
        # If we are unable to download the article, we do not
        # want to count it. Set 'authors' and 'publish_date' such
        # that the article is not trusted and not saved
        article_features["authors"] = []
        article_features["publish_date"] = None

    return article_features


def get_valid_articles(tweet_df, db_article_collection):
    articles = {}
    tweet_url_list = tweet_df.collected_urls.to_list()
    all_urls = [url_info["url"] for url_list in tweet_url_list for url_info in url_list]
    already_in_db_urls = [el["url"] for el in db_article_collection.find({"url": {"$in": all_urls}}, {"url"})]


    for idx, url_list in enumerate(tweet_url_list):
        for url_info in url_list:
            if url_info["url"] in articles.keys():
                articles[url_info["url"]]["id"].append(url_info["tweet_id"])
                articles[url_info["url"]]["object_id"].append(
                    tweet_df.iloc[idx].object_id
                )
                continue
            potential_article = Article(url_info["url"])
            if potential_article.is_valid_url():
                article_features = {}
                if not url_info["url"] in already_in_db_urls:
                    article_features = load_article_content(potential_article)
                    # we assume that articles without author and publish_date are not proper newspaper articles
                    if (
                            article_features["authors"] == []
                            and article_features["publish_date"] == None
                    ):
                        continue
                article_features["id"] = [url_info["tweet_id"]]
                article_features["object_id"] = [tweet_df.iloc[idx].object_id]
                article_features["url"] = url_info["url"]
                articles[article_features["url"]] = article_features
    return articles
