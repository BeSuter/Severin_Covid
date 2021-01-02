import configparser
import os
import time
import pickle

from pymongo import MongoClient


def get_config(filename):
    parser = configparser.ConfigParser()
    parser.read(filename)
    return parser._sections


def get_db_collection(collection_name):
    c = get_config("./config_mongodb.ini")["MongoDB"]
    mongoClient = MongoClient(
        c["ip"],
        int(c["port"]),
        username=c["username"],
        password=c["password"],
        authSource=c["authsource"],
    )
    db = mongoClient[c["database"]]
    if os.getenv("DEBUG", False):
        return db[collection_name + "_test"]
    return db[collection_name]


def absolute_filepaths(directory):
    abs_paths = []
    for dirpath, _, filenames in os.walk(directory):
        for f in filenames:
            abs_paths.append(os.path.abspath(os.path.join(dirpath, f)))
    return abs_paths


def load_pickle(filepath):
    return pickle.load(open(filepath, "rb"))


def timing(f):
    def wrap(*args, **kwargs):
        time1 = time.time()
        ret = f(*args, **kwargs)
        time2 = time.time()
        duration = time2 - time1

        duration_message = (
            f"{f.__name__} function took {duration} s or {duration / 60.0} min."
        )
        print("-" * len(duration_message))
        print(time.asctime())
        print(duration_message)
        print(f"Args were: {args}")
        print("-" * len(duration_message))

        return ret

    return wrap
