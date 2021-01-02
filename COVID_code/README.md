# twitterDSlab2020

## Getting started
```
cd /home/dslab2020/twitterNewsDSLab2020
source .tenv/bin/activate

```

## Running Everything
```
cd stable
./alarmclock.sh
```

## Use Jupyter lab
```
inside .tenv
jupyter lab --no-browser
on your local machine
ssh -N -L 8888:localhost:8888 {user}@{server_ip}
```

## Use Tmux to checkout processes


## Write to MongoDB

In order to push to MongoDB you can do:
```bash
mongo
> use tweet_data
> db.myNewCollection1.insertOne( { x: 1 } )
```

This creates a new collection for you myNewCollection1 and inserts a new element
Then you can use that collection with the pymongo client
```python
from pymongo import MongoClient
import os, configparser

file_path = 'mongodb.ini'
parser = configparser.ConfigParser()
parser.read(file_path)
c = parser._sections["MongoDB"]

mongoClient = MongoClient(c['ip'], int(c['port']),
                username=c['username'], password=c['password'],
                authSource=c['authsource'])
db_name = c["database"]
db = mongoClient[db_name]
db_collection = db['myNewCollection1']

db_collection.insert_many(tweets_batch)
```
