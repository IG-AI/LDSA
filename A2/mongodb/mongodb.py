import json, os
from pymongo import MongoClient


def create_client():
    mongodb_uri = os.getenv('MONGODB_URI')
    client = MongoClient(mongodb_uri)
    return client


def create_database(client):
    tweet_database = client["database"]
    tweet_collection = tweet_database["tweet_text"]
    return (tweet_database, tweet_collection)


def insert_json(path, tweet_collection):
    files = os.listdir(path)
    count = 0
    for file in files:
        count += 1
        with open(os.path.join(path, file), 'r') as f:
            print("--------------------------\nLoading file: " + file)
            for line in f:
                if not line.isspace():
                    #print("Loading line: " + line)
                    data = json.loads(line)
                    tweet_collection.insert_one(data)

    return tweet_collection


if __name__ == '__main__':
    client = create_client()
    tweet_database, tweet_collection = create_database(client)
    tweet_collection = insert_json('/home/g_a/PycharmProjects/MongoDB/tweets/files', tweet_collection)
