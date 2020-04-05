import json, os
from pymongo import MongoClient
from mapper import mapper
from reducer import reducer


class MongoDataBase:
    def __init__(self, path='/home/g_a/LDSA/A2/mongodb/tweets/files/'):
        self.tweets_path = path
        self.client = self.create_client()
        twitter_db, twitter_collection = self.create_database(self.client)
        self.twitter_db = twitter_db
        self.twitter_collection = twitter_collection

    @staticmethod
    def create_client():
        mongodb_uri = os.getenv('MONGODB_URI')
        client = MongoClient(mongodb_uri)
        return client

    def create_database(self, client):
        twitter_db = client["twitter_db"]
        collection_list = twitter_db.list_collection_names()
        if "twitter_collection" in collection_list:
            twitter_collection = twitter_db["twitter_collection"]
        else:
            twitter_collection = self.twitter_db.get_collection("twitter_collection")
        return twitter_db, twitter_collection

    def insert_json_dir(self, path=None, collection=None):
        path = path or self.tweets_path
        collection = collection or self.twitter_collection
        files = os.listdir(path)
        for file in files:
            with open(os.path.join(path, file), 'r') as f:
                print("--------------------------\nLoading file: " + file)
                for line in f:
                    if not line.isspace():
                        data = json.loads(line)
                        collection.insert_one(data)

        return collection

    def insert_json_file(self, file=None, collection=None):
        file = file or self.tweets_path
        collection = collection or self.twitter_collection
        with open(file, 'r') as f:
            print("Loading file: " + file)
            for line in f:
                if not line.isspace():
                    data = json.loads(line)
                    collection.insert_one(data)

        return collection

    def access_text_data(self):
        twitter_collection = self.twitter_db.get_collection("twitter_collection")
        tweets_text = twitter_collection.find()
        return tweets_text

    def mapper(self, input):
        return mapper(input)

    def reducer(self, input):
        return reducer(input)

    def delete_collection(self, collection_name="twitter_collection"):
        self.twitter_db.drop_collection(collection_name)

    #def __del__(self):
    #   self.delete_collection()


if __name__ == '__main__':
    MongoDB = MongoDataBase()
    MongoDB.insert_json_dir()
    tweets_text = MongoDB.access_text_data()
    mapped_data = MongoDB.mapper(tweets_text)
    reduced_data = MongoDB.reducer(mapped_data)
    print(reduced_data)
