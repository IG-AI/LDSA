import json, os
from pymongo import MongoClient
from mapper import mapper
from reducer import reducer


class MongoDataBase:
    def __init__(self, path='/home/ubuntu/LDSA/A2/tweet_analysis/input/'):
        self.tweets_path = path
        self.client = self.create_client()
        self.twitter_db = self.client["twitter_db"]
        self.twitter_collection = self.twitter_db["twitter_collection"]
        collection_list = self.twitter_db.list_collection_names()
        if "twitter_collection" not in collection_list:
            self.insert_json_dir()

    @staticmethod
    def create_client():
        client = MongoClient("mongodb://localhost:27017/")
        return client

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

    def delete_database(self, db_name="twitter_db"):
        self.client.drop_database(db_name)

    def __del__(self):
        self.client.close()


if __name__ == '__main__':
    MongoDB = MongoDataBase()
    tweets_text = MongoDB.access_text_data()
    mapped_data = MongoDB.mapper(tweets_text)
    reduced_data = MongoDB.reducer(mapped_data)
    print(reduced_data)
    #MongoDB.delete_collection()
    #MongoDB.delete_database()
