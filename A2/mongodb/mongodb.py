import json, os

from bson import Code
from pymongo import MongoClient, version


class MongoDataBase:
    def __init__(self, path="/home/ubuntu/LDSA/A2/tweet_analysis/input/"):
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
        temp_data = []
        for file in files:
            with open(os.path.join(path, file), 'r') as f:
                print("--------------------------\nLoading file: " + file)
                for line in f:
                    if not line.isspace():
                        data = json.loads(line)
                        temp_data.append(data["text"])

        temp_data = set(temp_data)
        for i in range(len(temp_data)):
            collection.insert_one({"text" : list(temp_data)[i]})

        return collection

    def insert_json_file(self, file=None, collection=None):
        file = file or self.tweets_path
        collection = collection or self.twitter_collection
        temp_data = []
        with open(file, 'r') as f:
            print("Loading file: " + file)
            for line in f:
                if not line.isspace():
                    data = json.loads(line)
                    temp_data.append(data["text"])

        temp_data = set(temp_data)
        for i in range(len(temp_data)):
            collection.insert_one({"text" : list(temp_data)[i]})

        return collection

    def access_text_data(self):
        twitter_collection = self.twitter_db.get_collection("twitter_collection")
        tweets_text = twitter_collection.find()
        return tweets_text

    def map_reduce(self):
        mapper = Code(
            """
            function () {
                var text = this.text
                const pronouns_list = ["han", "hon", "denna", "det", "denne", "den", "hen"]
                if (text) {
                    words = text.toLowerCase().split(/[^A-Öa-ö]/);
                    for(var i = words.length - 1; i >= 0; i--) {
                        if (pronouns_list.some(word => words[i].includes(word))) {
                            emit(words[i], 1);
                        }
                    }
                }
            };
            """)

        reducer = Code(
            """
            function (key, values) {
                var result = 0;
                for (var i = 0; i < values.length; i++) {
                    result += values[i];
                }
                return result;
            }
            """)

        return self.twitter_collection.map_reduce(mapper, reducer, "pronouns")

    def reducer(self, input):
        return reducer(input)

    def delete_collection(self, collection_name="twitter_collection"):
        self.twitter_db.drop_collection(collection_name)

    def delete_database(self, db_name="twitter_db"):
        self.client.drop_database(db_name)

    def __del__(self):
        self.client.close()


if __name__ == '__main__':
    print(version)
    MongoDB = MongoDataBase()
    result = MongoDB.map_reduce()
    for doc in result.find():
        print(doc)
    #tweets_text = MongoDB.access_text_data()
    #MongoDB.delete_collection()
    #MongoDB.delete_database()
