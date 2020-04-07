import json, os

from bson import Code
from pymongo import MongoClient, version

"""
  
"""
class MongoDataBase:
    def __init__(self, path="/home/ubuntu/LDSA/A2/tweet_analysis/input/"):
        """
        Makes the MongoDB client with localhost as address and 27017 as port, adds a database (twitter_db) and collection
        (twitter_collection). Chechs if that collection exist in the database and if not than it loads json-files from the path
        (default="/home/ubuntu/LDSA/A2/tweet_analysis/input/"). Everything except the text attribute removed and than
        duplications is removed. This is then loaded to the collection in the database on th client.

        :param path: string
            The path as string to the folder where the json-files are located.
        """
        self.tweets_path = path
        self.client = self.create_client()
        self.twitter_db = self.client["twitter_db"]
        self.twitter_collection = self.twitter_db["twitter_collection"]
        collection_list = self.twitter_db.list_collection_names()
        if "twitter_collection" not in collection_list:
            self.insert_json_dir()

    @staticmethod
    def create_client():
        """
        Creates MongoDB client with address localhost:27017.

        :return: MongoClient
            The created MongoDB client.
        """
        client = MongoClient("mongodb://localhost:27017/")
        return client

    def insert_json_dir(self, path=None, collection=None):
        """
        Loads attribute text from all json-files from provided path (if none is provided than it uses self.path),
        removes duplications and inserts it to provided collection (if none is provided than it uses
        self.twitter_collection).

        :param path: string
            The path to the folder which the json-files which should be loaded are located. If none are provided then
            it just uses self.path.
        :param collection: Collection
            The collection to which the json-files should be loaded. If none are provided then it just uses
            self.twitter_collection.
        :return: Collection
            The collection that been used.

        """
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
        for tweet in temp_data:
            collection.insert_one({"text": tweet})

        return collection

    def insert_json_file(self, file=None, collection=None):
        """
        Loads attribute text from a json-file from provided path (if none is provided than it uses self.path), removes
        duplications and inserts it to provided collection (if none is provided than it uses self.twitter_collection).

        :param path: string
            The path to the json-file which should be loaded. If none are provided then it just uses self.path.
        :param collection: Collection
            The collection to which the json-file should be loaded. If none are provided then it uses
            self.twitter_collection.
        :return: Collection
            The collection that been used.

        """
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
        for tweet in temp_data:
            collection.insert_one({"text": tweet})

        return collection

    def map_reduce(self):
        """
        Makes two bson Code objects, one containing a map function and one containing a reduce function in javascript.

        The map function (mapper) takes all text in the called collection (twitter_collection). If this is not empty then it
        splits the text into words based on a regex that excludes everything except letter a-ö and A-Ö. Then it loops
        through the list of split words and checks if they are any of the presat pronouns (han, hon, denna, det, denne, den
        and hen). If it is then it emits that word with the count of one to the reducer.

        The reducer listens for emits from the mapper and returns all occurrences of a given word.

        :return: dict
            A dictionary of unique words from the mapper and the sum from the reducer with the title pronouns.
        """
        mapper = Code(
            """
            function () {
                var text = this.text
                const pronouns_list = ["han", "hon", "denna", "det", "denne", "den", "hen"]
                if (text) {
                    words = text.toLowerCase().split(/[^\u0041-\u005A\u0061-\u007A\u00C4-\u00C5\u00D6\u00E4-\u00E5\u00F6]/)
                    for(var i = words.length - 1; i >= 0; i--) {
                        if (pronouns_list.includes(words[i])) {
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

    def print_map_reduce(self):
        """
        Print and returns the result from self.map_reduce().
        :return: dict
            The result from self.map_reduce().
        """
        result = self.map_reduce()
        for doc in result.find():
            print(doc)
        return result

    def delete_collection(self, collection_name="twitter_collection"):
        """
        Remove provide collection (default = twitter_collection).

        :param collection_name = "twitter_collection": string
            Name of the collection that should be removed.
        """
        self.twitter_db.drop_collection(collection_name)

    def delete_database(self, db_name="twitter_db"):
        """
        Remove provide database (default = twitter_db).

        :param db_name = "twitter_db": string
            Name of the database that should be deleted.
        """
        self.client.drop_database(db_name)

    def __del__(self):
        """
        When objects gets deleted then it closes the client.
        """
        self.client.close()


if __name__ == '__main__':
    print("MongoDB version: " + version)
    MongoDB = MongoDataBase()
    MongoDB.print_map_reduce()
    # MongoDB.delete_collection()
    # MongoDB.delete_database()
