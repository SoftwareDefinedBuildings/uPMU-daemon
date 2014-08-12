__author__ = 'immesys'
_client = MongoClient()
db = _client.upmu_database

def decode_name(namestr):
