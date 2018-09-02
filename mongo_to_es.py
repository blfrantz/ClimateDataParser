import os
import time
from datetime import datetime
from copy import deepcopy
import glob

from tqdm import tqdm
from pymongo import MongoClient, DeleteOne, UpdateOne
from bson import ObjectId
from elasticsearch import Elasticsearch, helpers

GHCN_FILES = 'F:\dev\ClimateExample\project\data'

MONGO_HOST = 'localhost'
MONGO_PORT = 27017
MONGO_DB = 'ghcn'
MONGO_COL = 'monthly_v3'

ES_HOSTS = ['localhost:9200']
ES_INDEX = 'ghcn_monthly_v3'
ES_TYPE = 'monthly'
ES_STATE = '_es_state'

BATCH_SIZE = 500

class MongoToElasticsearch():
  def __init__(self, index=ES_INDEX, collection=MONGO_COL):
    self._es = Elasticsearch(ES_HOSTS)
    self._index = index
    self._col = MongoClient(MONGO_HOST, MONGO_PORT)[MONGO_DB][collection]

    self._setup_index()

  def _setup_index(self):
    if not self._es.indices.exists(self._index):
      self._es.indices.create(
        index=self._index,
        body={
          'settings': {
            'index': {
              'refresh_interval': '1m'
            }
          },
          'mappings': {
            ES_TYPE: {
              'properties': {
                'meta': {
                  'properties': {
                    'location': {
                      'type': 'geo_point'
                    }
                  }
                }
              }
            }
          }
        }
      )

  def _transform(self, obj):
    action = {
      '_index': self._index,
      '_id': str(obj['_id']),
      '_type': ES_TYPE
    }
    del obj['_id']

    if obj[ES_STATE] == 'delete':
      action['_op_type'] = 'delete'
    del obj[ES_STATE]

    action['_source'] = obj

    return action

  def _insert_batch(self, batch):
    mongo_batch = []

    for ok, result in helpers.parallel_bulk(self._es, batch):
      action, result = result.popitem()
      oid = ObjectId(result['_id'])

      if ok:
        mongo_update = UpdateOne(
          {'_id': oid},
          {'$set': {ES_STATE: 'complete'}}
        )
        mongo_batch.append(mongo_update)
      else:
        mongo_update = UpdateOne(
          {'_id': oid},
          {'$set': {ES_STATE: 'error'}}
        )
        mongo_batch.append(mongo_update)
        print('Failed to %s: %s', action, result['_id'])

    self._col.bulk_write(mongo_batch)

  def run(self):
    batch = []
    query = {
      '$or': [
        {ES_STATE: 'insert'},
        {ES_STATE: 'update'},
        {ES_STATE: 'remove'}
      ]
    }

    with tqdm(total=self._col.count_documents(query)) as pbar:
      for obj in self._col.find(query):
        batch.append(self._transform(obj))

        if len(batch) == BATCH_SIZE:
          self._insert_batch(batch)
          batch = []
          pbar.update(BATCH_SIZE)

    # Flush remaining
    self._insert_batch(batch)


if __name__ == '__main__':
  etl = MongoToElasticsearch()
  while True:
    try:
      etl.run()
      time.sleep(5)  # Poll for changes every 5s
    except Exception as e:
      print('Unhandled error: %s', e)
      time.sleep(60)
