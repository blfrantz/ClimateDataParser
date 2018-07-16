import os
from datetime import datetime
from copy import deepcopy
import glob

import bonobo
from pymongo import MongoClient

GHCN_FILES = 'F:\dev\ClimateExample\project\data'

MONGO_HOST = 'localhost'
MONGO_PORT = 27017
MONGO_DB = 'ghcn'
MONGO_COL = 'monthly_v3'

# BATCH_SIZE = 10000

class GhcnToMongo():
  def __init__(self, dat_file, meta_file, countries_file):
    self._data = dat_file
    self._countries = self._get_countries(countries_file)
    self._meta = self._get_meta(meta_file)

    self._db = MongoClient(MONGO_HOST, MONGO_PORT)[MONGO_DB]

    # self._batch = []

  @staticmethod
  def _get_countries(countries_file):
    countries = {}
    with open(countries_file, 'r') as f:
      for line in f.readlines():
        if line:
          countries[line[0:3]] = line[4:].strip()
    return countries

  @classmethod
  def _get_meta(cls, meta_file):
    meta = {}
    with open(meta_file, 'r') as f:
      for line in f.readlines():
        if line:
          meta[line[0:11]] = {
            'location': {
              'lat': cls._try_cast(line[12:20].strip(), float),
              'lon': cls._try_cast(line[21:30].strip(), float)
            },
            'stnelev': cls._try_cast(line[31:37].strip(), float),
            'name': line[38:68].strip(),
            'grelev': cls._try_cast(line[69:73].strip(), int),
            'popcls': line[73:74].strip(),
            'popsiz': cls._try_cast(line[74:79].strip(), int),
            'topo': line[79:81].strip(),
            'stveg': line[81:83].strip(),
            'stloc': line[83:85].strip(),
            'ocndis': cls._try_cast(line[85:87].strip(), int),
            'airstn': line[87:88].strip(),
            'towndis': cls._try_cast(line[88:90].strip(), int),
            'grveg': line[90:106].strip(),
            'popcss': line[106:107].strip()
          }
    return meta

  @staticmethod
  def _try_cast(val, to):
    if val:
      try:
        return to(val)
      except Exception:
        return None
    return None

  def _read(self):
    with open(self._data, 'r') as f:
      for line in f.readlines():
        yield line

  def _transform(self, row):
    base_obj = {
      'country': self._countries[row[0:3]],
      'meta': self._meta[row[0:11]],
      'element': row[15:19]
    }

    year = int(row[11:15])
    increments = int((len(row)-19) / 8)

    if increments == 12:  # monthly
      for mo in range(increments):
        obj = deepcopy(base_obj)
        obj['date'] = datetime(year=year, month=mo+1, day=1)

        i = 19 + (mo * 8)
        if row[i:i+5] is not '-9999':  # Skip missing temps
          obj['value'] = float(row[i:i+5].strip()) / 100  # Convert to whole degrees C
          obj['dmflag'] = row[i+5:i+6].strip()
          obj['qcflag'] = row[i+6:i+7].strip()
          obj['dsflag'] = row[i+7:i+8].strip()
          yield obj
    else:
      print("Got unexpected # of increments, maybe we" +
        " don't support this type of input.")

  def _write(self, row):
    row['es_state'] = 'unprocessed'
    self._db[MONGO_COL].insert_one(row)
    # self._batch.append(row)
    # if len(self._batch) == BATCH_SIZE:
    #   self._db[MONGO_COL].insert_many(self._batch)
    #   self._batch = []

  def run(self):
    graph = bonobo.Graph(
      self._read,
      self._transform,
      self._write
    )

    bonobo.run(graph)

if __name__ == '__main__':
  countries = os.path.join(GHCN_FILES, 'country-codes')
  for file in glob.glob(os.path.join(GHCN_FILES, '*.dat')):
    meta = os.path.splitext(file)[0] + '.inv'
    GhcnToMongo(file, meta, countries).run()