# Copyright 2014 NeuroData (http://neurodata.io)
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import time
from toolz import interleave
import types
from kvindex import KVIndex
from redispool import RedisPool
from ndmanager.readerlock import ReaderLock
from ndmanager.writerlock import WriterLock
import redis
import django
from django.conf import settings
from spatialdberror import SpatialDBError
import logging
logger = logging.getLogger("neurodata")


class RedisKVIndex(KVIndex):
  

  def __init__(self, db):
    """Connect to the Redis backend"""
    
    self.db = db
    try:
      self.client = redis.StrictRedis(connection_pool=RedisPool.blocking_pool)
      self.pipe = self.client.pipeline(transaction=False)
    except redis.ConnectionError as e:
      logger.error("Could not connect to Redis server. {}".format(e))
      raise SpatialDBError("Could not connect to Redis server. {}".format(e))
  

  def getIndexStore(self):
    """Generate the name of the Index Store"""
    return settings.REDIS_INDEX_KEY

  def getIndexList(self, ch, resolution, listofidxs):
    """Generate the name of the Index Store"""
    return ['{}&{}&{}&{}'.format(self.db.proj.getProjectName(), ch.getChannelName(), resolution, index) for index in listofidxs]
  
  def cleanIndexList(self, index_list):
    return [ index.split('&')[-1] for index in index_list]
  
  def getCubeIndex(self, ch, resolution, listofidxs, listoftimestamps=None):
    """Retrieve the indexes of inserted cubes"""

    index_store = self.getIndexStore()
    index_store_temp = index_store+'&temp'
    
    try:
      if listoftimestamps:
        pass
        # self.client.zadd(list(interleave([listofidxs, listoftimestamps])))
      else:
        index_list = list(interleave([[1]*len(listofidxs), self.getIndexList(ch, resolution, listofidxs)]))
        self.client.zadd(index_store_temp, *index_list)
      # self.client.zinterstore(index_store_temp, [index_store_temp, index_store] )
      self.client.zunionstore(index_store_temp, {index_store_temp : 1, index_store : 0}, 'MIN')
      ids_to_fetch = self.client.zrevrangebyscore(index_store_temp, '+inf', 1)
      self.client.delete(index_store_temp)
    except Exception, e:
      logger.error("Error retrieving cube indexes into the database. {}".format(e))
      raise SpatialDBError("Error retrieving cube indexes into the database. {}".format(e))
    
    return self.cleanIndexList(ids_to_fetch)
 
  def putCubeIndex(self, ch, resolution, listofidxs, listoftimestamps=None):
    """Add the listofidxs to the store"""
    
    try: 
      if listoftimestamps:
        # self.client.sadd( self.getIndexStore(ch, resolution), *zip(listoftimestamps, listofidxs))
        pass
      else:
        cachedtime_list = [time.time()]*len(listofidxs)
        index_list = list(interleave([cachedtime_list, self.getIndexList(ch, resolution, listofidxs)]))
        self.client.zadd(self.getIndexStore(), *index_list)
    except Exception, e:
      logger.error("Error inserting cube indexes into the database. {}".format(e))
      raise SpatialDBError("Error inserting cube indexes into the database. {}".format(e))
