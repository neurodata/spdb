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
import itertools
from toolz import interleave
from kvindex import KVIndex
from redispool import RedisPool
import redis
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

  def getIndexList(self, ch, listoftimestamps, listofidxs, resolution, neariso=False):
    """Generate the name of the Index Store"""
    if neariso:
      return ['{}&{}&{}&{}&{}&neariso'.format(self.db.proj.project_name, ch.channel_name, resolution, index, timestamp) for (index, timestamp) in itertools.product(listofidxs, listoftimestamps)]
    else:
      return ['{}&{}&{}&{}&{}'.format(self.db.proj.project_name, ch.channel_name, resolution, index, timestamp) for (index, timestamp) in itertools.product(listofidxs, listoftimestamps)]
    # return ['{}&{}&{}&{}'.format(self.db.proj.project_name, ch.channel_name, resolution, index) for index in listofidxs]
  
  def cleanIndexList(self, index_list, neariso=False):
    if neariso:
      return [index.split('&')[-3] for index in index_list]
    else:
      return [ index.split('&')[-2] for index in index_list]
  
  def getCubeIndex(self, ch, listoftimestamps, listofidxs, resolution, neariso=False):
    """Retrieve the indexes of inserted cubes"""

    index_store = self.getIndexStore()
    index_store_temp = index_store+'&temp'
    index_list_size = len(listofidxs)*len(listoftimestamps)
    
    try:
      index_list = list(interleave([[1]*index_list_size, self.getIndexList(ch, listoftimestamps, listofidxs, resolution, neariso)]))
      self.client.zadd(index_store_temp, *index_list)
      
      # if listoftimestamps:
        # # TODO KL Test this
        # index_list = list(interleave([[1]*len(listofidxs), self.getIndexList(ch, resolution, listofidxs)]))
        # self.client.zadd(index_store_temp, *index_list)
      # else:
        # index_list = list(interleave([[1]*len(listofidxs), self.getIndexList(ch, resolution, listofidxs)]))
        # self.client.zadd(index_store_temp, *index_list)
      # self.client.zinterstore(index_store_temp, [index_store_temp, index_store] )
      self.client.zunionstore(index_store_temp, {index_store_temp : 1, index_store : 0}, 'MIN')
      ids_to_fetch = self.client.zrevrangebyscore(index_store_temp, '+inf', 1)
      self.client.delete(index_store_temp)
    except Exception, e:
      logger.error("Error retrieving cube indexes into the database. {}".format(e))
      raise SpatialDBError("Error retrieving cube indexes into the database. {}".format(e))
    
    return self.cleanIndexList(ids_to_fetch, neariso=neariso)
 
  def putCubeIndex(self, ch, listoftimestamps, listofidxs, resolution, neariso=False):
    """Add the listofidxs to the store"""
    
    try: 
      cachedtime_list = [time.time()]*len(listofidxs)*len(listoftimestamps)
      index_list = list(interleave([cachedtime_list, self.getIndexList(ch, listoftimestamps, listofidxs, resolution, neariso)]))
      self.client.zadd(self.getIndexStore(), *index_list)
      # if listoftimestamps:
        # # TODO KL Test this
        # cachedtime_list = [time.time()]*len(listofidxs)
        # index_list = list(interleave([cachedtime_list, self.getIndexList(ch, resolution, listofidxs)]))
        # self.client.zadd(self.getIndexStore(), *index_list)
      # else:
        # cachedtime_list = [time.time()]*len(listofidxs)
        # index_list = list(interleave([cachedtime_list, self.getIndexList(ch, resolution, listofidxs)]))
        # self.client.zadd(self.getIndexStore(), *index_list)
    except Exception, e:
      logger.error("Error inserting cube indexes into the database. {}".format(e))
      raise SpatialDBError("Error inserting cube indexes into the database. {}".format(e))
