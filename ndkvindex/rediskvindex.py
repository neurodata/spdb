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

import types

from kvindex import KVIndex
import redis

from spatialdberror import SpatialDBError
import logging
logger=logging.getLogger("neurodata")


class RedisKVIndex(KVIndex):

  def __init__(self, db):
    """Connect to the Redis backend"""
    
    self.db = db
    try:
      self.client = redis.StrictRedis(host=self.db.proj.getDBHost(), port=6379, db=0)
      self.pipe = self.client.pipeline(transaction=False)
    except redis.ConnectionError as e:
      logger.error("Could not connect to Redis server. {}".format(e))
      raise SpatialDBError("Could not connect to Redis server. {}".format(e))


  def getIndexStore(self, ch, resolution):
    """Generate the name of the Index Store"""
    return '{}_{}_{}'.format(self.db.proj.getProjectName(), ch.getChannelName(), resolution)


  def getCubeIndex(self, ch, resolution, listofidxs, listoftimestamps=None):
    """Retrieve the indexes of inserted cubes"""

    index_store = self.getIndexStore(ch, resolution)
    index_store_temp = index_store+'_temp'
    
    try:
      if listoftimestamps:
        self.client.sadd(index_store_temp, *zip(listoftimestamps, listofidxs))
      else:
        self.client.sadd(index_store_temp, *listofidxs)
      ids_to_fetch = self.client.sdiff( index_store_temp, index_store )
      self.client.delete(index_store_temp)
    except Exception, e:
      logger.error("Error retrieving cube indexes into the database. {}".format(e))
      raise SpatialDBError("Error retrieving cube indexes into the database. {}".format(e))
    
    return list(ids_to_fetch)
 

  def putCubeIndex(self, ch, resolution, listofidxs, listoftimestamps=None):
    """Add the listofidxs to the store"""
    
    try: 
      if listoftimestamps:
        self.client.sadd( self.getIndexStore(ch, resolution), *zip(listoftimestamps, listofidxs))
      else:
        self.client.sadd( self.getIndexStore(ch, resolution), *listofidxs)
    except Exception, e:
      logger.error("Error inserting cube indexes into the database. {}".format(e))
      raise SpatialDBError("Error inserting cube indexes into the database. {}".format(e))
