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

from __future__ import division
from django.conf import settings
import redis
from spdb.redispool import RedisPool
from spdb.ndmanager.redislock import RedisLock
from spdb.ndmanager.writerlock import WriterLock
from spdb.spatialdberror import SpatialDBError
import logging
logger=logging.getLogger("neurodata")

class RedisManager(object):

  def __init__(self):
    try:
      self.client = redis.StrictRedis(connection_pool=RedisPool.blocking_pool)
      self.pipe = self.client.pipeline(transaction=True)
    except redis.ConnectionError as e:
      logger.error("{}".format(e))
      raise SpatialDBError("{}".format(e))
  
  def close(self):
    """Closing the connection"""
    self.client = None

  def info(self):
    """Info in redis"""
    return self.client.info()
  
  def execute(self):
    """Execute pipe transaction"""
    self.pipe.execute()
  
  def memoryRatio(self):
    """Fetch the memory ratio"""
    info = self.info()
    logger.debug("Memory Ratio: {}".format((info['used_memory_rss'] / info['total_system_memory']) * 100))
    return (info['used_memory_rss'] / info['total_system_memory']) * 100

  def memoryUpperBound(self):
    """Check if memory is full or not"""
    return True if self.memoryRatio() > settings.REDIS_MEMORY_RATIO_UPPER_BOUND else False

  def memoryLowerBound(self):
    """Check if memory is now empty"""
    return True if settings.REDIS_MEMORY_RATIO_LOWER_BOUND < self.memoryRatio() else False
  
  def getIndexStore(self):
    """Get the name of the index store"""
    return settings.REDIS_INDEX_KEY

  def getLRUIndex(self):
    """Get the index from the index store"""
    return self.client.zrangebyscore(self.getIndexStore(), '-inf', '+inf', 0, 100) 
  
  def deleteLRUIndex(self, index_list):
    """Delete the index from the index store"""
    if index_list:
      self.client.zrem(self.getIndexStore(), *index_list)
  
  def deleteCubes(self, index_list):
    """Delete the cuboids from redis cache"""
    if index_list:
      self.client.delete(*index_list)
  
  def flushMemory(self):
    self.client.flushdb()
 
  @WriterLock
  def emptyMemory(self):
    """Empty memory from cache"""
    try:
      while self.memoryLowerBound():
        index_list = self.getLRUIndex()
        # logger.debug("empty memory: removing indexes: ", index_list)
        self.deleteCubes(index_list)
        self.deleteLRUIndex(index_list)
    except Exception as e:
      raise
