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
import redis
import itertools
from redispool import RedisPool
from kvio import KVIO
from spatialdberror import SpatialDBError
import logging
logger=logging.getLogger("neurodata")


class RedisKVIO(KVIO):

  def __init__ ( self, db ):
    """Connect to the Redis backend"""
    
    self.db = db
    try:
      self.client = redis.StrictRedis(connection_pool=RedisPool.blocking_pool)
      self.pipe = self.client.pipeline(transaction=False)
    except redis.ConnectionError as e:
      logger.error("Could not connect to Redis server. {}".format(e))
      raise SpatialDBError("Could not connect to Redis server. {}".format(e))
   
  def generateKeys(self, ch, timestamp_list, zidx_list, resolution, neariso=False):
    """Generate a key for Redis"""
    
    key_list = []
    for timestamp in timestamp_list:
      for zidx in zidx_list:
        if neariso:
          key_list.append( '{}&{}&{}&{}&{}&neariso'.format(self.db.proj.project_name, ch.channel_name, resolution, zidx, timestamp) )
        else:
          key_list.append( '{}&{}&{}&{}&{}'.format(self.db.proj.project_name, ch.channel_name, resolution, zidx, timestamp) )
    
    # if isinstance(timestamp, types.ListType):
      # for tvalue in timestamp:
        # key_list.append( '{}&{}&{}&{}&{}'.format(self.db.proj.project_name, ch.channel_name, resolution, tvalue, zidx_list[0]) )
    # else:
      # for zidx in zidx_list:
        # if timestamp == None:
          # key_list.append( '{}&{}&{}&{}'.format(self.db.proj.project_name, ch.channel_name, resolution, zidx) )
        # else:
          # key_list.append( '{}&{}&{}&{}&{}'.format(self.db.proj.project_name, ch.channel_name, resolution, timestamp, zidx) )

    return key_list


  def getCube(self, ch, timestamp, zidx, resolution, update=False, neariso=False):
    """Retrieve a single cube from the database"""
    
    try:
      rows = self.client.mget( self.generateKeys(ch, [timestamp], [zidx], resolution, neariso) )  
    except Exception, e:
      logger.error("Error retrieving cubes into the database. {}".format(e))
      raise SpatialDBError("Error retrieving cubes into the database. {}".format(e))
    
    if rows[0]:
      return rows[0]
    else:
      return None

  def getCubes(self, ch, listoftimestamps, listofidxs, resolution, neariso=False):
    """Retrieve multiple cubes from the database"""
    
    try:
      rows = self.client.mget( self.generateKeys(ch, listoftimestamps, listofidxs, resolution, neariso) )
    except Exception, e:
      logger.error("Error retrieving cubes into the database. {}".format(e))
      raise SpatialDBError("Error retrieving cubes into the database. {}".format(e))
    
    for (timestamp, zidx), row in zip(itertools.product(listoftimestamps, listofidxs), rows):
      yield(zidx, timestamp, row)

    # for idx in listofidxs:
      # for zidx, timestamp, row in zip([idx]*len(listoftimestamps), listoftimestamps, rows):
        # yield ( zidx, timestamp, row )


  # def getTimeCubes(self, ch, idx, listoftimestamps, resolution):
    # """Retrieve multiple cubes from the database"""
    
    # try:
      # rows = self.client.mget( self.generateKeys(ch, resolution, [idx], listoftimestamps) )
    # except Exception, e:
      # logger.error("Error inserting cubes into the database. {}".format(e))
      # raise SpatialDBError("Error inserting cubes into the database. {}".format(e))
    
    # for idx, timestamp, row in zip([idx]*len(listoftimestamps), listoftimestamps, rows):
      # yield ( idx, timestamp, row )


  def putCube(self, ch, timestamp, zidx, resolution, cubestr, update=False, neariso=False):
    """Store a single cube into the database"""
    
    # generating the key
    key_list = self.generateKeys(ch, [timestamp], [zidx], resolution, neariso=neariso)
    
    try:
      self.client.mset( dict(zip(key_list, [cubestr])) )
    except Exception, e:
      logger.error("Error inserting cube into the database. {}".format(e))
      raise SpatialDBError("Error inserting cube into the database. {}".format(e))

  def putCubes(self, ch, listoftimestamps, listofidxs, resolution, listofcubes, update=False, neariso=False):
    """Store multiple cubes into the database"""
    
    import blosc
    print "inserting cube of shape: {}, res: {}".format(blosc.unpack_array(listofcubes[0]).shape, resolution)
    # generating the list of keys
    key_list = self.generateKeys(ch, listoftimestamps, listofidxs, resolution, neariso)
    
    try:
      self.client.mset( dict(zip(key_list, listofcubes)) )
    except Exception, e:
      logger.error("Error inserting cubes into the database. {}".format(e))
      raise SpatialDBError("Error inserting cubes into the database. {}".format(e))
