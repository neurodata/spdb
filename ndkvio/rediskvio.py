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
from spdb.ndmanager.readerlock import ReaderLock
from spdb.ndkvio.s3io import S3IO
from spdb.ndkvindex.kvindex import KVIndex
import logging
logger=logging.getLogger("neurodata")


class RedisKVIO(KVIO):

  def __init__ ( self, db ):
    """Connect to the Redis backend"""
    
    self.db = db
    self.kvindex = KVIndex.getIndexEngine(db)
    self.s3io = S3IO(db)
    try:
      self.client = redis.StrictRedis(connection_pool=RedisPool.blocking_pool)
      self.pipe = self.client.pipeline(transaction=False)
    except redis.ConnectionError as e:
      logger.error("Could not connect to Redis server. {}".format(e))
      raise SpatialDBError("Could not connect to Redis server. {}".format(e))
  
  def close(self):
    self.kvindex.close()

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

  def getDirectCube(self, ch, timestamp, zidx, resolution, update=False, neariso=False):
    """Retrieve a single cube from s3"""
    
    return self.s3io.getCube(ch, timestamp, zidx, resolution, update=update, neariso=neariso)

  @ReaderLock
  def getCube(self, ch, timestamp, zidx, resolution, update=False, neariso=False, direct=False):
    """Retrieve a single cube from the database"""
    
    if direct:
      return self.getDirectCube(ch, timestamp, zidx, resolution, update=update, neariso=neariso)
    else:
      return self.getCacheCube(ch, timestamp, zidx, resolution, update=update, neariso=neariso)


  def getCacheCube(self, ch, timestamp, zidx, resolution, update=False, neariso=False):
    """Retrieve a single cube from the database"""
    
    # list of id to fetch which do not exist in cache
    id_to_fetch = self.kvindex.getCubeIndex(ch, [timestamp], [zidx], resolution, neariso=neariso)
    # check if there are any ids to fetch
    if id_to_fetch:
      # fetch the supercuboid from s3
      cuboid = self.getDirectCube(ch, timestamp, zidx, resolution, update=update, neariso=neariso)
      if cuboid:
        for listofidxs, listoftimestamps, listofcubes in cuboid:
          self.putCacheCubes(ch, listoftimestamps, listofidxs, resolution, listofcubes, update=update, neariso=neariso)

    try:
      rows = self.client.mget( self.generateKeys(ch, [timestamp], [zidx], resolution, neariso) )  
    except Exception, e:
      logger.error("Error retrieving cubes into the database. {}".format(e))
      raise SpatialDBError("Error retrieving cubes into the database. {}".format(e))
    
    if rows[0]:
      return rows[0]
    else:
      return None
  
  @ReaderLock
  def getCubes(self, ch, listoftimestamps, listofidxs, resolution, neariso=False, direct=False):
    if direct:
      return self.getDirectCubes(ch, listoftimestamps, listofidxs, resolution, neariso=neariso)
    else:
      return self.getCacheCubes(ch, listoftimestamps, listofidxs, resolution, neariso=neariso)
  

  def getDirectCubes(self, ch, listoftimestamps, listofidxs, resolution, neariso=False):
    """Retrieve cubes directly from s3"""
    return self.s3io.getCubes(ch, listoftimestamps, listofidxs, resolution, neariso=neariso)
    
  
  def getCacheCubes(self, ch, listoftimestamps, listofidxs, resolution, neariso=False):
    """Retrieve multiple cubes from the database"""
    try:
      ids_to_fetch = self.kvindex.getCubeIndex(ch, listoftimestamps, listofidxs, resolution, neariso=neariso)
      # checking if the index exists inside the database or not
      if ids_to_fetch:
        super_cuboids = self.getDirectCubes(ch, listoftimestamps, ids_to_fetch, resolution, neariso=neariso)
        for superlistofidxs, superlistoftimestamps, superlistofcubes in super_cuboids:
          # call putCubes and update index in the table before returning data
          self.putCubes(ch, superlistoftimestamps, superlistofidxs, resolution, superlistofcubes, update=True, neariso=neariso)
      
      rows = self.client.mget( self.generateKeys(ch, listoftimestamps, listofidxs, resolution, neariso) )
      for (timestamp, zidx), row in zip(itertools.product(listoftimestamps, listofidxs), rows):
        yield(zidx, timestamp, row)
    
    except Exception, e:
      logger.error("Error retrieving cubes into the database. {}".format(e))
      raise SpatialDBError("Error retrieving cubes into the database. {}".format(e))
    
    # for idx in listofidxs:
      # for zidx, timestamp, row in zip([idx]*len(listoftimestamps), listoftimestamps, rows):
        # yield ( zidx, timestamp, row )

  def putCube(self, ch, timestamp, zidx, resolution, cubestr, update=False, neariso=False, direct=False):
    """Store a single cube into the database"""
    
    if direct:
      self.s3io.putCube(ch, timestamp, zidx, resolution, cubestr, update=update, neariso=neariso)
    else:
      self.kvindex.putCubeIndex(ch, [zidx], [timestamp], resolution, neariso=neariso)
      # generating the key
      key_list = self.generateKeys(ch, [timestamp], [zidx], resolution, neariso=neariso)
    
      try:
        self.client.mset( dict(zip(key_list, [cubestr])) )
      except Exception, e:
        logger.error("Error inserting cube into the database. {}".format(e))
        raise SpatialDBError("Error inserting cube into the database. {}".format(e))


  def putCubes(self, ch, listoftimestamps, listofidxs, resolution, listofcubes, update=False, neariso=False, direct=False):
    """Store multiple cubes into the database"""

    if direct:
      return self.s3io.putCubes(ch, listoftimestamps, listofidxs, resolution, listofcubes, update=update, neariso=neariso)
    else:
      self.kvindex.putCubeIndex(ch, listoftimestamps, listofidxs, resolution, neariso=neariso)
      return self.putCacheCubes(ch, listoftimestamps, listofidxs, resolution, listofcubes, update=update, neariso=neariso)

    
  
  def putCacheCubes(self, ch, listoftimestamps, listofidxs, resolution, listofcubes, update=False, neariso=False):
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
