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
import blosc
from sets import Set
from operator import add, sub, mul, div, mod
from redispool import RedisPool
from kvio import KVIO
from spatialdberror import SpatialDBError
from ndlib.ndctypelib import *
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


  def generateSuperZindexes(self, listofidxs, resolution):
    """Generate a list of super zindexes from a given list of zindexes"""
    
    [ximagesz, yimagesz, zimagesz] = self.db.proj.datasetcfg.dataset_dim(resolution)
    [xcubedim, ycubedim, zcubedim] = cubedim = self.db.proj.datasetcfg.get_cubedim(resolution)
    [xoffset, yoffset, zoffset] = self.db.proj.datasetcfg.get_offset(resolution)
    [xsupercubedim, ysupercubedim, zsupercubedim] = super_cubedim = self.db.proj.datasetcfg.get_supercubedim(resolution)
    
    listofsuperidxs = Set([])
    # super_cubedim = map(mul, cubedim, SUPERCUBESIZE)
    if listofidxs:
      for zidx in listofidxs:
        [x, y, z] = MortonXYZ(zidx)
        corner = map(mul, MortonXYZ(zidx), cubedim)
        [x,y,z] = map(div, corner, super_cubedim)
        listofsuperidxs.add(XYZMorton([x,y,z]))
      return list(listofsuperidxs)
    else:
      return None

  def breakCubes(self, timestamp, super_zidx, resolution, super_cube):
    """Breaking the supercuboids into cuboids"""
    
    super_cube = blosc.unpack_array(super_cube)
    print "breaking supercube shape: {}".format(super_cube.shape)
    # Empty lists for zindx and cube data
    zidx_list = []
    cube_list = []
    
    # SuperCube Size
    [xnumcubes, ynumcubes, znumcubes] = self.db.datasetcfg.supercube_size
    
    # Cube dimensions
    cubedim = self.db.datasetcfg.get_cubedim(resolution)
    [x,y,z] = MortonXYZ(super_zidx)
    # start = map(mul, cubedim, [x,y,z])
    start = map(mul, [x,y,z], self.db.datasetcfg.supercube_size)
    
    for z in range(znumcubes):
      for y in range(ynumcubes):
        for x in range(xnumcubes):
          zidx = XYZMorton(map(add, start, [x,y,z]))

          # Parameters in the cube slab
          index = map(mul, cubedim, [x,y,z])
          end = map(add, index, cubedim)

          cube_data = super_cube[:,index[2]:end[2], index[1]:end[1], index[0]:end[0]]
          zidx_list.append(zidx)
          # print "mini cube:", cube_data.shape
          cube_list.append(blosc.pack_array(cube_data))
    
    return zidx_list, [timestamp]*len(zidx_list), cube_list
    # for zidx, timestamp, cube_str in zip(zidx_list, [timestamp]*len(zidx_list), cube_list):
      # yield(zidx, timestamp, cube_str)
  
  def generateKeys(self, ch, timestamp_list, zidx_list, resolution, neariso=False):
    """Generate a key for Redis cache"""
    
    key_list = []
    for timestamp, zidx in itertools.product(timestamp_list, zidx_list):
      if neariso:
        key_list.append( '{}&{}&{}&{}&{}&neariso'.format(self.db.proj.project_name, ch.channel_name, resolution, zidx, timestamp) )
      else:
        key_list.append( '{}&{}&{}&{}&{}'.format(self.db.proj.project_name, ch.channel_name, resolution, zidx, timestamp) )
    
    return key_list
    

  @ReaderLock
  def getCube(self, ch, timestamp, zidx, resolution, update=False, neariso=False, direct=False):
    """Retrieve a single cube from the database"""
    
    if direct:
      return self.s3io.getCube(ch, timestamp, zidx, resolution, update=update, neariso=neariso)
    else:
      return self.getCacheCube(ch, timestamp, zidx, resolution, update=update, neariso=neariso)


  def getCacheCube(self, ch, timestamp, zidx, resolution, update=False, neariso=False):
    """Retrieve a single cube from the cache"""
    
    # list of id to fetch which do not exist in cache
    ids_to_fetch = self.kvindex.getCubeIndex(ch, [timestamp], [zidx], resolution, neariso=neariso)
    listofsuperidxs = self.generateSuperZindexes(ids_to_fetch, resolution)
    # fetch the supercuboid from s3
    super_cuboid = self.s3io.getCube(ch, timestamp, listofsuperidxs[0], resolution, update=update, neariso=neariso)
    if super_cuboid:
      for listofidxs, listoftimestamps, listofcubes in self.breakCubes(timestamp, zidx, resolution, super_cuboid):
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
    """Retrieve multiple cubes from the database"""
    if direct:
      return self.s3io.getCubes(ch, listoftimestamps, listofidxs, resolution, neariso=neariso)
    else:
      return self.getCacheCubes(ch, listoftimestamps, listofidxs, resolution, neariso=neariso)
  

  def getCacheCubes(self, ch, listoftimestamps, listofidxs, resolution, neariso=False):
    """Retrieve multiple cubes from the cache"""
    try:
      ids_to_fetch = self.kvindex.getCubeIndex(ch, listoftimestamps, listofidxs, resolution, neariso=neariso)
      super_listofidxs = self.generateSuperZindexes(ids_to_fetch, resolution)
      super_cuboids = self.s3io.getCubes(ch, listoftimestamps, super_listofidxs, resolution, neariso=neariso)
      if super_cuboids:
        for super_zidx, time_index, super_cuboid in super_cuboids:
          superlistofidxs, superlistoftimestamps, superlistofcubes = self.breakCubes(time_index, super_zidx, resolution, super_cuboid)
          # call putCubes and update index in the table before returning data
          self.putCacheCubes(ch, superlistoftimestamps, superlistofidxs, resolution, superlistofcubes, update=True, neariso=neariso)
      
      # fetch all cubes from redis
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
      self.putCacheCube(ch, timestamp, zidx, resolution, cubestr, update=update, neariso=neariso)

  def putCacheCube(self, ch, timestamp, zidx, resolution, cube_str, update=False, neariso=False):
    """Store a single cube in the cache"""
    # generating the key
    key_list = self.generateKeys(ch, [timestamp], [zidx], resolution, neariso=neariso)
  
    try:
      self.client.mset( dict(zip(key_list, [cube_str])) )
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
    """Store multiple cubes in the cache"""
    
    import blosc
    print "inserting cube of shape: {}, res: {}".format(blosc.unpack_array(listofcubes[0]).shape, resolution)
    # generating the list of keys
    key_list = self.generateKeys(ch, listoftimestamps, listofidxs, resolution, neariso)
    
    try:
      self.client.mset( dict(zip(key_list, listofcubes)) )
    except Exception, e:
      logger.error("Error inserting cubes into the database. {}".format(e))
      raise SpatialDBError("Error inserting cubes into the database. {}".format(e))
