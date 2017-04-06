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

import boto3
import botocore
import blosc
import itertools
from ndlib.ndctypelib import *
from ndingest.nddynamo.cuboidindexdb import CuboidIndexDB
from ndingest.ndbucket.cuboidbucket import CuboidBucket
from spdb.spatialdberror import SpatialDBError
import logging
logger=logging.getLogger("neurodata")


class S3IO:

  def __init__(self, db):
    """Connect to the S3 backend"""
    
    try:
      self.db = db
      self.project_name = self.db.proj.project_name
      self.cuboidindex_db = CuboidIndexDB(self.project_name)
      self.cuboid_bucket = CuboidBucket(self.project_name)
    except Exception, e:
      logger.error("Cannot connect to S3 backend")
      raise SpatialDBError("Cannot connect to S3 backend")
  
  def __del__(self):
    """Close the connection to the S3 backend"""
    pass
   
  
  def supercube_compatibility(self, super_cube):
    
    super_cube = blosc.unpack_array(super_cube)
    # print "Supercube compatibility {}".format(super_cube.shape)
    if len(super_cube.shape) == 3:
      return blosc.pack_array(super_cube.reshape((1,) + super_cube.shape))
    else:
      return blosc.pack_array(super_cube)
  
  def getCube(self, ch, timestamp, super_zidx, resolution, update=False, neariso=False):
    """Retrieve a cube from the database by token, resolution, and zidx"""
    
    try:
      super_cube = self.cuboid_bucket.getObject(ch.channel_name, resolution, super_zidx, timestamp, neariso=neariso)
      super_cube = self.supercube_compatibility(super_cube)
      return zidx, timestamp, super_cube
    except botocore.exceptions.DataNotFoundError as e:
      logger.error("Cannot find s3 object for zindex {}. {}".format(super_zidx, e))
      raise SpatialDBError("Cannot find s3 object for zindex {}. {}".format(super_zidx, e))
    except botocore.exceptions.ClientError as e:
      if e.response['Error']['Code'] == 'NoSuckKey':
        return None
    

  def getCubes(self, ch, listoftimestamps, superlistofidxs, resolution, neariso=False):
    """Retrieve multiple cubes from the database"""
    
    for (time_index, super_zidx) in itertools.product(listoftimestamps, superlistofidxs):
      try:
        super_cube = self.cuboid_bucket.getObject(ch.channel_name, resolution, super_zidx, time_index, neariso=neariso)
        super_cube = self.supercube_compatibility(super_cube)
        yield ( super_zidx, time_index, super_cube)
        # for item in self.breakCubes(time_index, super_zidx, resolution, blosc.unpack_array(super_cube)):
          # yield(item)
      except botocore.exceptions.DataNotFoundError as e:
        logger.error("Cannot find the s3 object for zindex {}. {}".format(super_zidx, e))
        raise SpatialDBError("Cannot find the s3 object for zindex {}. {}".format(super_zidx, e))
      except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'NoSuckKey':
          continue
        if e.response['Error']['Code'] == 'NoSuchBucket':
          pass
 
  def putCubes ( self, ch, listoftimestamps, listofidxs, resolution, listofcubes, update=False, neariso=False):
    """Store multiple cubes into the database"""
    # KL TODO This should be replaced by Blaze
    # KL TODO Remember to convert listofcubes into listofsupercubes, listofidxs into listofsuperidxs
    # basically an exact opposite of breakCubes() like combineCubes()
    return NotImplemented
  
  def putCube ( self, ch, timestamp, zidx, resolution, cubestr, update=False, neariso=False):
    """Store a cube from the annotation database"""
    
    # super_zidx = self.generateSuperZindex(zidx, resolution)
    super_zidx = zidx
    print "insert S3 cube shape:", blosc.unpack_array(cubestr).shape
    [x, y, z] = MortonXYZ(super_zidx)
    self.cuboidindex_db.putItem(ch.channel_name, resolution, x, y, z, timestamp, neariso=neariso)
    return self.cuboid_bucket.putObject(ch.channel_name, resolution, super_zidx, timestamp, cubestr, neariso=neariso)
