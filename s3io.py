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
import hashlib
from sets import Set
from operator import add, sub, mul, div, mod

from django.conf import settings
import ndlib
from s3util import generateS3BucketName, generateS3Key

from spatialdberror import SpatialDBError
import logging
logger=logging.getLogger("neurodata")


class S3IO:

  def __init__ ( self, db ):
    """Connect to the S3 backend"""
    
    try:
      self.db = db
      self.client = boto3.client('s3', aws_access_key_id=settings.AWS_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)
      self.project_name = self.db.proj.getProjectName()
    except Exception, e:
      logger.error("Cannot connect to S3 backend")
      raise SpatialDBError("Cannot connect to S3 backend")
  
  def __del__(self):
    """Close the connection to the S3 backend"""
    pass
   
  
  def generateSuperZindex(self, zidx, resolution):
    """Generate super zindex from a given zindex"""
    
    [[ximagesz, yimagesz, zimagesz], timerange] = self.db.proj.datasetcfg.imageSize(resolution)
    [xcubedim, ycubedim, zcubedim] = cubedim = self.db.proj.datasetcfg.getCubeDims()[resolution]
    [xoffset, yoffset, zoffset] = self.db.proj.datasetcfg.getOffset()[resolution]
    [xsupercubedim, ysupercubedim, zsupercubedim] = super_cubedim = self.db.proj.datasetcfg.getSuperCubeDims()[resolution]
    
    # super_cubedim = map(mul, cubedim, SUPERCUBESIZE)
    [x, y, z] = ndlib.MortonXYZ(zidx)
    corner = map(mul, ndlib.MortonXYZ(zidx), cubedim)
    [x,y,z] = map(div, corner, super_cubedim)
    # print zidx, corner, [x,y,z], ndlib.XYZMorton([x,y,z])
    return ndlib.XYZMorton([x,y,z])

  def breakCubes(self, super_zidx, resolution, super_cube):
    """Breaking the supercube into cubes"""
    
    # Empty lists for zindx and cube data
    zidx_list = []
    cube_list = []
    
    # SuperCube Size
    [xnumcubes, ynumcubes, znumcubes] = self.db.datasetcfg.getSuperCubeSize()
    
    # Cube dimensions
    cubedim = self.db.datasetcfg.cubedim[resolution]
    [x,y,z] = ndlib.MortonXYZ(super_zidx)
    # start = map(mul, cubedim, [x,y,z])
    start = map(mul, [x,y,z], self.db.datasetcfg.getSuperCubeSize())
    
    for z in range(znumcubes):
      for y in range(ynumcubes):
        for x in range(xnumcubes):
          zidx = ndlib.XYZMorton(map(add, start, [x,y,z]))

          # Parameters in the cube slab
          index = map(mul, cubedim, [x,y,z])
          end = map(add, index, cubedim)

          cube_data = super_cube[index[2]:end[2], index[1]:end[1], index[0]:end[0]]
          zidx_list.append(zidx)
          cube_list.append(blosc.pack_array(cube_data))
    
    return zidx_list, cube_list
  
  def getCube(self, ch, zidx, timestamp, resolution, update=False):
    """Retrieve a cube from the database by token, resolution, and zidx"""
    
    super_zidx = self.generateSuperZindex(zidx, resolution)
    try:
      super_cube = self.client.get_object(Bucket=generateS3BucketName(), Key=generateS3Key(self.project_name, ch.getChannelName(), resolution, super_zidx)).get('Body').read()
      return self.breakCubes(zidx, resolution, blosc.unpack_array(super_cube))
    except botocore.exceptions.DataNotFoundError as e:
      logger.error("Cannot find s3 object for zindex {}. {}".format(super_zidx, e))
      raise SpatialDBError("Cannot find s3 object for zindex {}. {}".format(super_zidx, e))
    
  
  def getCubes(self, ch, listofidxs, resolution, neariso=False):
    """Retrieve multiple cubes from the database"""
    
    super_listofidxs = Set([])
    for zidx in listofidxs:
      super_listofidxs.add(self.generateSuperZindex(zidx, resolution))
   
    for super_zidx in super_listofidxs:
      try:
        super_cube = self.client.get_object(Bucket=generateS3BucketName(), Key=generateS3Key(self.project_name, ch.getChannelName(), resolution, super_zidx)).get('Body').read()
        yield ( self.breakCubes(super_zidx, resolution, blosc.unpack_array(super_cube)) )
      except botocore.exceptions.DataNotFoundError as e:
        logger.error("Cannot find the s3 object for zindex {}. {}".format(super_zidx, e))
        raise SpatialDBError("Cannot find the s3 object for zindex {}. {}".format(super_zidx, e))
      except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'NoSuckKey':
          continue
        if e.response['Error']['Code'] == 'NoSuchBucket':
          pass
  

  def getTimeCubes(self, ch, listofidxsidx, listoftimestamps, resolution):
    """Retrieve multiple cubes from the database"""
    return
 
  def putCubes ( self, ch, listofidxs, resolution, listofcubes, update=False):
    """Store multiple cubes into the database"""
    return
  
  def putCube ( self, ch, resolution, super_zidx, cubestr, timestamp=None, update=False ):
    """Store a cube from the annotation database"""
    
    # super_zidx = self.generateSuperZindex(zidx, resolution)
    try:
      response = self.client.put_object(Bucket=generateS3BucketName(), Key=generateS3Key(self.project_name, ch.getChannelName(), resolution, super_zidx), Body=cubestr)
    except botocore.exceptions.EndpointConnectionError as e:
      logger.error("Cannot write s3 object. {}".format(e))
      raise SpatialDBError("Cannot write s3 object. {}".format(e))
    
    return response
