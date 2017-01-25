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

import numpy as np
import zlib
import cStringIO
import blosc
from abc import abstractmethod
from ndlib.ndctypelib import  overwriteDense_ctype
from ndlib.ndtype import ANNOTATION_CHANNELS, TIMESERIES_CHANNELS, DTYPE_uint8, DTYPE_uint16, DTYPE_uint32, DTYPE_uint64, DTYPE_float32, DTYPE_int8, DTYPE_int16, DTYPE_int32 
from spdb.spatialdberror import SpatialDBError
import logging
logger = logging.getLogger("neurodata")

"""
.. module:: Cube
    :synopsis: Manipulate the in-memory data representation of the 3-d cube of data that contains annotations.  
"""

class Cube(object):

  def __init__(self, cube_size):
    """Create empty array of cubesize. Express cube_size in [x,y,z]"""

    # cubesize is in z,y,x for interactions with tile/image data
    self.zdim, self.ydim, self.xdim = self.cubesize = cube_size[::-1]
    # self.data = np.empty ( self.cubesize )
    self._newcube = False
  
  def fromZeros ( self ):
    """Determine if the cube was created from all zeros?"""
    if self._newcube == True:
      return True
    else:
      return False
  
  def zeros(self):
    """Create a cube of all zeros"""
    self._newcube = True

  def addData ( self, other, index ):
    """Add data to a larger cube from a smaller cube"""
    
    xoffset = index[0]*other.xdim
    yoffset = index[1]*other.ydim
    zoffset = index[2]*other.zdim

    self.data [ zoffset:zoffset+other.zdim,\
                yoffset:yoffset+other.ydim,\
                xoffset:xoffset+other.xdim]\
            = other.data [:,:,:]

  def addData_new ( self, other, index ):
    """Add data to a larger cube from a smaller cube"""

    xoffset = index[0]*other.xdim
    yoffset = index[1]*other.ydim
    zoffset = index[2]*other.zdim

    np.copyto ( self.data[zoffset:zoffset+other.zdim,yoffset:yoffset+other.ydim,xoffset:xoffset+other.xdim],other.data [:,:,:] )
  
  def trim ( self, xoffset, xsize, yoffset, ysize, zoffset, zsize ):
    """Trim off the excess data"""
    self.data = self.data[zoffset:zoffset+zsize, yoffset:yoffset+ysize, xoffset:xoffset+xsize]

  def fromNPZ ( self, compressed_data ):
    """Load the cube from a pickled and zipped blob"""
    
    try:
      self.data = np.load ( cStringIO.StringIO ( zlib.decompress ( compressed_data[:] ) ) )
    except:
      logger.error("Failed to decompress database cube. Data integrity concern.")
      raise SpatialDBError("Failed to decompress database cube. Data integrity concern.")

    self._newcube = False


  def toNPZ ( self ):
    """Pickle and zip the object"""
    try:
      # Create the compressed cube
      fileobj = cStringIO.StringIO()
      np.save (fileobj, self.data)
      return  zlib.compress (fileobj.getvalue())
    except:
      logger.error("Failed to compress database cube. Data integrity concern.")
      raise SpatialDBError("Failed to compress database cube. Data integrity concern.")
 
  def toBlosc ( self ):
    """Pack the object"""
    try:
      # Create the compressed cube
      return blosc.pack_array(self.data) 
    except:
      logger.error("Failed to compress database cube. Data integrity concern.")
      raise SpatialDBError("Failed to compress database cube. Data integrity concern.")
  
  def fromBlosc(self, compressed_data):
    """Load the cube from a pickled and zipped blob"""
    
    try:
      self.data = blosc.unpack_array(compressed_data[:])
      self.zdim, self.ydim, self.xdim = self.data.shape
    except:
      logger.error("Failed to decompress database cube. Data integrity concern.")
      raise SpatialDBError("Failed to decompress database cube. Data integrity concern.")

    self._newcube = False
  
  def overwrite(self, write_data):
    """Get's a dense voxel region and overwrites all non-zero values"""

    if (self.data.dtype != write_data.dtype ):
      logger.error("Conflicting data types for overwrite")
      raise SpatialDBError ("Conflicting data types for overwrite")

    self.data = overwriteDense_ctype(self.data, write_data)
  
  def isNotZeros(self):
    """Check if the cube has any data"""
    return np.any(self.data)
  
  @abstractmethod
  def RGBAChannel(self):
    """Return a RGBAChannel Method definition"""
    return NotImplemented

  @abstractmethod
  def xyImage(self):
    """Create a xy slice"""
    return NotImplemented

  @abstractmethod
  def yzImage(self, zscale):
    """Create a yz slice"""
    return NotImplemented

  @abstractmethod
  def xzImage(self, zscale):
    """Create a xz slice"""
    return NotImplemented

  # factory method for cube
  @staticmethod
  def CubeFactory(cubedim, channel_type, datatype, timerange=[0,1]):
    
    if channel_type in ANNOTATION_CHANNELS and datatype in DTYPE_uint32:
      from anncube32 import AnnotateCube32
      return AnnotateCube32(cubedim)
    elif channel_type in TIMESERIES_CHANNELS:
      if datatype in DTYPE_uint8:
        from timecube8 import TimeCube8
        return TimeCube8(cubedim, timerange)
      elif datatype in DTYPE_uint16:
        from timecube16 import TimeCube16 
        return TimeCube16(cubedim, timerange)
      elif datatype in DTYPE_uint32:
        from timecube32 import TimeCube32 
        return TimeCube32(cubedim, timerange)
      elif datatype in DTYPE_int8:
        from timecubeI8 import TimeCubeI8
        return TimeCubeI8(cubedim, timerange)
      elif datatype in DTYPE_int16:
        from timecubeI16 import TimeCubeI16 
        return TimeCubeI16(cubedim, timerange)
      elif datatype in DTYPE_int32:
        from timecubeI32 import TimeCubeI32 
        return TimeCubeI32(cubedim, timerange)
      elif datatype in DTYPE_float32:
        from timecubefloat32 import TimeCubeFloat32
        return TimeCubeFloat32(cubedim, timerange)
    else:
      logger.error("Could not find a cube type for this channel.  Bad channel type?")
      raise SpatialDBError("Could not find a cube type for this channel.  Bad channel type?")
