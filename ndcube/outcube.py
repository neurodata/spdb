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
from ndlib.ndtype import ANNOTATION_CHANNELS, TIMESERIES_CHANNELS, DTYPE_uint8, DTYPE_uint16, DTYPE_uint32, DTYPE_uint64, DTYPE_float32
from spdb.spatialdberror import SpatialDBError
import logging
logger = logging.getLogger("neurodata")

"""
.. module:: OutCube
    :synopsis: the 4-d time+space output cube.
"""

# RBTODO maybe an outcube3d for backward compatibility.

class OutCube():

  def __init__(self, cube_size, timerange, datatype):
    """Create empty array of cubesize. Express cube_size in [t,x,y,z]"""

    self.timeoffset = timerange[0]
    self.zdim, self.ydim, self.xdim = self.cubesize = cube_size[::-1]
    self.data = np.zeros ( [timerange[1]+1-timerange[0], self.zdim, self.ydim, self.xdim], dtype=datatype )


  def addData ( self, other, index, timestamp ):
    """Add data to a larger cube from a smaller cube"""
    
    xoffset = index[0]*other.xdim
    yoffset = index[1]*other.ydim
    zoffset = index[2]*other.zdim

    self.data [ timestamp,\
                zoffset:zoffset+other.zdim,\
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
    self.data = self.data[:,zoffset:zoffset+zsize, yoffset:yoffset+ysize, xoffset:xoffset+xsize]

