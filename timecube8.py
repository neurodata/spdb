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
from PIL import Image

from cube import Cube

from spatialdberror import SpatialDBError
import logging
logger=logging.getLogger("neurodata")


class TimeCube8(Cube):

  def __init__(self, cubesize=[128,128,16], timerange=[0,0]):
    """Create empty array of cubesize"""

    # call the base class constructor
    Cube.__init__(self,cubesize)
    # note that this is self.cubesize (which is transposed) in Cube
    self.timerange = timerange
    self.data = np.zeros ([self.timerange[1]-self.timerange[0]]+self.cubesize, dtype=np.uint8)

    # variable that describes when a cube is created from zeros rather than loaded from another source
    self._newcube = False

  def addData(self, other, index, time):
    """Add data to a larger cube from a smaller cube"""

    xoffset = index[0]*other.xdim
    yoffset = index[1]*other.ydim     
    zoffset = index[2]*other.zdim
    
    self.data [ time-self.timerange[0], zoffset:zoffset+other.zdim, yoffset:yoffset+other.ydim, xoffset:xoffset+other.xdim]\
        = other.data [:,:,:]

  def trim(self, xoffset, xsize, yoffset, ysize, zoffset, zsize):
    """Trim off the excess data"""
    self.data = self.data[:, zoffset:zoffset+zsize, yoffset:yoffset+ysize, xoffset:xoffset+xsize]

  def zeros(self):
    """Create a cube of all zeros"""
    self._newcube = True
    self.data = np.zeros(self.cubesize, np.uint8)

  def xyImage ( self ):
    """Create xy slice"""
    zdim,ydim,xdim = self.data.shape[1:]
    return Image.frombuffer ( 'L', (xdim,ydim), self.data[0,0,:,:].flatten(), 'raw', 'L', 0, 1 ) 

  def xzImage ( self, zscale ):
    """Create xz slice"""
    zdim,ydim,xdim = self.data.shape[1:]
    outimage = Image.frombuffer ( 'L', (xdim,zdim), self.data[0,:,0,:].flatten(), 'raw', 'L', 0, 1 ) 
    #if the image scales to 0 pixels it don't work
    return outimage.resize ( [xdim, int(zdim*zscale)] )

  def yzImage ( self, zscale ):
    """Create yz slice"""
    zdim,ydim,xdim = self.data.shape[1:]
    outimage = Image.frombuffer ( 'L', (ydim,zdim), self.data[0,:,:,0].flatten(), 'raw', 'L', 0, 1 ) 
    #if the image scales to 0 pixels it don't work
    return outimage.resize ( [ydim, int(zdim*zscale)] )
