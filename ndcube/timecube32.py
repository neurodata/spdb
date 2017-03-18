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
from timecube import TimeCube
from spatialdberror import SpatialDBError
import logging
logger=logging.getLogger("neurodata")


class TimeCube32(TimeCube):

  def __init__(self, cube_size=[128,128,16], time_range=[0,1]):
    """Create empty array of cubesize"""

    # call the base class constructor
    super(TimeCube32, self).__init__(cube_size, time_range)
    # note that this is self.cubesize (which is transposed) in Cube
    self.data = np.zeros ([self.time_range[1]-self.time_range[0]]+self.cubesize, dtype=np.uint32)

  def zeros(self):
    """Create a cube of all zeros"""
    super(TimeCube32, self).zeros()
    self.data = np.zeros([self.time_range[1]-self.time_range[0]]+self.cubesize, np.uint32)

  def xyImage ( self ):
    """Create the specified slice (index)"""

    zdim,ydim,xdim = self.data.shape
    return Image.fromarray( self.data[0,:,:], "RGBA")

  def xzImage ( self, zscale ):
    """Create the specified slice (index)"""

    zdim,ydim,xdim = self.data.shape
    outimage = Image.fromarray( self.data[:,0,:], "RGBA")
    return outimage.resize ( [xdim, int(zdim*zscale)] )

  def yzImage ( self, zscale ):
    """Create the specified slice (index)"""

    zdim,ydim,xdim = self.data.shape
    outimage = Image.fromarray( self.data[:,:,0], "RGBA")
    return outimage.resize ( [ydim, int(zdim*zscale)] )

  def RGBAChannel ( self ):
  """Convert the uint32 back into 4x8 bit channels"""

    zdim, ydim, xdim = self.data.shape
    newcube = np.zeros( (4, zdim, ydim, xdim), dtype=np.uint8 )
    newcube[0,:,:,:] = np.bitwise_and(self.data, 0xff, dtype=np.uint8)
    newcube[1,:,:,:] = np.uint8 ( np.right_shift( self.data, 8) & 0xff )
    newcube[2,:,:,:] = np.uint8 ( np.right_shift( self.data, 16) & 0xff )
    newcube[3,:,:,:] = np.uint8 ( np.right_shift (self.data, 24) )
    self.data = newcube
