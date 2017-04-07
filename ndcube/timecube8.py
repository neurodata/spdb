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
from timecube import TimeCube


class TimeCube8(TimeCube):

  def __init__(self, cube_size=[128,128,16], time_range=[0,1]):
    """Create empty array of cubesize"""

    # call the base class constructor
    super(TimeCube8, self).__init__(cube_size, time_range)
    # note that this is self.cubesize (which is transposed) in Cube
    self.data = np.zeros ([self.time_range[1]-self.time_range[0]]+self.cubesize, dtype=np.uint8)

  def zeros(self):
    """Create a cube of all zeros"""
    super(TimeCube8, self).zeros()
    self.data = np.zeros([self.time_range[1]-self.time_range[0]]+self.cubesize, np.uint8)

  def xyImage ( self, window=None ):
    """Create xy slice"""
    if len(self.data.shape) == 3:
      zdim, ydim, xdim = self.data.shape
      return Image.frombuffer ( 'L', (xdim,ydim), self.data[0,:,:].flatten(), 'raw', 'L', 0, 1 ) 
    else:
      zdim,ydim,xdim = self.data.shape[1:]
      return Image.frombuffer ( 'L', (xdim,ydim), self.data[0,0,:,:].flatten(), 'raw', 'L', 0, 1 ) 

  def xzImage ( self, zscale ):
    """Create xz slice"""

    if len(self.data.shape) == 3:
      zdim, ydim, xdim = self.data.shape
      outimage = Image.frombuffer ( 'L', (xdim,zdim), self.data[:,0,:].flatten(), 'raw', 'L', 0, 1 ) 
      # if the image scales to 0 pixels it don't work
      return outimage.resize ( [xdim, int(zdim*zscale)] )
    else:
      zdim,ydim,xdim = self.data.shape[1:]
      outimage = Image.frombuffer ( 'L', (xdim,zdim), self.data[0,:,0,:].flatten(), 'raw', 'L', 0, 1 ) 
      #if the image scales to 0 pixels it don't work
      return outimage.resize ( [xdim, int(zdim*zscale)] )

  def yzImage ( self, zscale ):
    """Create yz slice"""

    if len(self.data.shape) == 3:
      zdim, ydim, xdim = self.data.shape
      outimage = Image.frombuffer ( 'L', (ydim,zdim), self.data[:,:,0].flatten(), 'raw', 'L', 0, 1 ) 
      # if the image scales to 0 pixels it don't work
      return outimage.resize ( [ydim, int(zdim*zscale)] )
    else:
      zdim,ydim,xdim = self.data.shape[1:]
      outimage = Image.frombuffer ( 'L', (ydim,zdim), self.data[0,:,:,0].flatten(), 'raw', 'L', 0, 1 ) 
      #if the image scales to 0 pixels it don't work
      return outimage.resize ( [ydim, int(zdim*zscale)] )
  
  def frombuffer(self, slice_data):
    """Convery an array into Image"""
    
    from PIL import Image
    return Image.frombuffer('L', (slice_data.shape), slice_data.flatten(), 'raw', 'L', 0, 1)
