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


class TimeCube16(TimeCube):

  def __init__(self, cube_size=[128,128,16], time_range=[0,1]):
    """Create empty array of cubesize"""

    # call the base class constructor
    super(TimeCube16, self).__init__(cube_size, time_range)
    # note that this is self.cubesize (which is transposed) in Cube
    self.data = np.zeros ([self.time_range[1]-self.time_range[0]]+self.cubesize, dtype=np.uint16)

  def zeros ( self ):
    """Create a cube of all 0"""
    super(TimeCube16, self).zeros()
    self.data = np.zeros ( self.cubesize, dtype=np.uint16 )

  def xyImage ( self ):
    """Create xy slice"""
    # This works for 16-> conversions
    zdim,ydim,xdim = self.data.shape[1:]
    if self.data.dtype == np.uint8:  
      return Image.frombuffer ( 'L', (xdim,ydim), self.data[0,0,:,:].flatten(), 'raw', 'L', 0, 1)
    else:
      outimage = Image.frombuffer ( 'I;16', (xdim,ydim), self.data[0,0,:,:].flatten(), 'raw', 'I;16', 0, 1)
      return outimage.point(lambda i:i*(1./256)).convert('L')

  def xzImage ( self, zscale ):
    """Create xz slice"""
    zdim,ydim,xdim = self.data.shape[1:]
    if self.data.dtype == np.uint8:  
      outimage = Image.frombuffer ( 'L', (xdim,zdim), self.data[0,:,0,:].flatten(), 'raw', 'L', 0, 1)
    else:
      outimage = Image.frombuffer ( 'I;16', (xdim,zdim), self.data[0,:,0,:].flatten(), 'raw', 'I;16', 0, 1)
      outimage = outimage.point(lambda i:i*(1./256)).convert('L')
    
    return  outimage.resize ( [xdim, int(zdim*zscale)] )

  def yzImage ( self, zscale ):
    """Create yz slice"""
    zdim,ydim,xdim = self.data.shape[1:]
    if self.data.dtype == np.uint8:  
      outimage = Image.frombuffer ( 'L', (ydim,zdim), self.data[0,:,:,0].flatten(), 'raw', 'L', 0, 1)
    else:
      outimage = Image.frombuffer ( 'I;16', (ydim,zdim), self.data[0,:,:,0].flatten(), 'raw', 'I;16', 0, 1)
      outimage = outimage.point(lambda i:i*(1./256)).convert('L')
    
    return outimage.resize ( [ydim, int(zdim*zscale)] )
