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

  def zeros(self):
    """Create a cube of all zeros"""
    super(TimeCube16, self).zeros()
    self.data = np.zeros([self.time_range[1]-self.time_range[0]]+self.cubesize, np.uint16)

  def frombuffer(self, slice_data):
    """Convery an array into Image"""
    
    from PIL import Image
    return Image.frombuffer('I;16', (slice_data.shape), slice_data.flatten(), 'raw', 'I;16', 0, 1)

# RBTODO images for 16 bit
#  def xyImage ( self, window=None ):
#    """Create xy slice"""
#
#    if window=None:
#      raise Exception("No window provided. Can't create image")
#    if len(self.data.shape) == 3:
#      zdim, ydim, xdim = self.data.shape
#      return Image.frombuffer ( 'L', (xdim,ydim), self.data[0,:,:].flatten(), 'raw', 'L', 0, 1 ) 
#    else:
#      zdim,ydim,xdim = self.data.shape[1:]
#      return Image.frombuffer ( 'L', (xdim,ydim), self.data[0,0,:,:].flatten(), 'raw', 'L', 0, 1 ) 
#
