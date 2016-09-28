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
from cube import Cube


class TimeCube(Cube):

  def __init__(self, cube_size, time_range):
    """Create empty array of cubesize. Express cube_size in [x,y,z]"""

    # cubesize is in z,y,x for interactions with tile/image data
    self.zdim, self.ydim, self.xdim = self.cubesize = [cube_size[2], cube_size[1], cube_size[0]]
    self.time_range = time_range
    # RB this next line is not typed and produces floats.  Cube needs to be created in the derived classes
    # self.data = np.empty ( self.cubesize )
    self._newcube = False
  
  # @override(Cube)
  def addData(self, other, index, time):
    """Add data to a larger cube from a smaller cube"""

    xoffset = index[0]*other.xdim
    yoffset = index[1]*other.ydim     
    zoffset = index[2]*other.zdim
    
    self.data [ time-self.time_range[0], zoffset:zoffset+other.zdim, yoffset:yoffset+other.ydim, xoffset:xoffset+other.xdim]\
        = other.data [:,:,:]
  
  # @override(Cube)
  def trim(self, xoffset, xsize, yoffset, ysize, zoffset, zsize):
    """Trim off the excess data"""
    self.data = self.data[:, zoffset:zoffset+zsize, yoffset:yoffset+ysize, xoffset:xoffset+xsize]

  def zeros(self):
    """Create a cube of all zeros"""
    super(TimeCube, self).zeros()
    self.data = np.zeros([self.time_range[1]-self.time_range[0]]+self.cubesize, np.uint8)
  