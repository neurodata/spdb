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

from .timecube import TimeCube

import logging
logger=logging.getLogger("neurodata")


class TimeCubeFloat32(TimeCube):

  # Constructor 
  def __init__(self, cube_size=[128,128,16], time_range=[0,1]):
    """Create empty array of cubesize"""

    # call the base class constructor
    super(TimeCubeFloat32, self).__init__(cube_size, time_range)
    self.data = np.zeros ([self.time_range[1]-self.time_range[0]]+self.cubesize, dtype=np.float32)

  def zeros(self):
    """Create a cube of all 0"""

    super(TimeCubeFloat32, self).zeros()
    self.data = np.zeros([self.time_range[1]-self.time_range[0]]+self.cubesize, np.float32)


  def xyImage(self):
    """Create xy slice"""
    zdim,ydim,xdim = self.data.shape[1:]

    # translate the 0-1 map down to to 256 value
    imgdata = np.uint8(self.data*256)

    # convert the data into a red heatmap
    rgbdata = np.zeros ( [ydim,xdim,3], dtype=np.uint8 )
    rgbdata[:,:,0] = imgdata[0,:,:]

    return Image.frombuffer('RGB', (xdim,ydim), rgbdata, 'raw', 'RGB', 0, 1)

  def xzImage(self, zscale):
    """Create xz slice"""
    zdim,ydim,xdim = self.data.shape[1:]

    # translate the 0-1 map down to to 256 value
    imgdata = np.uint8(self.data*256)

    # convert the data into a red heatmap
    rgbdata = np.zeros ( [zdim,xdim,3], dtype=np.uint8 )
    rgbdata[:,:,0] = imgdata[:,0,:]

    return Image.frombuffer('RGB', (xdim,zdim), rgbdata, 'raw', 'RGB', 0, 1).resize([xdim, int(zdim*zscale)])

  def yzImage(self, zscale):
    """Create yz slice"""
    zdim,ydim,xdim = self.data.shape[1:]

    # translate the 0-1 map down to to 256 value
    imgdata = np.uint8(self.data*256)

    # convert the data into a red heatmap
    rgbdata = np.zeros ( [zdim,ydim,3], dtype=np.uint8 )
    rgbdata[:,:,0] = imgdata[:,:,0]

    return Image.frombuffer('RGB', (ydim,zdim), rgbdata, 'raw', 'RGB', 0, 1).resize([ydim, int(zdim*zscale)])
