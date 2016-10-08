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
logger = logging.getLogger("neurodata")


class ImageCubeFloat32(Cube):

  # Constructor 
  def __init__(self, cubesize=[128,128,16]):
    """Create empty array of cubesize"""

    # call the base class constructor
    Cube.__init__(self,cubesize)
    # note that this is self.cubesize (which is transposed) in Cube
    self.data = np.zeros ( self.cubesize, dtype=np.float32 )

  # was the cube created from zeros?
  def fromZeros ( self ):
    """Determine if the cube was created from all zeros?"""
    if self._newcube == True:
      return True
    else: 
      return False

  # create an all zeros cube
  def zeros ( self ):
    """Create a cube of all 0"""
    self._newcube = True
    self.data = np.zeros ( self.cubesize, dtype=np.float32 )

  def overwrite ( self, annodata ):
    """Get's a dense voxel region and overwrites all non-zero values"""

    vector_func = np.vectorize ( lambda a,b: b if b!=0.0 else a ) 
    self.data = vector_func ( self.data, annodata ) 

  
  def xyImage ( self ):
    """Create xy slice"""
    zdim,ydim,xdim = self.data.shape

    # translate the 0-1 map down to to 256 value
    imgdata = np.uint8(self.data*256)

    # convert the data into a red heatmap
    rgbdata = np.zeros ( [ydim,xdim,3], dtype=np.uint8 )
    rgbdata[:,:,0] = imgdata[0,:,:]

    return Image.frombuffer ( 'RGB', (xdim,ydim), rgbdata, 'raw', 'RGB', 0, 1 )


  def xzImage ( self, zscale  ):
    """Create xz slice"""
    zdim,ydim,xdim = self.data.shape

    # translate the 0-1 map down to to 256 value
    imgdata = np.uint8(self.data*256)

    # convert the data into a red heatmap
    rgbdata = np.zeros ( [zdim,xdim,3], dtype=np.uint8 )
    rgbdata[:,:,0] = imgdata[:,0,:]

    return Image.frombuffer ( 'RGB', (xdim,zdim), rgbdata, 'raw', 'RGB', 0, 1 ).resize([xdim, int(zdim*zscale)])

  def yzImage ( self, zscale ):
    """Create yz slice"""
    zdim,ydim,xdim = self.data.shape

    # translate the 0-1 map down to to 256 value
    imgdata = np.uint8(self.data*256)

    # convert the data into a red heatmap
    rgbdata = np.zeros ( [zdim,ydim,3], dtype=np.uint8 )
    rgbdata[:,:,0] = imgdata[:,:,0]

    return Image.frombuffer ( 'RGB', (ydim,zdim), rgbdata, 'raw', 'RGB', 0, 1 ).resize([ydim, int(zdim*zscale)])
