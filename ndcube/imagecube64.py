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


class ImageCube64(Cube):

  def __init__(self, cube_size=[128,128,16]):
    """Create empty array of cubesize"""

    # call the base class constructor
    super(ImageCube64, self).__init__(cube_size)
    # note that this is self.cubesize (which is transposed) in Cube
    self.data = np.zeros ( self.cubesize, dtype=np.uint64 )

  def zeros ( self ):
    """Create a cube of all 0"""
    super(ImageCube64, self).zeros()
    self.data = np.zeros ( self.cubesize, dtype=np.uint64 )

  def xyImage ( self ):
    """Create xy slice"""

    self.extractChannel()
    return Image.fromarray( self.data, "RGBA")

  def xzImage ( self, zscale ):
    """Create xz slice"""

    zdim,ydim,xdim = self.data.shape
    self.extractChannel()
    outimage = Image.fromarray( self.data, "RGBA")
    return outimage.resize ( [xdim, int(zdim*zscale)] )

  def yzImage ( self, zscale ):
    """Create yz slice"""

    zdim,ydim,xdim = self.data.shape
    self.extractChannel()
    outimage = Image.fromarray( self.data, "RGBA")
    return outimage.resize ( [ydim, int(zdim*zscale)] )
  
  def extractChannel ( self ):
    """Convert the uint32 back into 4x8 bit channels"""

    zdim, ydim, xdim = self.data.shape
    newcube = np.zeros( (ydim, xdim, 4), dtype=np.uint8 )
    newcube[:,:,0] = np.bitwise_and(self.data, 0xffff, dtype=np.uint8)
    newcube[:,:,1] = np.uint8 ( np.right_shift( self.data, 16) & 0xffff )
    newcube[:,:,2] = np.uint8 ( np.right_shift( self.data, 32) & 0xffff )
    newcube[:,:,3] = np.uint8 ( np.right_shift (self.data, 48) )
    self.data = newcube

  def RGBAChannel ( self ):
    """Convert the uint32 back into 4x8 bit channels"""

    zdim, ydim, xdim = self.data.shape
    newcube = np.zeros( (4, zdim, ydim, xdim), dtype=np.uint16 )
    newcube[0,:,:,:] = np.bitwise_and(self.data, 0xffff, dtype=np.uint16)
    newcube[1,:,:,:] = np.uint16 ( np.right_shift( self.data, 16) & 0xffff )
    newcube[2,:,:,:] = np.uint16 ( np.right_shift( self.data, 32) & 0xffff )
    newcube[3,:,:,:] = np.uint16 ( np.right_shift (self.data, 48) )
    self.data = newcube
