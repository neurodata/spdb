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

"""
.. module:: maskcube
    :synopsis: Manipulate the in-memory data representation of the 3-d cube of data includes loading, export, read and write routines.
    All the interfaces to this class are in x,y,z order. Cube data goes in z,y,x order this is compatible with images and more efficient?
"""

class MaskCube(Cube):

  # Constructor 
  def __init__(self, cube_size=[64,64,64]):
    """Create empty array of cubesize"""

    # call the base class constructor
    super(MaskCube, self).__init__(cube_size)
    # note that this is self.cubesize (which is transposed) in Cube
    self.data = np.zeros ( self.cubesize, dtype=np.uint8 )

  def _pack (self, unpacked ):
    """Pack an array into a bitmap"""
    pass

  def _unpack ( self ):
    """Unpack a bitmap into an array"""
    pass
    #return unpacked

  def xySlice ( self, fileobj ):
  """Create the specified slice (index) at filename"""

    zdim,ydim,xdim = self.data.shape
    outimage = Image.frombuffer ( 'L', (xdim,ydim), self.data[0,:,:].flatten(), 'raw', 'L', 0, 1 ) 
    outimage.save ( fileobj, "PNG" )

  def xzSlice ( self, zscale, fileobj  ):
  """Create the specified slice (index) at filename"""

    zdim,ydim,xdim = self.data.shape
    outimage = Image.frombuffer ( 'L', (xdim,zdim), self.data[:,0,:].flatten(), 'raw', 'L', 0, 1 ) 
    #if the image scales to 0 pixels it don't work
    newimage = outimage.resize ( [xdim, int(zdim*zscale)] )
    newimage.save ( fileobj, "PNG" )

  def yzSlice ( self, zscale, fileobj  ):
    """Create the specified slice (index) at filename"""

    zdim,ydim,xdim = self.data.shape
    outimage = Image.frombuffer ( 'L', (ydim,zdim), self.data[:,:,0].flatten(), 'raw', 'L', 0, 1 ) 
    #if the image scales to 0 pixels it don't work
    newimage = outimage.resize ( [ydim, int(zdim*zscale)] )
    newimage.save ( fileobj, "PNG" )

  # load the object from a Numpy pickle
  def fromNPZ ( self, pandz ):
    """Load the cube from a pickled and zipped blob"""

    super(MaskCube, self).fromNPZ(pandz)
    self.data = np.unpackbits ( self.data )

  # return a numpy pickle to be stored in the database
  def toNPZ ( self ):
    """Pickle and zip the object"""

    self.data = np.packbits ( self.data )
    super(MaskCube, self).toNPZ()
