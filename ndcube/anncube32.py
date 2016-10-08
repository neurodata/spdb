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

from PIL import Image
from ndctypelib import *
from cube import Cube
from spatialdberror import SpatialDBError 
import logging
logger=logging.getLogger("neurodata")


class AnnotateCube32(Cube):
  """AnnotateCube: manipulate the in-memory data representation of the 3-d cube of data that contains annotations"""

  def __init__(self, cubesize):
    """Create empty array of cubesize[x,y,z]"""

    # call the base class constructor
    Cube.__init__( self, cubesize )
    self.data = np.zeros(self.cubesize, dtype=np.uint32)
    # variable that describes when a cube is created from zeros rather than loaded from another source
    self._newcube = False


  def getVoxel ( self, voxel ):
    """Return the value at the voxel specified as [x,y,z]"""
    return self.data [ voxel[2], voxel[1], voxel[0] ]


  def fromZeros ( self ):
    """Determine if the cube was created from all zeros?"""
    if self._newcube == True:
      return True
    else: 
      return False


  def zeros ( self ):
    """Create a cube of all 0"""
    self._newcube = True
    self.data = np.zeros ( self.cubesize, dtype=np.uint32 )


  # Add annotations
  #
  #  We are mostly going to assume that annotations are non-overlapping.  When they are,
  #  we are going to be less than perfectly efficient.
  #  
  #  Returns a list of exceptions  
  #
  #  Exceptions are uint8 to keep them small.  Max cube size is 256^3.
  #
  def annotate ( self, annid, offset, locations, conflictopt ):
    """Add annotation by a list of locations"""

    try:
      self.data, exceptions = annotate_ctype( self.data, annid, offset, np.array(locations, dtype=np.uint32), conflictopt )
      return exceptions
    except IndexError, e:
      raise SpatialDBError ("Voxel list includes out of bounds request.")


  def shave ( self, annid, offset, locations ):
    """Remove annotation by a list of locations"""

    self.data , exceptions, zeroed = shave_ctype ( self.data, annid, offset, np.array(locations, dtype=np.uint32))
    return exceptions, zeroed


  def xyImage ( self ):
    """Create the specified slice (index)"""

    zdim,ydim,xdim = self.data.shape
    imagemap = np.zeros ( [ ydim, xdim ], dtype=np.uint32 )

    # false color redrawing of the region
    imagemap = recolor_ctype ( self.data.reshape( (imagemap.shape[0], imagemap.shape[1]) ), imagemap )

    return Image.frombuffer ( 'RGBA', (xdim,ydim), imagemap, 'raw', 'RGBA', 0, 1 )


  def xzImage ( self, scale ):
    """Create the specified slice (index)"""

    zdim,ydim,xdim = self.data.shape
    imagemap = np.zeros ( [ zdim, xdim ], dtype=np.uint32 )

    # false color redrawing of the region
    imagemap = recolor_ctype ( self.data.reshape( (imagemap.shape[0], imagemap.shape[1]) ), imagemap )

    outimage = Image.frombuffer ( 'RGBA', (xdim,zdim), imagemap, 'raw', 'RGBA', 0, 1 )
    return outimage.resize ( [xdim, int(zdim*scale)] )


  def yzImage ( self, scale ):
    """Create the specified slice (index)"""

    zdim,ydim,xdim = self.data.shape
    imagemap = np.zeros ( [ zdim, ydim ], dtype=np.uint32 )

    # false color redrawing of the region
    imagemap = recolor_ctype ( self.data.reshape( (imagemap.shape[0], imagemap.shape[1]) ), imagemap )

    outimage = Image.frombuffer ( 'RGBA', (ydim,zdim), imagemap, 'raw', 'RGBA', 0, 1 )
    return  outimage.resize ( [ydim, int(zdim*scale)] )


  def preserve ( self, annodata ):
    """Get's a dense voxel region and overwrites all non-zero values"""
    self.data = exceptionDense_ctype ( self.data, annodata )

  def exception ( self, annodata ):
    """Get's a dense voxel region and overwrites all non-zero values"""

    # get all the exceptions not equal and both annotated
    exdata = ((self.data-annodata)*self.data*annodata!=0) * annodata 
    self.data = exceptionDense_ctype ( self.data, annodata )

    # return the list of exceptions ids and the exceptions
    return exdata

  def shaveDense ( self, annodata ):
    """Remove the specified voxels from the annotation"""

    # get all the exceptions that are equal to the annid in both datasets
    shavedata = ((self.data-annodata)==0) * annodata 

    # find all shave requests that don't match the dense data
    exdata = (self.data != annodata) * annodata
    self.data = shaveDense_ctype ( self.data, shavedata )

    return exdata

  
  def zoomData ( self, factor ):
    """ Cube data zoomed in """

    newdata = np.zeros ( [self.data.shape[0], self.data.shape[1]*(2**factor), self.data.shape[2]*(2**factor)], dtype=np.uint32) 
    zoomInData_ctype_OMP ( self.data, newdata, int(factor) )
    self.data = newdata

  
  def downScale ( self, factor ):
    """ Cube data zoomed out """

    newdata = np.zeros ( [self.data.shape[0], self.data.shape[1]/(2**factor), self.data.shape[2]/(2**factor)], dtype=np.uint32) 
    zoomOutData_ctype ( self.data, newdata, int(factor) )
    self.data = newdata
