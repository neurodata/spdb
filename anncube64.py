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
import array
import cStringIO
from PIL import Image
import zlib

import ndlib
from cube import Cube

from spatialdberror import SpatialDBError 
import logging
logger=logging.getLogger("neurodata")


class AnnotateCube64(Cube):

  def __init__(self, cubesize):
    """Create empty array of cubesize[x,y,z]"""

    # call the base class constructor
    Cube.__init__( self, cubesize )
    self.data = np.zeros(self.cubesize, dtype=np.uint64)
    # variable that describes when a cube is created from zeros rather than loaded from another source
    self._newcube = False
 

  def fromZeros ( self ):
    """Determine if the cube was created from all zeros?"""
    if self._newcube == True:
      return True
    else: 
      return False


  def zeros ( self ):
    """Create a cube of all zeros"""
    self._newcube = True
    self.data = np.zeros (self.cubesize, dtype=np.uint64)


  def shave ( self, annid, offset, locations ):
    """Remove annotation by a list of locations"""

    self.data , exceptions, zeroed = ndlib.shave_ctype ( self.data, annid, offset, np.array(locations, dtype=np.uint32))
    return exceptions, zeroed
  

  def xySlice ( self, fileobj ):
    """Create the specified slice (index)"""

    zdim,ydim,xdim = self.data.shape
    imagemap = np.zeros ( [ ydim, xdim ], dtype=np.uint32 )

    # false color redrawing of the region
    ndlib.recolor64_ctype ( self.data.reshape((imagemap.shape[0],imagemap.shape[1])), imagemap )

    outimage = Image.frombuffer ( 'RGBA', (xdim,ydim), imagemap, 'raw', 'RGBA', 0, 1 )
    outimage.save ( fileobj, "PNG" )


  def xzSlice ( self, scale, fileobj ):
    """Create the specified slice (index)"""

    zdim,ydim,xdim = self.data.shape
    imagemap = np.zeros ( [ zdim, xdim ], dtype=np.uint32 )

    # false color redrawing of the region
    ndlib.recolor64_ctype ( self.data.reshape((imagemap.shape[0],imagemap.shape[1])), imagemap )

    outimage = Image.frombuffer ( 'RGBA', (xdim,zdim), imagemap, 'raw', 'RGBA', 0, 1 )
    newimage = outimage.resize ( [xdim, int(zdim*scale)] )
    newimage.save ( fileobj, "PNG" )


  def yzSlice ( self, scale, fileobj ):
    """Create the specified slice (index)"""

    zdim,ydim,xdim = self.data.shape
    imagemap = np.zeros ( [ zdim, ydim ], dtype=np.uint32 )

    # false color redrawing of the region
    ndlib.recolor64_ctype ( self.data.reshape((imagemap.shape[0],imagemap.shape[1])), imagemap )

    outimage = Image.frombuffer ( 'RGBA', (ydim,zdim), imagemap, 'raw', 'RGBA', 0, 1 )
    newimage = outimage.resize ( [ydim, int(zdim*scale)] )
    newimage.save ( fileobj, "PNG" )


  def overwrite ( self, annodata ):
    """Get's a dense voxel region and overwrites all non-zero values"""

    vector_func = np.vectorize ( lambda a,b: b if b!=0 else a ) 
    self.data = vector_func ( self.data, annodata ) 

  def preserve ( self, annodata ):
    """Get's a dense voxel region and overwrites all non-zero values"""

    vector_func = np.vectorize ( lambda a,b: b if b!=0 and a==0 else a ) 
    self.data = vector_func ( self.data, annodata ) 

  def exception ( self, annodata ):
    """Get's a dense voxel region and overwrites all non-zero values"""

    # get all the exceptions
    # not equal and both annotated
    exdata = ((self.data-annodata)*self.data*annodata!=0) * annodata 

    # then annotate to preserve 
    vector_func = np.vectorize ( lambda a,b: b if b!=0 and a==0 else a ) 
    self.data = vector_func ( self.data, annodata ) 

    # return the list of exceptions ids and the exceptions
    return exdata

  def shaveDense ( self, annodata ):
    """Remove the specified voxels from the annotation"""

    # get all the exceptions that are equal to the annid in both datasets
    shavedata = ((self.data-annodata)==0) * annodata 

    # find all shave requests that don't match the dense data
    exdata = (self.data != annodata) * annodata

    # then shave 
    vector_func = np.vectorize ( lambda a,b: 0 if b!=0 else a ) 
    self.data = vector_func ( self.data, shavedata ) 

    # return the list of exceptions ids and the exceptions
    return exdata

  # placeholder function move and optimize
  def zoomData ( self, factor ):
    """Cube data zoomed up"""

    newdata = np.zeros ( [self.data.shape[0], self.data.shape[1]*(2**factor), self.data.shape[2]*(2**factor)], dtype=np.uint64) 

    ndlib.zoomData64_ctype ( self.data, newdata, int(factor) )

    self.data = newdata
