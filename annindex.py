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
from io import BytesIO
import blosc
import logging
logger=logging.getLogger("neurodata")


class AnnotateIndex:

  def __init__(self,kvio,proj):
    """Give an active connection. This puts all index operations in the same transation as the calling db."""

    self.proj = proj
    self.kvio = kvio
   

  def getIndex ( self, ch, entityid, timestamp, resolution, update=False):
    """Retrieve the index for the annotation with id"""  
    
    idxstr = self.kvio.getIndex(ch, entityid, timestamp, resolution, update)
    if idxstr:
      return blosc.unpack_array(idxstr)
    else:
      return []
       
  
  def putIndex ( self, ch, entityid, timestamp, resolution, index, update=False ):
    """Write the index for the annotation with id"""

    self.kvio.putIndex(ch, entityid, timestamp, resolution, blosc.pack_array(index), update)


  def updateIndexDense(self, ch, index, timestamp, resolution ):
    """Updated the database index table with the input index hash table"""

    for key, value in index.items():
      cubelist = list(value)
      cubeindex = np.array(cubelist, dtype=np.uint64)
      
      curindex = self.getIndex(ch, key, timestamp, resolution, True)
         
      if curindex == []:
        self.putIndex(ch, key, timestamp, resolution, cubeindex, False )
            
      else:
        # Update index to the union of the currentIndex and the updated index
        newIndex = np.union1d(curindex, cubeindex)
        self.putIndex(ch, key, timestamp, resolution, newIndex, True)

  
  def deleteIndexResolution ( self, ch, annid, res ):
    """delete the index for a given annid at the given resolution"""
    
    # delete Index table for each resolution
    self.kvio.deleteIndex(ch, annid, res)
  
  
  def deleteIndex ( self, ch, annid, resolutions ):
    """delete the index for a given annid"""
    
    #delete Index table for each resolution
    for res in resolutions:
      self.kvio.deleteIndex(ch,annid,res)


  def updateIndex ( self, ch, entityid, index, timestamp, resolution ):
    """Updated the database index table with the input index hash table"""

    curindex = self.getIndex(ch, entityid, resolution, True)

    if curindex == []:
        
      self.kvio.putIndex(ch, entityid, timestamp, resolution, blosc.pack_array(index))

    else:
        
      # Update Index to the union of the currentIndex and the updated index
      newIndex = np.union1d(curindex, index)

      # Update the index in the database
      self.kvio.putIndex(ch, entityid, timestamp, resolution, blosc.pack_array(newIndex), True )
