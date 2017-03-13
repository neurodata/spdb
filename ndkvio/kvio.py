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

from abc import ABCMeta, abstractmethod
from ndlib.ndtype import *

class KVIO(object):
  # __metaclass__ = ABCMeta

  def __init__ ( self, db ):
    """Constructor for the class"""
    self.db = db
  
  def __del__(self, db):
    """Destructor for the class"""
    self.close()

  def close ( self ):
    """Close the connection"""
    pass

  def startTxn ( self ):
    """Start a transaction. Ensure database is in multi-statement mode."""
    pass
    
  def commit ( self ): 
    """Commit the transaction. Moved out of __del__ to make explicit."""
    pass
    
  def rollback ( self ):
    """Rollback the transaction. To be called on exceptions."""
    pass
  
  @abstractmethod
  def getCube(self, ch, timestamp, zidx, resolution, update=False, neariso=False):
    """Retrieve a single cube from the database"""
    return NotImplemented

  @abstractmethod
  def getCubes(self, ch, listoftimestamps, listofidxs, resolution, neariso=False):
    """Retrieve multiple cubes from the database"""
    return NotImplemented
  
  @abstractmethod
  def putCube(self, ch, timestamp, zidx, resolution, cubestr, update=False, neariso=False):
    """Store a single cube into the database"""
    return NotImplemented
  
  @abstractmethod
  def putCubes(self, ch, listoftimestamps, listofidxs, resolution, listofcubes, update=False, neariso=False):
    """Store multiple cubes into the database"""
    return NotImplemented
  
  # Factory method for KVIO Engine
  @staticmethod
  def KVIOFactory(db):
    
    if db.KVENGINE == MYSQL:
      from mysqlkvio import MySQLKVIO
      return MySQLKVIO(db)
    elif db.KVENGINE == REDIS:
      from rediskvio import RedisKVIO
      return RedisKVIO(db)
    else:
      return KVIO(db)
