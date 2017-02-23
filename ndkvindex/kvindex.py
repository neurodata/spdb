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
import logging
logger=logging.getLogger("neurodata")


class KVIndex(object):

  def __init__(self, db):
    """Constructor for the class"""
    self.db = db
  

  def __del__ (self, db):
    """Desctructor for the class"""
    self.close()


  def close ( self ):
    """Close the connection"""
    pass

    
  @abstractmethod
  def getCubeIndex(self, ch, listoftimestamps, listofidxs, resolution, neariso=False):
    """Fetch the index list of inserted cubes"""
    return NotImplemented
  

  @abstractmethod
  def putCubeIndex(self, ch, listoftimestamps, listofidxs, resolution, listofidxs, neariso=False):
    """Insert the index list of fetched cubes"""
    return NotImplemented
  

  @abstractmethod
  def getIndexStore(self, ch, resolution):
    """Get the name of the index store"""
    return NotImplemented
  

  # Factory method for KVIO Engine
  @staticmethod
  def getIndexEngine(db):
    
    from rediskvindex import RedisKVIndex
    return RedisKVIndex(db)
    # if db.KVENGINE == MYSQL:
      # from mysqlkvindex import MySQLKVIndex
      # return MySQLKVIO(db)
    # elif db.KVENGINE == REDIS:
      # from redismyqlindex import RedisKVIndex
      # return RedisKVIO(db)
    # else:
      # return KVIO(db)
