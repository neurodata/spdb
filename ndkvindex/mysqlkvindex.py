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

import MySQLdb
from .kvindex import KVIndex
from spatialdberror import SpatialDBError
import logging
logger=logging.getLogger("neurodata")

class MySQLKVIndex(KVIndex):

  def __init__ ( self, db ):
    """Connect to the database"""

    self.db = db
    self.conn = None
    
    # Connection info 
    try:
      self.conn = MySQLdb.connect (host = self.db.proj.getDBHost(), user = self.db.proj.getDBUser(), passwd = self.db.proj.getDBPasswd(), db = self.db.proj.getDBName())

    except MySQLdb.Error as e:
      self.conn = None
      logger.error("Failed to connect to database: {}, {}".format(self.db.proj.getDBHost(), self.db.proj.getDBName()))
      raise SpatialDBError("Failed to connect to database: {}, {}".format(self.db.proj.getDBHost(), self.db.proj.getDBName()))

  
  def __del__(self):
    """Close the database connection"""
    self.close()

  def close ( self ):
    """Close the connection"""
    if self.conn:
      self.conn.close()

  
  def getIndexStore(self, ch, resolution):
    """Generate the name of the index store"""
    return '{}_res{}_index'.format(ch.getChannelName(), resolution)


  def getCubeIndex(self, ch, resolution, listofidxs, timestamp):
    
    cursor = self.conn.cursor()
    
#    if listoftimestamps:
#      sql = "SELECT zindex, timestamp FROM {} WHERE zindex={} and timestamp in (%s)".format(self.getIndexStore(ch, resolution), listofidxs[0])
#    else:

    sql = "SELECT zindex FROM {} WHERE zindex in (%s) AND timestamp = {}".format(self.getIndexStore(ch, resolution),timestamp) 

    # creats a %s for each list element
    in_p=', '.join(map(lambda x: '%s', listofidxs))
    # replace the single %s with the in_p string
    sql = sql % in_p

    try:
      rc = cursor.execute(sql, listofidxs)
      ids_existing = cursor.fetchall()
      if ids_existing:
        ids_to_fetch = set(listofidxs).difference( set(i[0] for i in ids_existing))
        return list(ids_to_fetch)
      else:
        return listofidxs
    
    except MySQLdb.Error as e:
      logger.error("Error selecting zindex: {}: {}. sql={}".format(e.args[0], e.args[1], sql))
      raise SpatialDBError("Error selecting zindex: {}: {}. sql={}".format(e.args[0], e.args[1], sql))
    
    finally:
      # close the local cursor if not in a transaction and commit right away
      cursor.close()

  
  def putCubeIndex(self, ch, resolution, listofidxs, timestamp):
    
    cursor = self.conn.cursor()
    
#    if listoftimestamps:
#      sql = "REPLACE INTO {} (zindex, timestamp) VALUES (%s,%s)".format(self.getIndexStore(ch, resolution))
#    else:  

    sql = "REPLACE INTO {} (zindex, timestamp VALUE (%s,{})".format(self.getIndexStore(ch, resolution), timestamp)
    
    try:
      cursor.executemany(sql, listofidxs)
    
    except MySQLdb.Error as e:
      logger.error("Error inserting zindex: {}: {}. sql={}".format(e.args[0], e.args[1], sql))
      raise SpatialDBError("Error inserting zindex: {}: {}. sql={}".format(e.args[0], e.args[1], sql))
    
    finally:
      # close the local cursor if not in a transaction and commit right away
      cursor.close()
    
    # commit if not in a txn
    self.conn.commit()
