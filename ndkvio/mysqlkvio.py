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

import itertools
import MySQLdb
from kvio import KVIO
from ndtype import OLDCHANNEL
from spatialdberror import SpatialDBError
import logging
logger=logging.getLogger("neurodata")


#RBTODO fix transaction support for putcubes


"""Helpers function to do cube I/O in across multiple DBs.
    This uses the state and methods of spatialdb"""

class MySQLKVIO(KVIO):

  def __init__ ( self, db ):
    """Connect to the database"""

    self.db = db
    self.conn = None
    
    # Connection info 
    try:
      self.conn = MySQLdb.connect (host = self.db.proj.host, user = self.db.proj.kvengine_user, passwd = self.db.proj.kvengine_password, db = self.db.proj.dbname)

    except MySQLdb.Error, e:
      self.conn = None
      logger.error("Failed to connect to database: {}, {}".format(self.db.proj.host, self.db.proj.dbname))
      raise SpatialDBError("Failed to connect to database: {}, {}".format(self.db.proj.host, self.db.proj.dbname))

    # start with no cursor
    self.txncursor = None
  
  def __del__(self):
    """Close the database connection"""
    self.close()

  def close ( self ):
    """Close the connection"""
    if self.conn:
      self.conn.close()

  def startTxn ( self ):
    """Start a transaction.  Ensure database is in multi-statement mode."""
    
    self.txncursor = self.conn.cursor()
    sql = "START TRANSACTION"
    self.txncursor.execute ( sql )

  def commit ( self ):
    """Commit the transaction.  Moved out of del to make explicit.""" 

    if self.txncursor:
      self.conn.commit()
      self.txncursor.close()
      self.txncursor = None

  def rollback ( self ):
    """Rollback the transaction.  To be called on exceptions."""

    if self.txncursor:
      self.conn.rollback()
      self.txncursor.close()
      self.txncursor = None
  

  def getCube(self, ch, timestamp, zidx, resolution, update=False, neariso=False, direct=False):
    """Retrieve a cube from the database by token, resolution, and zidx"""

    # if in a TxN us the transaction cursor.  Otherwise create one.
    if self.txncursor is None:
      cursor = self.conn.cursor()
    else:
      cursor = self.txncursor
   
    if not neariso:
      sql = "SELECT cube FROM {} WHERE (zindex,timestamp) = ({},{})".format(ch.getTable(resolution), zidx, timestamp)
    else:
      sql = "SELECT cube FROM {} WHERE (zindex,timestamp) = ({},{})".format(ch.getNearIsoTable(resolution), zidx, timestamp)

    if update:
      sql += " FOR UPDATE"

    try:
      cursor.execute ( sql )
      row = cursor.fetchone()
    except MySQLdb.Error, e:
      logger.error("Failed to retrieve data cube: {}: {}. sql={}".format(e.args[0], e.args[1], sql))
      raise SpatialDBError("Failed to retrieve data cube: {}: {}. sql={}".format(e.args[0], e.args[1], sql))

    # commit if not in a txn
    if self.txncursor is None:
      self.conn.commit()
      cursor.close()
    
    # If we can't find a cube, assume it hasn't been written yet
    if row is None:
      return None
    else: 
      return row[0]

  
  def getCubes(self, ch, listoftimestamps, listofidxs, resolution, neariso=False, direct=False):

    # if in a TxN us the transaction cursor.  Otherwise create one.
    if self.txncursor is None:
      cursor = self.conn.cursor()
    else:
      cursor = self.txncursor
    
    # creating a list of tuples (zindex, timestamp) and unrolling them for sql execution as [zindex, timestamp, zindex, timestamp]
    index_list = list(itertools.chain.from_iterable(itertools.product(listofidxs, listoftimestamps)))

    if neariso:
      print "Fetching data from isotropic tables"
      sql = "SELECT zindex, timestamp, cube FROM {} WHERE (zindex, timestamp) in (%s)".format( ch.getNearIsoTable(resolution) ) 
    else:
      print "Fetching data from normal tables"
      sql = "SELECT zindex, timestamp, cube FROM {} WHERE (zindex, timestamp) in (%s)".format( ch.getTable(resolution) ) 

    # creats a %s for each list element
    in_p=', '.join(map(lambda x: '(%s,%s)', [1]*(len(index_list)/2)))
    # replace the single %s with the in_p string
    sql = sql % in_p

    try:
      rc = cursor.execute(sql, index_list)
    
      # Get the objects and add to the cube
      while ( True ):
        try: 
          retval = cursor.fetchone() 
        except:
          break
        if retval is not None:
          yield ( retval )
        else:
          return
    
    except MySQLdb.Error, e:
      logger.error("Failed to retrieve data cubes: {}: {}. sql={}".format(e.args[0], e.args[1], sql))
      raise SpatialDBError("Failed to retrieve data cubes: {}: {}. sql={}".format(e.args[0], e.args[1], sql))


    finally:
      # close the local cursor if not in a transaction
      if self.txncursor is None:
        cursor.close()
   
  
  def putCube ( self, ch, timestamp, zidx, resolution, cubestr, update=False, neariso=False, direct=False):
    """Store a cube from the annotation database"""
    
    # if in a TxN us the transaction cursor.  Otherwise create one.
    if self.txncursor is None:
      cursor = self.conn.cursor()
    else:
      cursor = self.txncursor
    
    try:
      # we created a cube from zeros
      if not update:
        if not neariso:
          print "Inserting data from normal tables"
          sql = "INSERT INTO {} (zindex, timestamp, cube) VALUES (%s, %s, %s)".format(ch.getTable(resolution))
        else:
          print "Inserting data from isotropic tables"
          sql = "INSERT INTO {} (zindex, timestamp, cube) VALUES (%s, %s, %s)".format(ch.getNearIsoTable(resolution))
        # this uses a cursor defined in the caller (locking context): not beautiful, but needed for locking
        cursor.execute ( sql, (zidx, timestamp, cubestr) ) 
      
      else:
        if not neariso:
          print "Updating data from normal tables"
          sql = "UPDATE {} SET cube=(%s) WHERE (zindex,timestamp)=({},{})".format(ch.getTable(resolution), zidx, timestamp)
        else:
          print "Updating data from isotropic tables"
          sql = "UPDATE {} SET cube=(%s) WHERE (zindex,timestamp)=({},{})".format(ch.getNearIsoTable(resolution), zidx, timestamp)
        cursor.execute( sql, (cubestr,) )
      
    except MySQLdb.Error, e:
      logger.error("Error updating/inserting cube: {}: {}. sql={}".format(e.args[0], e.args[1], sql))
      raise SpatialDBError("Error updating/inserting cube: {}: {}. sql={}".format(e.args[0], e.args[1], sql))
    
    finally:

      # close the local cursor if not in a transaction
      if self.txncursor is None:
        cursor.close()

        # commit if not in a txn
        self.conn.commit()


  def getIndex ( self, ch, annid, timestamp, resolution, update ):
    """MySQL fetch index routine"""

    # if in a TxN us the transaction cursor.  Otherwise create one.
    if self.txncursor is None:
      cursor = self.conn.cursor()
    else:
      cursor = self.txncursor

    # get the block from the database                                            
    sql = "SELECT cube FROM {} WHERE annid = {} AND timestamp = {}".format( ch.getIdxTable(resolution), annid, timestamp )
    if update:
      sql += " FOR UPDATE"
    
    try:
      cursor.execute ( sql )
      row = cursor.fetchone ()
    
    except MySQLdb.Error, e:
      logger.error("Failed to retrieve cube {}: {}. sql={}".format(e.args[0], e.args[1], sql))
      raise SpatialDBError("Failed to retrieve cube {}: {}. sql={}".format(e.args[0], e.args[1], sql))
    
    except BaseException, e:
      logger.exception("Unknown exception")
      raise SpatialDBError("Unknown exception")
    
    finally:
      # close the local cursor if not in a transaction
      if self.txncursor is None:
        cursor.close()
   
    # If we can't find a index, they don't exist                                
    if row is None:
       return []
    else:
       return row[0]


  def putIndex ( self, ch, zidx, timestamp, resolution, indexstr, update ):
    """MySQL put index routine"""

    # if in a TxN us the transaction cursor.  Otherwise create one.
    if self.txncursor is None:
      cursor = self.conn.cursor()
    else:
      cursor = self.txncursor

    if not update:
      sql = "INSERT INTO {} ( annid, timestamp, cube) VALUES ( %s, %s, %s )".format( ch.getIdxTable(resolution) )
      
      try:
         cursor.execute ( sql, (zidx, timestamp, indexstr) )
      
      except MySQLdb.Error, e:
         logger.error("Error updating index {}: {}. sql={}".format(e.args[0], e.args[1], sql))
         raise SpatialDBError("Error updating index {}: {}. sql={}".format(e.args[0], e.args[1], sql))
      
      except BaseException, e:
         logger.exception("Unknown error when updating index")
         raise SpatialDBError("Unknown error when updating index")
      
      finally:
        # close the local cursor if not in a transaction
        if self.txncursor is None:
          cursor.close()

    else:
      # update index in the database
      sql = "UPDATE {} SET cube=(%s) WHERE annid={}".format( ch.getIdxTable(resolution), zidx )
      
      try:
         cursor.execute ( sql, (indexstr,) )
      
      except MySQLdb.Error, e:
         logger.error("Error updating exceptions {}: {}. sql={}".format(e.args[0], e.args[1], sql))
         raise SpatialDBError("Error updating exceptions {}: {}. sql={}".format(e.args[0], e.args[1], sql))
      
      except:
        logger.exception("Unknown exception")
        raise SpatialDBError("Unknown exception")
      
      finally:
        # close the local cursor if not in a transaction
        if self.txncursor is None:
          cursor.close()

    # commit if not in a txn
    if self.txncursor is None:
      self.conn.commit()


  def deleteIndex ( self, ch, annid, resolution ):
    """MySQL update index routine"""

    # if in a TxN us the transaction cursor.  Otherwise create one.
    if self.txncursor is None:
      cursor = self.conn.cursor()
    else:
      cursor = self.txncursor

    sql = "DELETE FROM {} WHERE annid={}".format( ch.getIdxTable(resolution), annid )
    
    try:
       cursor.execute(sql)
    
    except MySQLdb.Error, e:
       logger.error("Error deleting the index {}: {}. sql={}".format(e.args[0], e.args[1], sql))
       raise SpatialDBError("Error deleting the index {}: {}. sql={}".format(e.args[0], e.args[1], sql))
    
    except:
      logger.exception("Unknown exception")
      raise SpatialDBError("Unknown exception")
    
    finally:
      # close the local cursor if not in a transaction
      if self.txncursor is None:
        cursor.close()

    # commit if not in a txn
    if self.txncursor is None:
      self.conn.commit()


  def getExceptions ( self, ch, zidx, timestamp, resolution, annid ):
    """Load a the list of excpetions for this cube."""

    # if in a TxN us the transaction cursor.  Otherwise create one.
    if self.txncursor is None:
      cursor = self.conn.cursor()
    else:
      cursor = self.txncursor

    # get the block from the database
    sql = "SELECT exlist FROM {} where zindex={} AND timestamp={} AND id={}".format( ch.getExceptionsTable(resolution), zidx, timestamp, annid )
    try:
      cursor.execute(sql)
      row = cursor.fetchone()
    
    except MySQLdb.Error, e:
      logger.error("Error reading exceptions {}: {}. sql={}".format(e.args[0], e.args[1], sql))
      raise SpatialDBError("Error reading exceptions {}: {}. sql={}".format(e.args[0], e.args[1], sql))
    
    finally:
      # close the local cursor if not in a transaction
      if self.txncursor is None:
        cursor.close()

    # If we can't find a list of exceptions, they don't exist
    if row is None:
      return []
    else: 
      return row[0] 


  def deleteExceptions ( self, ch, zidx, timestamp, resolution, annid ):
    """Delete a list of exceptions for this cuboid"""

    # if in a TxN us the transaction cursor.  Otherwise create one.
    if self.txncursor is None:
      cursor = self.conn.cursor()
    else:
      cursor = self.txncursor

    sql = "DELETE FROM {} WHERE zindex ={} AND timestamp={} AND id ={}".format( ch.getExceptionsTable(resolution), zidx, timestamp, annid ) 
    try:
      cursor.execute ( sql )
    
    except MySQLdb.Error, e:
      if self.txncursor is None:
        cursor.close()
      logger.error("Error deleting exceptions {}: {}. sql={}".format(e.args[0], e.args[1], sql))
      raise SpatialDBError("Error deleting exceptions {}: {}. sql={}".format(e.args[0], e.args[1], sql))
    
    finally:
      # commit if not in a txn
      if self.txncursor is None:
        self.conn.commit()
        cursor.close()


  def putExceptions ( self, ch, zidx, timestamp, resolution, annid, excstr, update=False ):
    """Store a list of exceptions"""
    """This should be done in a transaction"""

    # if in a TxN us the transaction cursor.  Otherwise create one.
    if self.txncursor is None:
      cursor = self.conn.cursor()
    else:
      cursor = self.txncursor


    if not update:

      sql = "INSERT INTO {} (zindex, timestamp, id, exlist) VALUES (%s, %s, %s, %s)".format( ch.getExceptionsTable(resolution) )
      try:
        cursor.execute ( sql, (zidx, timestamp, annid, excstr))
      except MySQLdb.Error, e:
        if self.txncursor is None:
          cursor.close()
        raise
        logger.error("Error inserting exceptions {}: {}. sql={}".format(e.args[0], e.args[1], sql))
        raise SpatialDBError("Error inserting exceptions {}: {}. sql={}".format(e.args[0], e.args[1], sql))

    # In this case we have an update query
    else:

      sql = "UPDATE {} SET exlist=(%s) WHERE zindex=%s AND timestamp=%s AND id=%s".format( ch.getExceptionsTable(resolution) )
      try:
        cursor.execute ( sql, (excstr, zidx, timestamp, annid))
      except MySQLdb.Error, e:
        if self.txncursor is None:
          cursor.close()
        raise
        logger.error("Error updating exceptions {}: {}. sql={}".format(e.args[0], e.args[1], sql))
        raise SpatialDBError("Error updating exceptions {}: {}. sql={}".format(e.args[0], e.args[1], sql))

    # commit if not in a txn
    if self.txncursor is None:
      self.conn.commit()
      cursor.close()
