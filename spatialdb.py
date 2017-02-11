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

#RBTODO make everything work on zoomout -- just cutout and bounding box now

import numpy as np
from io import BytesIO
import zlib
from collections import defaultdict
import itertools
import blosc
from contextlib import closing
from operator import add, sub, floordiv, mod, mul
from spdb.ndcube.cube import Cube
from .ndmanager.readerlock import ReaderLock
import spdb.s3io as s3io
from spdb.ndkvio.kvio import KVIO
from . import annindex
from spdb.ndkvindex.kvindex import KVIndex
from ndlib.ndctypelib import *
from ndlib.ndtype import ANNOTATION_CHANNELS, TIMESERIES_CHANNELS, EXCEPTION_TRUE, PROPAGATED, MYSQL, CASSANDRA, RIAK, DYNAMODB, REDIS, S3_TRUE, S3_FALSE, UNDER_PROPAGATION, NOT_PROPAGATED
from spdb.spatialdberror import SpatialDBError
import logging
logger=logging.getLogger("neurodata")


"""
.. module:: spatialdb
    :synopsis: Manipulate/create/read from the Morton-order cube store

.. moduleauthor:: Kunal Lillaney <lillaney@jhu.edu>
"""

class SpatialDB: 

  def __init__ (self, proj):
    """Connect with the brain databases"""
    
    self.datasetcfg = proj.datasetcfg 
    self.proj = proj
    
    # Set the S3 backend for the data
    self.s3io = s3io.S3IO(self)

    # Are there exceptions?
    #self.EXCEPT_FLAG = self.proj.getExceptions()
    self.KVENGINE = self.proj.kvengine
    self.NPZ = False
    
    self.kvio = KVIO.getIOEngine(self)
    self.kvindex = KVIndex.getIndexEngine(self)

    # else:
      # raise SpatialDBError ("Unknown key/value store. Engine = {}".format(self.proj.getKVEngine()))

    self.annoIdx = annindex.AnnotateIndex ( self.kvio, self.proj )

  def close ( self ):
    """Close the connection"""

    self.kvio.close()
    self.kvindex.close()


  def getCube(self, ch, timestamp, zidx, resolution, update=False):
    """Load a cube from the database"""

    # get the size of the image and cube
    cubedim = self.datasetcfg.cubedim[resolution]
    # KL TODO add indexing here
    cube = Cube.CubeFactory(cubedim, ch.channel_type, ch.channel_datatype, [0,1])
  
    # get the block from the database
    cube_str = self.kvio.getCube(ch, timestamp, zidx, resolution, update=update)

    if not cube_str:
      cube.zeros()
    else:
      # handle the cube format here and decompress the cube
      if self.NPZ:
        assert(0)
        cube.fromNPZ(cube_str)
      else:
        cube.fromBlosc(cube_str)

    return cube

  @ReaderLock
  def getCubes(self, ch, listofidxs, resolution, listoftimestamps=None, neariso=False):
    """Return a list of cubes"""

#    if listoftimestamps is None:
#      if self.proj.s3backend == S3_TRUE:
#        ids_to_fetch = self.kvindex.getCubeIndex(ch, resolution, listofidxs)
#        # checking if the index exists inside the database or not
#        if ids_to_fetch:
#          # logger.debug("Cache Miss: {}".format(listofidxs))
#          super_cuboids = self.s3io.getCubes(ch, ids_to_fetch, resolution)
#          
#          # iterating over super_cuboids
#          for superlistofidxs, superlistofcubes in super_cuboids:
            # call putCubes and update index in the table before returning data
#            self.putCubes(ch, superlistofidxs, resolution, superlistofcubes, update=True)
#            
#      return self.kvio.getCubes(ch, listofidxs, resolution, neariso)
#    else:
    return self.kvio.getTimeCubes(ch, listofidxs, listoftimestamps, resolution)


  def putCubes(self, ch, listofidxs, resolution, listofcubes, update=False):
    """Insert a list of cubes"""
    
    if self.proj.s3backend == S3_TRUE:
      self.kvindex.putCubeIndex(ch, resolution, listofidxs)
    return self.kvio.putCubes(ch, listofidxs, resolution, listofcubes, update)

  
  def putCube(self, ch, timestamp, zidx, resolution, cube, update=False):
    """ Store a cube in the annotation database """

    # handle the cube format here
    if self.proj.s3backend == S3_TRUE:
      # KLTODO -- broken by tiemstamp changes
      if ch.channel_type in TIMESERIES_CHANNELS and timestamp is not None:
        self.kvindex.putCubeIndex(ch, resolution, [zidx], [timestamp])
      elif ch.channel_type not in TIMESERIES_CHANNELS and timestamp is None:
        self.kvindex.putCubeIndex(ch, resolution, [zidx])
      else:
        logger.error("Timestamp is not None for Image Channels.")
        raise SpatialDBError("Timestamp is not None for Image Channels.")

    if self.NPZ:
      self.kvio.putCube(ch, timestamp, zidx, resolution, cube.toNPZ(), not cube.fromZeros())
    else:
#      print(np.unique(cube.data),zidx)
      self.kvio.putCube(ch, timestamp, zidx, resolution, cube.toBlosc(), not cube.fromZeros())
  

  def getExceptions(self, ch, zidx, timestamp, resolution, annoid):
    """Load a cube from the annotation database"""

    excstr = self.kvio.getExceptions(ch, zidx, timestamp, resolution, annoid)
    if excstr:
      if self.NPZ:
        return np.load(BytesIO(zlib.decompress(excstr)))
      else:
        return blosc.unpack_array(excstr)
    else:
      return []


  def updateExceptions(self, ch, key, timestamp, resolution, exid, exceptions, update=False):
    """Merge new exceptions with existing exceptions"""

    curexlist = self.getExceptions(ch, key, timestamp, resolution, exid) 

    update = False

    if curexlist!=[]:
      oldexlist = [ XYZMorton ( trpl ) for trpl in curexlist ]
      newexlist = [ XYZMorton ( trpl ) for trpl in exceptions ]
      exlist = set(newexlist + oldexlist)
      exlist = [ MortonXYZ ( zidx ) for zidx in exlist ]
      update = True
    else:
      exlist = exceptions

    self.putExceptions ( ch, key, timestamp, resolution, exid, exlist, update)


  def putExceptions(self, ch, key, timestamp, resolution, exid, exceptions, update):
    """Package the object and transact with kvio"""
    
    exceptions = np.array ( exceptions, dtype=np.uint32 )

    if self.NPZ:
      fileobj = BytesIO()
      np.save ( fileobj, exceptions )
      excstr = fileobj.getvalue()
      self.kvio.putExceptions(ch, key, timestamp, resolution, exid, zlib.compress(excstr), update)
    else:
      self.kvio.putExceptions(ch, key, timestamp, resolution, exid, blosc.pack_array(exceptions), update)


  def removeExceptions(self, ch, key, timestamp, resolution, entityid, exceptions):
    """Remove a list of exceptions. Should be done in a transaction"""

    curexlist = self.getExceptions(ch, key, timestamp, resolution, entityid) 

    if curexlist != []:

      oldexlist = set([ XYZMorton ( trpl ) for trpl in curexlist ])
      newexlist = set([ XYZMorton ( trpl ) for trpl in exceptions ])
      exlist = oldexlist-newexlist
      exlist = [ MortonXYZ ( zidx ) for zidx in exlist ]

      self.putExceptions ( ch, key, timestamp, resolution, exid, exlist, True )


  def annotate(self, ch, entityid, timestamp, resolution, locations, conflictopt=b'O' ):
    """Label the voxel locations or add as exceptions is the are already labeled."""

    cubedim = self.datasetcfg.cubedim [ resolution ]

    # an item may exist across several cubes
    # convert the locations into Morton order

    # dictionary with the index
    cubeidx = defaultdict(set)
    cubelocs = locate_ctype ( np.array(locations, dtype=np.uint32), cubedim )

    # sort the arrary, by cubeloc
    cubelocs = quicksort ( cubelocs )
    #cubelocs2.view('u4,u4,u4,u4').sort(order=['f0'], axis=0)

    # get the nonzero element offsets 
    nzdiff = np.r_[np.nonzero(np.diff(cubelocs[:,0]))]
    # then turn into a set of ranges of the same element
    listoffsets = np.r_[0, nzdiff + 1, len(cubelocs)]

    # start a transaction if supported
    self.kvio.startTxn()
    for i in range(len(listoffsets)-1):

      # grab the list of voxels for the first cube
      voxlist = cubelocs[listoffsets[i]:listoffsets[i+1],:][:,1:4]
      #  and the morton key
      key = cubelocs[listoffsets[i],0]

      cube = self.getCube ( ch, timestamp, key, resolution, update=True )

      # get a voxel offset for the cube
      cubeoff = MortonXYZ( key )
      offset = np.asarray([cubeoff[0]*cubedim[0], cubeoff[1]*cubedim[1], cubeoff[2]*cubedim[2]], dtype=np.uint32)

      # add the items
      exceptions = np.array(cube.annotate(entityid, timestamp, offset, voxlist, conflictopt), dtype=np.uint8)

      # update the sparse list of exceptions
      if ch.getExceptions() == EXCEPTION_TRUE:
        if len(exceptions) != 0:
          self.updateExceptions(ch, key, timestamp, resolution, entityid, exceptions)

      self.putCube(ch, timestamp, key, resolution, cube )

      # add this cube to the index
      cubeidx[entityid].add(key)

    # write it to the database
    self.annoIdx.updateIndexDense(ch, cubeidx, timestamp, resolution)
    # commit cubes.  not commit controlled with metadata
    self.kvio.commit()


  def shave ( self, ch, entityid, timestamp, resolution, locations ):
    """Label the voxel locations or add as exceptions is the are already labeled."""

    cubedim = self.datasetcfg.cubedim [ resolution ]

    # dictionary with the index
    cubeidx = defaultdict(set)

    # convert voxels z coordinate
    cubelocs = locate_ctype ( np.array(locations, dtype=np.uint32), cubedim )

    # sort the arrary, by cubeloc
    cubelocs = quicksort ( cubelocs )
    #cubelocs.view('u4,u4,u4,u4').sort(order=['f0'], axis=0)

    # get the nonzero element offsets 
    nzdiff = np.r_[np.nonzero(np.diff(cubelocs[:,0]))]
    # then turn into a set of ranges of the same element
    listoffsets = np.r_[0, nzdiff + 1, len(cubelocs)]

    self.kvio.startTxn()

    try:

      for i in range(len(listoffsets)-1):

        # grab the list of voxels for the first cube
        voxlist = cubelocs[listoffsets[i]:listoffsets[i+1],:][:,1:4]
        #  and the morton key
        key = cubelocs[listoffsets[i],0]

        cube = self.getCube (ch, timestamp, key, resolution, update=True)

        # get a voxel offset for the cube
        cubeoff = MortonXYZ(key)
        #cubeoff2 = zindex.MortonXYZ(key)
        offset = np.asarray( [cubeoff[0]*cubedim[0],cubeoff[1]*cubedim[1],cubeoff[2]*cubedim[2]], dtype=np.uint32 )

        # remove the items
        exlist, zeroed = cube.shave (entityid, timestamp, offset, voxlist)
        # make sure that exceptions are stored as 8 bits
        exceptions = np.array(exlist, dtype=np.uint8)

        # update the sparse list of exceptions
        if ch.getExceptions == EXCEPTION_TRUE:
          if len(exceptions) != 0:
            self.removeExceptions ( ch, key, timestamp, resolution, entityid, exceptions )

        self.putCube (ch, timestamp, key, resolution, cube)

        # For now do no index processing when shaving.  Assume there are still some
        #  voxels in the cube???

    except:
      self.kvio.rollback()
      raise

    self.kvio.commit()


  def annotateDense ( self, ch, timestamp, corner, resolution, annodata, conflictopt=b'O' ):
    """Process all the annotations in the dense volume"""

    index_dict = defaultdict(set)

    # dim is in xyz, data is in zyxj
    dim = annodata.shape[::-1]

    # get the size of the image and cube
    [xcubedim, ycubedim, zcubedim] = cubedim = self.datasetcfg.cubedim [ resolution ]

    # round to the nearest larger cube in all dimensions
    start = [xstart, ystart, zstart] = list(map(floordiv, corner, cubedim))

    znumcubes = (corner[2]+dim[2]+zcubedim-1)//zcubedim - zstart
    ynumcubes = (corner[1]+dim[1]+ycubedim-1)//ycubedim - ystart
    xnumcubes = (corner[0]+dim[0]+xcubedim-1)//xcubedim - xstart

    offset = [xoffset, yoffset, zoffset] = list(map(mod, corner, cubedim))

    databuffer = np.zeros ([znumcubes*zcubedim, ynumcubes*ycubedim, xnumcubes*xcubedim], dtype=np.uint32 )
    databuffer [ zoffset:zoffset+dim[2], yoffset:yoffset+dim[1], xoffset:xoffset+dim[0] ] = annodata 

    # start a transaction if supported
    self.kvio.startTxn()

    try:

      for z in range(znumcubes):
        for y in range(ynumcubes):
          for x in range(xnumcubes):

            key = XYZMorton ([x+xstart,y+ystart,z+zstart])
            cube = self.getCube (ch, timestamp, key, resolution, update=True )
            
            if conflictopt == b'O':
              cube.overwrite ( 0, databuffer [ z*zcubedim:(z+1)*zcubedim, y*ycubedim:(y+1)*ycubedim, x*xcubedim:(x+1)*xcubedim ] )
            elif conflictopt == b'P':
              cube.preserve ( 0, databuffer [ z*zcubedim:(z+1)*zcubedim, y*ycubedim:(y+1)*ycubedim, x*xcubedim:(x+1)*xcubedim ] )
            elif conflictopt == b'E': 
              if ch.getExceptions() == EXCEPTION_TRUE:
                exdata = cube.exception ( timestamp, databuffer [ z*zcubedim:(z+1)*zcubedim, y*ycubedim:(y+1)*ycubedim, x*xcubedim:(x+1)*xcubedim ] )
                for exid in np.unique ( exdata ):
                  if exid != 0:
                    # get the offsets
                    exoffsets = np.nonzero ( exdata==exid )
                    # assemble into 3-tuples zyx->xyz
                    exceptions = np.array ( list(zip(exoffsets[2], exoffsets[1], exoffsets[0])), dtype=np.uint32 )
                    # update the exceptions
                    self.updateExceptions ( ch, key, timestamp, resolution, exid, exceptions )
                    # add to the index
                    index_dict[exid].add(key)
              else:
                logger.error("No exceptions for this project.")
                raise SpatialDBError ( "No exceptions for this project.")
            else:
              logger.error ( "Unsupported conflict option %s" % conflictopt )
              raise SpatialDBError ( "Unsupported conflict option %s" % conflictopt )
            
            self.putCube (ch, timestamp, key, resolution, cube )

            # update the index for the cube
            # get the unique elements that are being added to the data
            uniqueels = np.unique ( databuffer [ z*zcubedim:(z+1)*zcubedim, y*ycubedim:(y+1)*ycubedim, x*xcubedim:(x+1)*xcubedim ] )
            for el in uniqueels:
              index_dict[el].add(key) 

            # remove 0 no reason to index that
            if 0 in index_dict:
              del(index_dict[0])

      # update all indexes
      self.annoIdx.updateIndexDense(ch, index_dict, timestamp, resolution )
      # commit cubes.  not commit controlled with metadata

    except:
      self.kvio.rollback()
      raise
    
    self.kvio.commit()


  def annotateEntityDense ( self, ch, entityid, timestamp, corner, resolution, annodata, conflictopt=b'O' ):
    """Relabel all nonzero pixels to annotation id and call annotateDense"""

    annodata = annotateEntityDense_ctype ( annodata, entityid )
    return self.annotateDense ( ch, timestamp, corner, resolution, annodata, conflictopt )

  
  def shaveDense ( self, ch, entityid, timestamp, corner, resolution, annodata ):
    """Process all the annotations in the dense volume"""

    index_dict = defaultdict(set)

    # dim is in xyz, data is in zyxj
    dim = [ annodata.shape[2], annodata.shape[1], annodata.shape[0] ]

    # get the size of the image and cube
    [xcubedim, ycubedim, zcubedim] = cubedim = self.datasetcfg.cubedim [ resolution ]

    # Round to the nearest larger cube in all dimensions
    zstart = corner[2]//zcubedim
    ystart = corner[1]//ycubedim
    xstart = corner[0]//xcubedim

    znumcubes = (corner[2]+dim[2]+zcubedim-1)//zcubedim - zstart
    ynumcubes = (corner[1]+dim[1]+ycubedim-1)//ycubedim - ystart
    xnumcubes = (corner[0]+dim[0]+xcubedim-1)//xcubedim - xstart

    zoffset = corner[2]%zcubedim
    yoffset = corner[1]%ycubedim
    xoffset = corner[0]%xcubedim

    databuffer = np.zeros ([znumcubes*zcubedim, ynumcubes*ycubedim, xnumcubes*xcubedim], dtype=np.uint32 )
    databuffer [ zoffset:zoffset+dim[2], yoffset:yoffset+dim[1], xoffset:xoffset+dim[0] ] = annodata 

    # start a transaction if supported
    self.kvio.startTxn()

    try:

      for z in range(znumcubes):
        for y in range(ynumcubes):
          for x in range(xnumcubes):

            key = XYZMorton ([x+xstart,y+ystart,z+zstart])
            cube = self.getCube(ch, timestamp, key, resolution, update=True)

            exdata = cube.shaveDense ( timestamp, databuffer [ z*zcubedim:(z+1)*zcubedim, y*ycubedim:(y+1)*ycubedim, x*xcubedim:(x+1)*xcubedim ] )
            for exid in np.unique ( exdata ):
              if exid != 0:
                # get the offsets
                exoffsets = np.nonzero ( exdata==exid )
                # assemble into 3-tuples zyx->xyz
                exceptions = np.array ( list(zip(exoffsets[2], exoffsets[1], exoffsets[0])), dtype=np.uint32 )
                # update the exceptions
                self.removeExceptions ( key, timestamp, resolution, exid, exceptions )
                # add to the index
                index_dict[exid].add(key)

            self.putCube(ch, timestamp, key, resolution, cube)

            # update the index for the cube
            # get the unique elements that are being added to the data
            uniqueels = np.unique ( databuffer [ z*zcubedim:(z+1)*zcubedim, y*ycubedim:(y+1)*ycubedim, x*xcubedim:(x+1)*xcubedim ] )
            for el in uniqueels:
              index_dict[el].add(key) 

            # remove 0 no reason to index that
            del(index_dict[0])

      # update all indexes
      self.annoIdx.updateIndexDense(ch, index_dict, timestamp, resolution)

    except:
      self.kvio.rollback()
      raise

    # commit cubes.  not commit controlled with metadata
    self.kvio.commit()


  def shaveEntityDense ( self, ch, entityid, timestamp, corner, resolution, annodata ):
    """Takes a bitmap for an entity and calls denseShave. Renumber the annotations to match the entity id"""

    # make shaving a per entity operation
    vec_func = np.vectorize ( lambda x: 0 if x == 0 else entityid ) 
    annodata = vec_func ( annodata )

    self.shaveDense ( ch, entityid, timestamp, corner, resolution, annodata )


  def _zoominCutout ( self, ch, corner, dim, resolution ):
    """Scale to a smaller cutout that will be zoomed"""

    # scale the corner to lower resolution
    effcorner = corner[0]//(2**(ch.resolution-resolution)), corner[1]//(2**(ch.resolution-resolution)), corner[2]

    # pixels offset within big range
    xpixeloffset = corner[0]%(2**(ch.resolution-resolution))
    ypixeloffset = corner[1]%(2**(ch.resolution-resolution))

    # get the new dimension, snap up to power of 2
    outcorner = (corner[0]+dim[0],corner[1]+dim[1],corner[2]+dim[2])

    newoutcorner = (outcorner[0]-1)//(2**(ch.resolution-resolution))+1, (outcorner[1]-1)//(2**(ch.resolution-resolution))+1, outcorner[2]
    effdim = (newoutcorner[0]-effcorner[0],newoutcorner[1]-effcorner[1],newoutcorner[2]-effcorner[2])

    return effcorner, effdim, (xpixeloffset,ypixeloffset)


  def _zoomoutCutout ( self, ch, corner, dim, resolution ):
    """Scale to a larger cutout that will be shrunk"""

    # scale the corner to higher resolution
    effcorner = corner[0]*(2**(resolution-ch.resolution)), corner[1]*(2**(resolution-ch.resolution)), corner[2]

    effdim = dim[0]*(2**(resolution-ch.resolution)),dim[1]*(2**(resolution-ch.resolution)),dim[2]

    return effcorner, effdim 


  def cutout(self, ch, corner, dim, resolution, timerange=None, zscaling=None, annoids=None):
    """Extract a cube of arbitrary size. Need not be aligned."""

    [xcubedim, ycubedim, zcubedim] = cubedim = self.datasetcfg.get_cubedim(resolution) 
    # if cutout is below resolution, get a smaller cube and scaleup
    if ch.channel_type in ANNOTATION_CHANNELS and ch.resolution > resolution:
      # find the effective dimensions of the cutout (where the data is)
      effcorner, effdim, (xpixeloffset,ypixeloffset) = self._zoominCutout ( ch, corner, dim, resolution )
      effresolution = ch.resolution
    # if cutout is above resolution, get a large cube and scaledown
    elif ch.channel_type in ANNOTATION_CHANNELS and ch.resolution < resolution and ch.propagate not in [PROPAGATED]:  
      effcorner, effdim = self._zoomoutCutout ( ch, corner, dim, resolution )
      effresolution = ch.resolution
    # this is the default path when not scaling up the resolution
    else:
      # get the size of the image and cube
      effcorner = corner
      effdim = dim
      effresolution = resolution 

    # Round to the nearest larger cube in all dimensions
    zstart = effcorner[2]//zcubedim
    ystart = effcorner[1]//ycubedim
    xstart = effcorner[0]//xcubedim

    znumcubes = (effcorner[2]+effdim[2]+zcubedim-1)//zcubedim - zstart
    ynumcubes = (effcorner[1]+effdim[1]+ycubedim-1)//ycubedim - ystart
    xnumcubes = (effcorner[0]+effdim[0]+xcubedim-1)//xcubedim - xstart
  
    # use the requested resolution
    if zscaling == 'nearisotropic' and self.datasetcfg.nearisoscaledown[resolution] > 1:
      dbname = ch.getNearIsoTable(resolution)
    else:
      dbname = ch.getTable(effresolution)

    incube = Cube.CubeFactory ( cubedim, ch.channel_type, ch.channel_datatype, timerange=timerange )
    outcube = Cube.CubeFactory([xnumcubes*xcubedim, ynumcubes*ycubedim, znumcubes*zcubedim], ch.channel_type, ch.channel_datatype, timerange=timerange)
                                        
    # Build a list of indexes to access
    listofidxs = []
    for z in range ( znumcubes ):
      for y in range ( ynumcubes ):
        for x in range ( xnumcubes ):
          mortonidx = XYZMorton ( [x+xstart, y+ystart, z+zstart] )
          listofidxs.append ( mortonidx )

    # sort the indexes in morton order
    listofidxs.sort()
    
    # xyz offset stored for later use
    lowxyz = MortonXYZ ( listofidxs[0] )
    
    self.kvio.startTxn()

    try:


#  RBTODO this should work with listofidxs......why not.....for test_io.py
#
#      if zscaling == 'nearisotropic' and self.datasetcfg.nearisoscaledown[resolution] > 1:
#        cuboids = self.getCubes(ch, listofidxs, effresolution, True)
#      else:
#        cuboids = self.getCubes(ch, listofidxs, effresolution)

     for idx in listofidxs:
      cuboids = self.getCubes(ch, idx, resolution, list(range(timerange[0],timerange[1])))
        
        # use the batch generator interface
      for idx, timestamp, datastring in cuboids:

        # add the query result cube to the bigger cube
        curxyz = MortonXYZ(int(idx))
        offset = [ curxyz[0]-lowxyz[0], curxyz[1]-lowxyz[1], curxyz[2]-lowxyz[2] ]

        if self.NPZ:
          incube.fromNPZ(datastring[:])
        else:
          incube.fromBlosc(datastring[:])

        # apply exceptions if it's an annotation project
        if annoids!= None and ch.channel_type in ANNOTATION_CHANNELS:
          incube.data = filter_ctype_OMP ( incube.data, annoids )
          if ch.getExceptions() == EXCEPTION_TRUE:
            self.applyCubeExceptions ( ch, annoids, effresolution, idx, incube )
         
        # add it to the output cube
        outcube.addData( incube, timestamp, offset )

    except:
      self.kvio.rollback()
      raise

    self.kvio.commit()

    # if we fetched a smaller cube to zoom, correct the result
    if ch.channel_type in ANNOTATION_CHANNELS and ch.resolution > resolution:

      outcube.zoomData ( ch.resolution-resolution )

      # need to trim based on the cube cutout at resolution()
      outcube.trim ( corner[0]%(xcubedim*(2**(ch.resolution-resolution)))+xpixeloffset,dim[0], corner[1]%(ycubedim*(2**(ch.resolution-resolution)))+ypixeloffset,dim[1], corner[2]%zcubedim,dim[2] )

    # if we fetch a larger cube, downscale it and correct
    elif ch.channel_type in ANNOTATION_CHANNELS and ch.resolution < resolution and ch.propagate not in [PROPAGATED]:

      outcube.downScale (resolution - ch.resolution)

      # need to trime based on the cube cutout at resolution
      outcube.trim ( corner[0]%(xcubedim*(2**(ch.resolution-resolution))),dim[0], corner[1]%(ycubedim*(2**(ch.resolution-resolution))),dim[1], corner[2]%zcubedim,dim[2] )
      
    # need to trim down the array to size only if the dimensions are not the same
    elif dim[0] % xcubedim  == 0 and dim[1] % ycubedim  == 0 and dim[2] % zcubedim  == 0 and corner[0] % xcubedim  == 0 and corner[1] % ycubedim  == 0 and corner[2] % zcubedim  == 0:
      pass
    else:
      outcube.trim ( corner[0]%xcubedim,dim[0],corner[1]%ycubedim,dim[1],corner[2]%zcubedim,dim[2] )

    return outcube

  # alternate to getVolume that returns a annocube
  def annoCutout ( self, ch, annoids, timestamp, resolution, corner, dim, remapid=None ):
    """Fetch a volume cutout with only the specified annotation"""

    # cutout is zoom aware
    cube = self.cutout(ch, corner, dim, resolution, annoids=annoids, timerange=[timestamp,timestamp+1] )

    # KL TODO
    if remapid:
      vec_func = np.vectorize ( lambda x: np.uint32(remapid) if x != 0 else np.uint32(0) ) 
      cube.data = vec_func ( cube.data )

    return cube


  def getVoxel(self, ch, timestamp, resolution, voxel):
    """Return the identifier at a voxel"""
    
    # check propagate status, if not propagated then change voxel co-ordinates accordinig to base resolution
    if ch.propagate in [NOT_PROPAGATED, UNDER_PROPAGATION]:
      chres = ch.resolution
      if resolution > ch.resolution:
        # upsample the x and y coordinates 
        for i in range(2):
          voxel[i] = voxel[i] * (2**(resolution-ch.resolution))
      elif resolution < ch.resolution:
        # downsample the x and y coordinates 
        for i in range(2):
          voxel[i] = voxel[i] // (2**(ch.resolution-resolution))
      resolution = ch.resolution
    
    # get the size of the image and cube
    cubedim = self.datasetcfg.cubedim[resolution]
    [xoffset, yoffset, zoffset] = offset = self.datasetcfg.offset[resolution]

    # convert the voxel into zindex and offsets. Round to the nearest larger cube in all dimensions
    voxel = list(map(sub, voxel, offset))
    xyzcube = list(map(floordiv, voxel, cubedim))
    xyzoffset = list(map(mod, voxel, cubedim))
    #xyzcube = [ voxel[0]//xcubedim, voxel[1]//ycubedim, voxel[2]//zcubedim ]
    #xyzoffset =[ voxel[0]%xcubedim, voxel[1]%ycubedim, voxel[2]%zcubedim ]
    key = XYZMorton ( xyzcube )

    cube = self.getCube(ch, timestamp, key, resolution)

    if cube is None:
      return 0
    else:
      return cube.getVoxel(timestamp, xyzoffset)


  def applyCubeExceptions(self, ch, annoids, timestamp, resolution, idx, cube ):
    """Apply the expcetions to a specified cube and resolution"""

    # for the target ids
    for annoid in annoids:
      # apply exceptions
      exceptions = self.getExceptions( ch, idx, timestamp, resolution, annoid ) 
      for e in exceptions:
        cube.data[e[2],e[1],e[0]]=annoid

  
# RBTODO test zoom in and out.
  def zoomVoxels(self, voxels, resgap):
    """Convert voxels from one resolution to another based 
       on a positive number of hierarcy levels.
       This is used by both exception and the voxels data argument."""

    # correct for zoomed resolution
    newvoxels = []
    scaling = 2**(resgap)
    for vox in voxels:
      for numy in range(scaling):
        for numx in range(scaling):
          newvoxels.append ( (vox[0]*scaling + numx, vox[1]*scaling + numy, vox[2]) )
    return newvoxels


  def getLocations(self, ch, entityid, timestamp, res ):
    """Return the list of locations associated with an identifier"""

    # get the size of the image and cube
    resolution = int(res)
    
    # scale to project resolution
    if ch.resolution > resolution:
      effectiveres = ch.resolution 
    else:
      effectiveres = resolution


    voxlist = []
    
    zidxs = self.annoIdx.getIndex(ch,entityid,timestamp,resolution)

    for zidx in zidxs:

      cb = self.getCube(ch, timestamp, zidx, effectiveres) 

      # mask out the entries that do not match the annotation id
      # KL TODO
      vec_func = np.vectorize ( lambda x: entityid if x == entityid else 0 )
      annodata = vec_func ( cb.data )
    
      # where are the entries
      offsets = np.nonzero ( annodata[0,:,:,:] ) 
      voxels = np.array(list(zip(offsets[2], offsets[1], offsets[0])), dtype=np.uint32)

      # Get cube offset information
      [x,y,z] = MortonXYZ(zidx)
      xoffset = x * self.datasetcfg.cubedim[resolution][0] + self.datasetcfg.offset[resolution][0] 
      yoffset = y * self.datasetcfg.cubedim[resolution][1] + self.datasetcfg.offset[resolution][1]
      zoffset = z * self.datasetcfg.cubedim[resolution][2] + self.datasetcfg.offset[resolution][2]

      # Now add the exception voxels
      if ch.getExceptions() ==  EXCEPTION_TRUE:
        exceptions = self.getExceptions( ch, timestamp, zidx, resolution, entityid ) 
        if exceptions != []:
          voxels = np.append ( voxels.flatten(), exceptions.flatten())
          voxels = voxels.reshape(len(voxels)//3,3)

      # Change the voxels back to image address space
      [ voxlist.append([a+xoffset, b+yoffset, c+zoffset]) for (a,b,c) in voxels ] 

    # zoom out the voxels if necessary 
    if effectiveres > resolution:
      voxlist = self.zoomVoxels ( voxlist, effectiveres-resolution )

    return voxlist


  def getBoundingBox(self, ch, annids, res):
    """Return a corner and dimension of the bounding box for an annotation using the index"""
  
    # get the size of the image and cube
    resolution = int(res)

    # determine the resolution for project information
    if ch.resolution > resolution:
      effectiveres = ch.resolution 
      scaling = 2**(effectiveres-resolution)

    # if cutout is above resolution, get a large cube and scaledown
    elif ch.resolution < resolution and ch.propagate not in [PROPAGATED]:  
      effectiveres = ch.resolution 
      scaling = 2**(effectiveres-resolution)

    else:
      effectiveres = resolution
      scaling=1

    # all boxes in the indexes
    zidxs=[]
    for annid in annids:
     zidxs = itertools.chain(zidxs,self.annoIdx.getIndex(ch, annid, timestamp, effectiveres))

    # convert to xyz coordinates
    try:
      xyzvals = np.array ( [ MortonXYZ(zidx) for zidx in zidxs ], dtype=np.uint32 )
    # if there's nothing in the chain, the array creation will fail
    except:
      return None, None

    # the above will return a length zero array
    if len(xyzvals)==0:
      return None, None

    xmincube = np.amin(xyzvals[:,0]) 
    ymincube = np.amin(xyzvals[:,1]) 
    zmincube = np.amin(xyzvals[:,2]) 

    xmaxcube = np.amax(xyzvals[:,0]) 
    ymaxcube = np.amax(xyzvals[:,1]) 
    zmaxcube = np.amax(xyzvals[:,2]) 

    # intialize min/max values
    [xmin, ymin, zmin] = self.datasetcfg.imagesz[effectiveres]
    xmax = 0
    ymax = 0
    zmax = 0
    
    # must reinitialize the generator
    zidxs = itertools.chain(zidxs,self.annoIdx.getIndex(ch, annid, timestamp, effectiveres))
    for zidx in zidxs:
  
      [ xcube, ycube, zcube ] = MortonXYZ(zidx) 

      # load the cube if you need it
      if xcube == xmincube or ycube == ymincube or zcube == zmincube or xcube == xmaxcube or ycube == ymaxcube or zcube == zmaxcube:
        cb = self.getCube(ch, timestamp, zidx, effectiveres) 

        for annid in annids:

          # is it a min/max cube in any dimension
          # data are in z,y,x
          if xcube == xmincube:
            xmin = min ( xmin, min ( np.where(cb.data == annid )[2] ))
          if ycube == ymincube:
            ymin = min ( ymin, min ( np.where(cb.data == annid )[1] ))
          if zcube == zmincube:
            zmin = min ( zmin, min ( np.where(cb.data == annid )[0] ))
          if xcube == xmaxcube:
            xmax = max ( xmax, max ( np.where(cb.data == annid )[2] ))
          if ycube == ymaxcube:
            ymax = max ( ymax, max ( np.where(cb.data == annid )[1] ))
          if zcube == zmaxcube:
            zmax = max ( zmax, max ( np.where(cb.data == annid )[0] ))

    # convert to cube/scale coordinates
    cubedim = self.datasetcfg.cubedim [ effectiveres ] 

    xmin = int((xmincube*cubedim[0] + xmin) * scaling)
    ymin = int((ymincube*cubedim[1] + ymin) * scaling)
    zmin = (zmincube*cubedim[2] + zmin) 
    xmax = int((xmaxcube*cubedim[0] + xmax + 1) * scaling)
    ymax = int((ymaxcube*cubedim[1] + ymax + 1) * scaling)
    zmax = (zmaxcube*cubedim[2] + zmax + 1) 

    corner = [xmin, ymin, zmin]
    dim = [xmax-xmin, ymax-ymin, zmax-zmin]

    return (corner, dim)


  def getBoundingCube ( self, ch, annids, timestamp, res ):
    """Return a corner and dimension of the bounding cuboid for an annotation using the index"""
  
    # get the size of the image and cube
    resolution = int(res)

    # determine the resolution for project information
    if ch.resolution > resolution:
      effectiveres = ch.resolution 
      scaling = 2**(effectiveres-resolution)
    else:
      effectiveres = resolution
      scaling=1

    # all boxes in the indexes
    zidxs=[]
    for annid in annids:
      zidxs = itertools.chain(zidxs,self.annoIdx.getIndex(ch, annid, timestamp, effectiveres ))
    
    # convert to xyz coordinates
    try:
      xyzvals = np.array ( [ MortonXYZ(zidx) for zidx in zidxs ], dtype=np.uint32 )
    # if there's nothing in the chain, the array creation will fail
    except:
      return None, None

    cubedim = self.datasetcfg.cubedim [ resolution ] 

    # find the corners
    xmin = min(xyzvals[:,0]) * cubedim[0] * scaling
    xmax = (max(xyzvals[:,0])+1) * cubedim[0] * scaling
    ymin = min(xyzvals[:,1]) * cubedim[1] * scaling
    ymax = (max(xyzvals[:,1])+1) * cubedim[1] * scaling
    zmin = min(xyzvals[:,2]) * cubedim[2]
    zmax = (max(xyzvals[:,2])+1) * cubedim[2]

    corner = [ xmin, ymin, zmin ]
    dim = [ xmax-xmin, ymax-ymin, zmax-zmin ]

    return (corner,dim)


  def annoCubeOffsets ( self, ch, dataids, timestamp, resolution, remapid=False):
    """an iterable on the offsets and cubes for an annotation"""
   
    [xcubedim, ycubedim, zcubedim] = cubedim = self.datasetcfg.cubedim [ resolution ]

    # if cutout is below resolution, get a smaller cube and scaleup
    if ch.resolution > resolution:
      effectiveres = ch.resolution 
    else:
      effectiveres = resolution
    
    zidxs = set()
    for did in dataids:
      zidxs |= set ( self.annoIdx.getIndex(ch, did, timestamp, effectiveres))

    for zidx in zidxs:

      # get the cube and mask out the non annoid values
      cb = self.getCube(ch, timestamp, zidx, effectiveres) 
      if not remapid:
        cb.data = filter_ctype_OMP ( cb.data, dataids )
      else: 
        cb.data = filter_ctype_OMP ( cb.data, dataids )
        # KL TODO
        vec_func = np.vectorize ( lambda x: np.uint32(remapid) if x != 0 else np.uint32(0) ) 
        cb.data = vec_func ( cb.data )

      # zoom the data if not at the right resolution and translate the zindex to the upper resolution
      (xoff,yoff,zoff) = MortonXYZ ( zidx )
      offset = (xoff*xcubedim, yoff*ycubedim, zoff*zcubedim)

      # if we're zooming, so be it
      if resolution < effectiveres:
        cb.zoomData ( effectiveres-resolution )
        offset = (offset[0]*(2**(effectiveres-resolution)),offset[1]*(2**(effectiveres-resolution)),offset[2])

      # add any exceptions
      # Get exceptions if this DB supports it
      if ch.getExceptions() == EXCEPTION_TRUE:
        for exid in dataids:
          exceptions = self.getExceptions(ch, timestamp, zidx, effectiveres, exid) 
          if exceptions != []:
            if resolution < effectiveres:
                exceptions = self.zoomVoxels ( exceptions, effectiveres-resolution )
            # write as a loop first, then figure out how to optimize 
            # exceptions are stored relative to cube offset
            for e in exceptions:
              if not remapid:
                cb.data[e[2],e[1],e[0]]=exid
              else:
                cb.data[e[2],e[1],e[0]]=remapid

      yield (offset,cb.data)

  
  def deleteAnnoData ( self, ch, annoid ):
    """Delete the voxel data from the database for Annotation Id"""
    
    resolutions = self.datasetcfg.resolutions

    self.kvio.startTxn()

    try:

      for res in resolutions:
      
        # get the cubes that contain the annotation
        # RB timestamp 0 for now.
        zidxs = self.annoIdx.getIndex(ch, annoid, 0, res,True)
        
        # delete annotation data
        for key in zidxs:
          cube = self.getCube(ch, 0, key, res, update=True)
          # KL TODO
          vec_func = np.vectorize ( lambda x: np.uint32(0) if x == annoid else x )
          cube.data = vec_func ( cube.data )
          # remove the exceptions
          if ch.getExceptions == EXCEPTION_TRUE:
            self.kvio.deleteExceptions(ch, key, 0, res, annoid)
          self.putCube(ch, 0, key, res, cube)
        
      # delete index
      self.annoIdx.deleteIndex(ch, annoid,resolutions)

    except:
      self.kvio.rollback()
      raise

    self.kvio.commit()

  def writeBlazeCuboid(self, ch, corner, resolution, cuboiddata, timerange=None):
    """Write a blaze cuboid to the database"""

    # get the size of the image and cube
    cubedim = self.datasetcfg.cubedim [ resolution ]
    
    # Round to the nearest larger cube in all dimensions
    start = [xstart, ystart, zstart] = list(map(floordiv, corner, cubedim))
    
    # Generate the zindex for the BlazeCuboid
    zidx = XYZMorton(start)
    
    # insert the cuboid in the database
    self.kvio.putCube(ch, timestamp, zidx, resolution, cuboiddata, update=True)


#  def writeCuboids(self, ch, corner, resolution, cuboiddata, timerange=[0,0]):
#    """Write an arbitary size data to the database"""
#
#    # dim is in xyz, data is in zyx order
#    dim = cuboiddata.shape[::-1]
#    
#    # get the size of the image and cube
#    cubedim = self.datasetcfg.cubedim [ resolution ]
#    
#    # round to the nearest larger cube in all dimensions
#    start = [xstart, ystart, zstart] = map(floordiv, corner, cubedim)
#
#    znumcubes = (corner[2]+dim[2]+zcubedim-1)//zcubedim - zstart
#    ynumcubes = (corner[1]+dim[1]+ycubedim-1)//ycubedim - ystart
#    xnumcubes = (corner[0]+dim[0]+xcubedim-1)//xcubedim - xstart
#
#    offset = [xoffset, yoffset, zoffset] = map(mod, corner, cubedim)
#    
#    databuffer = np.zeros ([znumcubes*zcubedim, ynumcubes*ycubedim, xnumcubes*xcubedim], dtype=cuboiddata.dtype )
#    databuffer [ zoffset:zoffset+dim[2], yoffset:yoffset+dim[1], xoffset:xoffset+dim[0] ] = cuboiddata 
#
#    incube = Cube.CubeFactory(cubedim, ch.channel_type, ch.channel_datatype)
#    
#    self.kvio.startTxn()
#    
#    listofidxs = []
#    listofcubes = []
#
#    try:
#      for z in range(znumcubes):
#        for y in range(ynumcubes):
#          for x in range(xnumcubes):
#
#            listofidxs.append(XYZMorton ([x+xstart,y+ystart,z+zstart]))
#            incube.data = databuffer [ z*zcubedim:(z+1)*zcubedim, y*ycubedim:(y+1)*ycubedim, x*xcubedim:(x+1)*xcubedim ]
#            listofcubes.append(incube.toBlosc())
#
#      self.putCubes(ch, listofidxs, resolution, listofcubes, update=False)
#
#    except:
#      self.kvio.rollback()
#      raise
#
#    self.kvio.commit()


  def writeCuboid(self, ch, corner, resolution, cuboiddata, timerange=[0,1]):
    """
    Write a 4D volume to the key-value store.

    :param ch: Channel to write data to
    :type ch: Class Channel
    :param corner: Starting corner to write to
    :type corner: array-like or one-dimensional list of int
    :param resolution: Resolution to write at
    :type resolution: int
    :param cuboiddata: Data to be written
    :type cuboiddata: Multi-dimensional numpy array
    :type timerange: one-dimensional list of int
    :param timerange: Range of time. Defaults to None

    :returns: None
    """
   
    dim = cuboiddata.shape[::-1][:-1]

    # get the size of the image and cube
    [xcubedim, ycubedim, zcubedim] = cubedim = self.datasetcfg.cubedim[resolution]

    # round to the nearest larger cube in all dimensions
    start = [xstart, ystart, zstart] = list(map(floordiv, corner, cubedim))

    znumcubes = (corner[2]+dim[2]+zcubedim-1)//zcubedim - zstart
    ynumcubes = (corner[1]+dim[1]+ycubedim-1)//ycubedim - ystart
    xnumcubes = (corner[0]+dim[0]+xcubedim-1)//xcubedim - xstart

    offset = [xoffset, yoffset, zoffset] = list(map(mod, corner, cubedim))
    
    databuffer = np.zeros([timerange[1]-timerange[0]]+[znumcubes*zcubedim, ynumcubes*ycubedim, xnumcubes*xcubedim], dtype=cuboiddata.dtype )
    databuffer[:, zoffset:zoffset+dim[2], yoffset:yoffset+dim[1], xoffset:xoffset+dim[0]] = cuboiddata 

    self.kvio.startTxn()

    try:
      for z in range(znumcubes):
        for y in range(ynumcubes):
          for x in range(xnumcubes):
            for timestamp in range(timerange[0], timerange[1], 1):

              zidx = XYZMorton([x+xstart,y+ystart,z+zstart])

              cube = self.getCube(ch, timestamp, zidx, resolution, update=True)

              # overwrite the cube -- one timestamp in cube so write to time 0
              cube.overwrite(0, databuffer[timestamp-timerange[0], z*zcubedim:(z+1)*zcubedim, y*ycubedim:(y+1)*ycubedim, x*xcubedim:(x+1)*xcubedim])

              # update in the database
              self.putCube(ch, timestamp, zidx, resolution, cube)

    except:
      self.kvio.rollback()
      raise

    self.kvio.commit()
