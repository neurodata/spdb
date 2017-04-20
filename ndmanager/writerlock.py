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

import time
from django.conf import settings
import redis
from spdb.ndmanager.readerwriterlock import ReaderWriterLock
from spdb.spatialdberror import SpatialDBError
import logging
logger = logging.getLogger("neurodata")

class WriterLock(object):

  def __init__(self, func):
    try:
      self.rw_lock = ReaderWriterLock()
      self.func = func
    except Exception as e:
      logger.error("{}".format(e))
      raise SpatialDBError("{}".format(e))
 
  def __get__(self, obj, type=None):
    new_func = self.func.__get__(obj, type)
    return self.__class__(new_func)

  def __call__(self, *args, **kwargs):
    self.rw_lock.acquire_write()
    return_value = self.func(*args, **kwargs)
    self.rw_lock.release_write()
    return return_value
