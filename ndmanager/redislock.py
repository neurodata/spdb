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

from django.conf import settings
import redis
import logging
logger = logging.getLogger("neurodata")

class RedisLock(object):

  def __init__(self, func):
    try:
      # self.dlm = redlock.Redlock([{"host": settings.REDIS_INDEX_HOST, "port": settings.REDIS_INDEX_PORT, "db": settings.REDIS_INDEX_DB}, ])
      self.client = redis.StrictRedis(host=settings.REDIS_INDEX_HOST, port=settings.REDIS_INDEX_PORT, db=settings.REDIS_INDEX_DB)
      self.func = func
      self.lua_lock = None
    except Exception as e:
      print e
      raise
  
  def lock(self, timeout=None, sleep=0.1, blocking_timeout=None):
    try:
      # self.key = self.dlm.lock(settings.REDIS_LOCK, time)
      self.lua_lock = self.client.lock(settings.REDIS_LOCK, timeout=timeout, sleep=sleep, blocking_timeout=blocking_timeout)
      value = self.lua_lock.acquire()
      logger.warning("[MANAGER] Entering Lock")
      print "entering lock"
    except Exception as e:
      print e
      raise

  def unlock(self):
    try:
      # self.dlm.unlock(self.key)
      self.lua_lock.release()
      logger.warning("[MANAGER] Exiting Lock")
      print "exiting lock" 
    except Exception as e:
      print e
      raise
  
  def __get__(self, obj, type=None):
    new_func = self.func.__get__(obj, type)
    return self.__class__(new_func)

  def __call__(self, *args, **kwargs):
    self.lock()
    return_value = self.func(*args, **kwargs)
    self.unlock()
    return return_value
