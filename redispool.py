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

import redis
import django
from django.conf import settings
from spatialdberror import SpatialDBError
import logging
logger = logging.getLogger("neurodata")


class RedisPool(object):
  redis_pool = None

  def __init__(self):
    if self.redis_pool is not None:
      logger.error("An instantiation of the class already exists.")
      raise ValueError("An instantiation of the class already exists.")
  
  @classmethod
  def getPool(cls):
    if cls.redis_pool is None:
      cls.redis_pool = redis.BlockingConnectionPool(host=settings.REDIS_INDEX_HOST, port=settings.REDIS_INDEX_PORT, db=settings.REDIS_INDEX_DB, max_connections=settings.REDIS_INDEX_MAX_CONNECTIONS)
    return cls.redis_pool
