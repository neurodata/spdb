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

import os
import time
import redis
from contextlib import closing
from spdb.redispool import RedisPool
from django.conf import settings
import logging
logger=logging.getLogger("neurodata")

READER_COUNTER = 'reader_counter'
WRITER_COUNTER = 'writer_counter'
MESSAGE_CHANNEL = 'message_channel'
MESSAGE = 'ready'

class ReaderWriterLock(object):

  def __init__(self):
    self.client = redis.StrictRedis(connection_pool=RedisPool.blocking_pool)
    self.set_reader_count()
    self.set_writer_count()
    self._lua_lock = self.client.lock(settings.REDIS_LOCK, timeout=None, sleep=0.001, blocking_timeout=None)
  
  def acquire_read(self):
    if self._lua_lock.acquire() and self.get_writer_count() == 0:
      logger.debug("Entering Acquire Read Lock {}. Time:{}".format(os.getpid(), time.time()))
      try:
        self.increment_reader_count()
      finally:
        self._lua_lock.release()
        logger.debug("Exiting Acquire Read Lock {}. Time:{}".format(os.getpid(), time.time()))

  def release_read(self):
    if self._lua_lock.acquire():
      logger.debug("Entering Release Read Lock {}. Time:{}".format(os.getpid(), time.time()))
      try:
        self.decrement_reader_count()
        if not self.get_reader_count():
          self.notify_all()
      finally:
        self._lua_lock.release()
        logger.debug("Exiting Release Read Lock {}. Time:{}".format(os.getpid(), time.time()))

  def acquire_write(self):
    if self._lua_lock.acquire():
      logger.debug("Entering Acquire Write Lock {}. Time:{}".format(os.getpid(), time.time()))
      while self.get_reader_count() > 0:
        self.wait()
        logger.debug("Writer waking up {}. Time:{}".format(os.getpid(), time.time()))

  def release_write(self):
    self.set_writer_count(0)
    self._lua_lock.release()
    logger.debug("Exiting Release Write Lock {}. Time:{}".format(os.getpid(), time.time()))
  
  def notify_all(self):
    logger.debug("Notify all {}. Time:{}".format(os.getpid(), time.time()))
    self.client.publish(MESSAGE_CHANNEL, MESSAGE)

  def wait(self):
    with closing(self.client.pubsub()) as pubsub:
      self.set_writer_count(1)
      pubsub.subscribe(MESSAGE_CHANNEL)
      self._lua_lock.release()
      logger.debug("Exiting Acquire Write Lock Before Wait {}. Time:{}".format(os.getpid(), time.time()))
      for msg in pubsub.listen():
        if msg['data'] == MESSAGE:
          self._lua_lock.acquire()
          logger.debug("Entering Acquire Write Lock After Wait {}. Time:{}".format(os.getpid(), time.time()))
          return
  
  def get_writer_count(self):
    return int(self.client.get(WRITER_COUNTER))

  def set_writer_count(self, value=0):
    self.client.setnx(WRITER_COUNTER, value)

  def increment_writer_count(self):
    logger.debug("Incrementing writer count {}. Time:{}".format(os.getpid(), time.time()))
    self.client.incr(WRITER_COUNTER)

  def decrement_writer_count(self):
    logger.debug("Decrementing writer count {}. Time:{}".format(os.getpid(), time.time()))
    self.client.decr(WRITER_COUNTER)
  
  def get_reader_count(self):
    return int(self.client.get(READER_COUNTER))

  def set_reader_count(self, value=0):
    self.client.setnx(READER_COUNTER, value)

  def increment_reader_count(self):
    logger.debug("Incrementing reader count {}. Time:{}".format(os.getpid(), time.time()))
    self.client.incr(READER_COUNTER)

  def decrement_reader_count(self):
    logger.debug("Decrementing reader count {}. Time:{}".format(os.getpid(), time.time()))
    self.client.decr(READER_COUNTER)
