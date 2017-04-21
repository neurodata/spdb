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
import os
import sys
sys.path.append('../../django')
import ND.settings
os.environ['DJANGO_SETTINGS_MODULE'] = 'ND.settings'
import django
from django.conf import settings
from spdb.ndmanager.redismanager import RedisManager
import logging
logging.basicConfig(filename='/var/log/neurodata/ndmanager.log', 
                    filemode = 'w',
                    format = '[%(asctime)s] %(levelname)s [%(name)s:%(module)s:%(lineno)s]%(message)s',
                    datefmt = '%d/%b/%Y %H:%M:%S',
                    level = logging.DEBUG)
logger = logging.getLogger('ndmanager')


def daemonfunction():
    redis_manager = RedisManager()
    # check if memory cacpity has been breached 
    if redis_manager.memoryUpperBound():
      logger.info("[MANAGER]: Memory reached capacity. Removing Indices.")
      # remove LRU indexes from memory
      redis_manager.emptyMemory()

def run():
  """Run the ndmanager process"""
  logger.info("[MANAGER]: Starting manager.")
  # iterate over this loop
  while(True):
    daemonfunction()
    # sleep for 2 seconds and then continue
    time.sleep(2)

if __name__ == '__main__':
  # call run on main
  run()
