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
from redismanager import RedisManager
import logging
logging.basicConfig(filename='/var/log/neurodata/ndmanager.log', 
                    filemode = 'w',
                    format = '[%(asctime)s] %(levelname)s [%(name)s:%(module)s:%(lineno)s]%(message)s',
                    datefmt = '%d/%b/%Y %H:%M:%S',
                    level = logging.DEBUG)
logger = logging.getLogger('ndmanager')
# import pdb; pdb.set_trace()
# logger = logging.getLogger('neurodata')

# logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)
# fh = logging.FileHandler("/var/log/neurodata/manager.log", 'w')
# fh.setLevel(logging.DEBUG)
# logger.addHandler(fh)

pid = '/data/test.pid'

def run():
  logger.info("[MANAGER]: Starting manager.")
  while(True):
    redis_manager = RedisManager()
    if redis_manager.memoryFull():
      logger.info("[MANAGER]: Memory reached capacity. Removing Indices.")
      redis_manager.emptyMemory()
    time.sleep(2)


if __name__ == '__main__':
  run()
