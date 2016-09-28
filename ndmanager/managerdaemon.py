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
from daemonize import Daemonize
from redismanager import RedisManager
import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler("/var/log/neurodata/manager.log", 'w')
fh.setLevel(logging.DEBUG)
logger.addHandler(fh)
keep_fds = [fh.stream.fileno()]
logger.warning('start')

pid = '/data/test.pid'

def run():
  while(True):
    redis_manager = RedisManager()
    if redis_manager.memoryFull():
      logger.warning("[MANAGER]: Memory reached capacity. Removing Indices.")
      redis_manager.emptyMemory()
    time.sleep(2)


if __name__ == '__main__':
  # run()
  daemon = Daemonize(app='test_app', pid=pid, action=run, keep_fds=keep_fds)
  daemon.start()
