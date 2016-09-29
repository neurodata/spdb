#### Using the ndmanager service

* The service is deployed as an upstart job and one can start/stop/reload/restart it as any other upstart service
* The logs for the service are located in /var/log/neurodata/ndmanager.log
* Start/Stop/Restart ndmanager service
```console
sudo service ndmanager start/stop/restart/reload
```

##### Note
* The cache manager kicks in when redis consumption of memory breaches 60% and this value can be tweaked in the django.settings file (parameter named REDIS_MEMORY_RATIO). 
* We strongly recommend using an instance with a memory atleast 64GB to fully utilize the benefits of the cache. One can use a smaller memory instance then it is advised to set this value to be lower 30% or 40%.
