Sensory Service Batch Processor
===============================

Overview
--------
A worker processor for batch order processing.  Will take requests from the message queue, and then create a corresponding queue with data for a consumer.

**Start the service:**
```
    python ./sensory_service_batch_processor.py
```
The service reads data for the datasets off an AWS EFS which can be shared amoungst a large number of workers.


### Production Deployment

**Docker Container**

The recommended way to run the service is by using the official provided docker container.
The container should be deployed to a Docker Swarm as a service.

**Example**
```
docker service create \
--detach=false \
--replicas 1 \
--log-driver json-file \
--mount type=volume,volume-driver=cloudstor:aws,source=dicebox,destination=/dicebox \
--env SENSORY_SERVICE_SHARD_SIZE='100' \
--name sensorybatchprocessor shapeandshare/dicebox.sensorybatchprocessor
```

How to apply rolling updates of the service within the swarm:
```
docker service update --image shapeandshare/dicebox.sensorybatchprocessor:latest sensorybatchprocessor
```

In the examples above the Docker Swarm was deployed to AWS and had the Cloudstor:aws plugin enabled and active.
The sensory batch processor containers will store and read data from the shared storage.

**Global shared Cloudstor volumes mounted by all tasks in a swarm service.**

The below command is an example of how to create the shared volume within the docker swarm:
```
docker volume create -d "cloudstor:aws" --opt backing=shared dicebox
```


Contributing
------------
1. Fork the repository on Github
2. Create a named feature branch (like `add_component_x`)
3. Write your change
4. Write tests for your change (if applicable)
5. Run the tests, ensuring they all pass
6. Submit a Pull Request using Github

License and Authors
-------------------
MIT License

Copyright (c) 2017-2019 Joshua C. Burt

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.