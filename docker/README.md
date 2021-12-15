# Docker Build for Block Aggregator 


## The Sturcutre for Building the Block Aggregator Image

Under the "docker" directory,

 - The `certificates` directory is the place to hold the actual CA certificate used to enable the SSL communication between the Block Aggregator and the ClickHouse server.
 - The `bins` directory contains the utiltiy tools used for troubleshooting in the runtime execution environment of Block Aggregator. Currently, the tools `dstat` and `pstack` are provided.
 - The `conf` directory holds the clickhouse-client configuration file needed to enable the SSL communication between the Block Aggregator and the ClickHouse server.
 - The `nucolumnarbins` contains the shell scripts: 
    - `startup.sh` to start the docker container.
    - `crash_dump.sh` and `thread_dump.sh`, to dump the core file and produce thread dump in the runtime environment of Block Aggregator.

 - The `vimrc` file is used for the vim editor configuration in the runtime environment of Block Aggregator
 - The `Dockerfile_nucolumnaraggr` file is the actual docker file.
 - The `build_aggregator_image.sh` file is to build the docker image.
 - The `build-docker.sh` file holds an example script to invoke the actual docker build command on the docker file `Dockerfile_nucolumnaraggr`.
 
 
## Steps for Docker Build
  

#### Step 1: To Install the CA Certifcate 

Before invoking the docker image build, the actual CA Certificate file needs to be placed under the directory `certificates`. Afterwards,  the docker file  `Dockerfile_nucolumnaraggr` needs to be modified, to substitute the placeholder `$MY_CA_CERTIFICATE_FILE` with the actual CA certificate file name.  

Similarly, the file called `config.xml` under `./conf/clickhouse-client` needs to be modified, to substitute the placeholder `$MY_CA_CERTIFICATE_FILE` with the actual CA certificate file name.


#### Step 2: to Modify build-docker.sh 

Depending on the actual docker build environment, the docker build command can be different. Currently, the file `build-docker.sh` contains the example of  the scripts to invoke the docker image build command in a particular Jenkin CI server environment. This build-docker.sh needs to be modified to match your own docker build environment. 

This script should have the following inputs:

 - Input Parameter 1: the docker file specified in `build_aggregator_image.sh`
 - Input Parameter 2: the script directory specified in `build_aggregator_image.sh`
 - Input Parameter 3: the docker image repository specified in `build_aggregator_image.sh`
 - Input Parameter 4: the image tag chosen in `build_aggregator_image.sh`

The script output is the docker image pushed into the image repo destination.

  
#### Step 3: to Invoke Docker Image Build 
The actual image build is via the following command under the directory `docker`:

``` shell scripts
./build_aggregator_image.sh <mode> <image_repo>
```
   
The mode can be `release` or `debug`, which corresponds to the two build modes for the `cmake` build at the top directory of this repo. 

The `image_repo` is the actual image repository that will hold the output of the docker image.
