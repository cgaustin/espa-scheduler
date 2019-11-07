[![Build Status](https://travis-ci.org/USGS-EROS/espa-scheduler.svg?branch=develop)](https://travis-ci.org/USGS-EROS/espa-scheduler)

# ESPA Scheduler

A Mesos scheduler for ESPA Processing


## Deploying

espa-scheduler is run as a Docker container:

```
export MESOS_PRINCIPAL=bart
export MESOS_SECRET=$!MP$0N
export MESOS_MASTER=127.99.88.77
export ESPA_API=http://127.0.0.1:9876
export TASK_IMAGE=usgseros/espa-scheduler:latest
export AUXILIARY_MOUNT=/usr/local/bridge/usgs/auxiliary
export AUX_DIR=/usr/local/auxiliary
export STORAGE_MOUNT=/usr/local/espa-storage
export ESPA_STORAGE=/espa-storage
export ASTER_GED_SERVER_NAME=http://127.0.0.1:6688

docker run -p 9876:${HTTP_PORT} -e MESOS_PRINCIPAL=${MESOS_PRINCIPAL} \
                                -e MESOS_SECRET=${MESOS_SECRET} \
                                -e MESOS_MASTER=${MESOS_MASTER} \
                                -e ESPA_API=${ESPA_API} \
                                -e TASK_IMAGE=${TASK_IMAGE} \
                                -e AUXILIARY_MOUNT=${AUXILIARY_MOUNT} \
                                -e AUX_DIR=${AUX_DIR} \
                                -e STORAGE_MOUNT=${STORAGE_MOUNT} \
                                -e ESPA_STORAGE=${ESPA_STORAGE} \
                                -e ASTER_GED_SERVER_NAME=${ASTER_GED_SERVER_NAME} \
                                -it usgseros/espa-scheduler:latest
```


ESPA Scheduler is configured using these environment variables:

| ENV                     | Description                                                 | Default |
|-------------------------|-------------------------------------------------------------|---------|
| `MESOS_PRINCIPAL`       | Principal value for authenticating to your Mesos instance   |         |
| `MESOS_SECRET`          | Secret value for authenticating to your Mesos instance      |         |
| `MESOS_MASTER`          | The IP Address of the Mesos Master                          |         |
| `ESPA_API`              | The URL for the ESPA API instance to request work from      |         |
| `ESPA_USER`             | The ESPA user for the dev/tst/ops environment               |         |
| `ESPA_GROUP`            | The ESPA user-group for the dev/tst/ops environment         |         |
| `PRODUCT_REQUEST_COUNT` | The number of units to return from the ESPA API per request | 50      |   
| `MAX_CPU`               | The max number of CPUs to use on the system at a time       | 10      |
| `TASK_CPU`              | The number of CPUs to assign each Task                      | 1       |
| `TASK_MEM`              | The amount of memory (MB) to assign each Task               | 5120    |
| `TASK_IMAGE`            | The Docker Image to use for executing a Task                |         |
| `OFFER_REFUSE_SECONDS`  | The amount of time (seconds) to refuse subsequent offers    | 30      |
| `AUXILIARY_MOUNT`       | The local directory to mount to the ${AUX_DIR}              |         |
| `AUX_DIR`               | The dir mounted to ${AUXILIARY_MOUNT}, exposed to Task too  |         |
| `STORAGE_MOUNT`         | The local directory mounted to ${ESPA_STORAGE}              |         |
| `ESPA_STORAGE`          | The dir mounted to ${STORAGE_MOUNT}, exposed to Task too    |         |
| `ASTER_GED_SERVER_NAME` | Aster Data host, exposed to Task                            |         |
| `LANDSAT_FREQUENCY`     | How often to process Landsat units, given other frequencies | 3       |
| `MODIS_FREQUENCY`       | How often to process Modis units, given other frequencies   | 2       |
| `VIIRS_FREQUENCY`       | How often to process Viirs units, given other frequencies   | 1       |
| `SENTINEL_FREQUENCY`    | How often to process Sentinel units, given other frequencies| 1       |
| `PLOT_FREQUENCY`        | How often to process Plot units, given other frequencies    | 1       |
| `URS_MACHINE`           | The URL for accessing Earthdata login                       |         |
| `URS_LOGIN`             | The ESPA Earthdata login username                           |         |
| `URS_PASSWORD`          | The ESPA Earthdata login password                           |         |


# Operation
When the scheduler receives offers from Mesos, it'll check 2 things before accepting any offers and
launching new tasks:
1) The configuration value for 'run_mesos_tasks' in the ESPA API. if 'True', new tasks can be spawned
2) The current number of CPUs being occupied by running Tasks, and whether that number exceeds the 
   ${MAX_CPU} configuration value


# Building the image
docker build -t espa-scheduler:1.0.0 .


