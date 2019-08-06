# espa-scheduler
A Mesos scheduler for ESPA Processing

# install
pip install git+https://github.com/benoitc/http-parser
pip install pymesos

# Configuration - Required
export MESOS_PRINCIPAL=bart
export MESOS_SECRET=$!MP$0N
export MESOS_MASTER=127.99.88.77
export ESPA_API=http://127.0.0.1:9876

# Configuration - Optional (these are the defaul values)
export LANDSAT_FREQUENCY=3
export MODIS_FREQUENCY=2
export VIIRS_FREQUENCY=1
export PLOT_FREQUENCY=1
export MESOS_CORE_LIMIT=1
export PRODUCT_REQUEST_COUNT=50
export TASK_CPU=1
export TASK_MEM=5120
export OFFER_REFUSE_SECONDS=30
