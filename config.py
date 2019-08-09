import os
import itertools

def default_env(variable, value, operator=None):
    default = [variable, value]
    upcase_variable = variable.upper()
    if os.environ.get(upcase_variable):
        new_var = os.environ.get(upcase_variable)
        if operator:
            new_var = operator(new_var)
        default = [variable, new_var]
    return default

def product_frequency():
    de = default_env
    frequency = []
    frequency.append(['landsat', de('landsat_frequency', 3, int)[1]])
    frequency.append(['modis',   de('modis_frequency',   2, int)[1]])
    frequency.append(['viirs',   de('viirs_frequency',   1, int)[1]])
    frequency.append(['plot',    de('plot_frequency',    1, int)[1]])
    return list(itertools.chain.from_iterable(itertools.repeat(x[0], x[1]) for x in frequency))

def config():
    de = default_env
    return dict([
        de('mesos_principal', None),
        de('mesos_secret', None),
        de('mesos_master', None),
        de('mesos_core_limit', None),
        ['product_frequency', product_frequency()],
        de('espa_api', 'http://localhost:9876/production-api/v0'),
        de('product_request_count', 50, int),
        de('max_cpu', 10, int),
        de('task_cpu', 1, float),
        de('task_mem', 5120, int), # 5G
        de('task_image', None),
        de('offer_refuse_seconds', 30, int),
        de('auxiliary_mount', '/usr/local/usgs/bridge/auxiliaries'),
        de('aux_dir', '/usr/local/auxiliaries'), # name required by processing libs
        de('storage_mount', '/espa-storage'),
        de('espa_storage', '/espa-storage'), # name required by processing libs
        de('espa_xmlrpc', None),
        de('espa_cache_host_list', None),
        de('aster_ged_server_name', None)
    ])
