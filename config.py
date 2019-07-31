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

def config():
    de = default_env

    aux_dir = "/usr/local/auxiliaries/"
    if os.environ.get("AUX_DIR"):
        aux_dir = os.environ.get("AUX_DIR")

    frequency = []
    frequency.append(['landsat', de('landsat_frequency', 3, int)[1]])
    frequency.append(['modis',   de('modis_frequency',   2, int)[1]])
    frequency.append(['viirs',   de('viirs_frequency',   1, int)[1]])
    frequency.append(['plot',    de('plot_frequency',    1, int)[1]])
    product_frequency = list(itertools.chain.from_iterable(itertools.repeat(x[0], x[1]) for x in frequency))


    return dict([
        de('mesos_user', None),
        de('mesos_pass', None),
        de('mesos_master', None),
        de('mesos_jobscale', 1, int),
        ['product_frequency', product_frequency],
        de('espa_api', 'http://localhost:9876/production-api/v0'), # SET IN ENV
        de('task_cpu', 0.1, float),
        de('task_mem', 100, int)
    ])
