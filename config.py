import os

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

    aux_dir = "/usr/local/auxiliaries/"
    if os.environ.get("AUX_DIR"):
        aux_dir = os.environ.get("AUX_DIR")

    de = default_env
    return dict([
        de('mesos_user', None),
        de('mesos_pass', None),
        de('mesos_master', None),
        de('mesos_jobscale', 1, int),
        de('espa_api', 'http://localhost:9876/production-api/v0'), # SET IN ENV
    ])
