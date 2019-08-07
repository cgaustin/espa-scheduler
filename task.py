def env_vars(cfg):
    """Return list of dicts defining task environment vars"""
    return [{"name":"ESPA_FOO", "value":"666"}]

def volumes(cfg):
    """Return list of dicts defining task container volumes"""
    return [{"container_path": "/usgs",
             "host_path": "/usr/local/usgs",
             "mode": "RW"}]

def resources(cpus, memory):
    """Return list of task resource dicts"""
    return [{'name':'cpus', 'type':'SCALAR', 'scalar':{'value': cpus}},
            {'name':'mem' , 'type':'SCALAR', 'scalar':{'value': memory}}]

def command(work_json):
    """Return formatted command for the task container"""
    cmd = "run.py {}".format(work_json)
    return cmd

def build(id, offer, image_name, cpu, mem, work, cfg):
    task                        = Dict()
    task.task_id.value          = id
    task.agent_id.value         = offer.agent_id.value
    task.name                   = 'task {}'.format(id)
    task.container.type         = 'DOCKER'
    task.container.docker.image = image_name
    task.container.volumes      = volumes(cfg)
    task.resources              = resources(cpu, mem) 
    task.command.value          = command(work)
    task.command.environment.variables = env_vars(cfg)
    return task
