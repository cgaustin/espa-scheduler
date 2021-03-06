from addict import Dict
import json

def env_vars(cfg):
    """Return list of dicts defining task environment vars"""
    return [{"name":"ESPA_STORAGE",          "value":cfg.get('espa_storage')},
            {"name":"ESPA_API",              "value":cfg.get('espa_api')},
            {"name":"ASTER_GED_SERVER_NAME", "value":cfg.get('aster_ged_server_name')},
            {"name":"AUX_DIR",               "value":cfg.get('aux_dir')},
            {"name":"URS_MACHINE",           "value":cfg.get('urs_machine')},
            {"name":"URS_LOGIN",             "value":cfg.get('urs_login')},
            {"name":"URS_PASSWORD",          "value":cfg.get('urs_password')}]

def volumes(cfg):
    """Return list of dicts defining task container volumes"""
    aux_mount  = cfg.get('auxiliary_mount')
    aux_dest   = cfg.get('aux_dir')
    stor_mount = cfg.get('storage_mount')
    stor_dest  = cfg.get('espa_storage')

    return [{"container_path": aux_dest,  "host_path": aux_mount,  "mode": "RW"},
            {"container_path": stor_dest, "host_path": stor_mount, "mode": "RW"}]

def resources(cpus, memory, disk):
    """Return list of task resource dicts"""
    return [{'name':'cpus', 'type':'SCALAR', 'scalar':{'value': cpus}},
            {'name':'mem' , 'type':'SCALAR', 'scalar':{'value': memory}},
            {'name':'disk', 'type':'SCALAR', 'scalar':{'value': disk}}]

def command(work_json):
    """Return formatted command for the task container"""
    cmd = "python /src/processing/main.py '{}'".format(json.dumps([work_json]).replace(' ', ''))
    return cmd

def build(id, offer, image_name, cpu, mem, disk, work, cfg):
    task                        = Dict()
    task.task_id.value          = id
    task.agent_id.value         = offer['agent_id']['value']
    task.name                   = 'task {}'.format(id)
    task.container.type         = 'DOCKER'
    task.container.docker.image = image_name
    task.container.volumes      = volumes(cfg)
    task.resources              = resources(cpu, mem, disk) 
    task.command.value          = command(work) #"echo espa-task && sleep 500"
    task.command.environment.variables = env_vars(cfg)
    return task
