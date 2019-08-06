import sys
import uuid
import socket
import os
import signal
import logging

import espa
from config import config

from datetime import datetime
from pymesos import MesosSchedulerDriver, Scheduler, encode_data
from addict import Dict
from threading import Thread
from copy import deepcopy

def right_now():
    return datetime.now().strftime('%m-%d-%Y:%H:%M:%S.%f')

class EspaTask:
    def __init__(self, command=None, cpu, mem):
        self.taskId = "ESPATask-{}".format(right_now())
        self.command = command
        self.cpu = cpu
        self.mem = mem

    def toString(self):
        str = "TaskId:%s, Cmd:%s, CPU=%d, MEM=%d" % (self.taskId, self.command, self.cpu, self.mem)
        return str

    def __getResource(self, res, name):
        for r in res:
            if r.name == name:
                return r.scalar.value
        return 0.0

    def __updateResource(self, res, name, value):
        if value <= 0:
            return
        for r in res:
            if r.name == name:
                r.scalar.value -= value
        return

    def acceptOffer(self, offer):
        accept = True
        if self.cpu != 0:
            cpu = self.__getResource(offer.resources, "cpus")
            if self.cpu > cpu:
                accept = False
        if self.mem != 0:
            mem = self.__getResource(offer.resources, "mem")
            if self.mem > mem:
                accept = False
        if(accept == True):
            self.__updateResource(offer.resources, "cpus", self.cpu)
            self.__updateResource(offer.resources, "mem", self.mem)
            return True
        else:
            return False

class EspaScheduler(Scheduler):
    def __init__(self, cfg, espa_api):
        self.idleTaskList = []
        self.startingTaskList = {}
        self.runningTaskList = {}
        self.terminatingTaskList = {}
        self.required_cpus   = cfg.get('task_cpu')
        self.required_memory = cfg.get('task_mem')
        self.refuse_seconds  = cfg.get('offer_refuse_seconds')
        self.request_count   = cfg.get('product_request_count')
        self.products        = cfg.get('product_frequency')
        self.espa = espa_api


    # pre-populate the idleTaskList?
    # make request to api for large set of units
    # divide that up into XX chunks, add them to the idleTaskList
    # when a new offer comes in, pull the oldest, and resupply the list
    # with a new request

    def resourceOffers(self, driver, offers):
        logger.debug("Received new offers...")

        offer_ids = [i.id for i in offers] # addict Dicts()

        # check to see if any more tasks should be launched
        if not self.espa.run_mesos_tasks():
            logger.info("Mesos tasks disabled!")
            driver.declineOffer(offer_ids, {'refuse_seconds': self.refuse_seconds})
            return

        if not self.idleTaskList: # idleTaskList is empty, try re-populating it
            product_type = self.products.pop(0)
            units = espa.get_products_to_process([product_type], self.request_count)
            if units:
                [self.idleTaskList.append(u) for u in units]
                self.products.append(product_type)
            else:
                logger.info("No work to do for product_type: {}, declining offers!".format(product_type))
                driver.declineOffer(offer_ids, {'refuse_seconds': self.refuse_seconds})
                self.products.append(product_type)
                return

        for offer in offers:
            # running tasks x cores per task < max permitted for espa
            NewTask = EspaTask(cpu=self.required_cpus, mem=self.required_memory)
            if NewTask.acceptOffer(offer):
                logger.debug("Acceptable offer received..")
                task = Dict()
                task_id = NewTask.task_id
                task.task_id.value = task_id
                task.agent_id.value = offer.agent_id.value
                task.name = 'task {}'.format(task_id)
                task.container.type = 'DOCKER'
                task.container.docker.image = 'usgseros/espa-worker:0.0.2'
                task.resources = [
                    dict(name='cpus', type='SCALAR', scalar={'value': TryTask.cpu}),
                    dict(name='mem', type='SCALAR', scalar={'value': TryTask.mem}),
                ]
                # pull off a unit of work
                work = self.idleTaskList.pop(0)
                # format for passing as arg to container ?
                task.command.value = NewTask.build_command(work)
                task.command.environment.variables = [{"name":"ESPA_FOO", "value":"666"}]

                driver.launchTasks([offer.id], [task])

            else: # decline the offer
                driver.declineOffer([offer.id])

                    
    def statusUpdate(self, driver, update):
        task_id = update.task_id.value
        state = update.state
        if state == "TASK_STARTING":
            ETask = self.startingTaskList[task_id]
            logger.debug("Task %s is starting. " % task_id)
        elif state == "TASK_RUNNING":
            if task_id in self.startingTaskList:
                ETask = self.startingTaskList[task_id]
                logger.info("Task %s running in %s, moving to running list" % 
                             (task_id, update.container_status.network_infos[0].ip_addresses[0].ip_address))
                self.runningTaskList[task_id] = ETask
                del self.startingTaskList[task_id]
        elif state == "TASK_FAILED":
            ETask = None
            if task_id in self.startingTaskList:
                ETask = self.startingListTask[task_id]
                del self.startingTaskList[task_id]
            elif task_id in self.runningTaskList:
                ETask = self.runningTaskList[task_id]
                del self.runningTaskList[task_id]

            if ETask:
                logger.info("ESPA task: %s failed." % ETask.taskId)
                self.idleTaskList.append(ETask)
                driver.reviveOffers()
            else:
                logger.error("Received task failed for unknown task: %s" % task_id)
        else:
            logger.info("Received status %s for task id: %s" % (update.state, task_id))


def get_framework(cfg):
    framework = Dict()
    framework.user = cfg.get('mesos_user')
    framework.id.value = str(uuid.uuid4())
    framework.name = "ESPAScheduler"
    framework.hostname = socket.gethostname()
    framework.failover_timeout = 75 # whats sensible ?
    return framework

def main():
    cfg       = config()
    espa_api  = espa.api_connect(cfg.get('espa_api'))
    framework = get_framework(cfg)
    scheduler = EspaScheduler(cfg, espa_api)
    master    = cfg.get('mesos_master') 

    driver = MesosSchedulerDriver(scheduler, framework, master, use_addict=True)
    driver.run()

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger(__name__)
    main()
