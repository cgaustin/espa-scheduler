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
        self.taskId = "EspaTask-{}".format(right_now())
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
    def __init__(self, products, cpus, memory):
        self.idleTaskList = []
        self.startingTaskList = {}
        self.runningTaskList = {}
        self.terminatingTaskList = {}
        self.required_cpus = cpus
        self.required_memory = memory
        self.products = products

    # pre-populate the idleTaskList?
    # make request to api for large set of units
    # divide that up into XX chunks, add them to the idleTaskList
    # when a new offer comes in, pull the oldest, and resupply the list
    # with a new request



    def resourceOffers(self, driver, offers):
        logging.debug("Received new offers...")

        filters = {'refuse_seconds': 3}

        products = deepcopy(self.products)

        for offer in offers:
            while True:
                # running tasks x cores per task < max permitted for espa
                NewTask = EspaTask(cpu=self.required_cpus, mem=self.required_memory)
                if NewTask.acceptOffer(offer):
                    task = Dict()
                    task_id = NewTask.task_id
                    task.task_id.value = task_id
                    task.agent_id.value = offer.agent_id.value
                    task.name = 'task {}'.format(task_id)
                    #task.command.value = TryTask.command
                    task.container.type = 'DOCKER'
                    task.container.docker.image = 'usgseros/espa-worker:0.0.2'
                    task.resources = [
                        dict(name='cpus', type='SCALAR', scalar={'value': TryTask.cpu}),
                        dict(name='mem', type='SCALAR', scalar={'value': TryTask.mem}),
                    ]
                    # peel off a product
                    product_type = products.pop(0)
                    # make the request for work
                    work = espa.get_products(product_type)
                    # add that product to the back of the list
                    products.append(product_type)
                    task.command.value = NewTask.build_command(work)
                    task.command.environment.variables = [{"name":"ESPA_FOO", "value":"666"}]

                    self.startingTaskList[task_id] = TryTask
                    taskList.append(task)                    


                if work:
                    

                else:
                    # nothing to do, decline offer
                    self.declineOffer([offer.id])
                    


    def statusUpdate(self, driver, update):
        task_id = update.task_id.value
        state = update.state
        if state == "TASK_STARTING":
            ETask = self.startingTaskList[task_id]
            logging.debug("Task %s is starting. " % task_id)
        elif state == "TASK_RUNNING":
            if task_id in self.startingTaskList:
                ETask = self.startingTaskList[task_id]
                logging.info("Task %s running in %s, moving to running list" % 
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
                logging.info("ESPA task: %s failed." % ETask.taskId)
                self.idleTaskList.append(ETask)
                driver.reviveOffers()
            else:
                logging.error("Received task failed for unknown task: %s" % task_id)
        else:
            logging.info("Received status %s for task id: %s" % (update.state, task_id))


def main():

    cfg = config()

    framework = Dict()
    framework.user = cfg.get('mesos_user')
    framework.id.value = str(uuid.uuid4())
    framework.name = "ESPAScheduler"
    framework.hostname = socket.gethostname()
    framework.failover_timeout = 75 # whats sensible ?

    cpus = cfg.get('task_cpu')
    mem  = cfg.get('task_mem')

    driver = MesosSchedulerDriver(
        EspaScheduler(cfg, cpus, mem),
        framework,
        cfg.get('mesos_master'),
        use_addict=True)

    def signal_handler(signal, frame):
        driver.stop()

    def run_driver_thread():
        driver.run()

    driver_thread = Thread(target=run_driver_thread, args=())
    driver_thread.start()

    print('Scheduler running, Ctl+c to quit.')
    signal.signal(signal.SIGINT, signal_handler)

    while driver_thread.is_alive():
        time.sleep(1)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main()
