import sys
import uuid
import socket
import os
import signal
import time
import logging

import config

from pymesos import MesosSchedulerDriver, Scheduler, encode_data
from addict import Dict
from threading import Thread

class EspaTask:
    def __init__(self, taskId="espaTask", command=None, cpu, mem):
        self.taskId = taskId
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
    def __init__(self, cpus, memory):
        self.idleTaskList = []
        self.startingTaskList = {}
        self.runningTaskList = {}
        self.terminatingTaskList = {}
        self.required_cpus = cpus
        self.required_memory = memory

    # pre-populate the idleTaskList?
    # make request to api for large set of units
    # divide that up into XX chunks, add them to the idleTaskList
    # when a new offer comes in, pull the oldest, and resupply the list
    # with a new request


    def resourceOffers(self, driver, offers):
        logging.debug("Received new offers...")

        filters = {'refuse_seconds': 3}

        for offer in offers:
            taskList = []
            pendingTaskList = []

            while True:
                if len(self.idleTaskList) == 0:
                    break

                # running tasks x cores per task < max permitted for espa

                #FIFO
                NewTask = EspaTask(cpu=self.required_cpus, mem=self.required_memory)
                if NewTask.acceptOffer(offer):



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

    cfg = config.config()

    framework = Dict()
    framework.user = cfg.get('mesos_user')
    framework.id.value = str(uuid.uuid4())
    framework.name = "ESPAScheduler"
    framework.hostname = socket.gethostname()
    framework.failover_timeout = 75 # whats sensible ?

    driver = MesosSchedulerDriver(
        EspaScheduler(),
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
