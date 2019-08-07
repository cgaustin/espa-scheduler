import sys
import uuid
import socket
import os
import signal
import logging

from config import config
import espa
import task
from task import Task
from util import right_now

from pymesos import MesosSchedulerDriver, Scheduler, encode_data
from addict import Dict
from threading import Thread
from copy import deepcopy

class EspaScheduler(Scheduler):
    def __init__(self, cfg, espa_api):
        self.workList        = []
        self.idleList        = {}
        self.startingList    = {}
        self.runningList     = {}
        self.terminatingList = {}
        self.max_cpus        = cfg.get('max_cpu')
        self.required_cpus   = cfg.get('task_cpu')
        self.required_memory = cfg.get('task_mem')
        self.task_image      = cfg.get('task_image')
        self.refuse_seconds  = cfg.get('offer_refuse_seconds')
        self.request_count   = cfg.get('product_request_count')
        self.products        = cfg.get('product_frequency')
        self.cfg             = cfg
        self.espa            = espa_api

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
        if self.required_cpus != 0:
            cpu = self.__getResource(offer.resources, "cpus")
            if self.required_cpus > cpu:
                accept = False
        if self.required_memory != 0:
            mem = self.__getResource(offer.resources, "mem")
            if self.required_memory > mem:
                accept = False
        if(accept == True):
            self.__updateResource(offer.resources, "cpus", self.required_cpus)
            self.__updateResource(offer.resources, "mem", self.required_memory)
            return True
        else:
            return False

    def core_limit_reached(self):
        running_count = len(self.running_list)
        task_core_count = self.required_cpus
        core_utilization = running_count * task_core_count
        resp = False

        if core_utilization >= self.max_cpus:
            logger.debug("Max number of cores being used")
            resp = True

        return resp

    def resourceOffers(self, driver, offers):
        logger.debug("Received new offers...")

        # check to see if any more tasks should be launched
        if self.espa.mesos_tasks_disabled() or self.core_limit_reached():
            # decline the offers to free up the resources
            driver.declineOffer([i.id for i in offers], {'refuse_seconds': self.refuse_seconds})
            return

        # if workList is empty, try re-populating it
        if not self.workList:
            # pull the first item off the product types list
            product_type = self.products.pop(0)
            # get products to process for the product_type
            units = espa.get_products_to_process([product_type], self.request_count)
            # put that product type at the back of the list
            self.products.append(product_type)

            for u in units:
                # update retrieved products in espa to scheduled status
                espa.set_to_scheduled(u)
                # add the units of work to the workList
                self.workList.append(u)

            # if units are emtpy...
            if not units:
                logger.info("No work to do for product_type: {}, declining offers!".format(product_type))
                # decline the offer, freeing up the resources
                driver.declineOffer([i.id for i in offers], {'refuse_seconds': self.refuse_seconds}) 
                # there's no work to do, return
                return

        # we have work to do, check if there are usable offers
        for offer in offers:
            if self.acceptOffer(offer):
                logger.debug("Acceptable offer received..")
                # pull off a unit of work
                work     = self.workList.pop(0)
                new_task = task.build("ESPATask-{}".format(right_now()), offer, self.task_image, 
                                      self.required_cpus, self.required_memory, work, self.cfg)
                driver.launchTasks([offer.id], [new_task])
            else: # decline the offer
                driver.declineOffer([offer.id])

                    
    def statusUpdate(self, driver, update):
        task_id = update.task_id.value
        state = update.state
        if state == "TASK_STARTING":
            ETask = self.startingList[task_id]
            logger.debug("Task %s is starting. " % task_id)
        elif state == "TASK_RUNNING":
            if task_id in self.startingList:
                ETask = self.startingList[task_id]
                logger.info("Task %s running in %s, moving to running list" % 
                             (task_id, update.container_status.network_infos[0].ip_addresses[0].ip_address))
                self.runningList[task_id] = ETask
                del self.startingList[task_id]
        elif state == "TASK_FAILED":
            ETask = None
            if task_id in self.startingList:
                ETask = self.startingList[task_id]
                del self.startingList[task_id]
            elif task_id in self.runningList:
                ETask = self.runningList[task_id]
                del self.runningList[task_id]

            if ETask:
                logger.info("ESPA task: %s failed." % ETask.taskId)
                self.idleList.append(ETask)
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
