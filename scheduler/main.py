import sys
import uuid
import socket
import os
import signal
import logging

from scheduler.config import config
import scheduler.espa as espa
import scheduler.task as task
from scheduler.util import right_now

from pymesos import MesosSchedulerDriver, Scheduler, encode_data
from addict import Dict
from threading import Thread
from copy import deepcopy

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class EspaScheduler(Scheduler):
    def __init__(self, cfg, espa_api):
        self.workList        = []
        self.runningList     = {}
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
        running_count = len(self.runningList)
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
            units = self.espa.get_products_to_process([product_type], self.request_count).get("products")
            # put that product type at the back of the list
            self.products.append(product_type)

            # if units are emtpy...
            if not units:
                logger.info("No work to do for product_type: {}, declining offers!".format(product_type))
                # decline the offer, freeing up the resources
                driver.declineOffer([i.id for i in offers], {'refuse_seconds': self.refuse_seconds}) 
                # there's no work to do, return
                return
            else:
                for u in units:
                    # update retrieved products in espa to scheduled status
                    self.espa.set_to_scheduled(u)
                    # add the units of work to the workList
                    self.workList.append(u)

        # we have work to do, check if there are usable offers
        for offer in offers:
            if self.acceptOffer(offer):
                logger.debug("Acceptable offer received..")
                # pull off a unit of work
                work     = self.workList.pop(0)
                task_id  = "{}-{}".format(work.get('orderid'), work.get('scene'))
                new_task = task.build(task_id, offer, self.task_image, 
                                      self.required_cpus, self.required_memory, work, self.cfg)
                driver.launchTasks([offer.id], [new_task])
                self.idleList[task_id] = new_task
            else: # decline the offer
                driver.declineOffer([offer.id])
                    
    def statusUpdate(self, driver, update):
        # possible state values
        # http://mesos.apache.org/api/latest/java/org/apache/mesos/Protos.TaskState.html
        task_id = update.task_id.value
        orderid, scene = task_id.split("-")
        state = update.state
        healthy_states = ["TASK_STAGING", "TASK_STARTING", "TASK_RUNNING", "TASK_FINISHED"]
        
        if state in healthy_states:
            logger.debug("status update for: {}  new status: {}".format(task_id, state))

            if state == "TASK_RUNNING":
                if task_id not in self.runningList:
                    self.runningList[task_id] = right_now()

            if state == "TASK_FINISHED":
                try:
                    self.runningList.__delitem__(task_id)
                except KeyError:
                    logger.debug("Received TASK_FINISHED update for {}, which wasn't in the runningList".format(task_id))

        else: # something abnormal happened
            logger.error("abnormal task state for: {}, full update: {}".format(task_id, update))
            self.espa.set_scene_error(scene, orderid, update)
            if task_id in self.runningList:
                self.runningList.__delitem__(task_id)


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
    espa_api  = espa.api_connect(cfg)
    framework = get_framework(cfg)
    scheduler = EspaScheduler(cfg, espa_api)
    master    = cfg.get('mesos_master') 
    principal = cfg.get('mesos_principal')
    secret    = cfg.get('mesos_secret')

    driver = MesosSchedulerDriver(scheduler, framework, master,
                                  use_addict=True,
                                  principal=principal,
                                  secret=secret)
    driver.run()

if __name__ == '__main__':
    main()
