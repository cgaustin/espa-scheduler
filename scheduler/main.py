import addict

from multiprocessing import Process
import os
import pymesos
import schedule
import signal
import socket
import sys
from threading import Thread
import uuid

from scheduler import config, espa, logger, task, util

log = logger.get_logger()

class EspaScheduler(pymesos.Scheduler):
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
        self.healthy_states  = ["TASK_STAGING", "TASK_STARTING", "TASK_RUNNING", "TASK_FINISHED"]

    def _getResource(self, res, name):
        for r in res:
            if r.name == name:
                return r.scalar.value
        return 0.0

    def _updateResource(self, res, name, value):
        if value <= 0:
            return
        for r in res:
            if r.name == name:
                r.scalar.value -= value
        return

    def acceptOffer(self, offer):
        accept = True
        if self.required_cpus != 0:
            cpu = self._getResource(offer.resources, "cpus")
            if self.required_cpus > cpu:
                accept = False
        if self.required_memory != 0:
            mem = self._getResource(offer.resources, "mem")
            if self.required_memory > mem:
                accept = False
        if(accept == True):
            self._updateResource(offer.resources, "cpus", self.required_cpus)
            self._updateResource(offer.resources, "mem", self.required_memory)

        return accept

    def core_limit_reached(self):
        running_count = len(self.runningList)
        task_core_count = self.required_cpus
        core_utilization = running_count * task_core_count
        resp = False

        if core_utilization >= self.max_cpus:
            log.debug("Max number of cores being used")
            resp = True

        return resp

    def resourceOffers(self, driver, offers):
        log.debug("Received new offers...")
        response = addict.Dict()
        response.offers.length = len(offers)
        response.offers.accepted = 0
        response.offers.declined = 0

        # check to see if any more tasks should be launched
        if self.espa.mesos_tasks_disabled() or self.core_limit_reached():
            # decline the offers to free up the resources
            driver.declineOffer([i.id for i in offers], {'refuse_seconds': self.refuse_seconds})
            response.tasks.enabled = False
            return response
        else:
            response.tasks.enabled = True

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
                log.info("No work to do for product_type: {}, declining offers!".format(product_type))
                # decline the offer, freeing up the resources
                driver.declineOffer([i.id for i in offers], {'refuse_seconds': self.refuse_seconds}) 
                # there's no work to do, return
                response.work.length = 0
                return response
            else:
                for u in units:
                    # update retrieved products in espa to scheduled status
                    self.espa.set_to_scheduled(u)
                    # add the units of work to the workList
                    self.workList.append(u)
                response.work.length = len(units)
        else:
            response.work.length = len(self.workList)

        # we have work to do, check if there are usable offers
        for offer in offers:
            if self.acceptOffer(offer) and self.workList:
                log.debug("Acceptable offer received..")
                # pull off a unit of work
                work     = self.workList.pop(0)
                task_id  = "{}_@@@_{}".format(work.get('orderid'), work.get('scene'))
                new_task = task.build(task_id, offer, self.task_image, 
                                      self.required_cpus, self.required_memory, work, self.cfg)
                log.debug("New Task definition: {}".format(new_task))
                driver.launchTasks([offer.id], [new_task])
                response.offers.accepted += 1
            else: # decline the offer
                driver.declineOffer([offer.id])
                response.offers.declined += 1

        return response
                    
    def statusUpdate(self, driver, update):
        # possible state values
        # http://mesos.apache.org/api/latest/java/org/apache/mesos/Protos.TaskState.html
        task_id = update.task_id.value
        orderid, scene = task_id.split("_@@@_")
        state = update.state

        response = addict.Dict()
        response.task_id = task_id
        response.state = state

        if state in self.healthy_states:
            log.debug("status update for: {}  new status: {}".format(task_id, state))
            response.status = "healthy"

            if state == "TASK_RUNNING":
                response.list.name = "running"
                if task_id not in self.runningList:
                    self.runningList[task_id] = util.right_now()
                    response.list.status = "new"
                else:
                    response.list.status = "current"

            if state == "TASK_FINISHED":
                try:
                    self.runningList.__delitem__(task_id)
                except KeyError:
                    log.debug("Received TASK_FINISHED update for {}, which wasn't in the runningList".format(task_id))

        else: # something abnormal happened
            log.error("abnormal task state for: {}, full update: {}".format(task_id, update))
            response.status = "unhealthy"
            self.espa.set_scene_error(scene, orderid, update)
            if task_id in self.runningList:
                self.runningList.__delitem__(task_id)

        return response


def get_framework(cfg):
    framework = addict.Dict()
    framework.user = cfg.get('mesos_user')
    framework.id.value = str(uuid.uuid4())
    framework.name = "ESPAScheduler"
    framework.hostname = socket.gethostname()
    framework.failover_timeout = 75 # whats sensible ?
    return framework

def handle_orders(cfg, api):
    frequency = cfg.get('handle_orders_frequency')
    schedule.every(frequency).minutes.do(api.handle_orders)
    while True:
        schedule.run_pending()

def main():
    cfg       = config.config()
    espa_api  = espa.api_connect(cfg)
    framework = get_framework(cfg)
    scheduler = EspaScheduler(cfg, espa_api)
    master    = cfg.get('mesos_master') 
    principal = cfg.get('mesos_principal')
    secret    = cfg.get('mesos_secret')

    driver = pymesos.MesosSchedulerDriver(scheduler, framework, master,
                                          use_addict=True,
                                          principal=principal,
                                          secret=secret)

    driver_run   = Process(target=driver.run)
    schedule_run = Process(target=handle_orders, args=(cfg, espa_api,))

    schedule_run.start()
    driver_run.start()
    

if __name__ == '__main__':
    main()
