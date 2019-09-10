import addict
from multiprocessing import Process
import os
import schedule
from mesoshttp.client import MesosClient
from scheduler import config, espa, logger, task, util

log = logger.get_logger()
  

class ESPAFramework(object):

    def __init__(self, cfg, espa_api):
        master    = cfg.get('mesos_master') 
        principal = cfg.get('mesos_principal')
        secret    = cfg.get('mesos_secret')

        self.workList        = []
        self.runningList     = {}
        self.max_cpus        = cfg.get('max_cpu')
        self.required_cpus   = cfg.get('task_cpu')
        self.required_memory = cfg.get('task_mem')
        self.required_disk   = cfg.get('task_disk')
        self.task_image      = cfg.get('task_image')
        self.refuse_seconds  = cfg.get('offer_refuse_seconds')
        self.request_count   = cfg.get('product_request_count')
        self.products        = cfg.get('product_frequency')
        self.healthy_states  = ["TASK_STAGING", "TASK_STARTING", "TASK_RUNNING", "TASK_FINISHED"]
        self.espa = espa_api
        self.cfg  = cfg

        self.client = MesosClient(mesos_urls=[master])
        self.client.set_credentials(principal, secret)
        self.client.on(MesosClient.SUBSCRIBED, self.subscribed)
        self.client.on(MesosClient.OFFERS, self.offer_received)
        self.client.on(MesosClient.UPDATE, self.status_update)

    def _getResource(self, res, name):
        for r in res:
            if r['name'] == name:
                return r['scalar']['value']
        return 0.0

    def _updateResource(self, res, name, value):
        if value <= 0:
            return
        for r in res:
            if r['name'] == name:
                r['scalar']['value'] -= value
        return

    def core_limit_reached(self):
        running_count = len(self.runningList)
        task_core_count = self.required_cpus
        core_utilization = running_count * task_core_count
        resp = False

        if core_utilization >= self.max_cpus:
            log.debug("Max number of cores being used")
            resp = True

        return resp

    def subscribed(self, driver):
        log.warning('SUBSCRIBED')
        self.driver = driver

    def acceptOffer(self, offer):
        accept = True
        if self.required_cpus != 0:
            cpu = self._getResource(offer['resources'], "cpus")
            if self.required_cpus > cpu:
                accept = False
        if self.required_memory != 0:
            mem = self._getResource(offer['resources'], "mem")
            if self.required_memory > mem:
                accept = False
        if self.required_disk != 0:
            disk = self._getResource(offer['resources'], "disk")
            if self.required_disk > disk:
                accept = False
        if(accept == True):
            self._updateResource(offer['resources'], "cpus", self.required_cpus)
            self._updateResource(offer['resources'], "mem", self.required_memory)

        return accept

    def offer_received(self, offers):
        log.debug("Received new offers...")
        response = addict.Dict()
        response.offers.length = len(offers)
        response.offers.accepted = 0
        response.offers.declined = 0

        # check to see if any more tasks should be launched
        if self.espa.mesos_tasks_disabled() or self.core_limit_reached():
            # decline the offers to free up the resources
            [offer.decline({'filters': {'refuse_seconds': self.refuse_seconds}}) for offer in offers]
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
                [offer.decline({'filters': {'refuse_seconds': self.refuse_seconds}}) for offer in offers]
                # there's no work to do, return
                response.work.length = 0
                return response
            else:
                log.info("Work to do for product_type: {}, count: {}".format(product_type, len(units)))
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
            mesos_offer = offer.get_offer()
            if self.acceptOffer(mesos_offer) and self.workList:
                log.debug("Accepting offer")
                # pull off a unit of work
                work     = self.workList.pop(0)
                task_id  = "{}_@@@_{}".format(work.get('orderid'), work.get('scene'))
                new_task = task.build(task_id, mesos_offer, self.task_image, self.required_cpus, 
                                      self.required_memory, self.required_disk, work, self.cfg)
                log.debug("New Task definition: {}".format(new_task))
                # pymesos -> driver.launchTasks([offer.id], [new_task])
                offer.accept([new_task])
                response.offers.accepted += 1
            else: # decline the offer
                log.debug("Declining offer")
                offer.decline()
                # pymesos -> driver.declineOffer([offer.id])
                response.offers.declined += 1

        log.debug("resourceOffer response: {}".format(response))
        return response

    def status_update(self, update):
        # possible state values
        # http://mesos.apache.org/api/latest/java/org/apache/mesos/Protos.TaskState.html
        task_id = update['status']['task_id']['value']
        orderid, scene = task_id.split("_@@@_")
        state = update['status']['state']

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


def handle_orders(cfg, api):
    frequency = cfg.get('handle_orders_frequency')
    log.debug("calling handle_orders with frequency: {} minutes".format(frequency))
    schedule.every(frequency).minutes.do(api.handle_orders)
    while True:
        schedule.run_pending()

def main():
    cfg       = config.config()    
    espa_api  = espa.api_connect(cfg)
    framework = ESPAFramework(cfg, espa_api)

    framework_process = Process(target=framework.client.register)
    schedule_process  = Process(target=handle_orders, args=(cfg, espa_api,))

    schedule_process.start()
    framework_process.start()

if __name__ == '__main__':
    main()

