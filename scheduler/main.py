import addict
from multiprocessing import Process, Queue
from multiprocessing.queues import Empty
import os
import schedule
from mesoshttp.client import MesosClient
from scheduler import config, espa, logger, task, util

log = logger.get_logger()

def get_products_to_process(cfg, espa, work_list):
    max_scheduled = cfg.get('product_scheduled_max')
    products = cfg.get('product_frequency')
    request_count = cfg.get('product_request_count')

    if work_list.qsize() < max_scheduled:
        # pull the first item off the product types list
        product_type = products.pop(0)
        # get products to process for the product_type
        units = espa.get_products_to_process([product_type], request_count).get("products")
        # put that product type at the back of the list
        products.append(product_type)

        # if units are emtpy...
        if not units:
            log.info("No work to do for product_type: {}".format(product_type))
        else:
            log.info("Work to do for product_type: {}, count: {}, appending to work list".format(product_type, len(units)))
            for u in units:
                # update retrieved products in espa to scheduled status
                espa.set_to_scheduled(u)
                # add the units of work to the workList
                work_list.put(u)
    else:
        log.info("Max number of tasks scheduled, not requesting more products to process")
        
    return True

def scheduled_tasks(cfg, espa_api, work_list):
    product_frequency = cfg.get('product_request_frequency')
    handler_frequency = cfg.get('handle_orders_frequency')
    log.debug("calling get_products_to_process with frequency: {} minutes".format(product_frequency))
    log.debug("calling handle_orders with frequency: {} minutes".format(handler_frequency))
    schedule.every(product_frequency).minutes.do(get_products_to_process, cfg=cfg, espa=espa_api, work_list=work_list)
    schedule.every(handler_frequency).minutes.do(espa_api.handle_orders)
    while True:
        schedule.run_pending()
 

class ESPAFramework(object):

    def __init__(self, cfg, espa_api, worklist):
        master    = cfg.get('mesos_master') 
        principal = cfg.get('mesos_principal')
        secret    = cfg.get('mesos_secret')

        self.workList        = worklist
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

        self.client = MesosClient(mesos_urls=[master], frameworkName='ESPA Mesos Framework')
        self.client.verify = False
        self.client.set_credentials(principal, secret)
        self.client.on(MesosClient.SUBSCRIBED, self.subscribed)
        self.client.on(MesosClient.OFFERS, self.offer_received)
        self.client.on(MesosClient.UPDATE, self.status_update)

        # put some work on the workList
        get_products_to_process(cfg, self.espa, self.workList)

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

        log.debug("Number of cores being used: {}".format(core_utilization))
        if core_utilization >= self.max_cpus:
            log.debug("Max number of cores being used. Max = {}".format(self.max_cpus))
            resp = True

        return resp

    def subscribed(self, driver):
        log.warning('SUBSCRIBED')
        self.driver = driver

    def accept_offer(self, offer):
        accept = True
        resources = offer.get('resources')
        if self.required_cpus != 0:
            cpu = self._getResource(resources, "cpus")
            if self.required_cpus > cpu:
                accept = False
        if self.required_memory != 0:
            mem = self._getResource(resources, "mem")
            if self.required_memory > mem:
                accept = False
        if self.required_disk != 0:
            disk = self._getResource(resources, "disk")
            if self.required_disk > disk:
                accept = False
        if(accept == True):
            self._updateResource(resources, "cpus", self.required_cpus)
            self._updateResource(resources, "mem", self.required_memory)
        
        return accept

    def decline_offer(self, offer):
        options = {'filters': {'refuse_seconds': self.refuse_seconds}}
        log.debug("declining offer: {} with options: {}".format(offer, options))
        try:
            offer.decline(options)
        except Exception as error:
            log.error("Exception encountered declining offer: {}, error: {}".format(offer, error))
            raise
        return True        

    def offer_received(self, offers):
        response = addict.Dict()
        response.offers.length = len(offers)
        response.offers.accepted = 0
        log.debug("Received {} new offers...".format(response.offers.length))

        # check to see if Mesos tasks are enabled
        if self.espa.mesos_tasks_disabled():
            # decline the offers to free up the resources
            log.debug("mesos tasks disabled, declining {} offers".format(len(offers)))
            for offer in offers:
                self.decline_offer(offer)   
            response.tasks.enabled = False
            return response
        else:
            response.tasks.enabled = True

        # check to see if core limit has been reached
        if self.core_limit_reached():
            # decline the offers to free up the resources
            log.debug("Core utilization limit reached, declining {} offers".format(len(offers)))
            for offer in offers:
                self.decline_offer(offer)
            response.tasks.enabled = False
            return response
        else:
            response.tasks.enabled = True

        for offer in offers:
            mesos_offer = offer.get_offer()
            if self.accept_offer(mesos_offer):
                log.debug("Acceptable offer, checking for work to do")
                try:
                    work     = self.workList.get(False) # will raise multiprocessing.Empty if no objects present
                    task_id  = "{}_@@@_{}".format(work.get('orderid'), work.get('scene'))
                    new_task = task.build(task_id, mesos_offer, self.task_image, self.required_cpus, 
                                          self.required_memory, self.required_disk, work, self.cfg)
                    log.debug("New Task definition: {}".format(new_task))
                    offer.accept([new_task])
                    response.offers.accepted += 1
                except Empty:
                    log.debug("Work queue is empty, declining offer")
                    self.decline_offer(offer)
                except Exception as e:
                    log.error("Exception creating new task. offer: {}, exception: {}\n declining offer".format(offer, e))
                    self.decline_offer(offer)
            else:
                log.debug("Unacceptable offer, declining")
                self.decline_offer(offer)

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


def main():
    cfg       = config.config()    
    espa_api  = espa.api_connect(cfg)
    work_list = Queue() # multiprocessing Queue
    framework = ESPAFramework(cfg, espa_api, work_list)

    # Scheduled requests for espa processing work, and handle-orders call
    scheduled_process  = Process(target=scheduled_tasks, args=(cfg, espa_api, work_list,))

    try:
        scheduled_process.start()
        framework.client.register()
    except Exception as err:
        log.error("espa scheduler encountered an error, killing scheduled processes. tearing down framework. error: {}".format(err))
        framework.client.tearDown()
        scheduled_process.kill()

    
if __name__ == '__main__':
    main()

