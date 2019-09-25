import json
from scheduler import logger
import requests
import sys

from tenacity import retry
from tenacity import retry_if_exception_type
from tenacity import stop_after_attempt
from tenacity import wait_fixed
from tenacity import wait_random_exponential

log = logger.get_logger()

class APIException(Exception):
    """
    Handle exceptions thrown by the APIServer class
    """
    pass


class APIServer(object):
    """
    Simple class for a couple espa-api calls
    """
    def __init__(self, base_url, image):
        self.base = base_url
        self.image = image

    def request(self, method, resource=None, status=None, **kwargs):
        """
        Make a call into the API

        Args:
            method: HTTP method to use
            resource: API resource to touch

        Returns: response and status code

        """
        valid_methods = ('get', 'put', 'delete', 'head', 'options', 'post')

        if method not in valid_methods:
            raise APIException('Invalid method {}'.format(method))

        if resource and resource[0] == '/':
            url = '{}{}'.format(self.base, resource)
        elif resource:
            url = '{}/{}'.format(self.base, resource)
        else:
            url = self.base

        try:
            resp = requests.request(method, url, **kwargs)
        except requests.RequestException as e:
            raise APIException(e)

        if status and resp.status_code != status:
            self._unexpected_status(resp.status_code, url)

        return resp.json(), resp.status_code

    def get_configuration(self, key):
        """
        Retrieve a configuration value

        Args:
            key: configuration key

        Returns: value if it exists, otherwise None

        """
        config_url = '/configuration/{}'.format(key)

        resp, status = self.request('get', config_url, status=200)

        if key in resp.keys():
            return resp[key]

    @retry(stop=stop_after_attempt(2), wait=wait_fixed(10)) # 10 attempts, 60 second intervals
    def update_status(self, prod_id, order_id, val):
        """
        Update the status of a product

        Args:
            prod_id: scene name
            order_id: order id
            proc_loc: processing location
            val: status value

        Returns:
        """
        url = '/update_status'

        data_dict = {'name': prod_id,
                     'orderid': order_id,
                     'processing_loc': self.image,
                     'status': val}

        resp, status = self.request('post', url, json=data_dict, status=200)

        log.debug("ESPA API update_status call. data: {},  status: {},  response: {} ".format(data_dict, status, resp))

        return {"response": resp, "status": status, "data": data_dict}

    def set_to_scheduled(self, unit):
        prod_id = unit.get('scene')
        order_id = unit.get('orderid')
        self.update_status(prod_id, order_id, 'scheduled')
        return True

    @retry(stop=stop_after_attempt(10), wait=wait_fixed(60)) # 10 attempts, 60 second intervals
    def set_scene_error(self, prod_id, order_id, data):
        """
        Set a scene to error status

        Args:
            prod_id: scene name
            order_id: order id
            proc_loc: processing location
            log: log file contents

        Returns:
        """
        url = '/set_product_error'
        data_dict = {'name': prod_id,
                     'orderid': order_id,
                     'processing_loc': self.image,
                     'error': json.dumps(data)}

        resp, status = self.request('post', url, json=data_dict, status=200)

        log.debug("ESPA API set_scene_error call. data: {},  status: {},  response: {} ".format(data_dict, status, resp))
        return {"response": resp, "status": status, "data": data_dict}

    def get_products_to_process(self, product_type, limit, user=None, priority=None):
        """
        Retrieve products for processing

        Args:
            limit: number of products to grab
            user: specify a user
            priority: depricated, legacy support
            product_type: landsat and/or modis

        Returns: list of dicts
        """

        params = ['record_limit={}'.format(limit) if limit else None,
                  'for_user={}'.format(user) if user else None,
                  'priority={}'.format(priority) if priority else None,
                  'product_types={}'.format(product_type) if product_type else None]

        query = '&'.join([q for q in params if q])
        resp = []
        url = '/products?{}'.format(query)

        try:
            resp, status = self.request('get', url, status=200)
            log.debug("ESPA API get_products_to_process call. data: {},  status: {},  response: {} ".format(params, status, resp))
        except Exception as e:
            log.error("Error retrieving products to process. url: {}  exception: {}".format(url, e))

        return {"products": resp, "url": url}

    def handle_orders(self):
        url = '/handle-orders'
        status = False
        try:
            resp, status = self.request('get', url, status=200)
            log.debug("ESPA API handle_orders call. status: {},  response: {} ".format(status, resp))
        except Exception as e:
            log.error("Error executing handle-orders, exception: {}".format(e))

        return status

    def mesos_tasks_disabled(self):
        resp = True
        try:
            run = self.get_configuration('run_mesos_tasks')
            if run == 'True':
                log.debug('Mesos tasks enabled in ESPA')
                resp = False
            else:
                log.info("Mesos tasks disabled!")
        except Exception as e:
            log.error("Error retrieving run_mesos_tasks configuration, exception: {}".format(e))

        return resp

    @staticmethod
    def _unexpected_status(code, url):
        """
        Throw exception for an unhandled http status

        Args:
            code: http status that was received
            url: URL that was used
        """
        raise Exception('Received unexpected status code: {}\n'
                        'for URL: {}'.format(code, url))

    def test_connection(self):
        """
        Tests the base URL for the class
        Returns: True if 200 status received, else False
        """
        _, _ = self.request('get', status=200)
        return True


def api_connect(params):
    """
    Simple lead in method for using the API connection class

    Args:
        url: base URL to connect to

    Returns: initialized APIServer object if successful connection
             else None
    """
    url = params.get('espa_api')
    image = params.get('task_image')
    api = APIServer(url, image)
    api.test_connection() # throws exception if non-200 response to base url
    return api
