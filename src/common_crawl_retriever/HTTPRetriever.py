import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import time

"""
    Implements an HTTP interface.
"""
class HTTPRetriever(object):
    def __init__(self):
        self.http_session = None
        
        self.create_http_session()

    """
        Creates a requests HTTP session object with a retry strategy.
    """
    def create_http_session(self):
        # Create HTTP session with HTTP retry strategy.
        http_retry_strategy = Retry(
            total = 10, 
            status_forcelist = [429, 500, 502, 503, 504], 
            method_whitelist = ["HEAD", "GET", "OPTIONS"]
        )
        
        http_adapter = HTTPAdapter(max_retries = http_retry_strategy)
        
        self.http_session = requests.Session()
        self.http_session.mount("http://", http_adapter)
        self.http_session.mount("https://", http_adapter)

    """
        :param url: URL to get. 
        :param params: HTTP request parameters.
        :param headers: HTTP request headers. 
        :param sleep: time to sleep for avoinding HTTP congestion.

        :return: HTTP response.
    """
    def get(
        self, 
        url,
        params = None,
        headers = None, 
        sleep = 0
    ):
        time.sleep(sleep)

        response = self.http_session.get(
            url, 
            params = params, 
            headers = headers
        )

        response.raise_for_status()

        return response
