import concurrent.futures
import json
import math
import random
import re
import zlib

"""
    Implements parallel, partial HTTP requests for fetching Common Crawl content.
"""
class CommonCrawlDataRetriever(object):
    """
        :param http_concurrency: HTTP concurrent requests.
    """
    def __init__(
        self, 
        http_concurrency
    ):
        self.http_concurrency = http_concurrency

        print("COMMON CRAWL DATA: ctor: concurrency: {}".format(self.http_concurrency))

    """
        :param indices: RDD of Common Crawl indices.
        :param http_retriever: the object that we will delegate HTTP retrieval to.
        :param transformer: the object that we will delegate HTTP response transformation to.

        :return: list of lists of Common Crawl content.
    """
    def get(
        self, 
        indices, 
        http_retriever, 
        transformer
    ):
        print("COMMON CRAWL DATA: get: got {} indices to fetch".format(len(indices)))

        results = []

        with concurrent.futures.ThreadPoolExecutor(max_workers = self.http_concurrency) as executor:
            # Start the load operations and mark each future with its URL
            futures = {
                executor.submit(
                    http_retriever.get, 
                    "https://commoncrawl.s3.amazonaws.com/" + index["warc_filename"], 
                    params = None, 
                    headers = {"Range": "bytes={}-{}".format(int(index["warc_record_offset"]), int(index["warc_record_offset"]) + int(index["warc_record_length"]))}, 
                    # Quasi-exponential random sleep to avoid congesting HTTP requests.
                    # Reason for doing is to avoid HTTP 503s.
                    sleep = int(random.uniform(0, math.log(math.pow(len(indices), 3))))
                ): index for index in indices
            }
            
            for future in concurrent.futures.as_completed(futures):
                try:
                    # Get the Common Crawl index that this future maps to so as to retrieve
                    # fields "url" and "urlkey".
                    index = futures[future]

                    print("COMMON CRAWL DATA: get: got response for {}".format(index["url_surtkey"]))

                    # Partial GZipped response (binary).
                    response = future.result()

                    # Retrieve GZip content.
                    response_content = zlib.decompress(response.content, zlib.MAX_WBITS | 16)
                    
                    # Process response text.
                    # Strip HTML and extra whitespace characters from response.
                    # It is important to decode text using the encoding identifier provided with the Common Crawl index.
                    response_content = " ".join(re.split(r"\W+", transformer.get(response_content.decode(index["content_charset"]))))

                    response_json = {
                        "urlkey": index["url_surtkey"], 
                        "url": index["url"], 
                        "content": response_content
                    }

                    # Create list of lists so that we can handle it in a uniform way on the head node.
                    results.append([response_json])

                except Exception as e:
                    print("{}: {}".format("COMMON CRAWL DATA: get: ERROR", e))

        print("COMMON CRAWL DATA: get: got {} total results".format(len(results)))

        return results
