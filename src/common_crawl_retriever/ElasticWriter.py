import hashlib
import json
import requests

"""
    Implements the Hadoop / Elastic connector.
"""
class ElasticWriter(object):
    """
        Ctor.

        :param elastic_search_conf: Elastic-Hadoop connector configuration.
    """
    def __init__(
        self, 
        elastic_search_conf
    ):
        self.elastic_search_conf = elastic_search_conf


    """
        Transform a line of content to an Elastic Document.

        Elastic expects a format of the form: (0, "{'some_key': 'some_value', 'doc_id': 123}").

        :param row: a RDD row.
    """
    def to_elastic_document(self, row):
        try:
            elastic_document = dict()

            # Generate a doc ID. 
            doc_id = hashlib.sha224(row["url"].encode("utf-8")).hexdigest()

            elastic_document["doc_id"] = doc_id
            elastic_document["urlkey"] = row["urlkey"]
            elastic_document["url"] = row["url"]
            elastic_document["content"] = row["content"]
            
            return (doc_id , elastic_document)
        except Exception as e:
            print("JOB: COMMON CRAWL ELASTIC PUSH: Creating Elastic document failed: {}".format(e))

    """
        Pushes data to Elastic.

        :param elastic_rdd: the processed RDD to push to Elastic.
    """
    def create_index(self, elastic_rdd):
        try:
            elastic_rdd.saveAsNewAPIHadoopFile(
                path = "-", 
                outputFormatClass = "org.elasticsearch.hadoop.mr.EsOutputFormat", 
                keyClass = "org.apache.hadoop.io.NullWritable", 
                valueClass = "org.elasticsearch.hadoop.mr.LinkedMapWritable", 
                conf = self.elastic_search_conf
            )
        except Exception as e:
            print("JOB: COMMON CRAWL ELASTIC PUSH: Push to Elastic failed: {}".format(e))