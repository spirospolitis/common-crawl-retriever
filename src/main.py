# -*- coding: utf-8 -*-
import argparse
import hashlib
import itertools
import json
from pyspark import SparkConf, SparkContext
from pyspark.sql import Row, SparkSession, SQLContext
from pyspark.sql.types import ArrayType, StructType, StructField, StringType
from pyspark.sql.functions import col
import re
import requests
import sys

# Import package "common_crawl_retriever" as a .zip archive.
sys.path.append("common_crawl_retriever.zip")

from common_crawl_retriever import CommonCrawlDataRetriever, CommonCrawlIndexCopier, HTTPRetriever, ElasticWriter, HTMLTextExtractor, Logging


"""
    Prints execution parameters.
"""
def print_parameters(spark_session, args):
    if spark_session is not None:
        print("SPARK: session config")
        print("\t|-- app.name: {}".format(spark_session.sparkContext._conf.get("app.name")))

        print("\t|-- spark.driver.cores: {}".format(spark_session.sparkContext._conf.get("spark.driver.cores")))
        print("\t|-- spark.driver.memory: {}".format(spark_session.sparkContext._conf.get("spark.driver.memory")))
        print("\t|-- spark.executor.cores: {}".format(spark_session.sparkContext._conf.get("spark.executor.cores")))
        print("\t|-- spark.executor.memory: {}".format(spark_session.sparkContext._conf.get("spark.executor.memory")))
        print("\t|-- spark.executor.instances: {}".format(spark_session.sparkContext._conf.get("spark.executor.instances")))
        print("\t|-- spark.memory.offHeap.enabled: {}".format(spark_session.sparkContext._conf.get("spark.memory.offHeap.enabled")))
        print("\t|-- spark.memory.offHeap.size: {}".format(spark_session.sparkContext._conf.get("spark.memory.offHeap.size")))
        print("\t|-- spark.yarn.executor.memoryOverhead: {}".format(spark_session.sparkContext._conf.get("spark.yarn.executor.memoryOverhead")))

    print("APPLICATION: config")
    print("\t|-- app.jobs: {}".format(args.app_jobs))

    print("\t|-- az.storage.account: {}".format(args.az_storage_account))
    print("\t|-- az.storage.in.container.name: {}".format(args.az_storage_in_container_name))
    print("\t|-- az.storage.out.container.name: {}".format(args.az_storage_out_container_name))

    print("\t|-- az.in.scope.file.name: {}".format(args.az_in_scope_file_name))
    print("\t|-- az.in.cc.index.file.path: {}".format(args.az_in_cc_index_file_path))
    print("\t|-- az.out.cc.index.file.name: {}".format(args.az_out_cc_index_file_name))
    print("\t|-- az.out.cc.index.file.format: {}".format(args.az_out_cc_index_file_format))
    print("\t|-- az.out.cc.index.content.name: {}".format(args.az_out_cc_content_file_name))
    print("\t|-- az.out.cc.index.content.format: {}".format(args.az_out_cc_content_file_format))

    print("\t|-- cc.content.http.concurrency: {}".format(args.cc_content_http_concurrency))
    print("\t|-- cc.index.partitions: {}".format(args.cc_index_partitions))
    print("\t|-- cc.content.partitions: {}".format(args.cc_content_partitions))
    print("\t|-- cc.scope.subset: {}".format(args.cc_scope_subset))

    if "common-crawl-index-copier" in args.app_jobs:
        print("\t|-- cp.cc.index.id: {}".format(args.cp_cc_index_id))
        print("\t|-- cp.az.connection.string: {}".format(args.cp_az_connection_string))
        print("\t|-- cp.az.destination.container: {}".format(args.cp_az_destination_container))

    if "common-crawl-elastic-push" in args.app_jobs:
        print("\t|-- es.nodes: {}".format(args.es_nodes))
        print("\t|-- es.port: {}".format(args.es_port))
        print("\t|-- es.net.http.auth.user: {}".format(args.es_net_http_auth_user))
        print("\t|-- es.net.http.auth.pass: {}".format(args.es_net_http_auth_pass))
        print("\t|-- es.resource: {}".format(args.es_resource))
        print("\t|-- es.input.json: {}".format(args.es_input_json))
        print("\t|-- es.mapping.id: {}".format(args.es_mapping_id))
        print("\t|-- es.nodes.wan.only: {}".format(args.es_nodes_wan_only))

    print("DONE")

"""
    Retrieves latest Common Crawl index.
"""
def get_latest_cc_index_url():
    cdx_indexes = requests.get("https://index.commoncrawl.org/collinfo.json").json()

    return cdx_indexes[0]["cdx-api"]


"""
    Create and configure Spark session objects.
"""
def create_and_configure_spark_objects(args):
    spark_session = SparkSession.builder \
        .appName(args.app_name) \
        .config("spark.driver.cores", args.spark_driver_cores) \
        .config("spark.driver.memory", args.spark_driver_memory) \
        .config("spark.executor.cores", args.spark_executor_cores) \
        .config("spark.executor.memory", args.spark_executor_memory) \
        .config("spark.executor.instances", args.spark_executor_instances) \
        .config("spark.memory.offHeap.enabled", args.spark_memory_offHeap_enabled) \
        .config("spark.memory.offHeap.size", args.spark_memory_offHeap_size) \
        .config("spark.yarn.executor.memoryOverhead", args.spark_yarn_executor_memoryOverhead) \
        .getOrCreate()

    sql_context = SQLContext(spark_session.sparkContext)

    return spark_session, sql_context

"""
    Copies Common Crawl Index files in parquet format from S3 to Azure.
"""
def job_common_crawl_index_copier(args):
    common_crawl_index_copier = CommonCrawlIndexCopier.CommonCrawlIndexCopier(args)
    
    # Get file list.
    print("JOB: COMMON CRAWL INDEX COPIER: retrieving list of Common Crawl index files from S3 ...")
    s3_cc_index_files_list = common_crawl_index_copier.get_s3_cc_index_files_list()
    print("DONE")

    # Copy files.
    print("JOB: COMMON CRAWL INDEX COPIER: copying Common Crawl index files to Azure ...")
    common_crawl_index_copier.copy_s3_cc_index_files(s3_cc_index_files_list)
    print("DONE")

"""
    Processes the scraping scope input file.
"""
def prepare_scope_file(spark_session, sql_context, scope_df):
    def extract_domain(row):
        try:
            r = row["_c0"].encode("utf-8")

            # Eliminate 'www'.
            if re.match(r"^www.", r):
                r = re.sub(r"^www.", "", r)

            # Discard URLs.
            r = r.split("/")[0]

            # Keep domain only.
            # domain = re.findall(r'(?<=\.)([^.]+)(?:\.(?:co\.uk|ac\.us|[^.]+(?:$|\n)))', tld)[0]

            return r
        except Exception as e:
            pass

    scope_rdd = scope_df \
        .rdd \
        .map(
            lambda row: extract_domain(row)
        )
    
    scope_df = scope_rdd \
        .map(Row("url_host_name")) \
        .toDF() \
        .distinct()

    return scope_df


"""
    Reads and processes the scraping scope input file.
"""
def job_scope_retrieve(spark_session, sql_context, args):
    ###
    # Retrieval scope.
    ##
    print("JOB: reading scope file...")
    scope_df = spark_session \
        .read \
        .csv("wasbs://{}@{}.blob.core.windows.net/{}".format(args.az_storage_in_container_name, args.az_storage_account, args.az_in_scope_file_name))
    print("DONE")

    print("JOB: preparing scope file...")
    scope_df = prepare_scope_file(spark_session, sql_context, scope_df)
    print("DONE")

    return scope_df


"""
    Executes the Common Crawl Index preparation job.
"""
def job_common_crawl_index_retrieve(spark_session, sql_context, scope_df, args):
    print("JOB: COMMON CRAWL INDEX RETRIEVE: reading Common Crawl index files...")
    cc_index_df = spark_session \
        .read \
        .parquet("wasbs://{}@{}.blob.core.windows.net/{}/*.parquet".format(args.az_storage_in_container_name, args.az_storage_account, args.az_in_cc_index_file_path))
    print("DONE")

    # Join scope_df, cc_index_df.
    scope_cc_index_joined_df = scope_df \
        .join(cc_index_df, scope_df["url_host_name"] == cc_index_df["url_host_name"])
    
    # Keep specific columns.
    scope_cc_index_joined_df = scope_cc_index_joined_df.select(
        "url_surtkey", 
        "url", 
        "fetch_status", 
        "content_mime_type", 
        "content_mime_detected", 
        "content_charset", 
        "content_languages", 
        "warc_filename", 
        "warc_record_offset", 
        "warc_record_length", 
        "warc_segment"
    )

    # Filter.
    print("JOB: COMMON CRAWL INDEX RETRIEVE: filtering...")
    scope_cc_index_joined_df = scope_cc_index_joined_df \
        .filter(
            (scope_cc_index_joined_df["content_languages"].like('%eng%')) & 
            ((scope_cc_index_joined_df["content_mime_type"]  == "text/html") | (scope_cc_index_joined_df["content_mime_type"]  == "application/xhtml+xml")) &  
            (scope_cc_index_joined_df["fetch_status"]  == "200") 
        )
    print("DONE")

    # Repartition.
    print("JOB: COMMON CRAWL INDEX RETRIEVE: repartitioning...")
    scope_cc_index_joined_df = scope_cc_index_joined_df \
        .repartition(numPartitions = args.cc_index_partitions)
    print("DONE")

    # Write CSV.
    if "csv" in args.az_out_cc_index_file_format:
        print("JOB: COMMON CRAWL INDEX RETRIEVE: writing to CSV...")
        scope_cc_index_joined_df \
            .write \
            .option("sep", "\t") \
            .option("header", "true") \
            .format("csv") \
            .mode("overwrite") \
            .save("wasbs://{}@{}.blob.core.windows.net/{}.csv".format(args.az_storage_out_container_name, args.az_storage_account, args.az_out_cc_index_file_name))
        print("DONE")

    # Write Parquet.
    if "parquet" in args.az_out_cc_index_file_format:
        print("JOB: COMMON CRAWL CONTENT RETRIEVE: writing to Parquet...")
        scope_cc_index_joined_df \
            .write \
            .format("parquet") \
            .mode("overwrite") \
            .save("wasbs://{}@{}.blob.core.windows.net/{}.parquet".format(args.az_storage_out_container_name, args.az_storage_account, args.az_out_cc_index_file_name))
        print("DONE")

    return scope_cc_index_joined_df


"""
    Executes the Common Crawl Content preparation job.
"""
def job_common_crawl_content_retrieve(spark_session, sql_context, cc_index_df, args):
    print("JOB: COMMON CRAWL CONTENT RETRIEVE: retrieving content...")
    cc_content_rdd = cc_index_df \
        .rdd \
        .repartition(numPartitions = args.cc_content_partitions) \
        .glom() \
        .map(
            lambda x: CommonCrawlDataRetriever.CommonCrawlDataRetriever(http_concurrency = args.cc_content_http_concurrency).get(
                indices = x, 
                http_retriever = HTTPRetriever.HTTPRetriever(), 
                transformer = HTMLTextExtractor.HTMLTextExtractor()
            )
        )
    print("DONE")

    # Flatten the nested data list acquired from Spark workers.
    print("JOB: COMMON CRAWL CONTENT RETRIEVE: flattening nested lists...")
    cc_content_rdd = cc_content_rdd \
        .flatMap(lambda x: itertools.chain(*x))
    print("DONE")

    # Define Common Crawl content DataFrame schema.
    cc_content_schema = StructType([
        StructField("content", StringType(), True), 
        StructField("url", StringType(), True), 
        StructField("urlkey", StringType(), True)
    ])

    print("JOB: COMMON CRAWL CONTENT RETRIEVE: creating DataFrame from RDD...")
    cc_content_df = sql_context \
        .createDataFrame(
            data = cc_content_rdd, 
            schema = cc_content_schema
        )
    print("DONE")

    # Drop duplicates, if any.
    print("JOB: COMMON CRAWL CONTENT RETRIEVE: dropping duplicates...")
    cc_content_df = cc_content_df \
        .dropDuplicates()
    print("DONE")

    # Repartition.
    print("JOB: COMMON CRAWL CONTENT RETRIEVE: repartitioning...")
    cc_content_df = cc_content_df \
        .repartition(numPartitions = args.cc_content_partitions)
    print("DONE")

    # Write CSV.
    if "csv" in args.az_out_cc_content_file_format:
        print("JOB: COMMON CRAWL CONTENT RETRIEVE: writing to CSV...")
        cc_content_df \
            .write \
            .option("sep", "\t") \
            .option("header", "true") \
            .format("csv") \
            .mode("overwrite") \
            .save("wasbs://{}@{}.blob.core.windows.net/{}.csv".format(args.az_storage_out_container_name, args.az_storage_account, args.az_out_cc_content_file_name))
        print("DONE")

    # Write Parquet.
    if "parquet" in args.az_out_cc_content_file_format:
        print("JOB: COMMON CRAWL CONTENT RETRIEVE: writing to Parquet...")
        cc_content_df \
            .write \
            .format("parquet") \
            .mode("overwrite") \
            .save("wasbs://{}@{}.blob.core.windows.net/{}.parquet".format(args.az_storage_out_container_name, args.az_storage_account, args.az_out_cc_content_file_name))
        print("DONE")

    return cc_content_df


"""
"""
def job_common_crawl_elastic_push(cc_content_df, args):
    elastic_writer = ElasticWriter.ElasticWriter(
        elastic_search_conf = {
            # Config parameter docs: https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html
            
            # List of Elasticsearch nodes to connect to. When using Elasticsearch remotely, do set this option. 
            # Note that the list does not have to contain every node inside the Elasticsearch cluster; these are discovered automatically by elasticsearch-hadoop by default (see below). 
            # Each node can also have its HTTP/REST port specified individually (e.g. mynode:9600).
            "es.nodes": args.es_nodes, 
            # Default HTTP/REST port used for connecting to Elasticsearch - this setting is applied to the nodes in es.nodes that do not have any port specified.
            "es.port" : args.es_port, 
            # Basic Authentication user name.
            "es.net.http.auth.user": args.es_net_http_auth_user, 
            # Basic Authentication password.
            "es.net.http.auth.pass": args.es_net_http_auth_pass, 
            # Elasticsearch resource location, where data is read and written to. Requires the format <index>/<type>.
            "es.resource" : args.es_resource,
            # Whether the input is already in JSON format or not (the default). Please see the appropriate section of each integration for more details about using JSON directly.
            "es.input.json" : args.es_input_json, 
            # The document field/property name containing the document id.
            "es.mapping.id": args.es_mapping_id, 
            # Whether the connector is used against an Elasticsearch instance in a cloud/restricted environment over the WAN, such as Amazon Web Services. 
            # In this mode, the connector disables discovery and only connects through the declared es.nodes during all operations, including reads and writes. 
            # Note that in this mode, performance is highly affected.
            "es.nodes.wan.only": args.es_nodes_wan_only, 
            # Timeout for HTTP/REST connections to Elasticsearch.copier
            "es.http.timeout": "10m", 
            # Number of retries for establishing a (broken) http connection. 
            # The retries are applied for each conversation with an Elasticsearch node. Once the retries are depleted, 
            # the connection will automatically be re-reouted to the next available Elasticsearch node 
            # (based on the declaration of es.nodes, followed by the discovered nodes - if enabled).
            "es.http.retries": "10", 
            # Size (in entries) for batch writes using Elasticsearch bulk API - (0 disables it). 
            # Companion to es.batch.size.bytes, once one matches, the batch update is executed. 
            # Similar to the size, this setting is per task instance; it gets multiplied at runtime by the total number of Hadoop tasks running.
            "es.batch.size.entries": "1000", 
            # Number of retries for a given batch in case Elasticsearch is overloaded and data is rejected. 
            # Note that only the rejected data is retried. If there is still data rejected after the retries have been performed, the Hadoop job is cancelled (and fails). 
            # A negative value indicates infinite retries; be careful in setting this value as it can have unwanted side effects.
            "es.batch.write.retry.count": "10"
        }
    )

    cc_elastic_rdd = cc_content_df \
        .rdd \
        .repartition(numPartitions = args.cc_elastic_partitions) \
        .map(
            lambda row: elastic_writer.to_elastic_document(row)
        )

    elastic_writer.create_index(cc_elastic_rdd)


if __name__ == "__main__":
    argument_parser = argparse.ArgumentParser()
    
    argument_parser.add_argument("--app.name", action = "store", dest = "app_name", nargs = "?", type = str, required = True, help = "<Optional str><Default: common-crawl-retriever> Spark application name")
    argument_parser.add_argument("--app.jobs", action = "store", dest = "app_jobs", nargs = "+", required = True, help = "<Required str> Job to execute. Valid jobs are: common-crawl-index-copier, common-crawl-index-retrieve, common-crawl-content-retrieve, common-crawl-elastic-push")

    argument_parser.add_argument("--spark.driver.cores", action = "store", dest = "spark_driver_cores", nargs = "?", const = "4", default = "4", type = str, required = False, help = "<Optional int><Default: 4> Spark driver cores")
    argument_parser.add_argument("--spark.driver.memory", action = "store", dest = "spark_driver_memory", nargs = "?", const = "1g", default = "1g", type = str, required = False, help = "<Optional str><Default: 1g> Spark driver memory in Java format (e.g. 512m, 1g)")
    argument_parser.add_argument("--spark.executor.cores", action = "store", dest = "spark_executor_cores", nargs = "?", const = "1", default = "1", type = str, required = False, help = "<Optional int><Default: 1> Spark executor cores to be used amongst all cores in the cluster")
    argument_parser.add_argument("--spark.executor.memory", action = "store", dest = "spark_executor_memory", nargs = "?", const = "1g", default = "1g", type = str, required = False, help = "<Optional int><Default: 1g> Spark executor memory in Java format (e.g. 512m, 1g)")
    argument_parser.add_argument("--spark.yarn.executor.memoryOverhead", action = "store", dest = "spark_yarn_executor_memoryOverhead", nargs = "?", const = "1g", default = "1g", type = str, required = False, help = "<Optional int><Default: 1g> Spark yarn executor memory overhead in Java format (e.g. 512m, 1g)")
    argument_parser.add_argument("--spark.executor.instances", action = "store", dest = "spark_executor_instances", nargs = "?", const = "4", default = "4", type = str, required = False, help = "<Optional int><Default: 4> Spark executor instances")
    argument_parser.add_argument("--spark.memory.offHeap.enabled", action = "store", dest = "spark_memory_offHeap_enabled", nargs = "?", const = False, default = False, type = bool, required = False, help = "<Optional bool><Default: False> Spark memory offheap enabled (true / false)")
    argument_parser.add_argument("--spark.memory.offHeap.size", action = "store", dest = "spark_memory_offHeap_size", nargs = "?", const = "1g", default = "1g", type = str, required = False, help = "<Optional str><Default: 1g> Spark memory offheap size in Java format (e.g. 512m, 1g)")
    
    argument_parser.add_argument("--az.storage.account", action = "store", dest = "az_storage_account", nargs = "?", type = str, required = False, help = "<Optional str> Azure storage account")
    argument_parser.add_argument("--az.storage.in.container.name", action = "store", dest = "az_storage_in_container_name", nargs = "?", type = str, required = False, help = "<Optional str> Azure storage application input container name")
    argument_parser.add_argument("--az.storage.out.container.name", action = "store", dest = "az_storage_out_container_name", nargs = "?", type = str, required = False, help = "<Optional str> Azure storage application output container name")
    argument_parser.add_argument("--az.in.scope.file.name", action = "store", dest = "az_in_scope_file_name", nargs = "?", type = str, required = False, help = "<Optional str> Name of scope file to use")
    argument_parser.add_argument("--az.in.cc.index.file.path", action = "store", dest = "az_in_cc_index_file_path", nargs = "?", type = str, required = False, help = "<Optional str> Path to Common Crawl Index parquet files")
    argument_parser.add_argument("--az.out.cc.index.file.name", action = "store", dest = "az_out_cc_index_file_name", nargs = "?", type = str, required = False, help = "<Optional str> Path to Common Crawl Index output file")
    argument_parser.add_argument("--az.out.cc.index.file.format", action = "store", dest = "az_out_cc_index_file_format", nargs = "?", type = str, required = False, help = "<Optional str> Common Crawl Index output format")
    argument_parser.add_argument("--az.out.cc.content.file.name", action = "store", dest = "az_out_cc_content_file_name", nargs = "?", type = str, required = False, help = "<Optional str> Path to Common Crawl Content output file")
    argument_parser.add_argument("--az.out.cc.content.file.format", action = "store", dest = "az_out_cc_content_file_format", nargs = "?", type = str, required = False, help = "<Optional str> Common Crawl Content output format")
    
    argument_parser.add_argument("--cc.index.partitions", action = "store", dest = "cc_index_partitions", nargs = "?", const = 10, default = 10, type = int, required = False, help = "<Optional int><Default: 10> Common Crawl index partitions")
    argument_parser.add_argument("--cc.content.http.concurrency", action = "store", dest = "cc_content_http_concurrency", nargs = "?", default = 50, type = int, required = False, help = "<Optional int><Default: 50> Common Crawl content HTTP concurrent requests")
    argument_parser.add_argument("--cc.content.partitions", action = "store", dest = "cc_content_partitions", nargs = "?", const = 10, default = 10, type = int, required = False, help = "<Optional int><Default: 10> Common Crawl content partitions")
    argument_parser.add_argument("--cc.elastic.partitions", action = "store", dest = "cc_elastic_partitions", nargs = "?", type = int, required = False, help = "<Optional str> How many partitions to create for sending to Elastic?")
    argument_parser.add_argument("--cc.scope.subset", action = "store", dest = "cc_scope_subset", nargs = "?", const = None, default = None, type = int, required = False, help = "<Optional int><Default: None> Just get a subset of the scope file (for testing)")
    
    ###
    # JOB common-crawl-index-copier
    ##
    argument_parser.add_argument("--cp.cc.index.id", action = "store", dest = "cp_cc_index_id", nargs = "?", type = str, required = False, help = "<Optional str> Common Crawl Index ID (e.g. CC-MAIN-2020-40)")
    argument_parser.add_argument("--cp.az.connection.string", action = "store", dest = "cp_az_connection_string", nargs = "?", type = str, required = False, help = "<Optional str> Azure container connection string")
    argument_parser.add_argument("--cp.az.destination.container", action = "store", dest = "cp_az_destination_container", nargs = "?", type = str, required = False, help = "<Optional str> Azure destination container")

    ###
    # JOB: common-crawl-elastic-push
    ##
    argument_parser.add_argument("--es.nodes", action = "store", dest = "es_nodes", nargs = "?", type = str, required = False, help = "<Optional str> ElasticSearch endpoint address")
    argument_parser.add_argument("--es.port", action = "store", dest = "es_port", nargs = "?", type = str, required = False, help = "<Optional str> ElasticSearch port")
    argument_parser.add_argument("--es.net.http.auth.user", action = "store", dest = "es_net_http_auth_user", nargs = "?", type = str, required = False, help = "<Optional str> ElasticSearch username")
    argument_parser.add_argument("--es.net.http.auth.pass", action = "store", dest = "es_net_http_auth_pass", nargs = "?", type = str, required = False, help = "<Optional str> ElasticSearch password")
    argument_parser.add_argument("--es.resource", action = "store", dest = "es_resource", nargs = "?", type = str, required = False, help = "<Optional str> ElasticSearch resource")
    argument_parser.add_argument("--es.input.json", action = "store", dest = "es_input_json", nargs = "?", type = str, required = False, help = "<Optional str: yes / no> Is input to ElasticSearch JSON or not?")
    argument_parser.add_argument("--es.mapping.id", action = "store", dest = "es_mapping_id", nargs = "?", type = str, required = False, help = "<Optional str> Is there a field in the mapping that should be used to specify the ES document ID?")
    argument_parser.add_argument("--es.nodes.wan.only", action = "store", dest = "es_nodes_wan_only", nargs = "?", type = str, required = False, help = "<Optional str> Elastic WAN-only flag")

    args = argument_parser.parse_args()

    spark_session = None
    sql_context = None

    ###
    # Print application parameters.
    ##
    print_parameters(spark_session, args)

    ###
    # Get Spark logging.
    ##
    # log = Logging.get_spark_logger(spark_session, args.app_name)

    ###
    # Job: Common Crawl index copying.
    ##
    if "common-crawl-index-copier" in args.app_jobs:
        print("JOB: COMMON CRAWL INDEX COPIER: Initializing...")
        job_common_crawl_index_copier(args)
        print("DONE")

        # If this is the only job, exit.
        if len(args.app_jobs) == 1:
            sys.exit()

    ###
    # Get or create / config Spark objects.
    ##
    print("SPARK: creating and configuring objects...")
    spark_session, sql_context = create_and_configure_spark_objects(args)
    print("DONE")

    ###
    # Job: Common Crawl index processing.
    ##
    cc_index_df = None

    if "common-crawl-index-retrieve" in args.app_jobs:
        print("JOB: COMMON CRAWL INDEX RETRIEVE: Initializing...")
        # Retrieval scope.
        scope_df = job_scope_retrieve(spark_session, sql_context, args)
        scope_df = scope_df \
            .persist()

        # Common Crawl index processing.
        cc_index_df = job_common_crawl_index_retrieve(spark_session, sql_context, scope_df, args)
        cc_index_df = cc_index_df \
            .persist()
        print("DONE")

    ###
    # Job: Common Crawl content retrieval.
    ##
    cc_content_df = None

    if "common-crawl-content-retrieve" in args.app_jobs:
        print("JOB: COMMON CRAWL CONTENT RETRIEVE: Initializing...")
        if cc_index_df is None:
            try:
                print("JOB: COMMON CRAWL CONTENT RETRIEVE: Reading Common Crawl index files...")
                if "csv" in args.az_out_cc_index_file_format:
                    cc_index_df = spark_session \
                        .read \
                        .csv("wasbs://{}@{}.blob.core.windows.net/{}.csv/*.csv".format(args.az_storage_out_container_name, args.az_storage_account, args.az_out_cc_index_file_name))
                
                if "parquet" in args.az_out_cc_index_file_format:
                    cc_index_df = spark_session \
                        .read \
                        .parquet("wasbs://{}@{}.blob.core.windows.net/{}.parquet/*.parquet".format(args.az_storage_out_container_name, args.az_storage_account, args.az_out_cc_index_file_name))

                cc_index_df = cc_index_df \
                    .persist()
                print("DONE")
            except Exception as e:
                print("JOB: COMMON CRAWL CONTENT RETRIEVE: Reading Common Crawl index files failed: {}".format(e))
                print("JOB: COMMON CRAWL CONTENT RETRIEVE: Have you built the Common Crawl Index?")

            cc_content_df = job_common_crawl_content_retrieve(spark_session, sql_context, cc_index_df, args)
        print("DONE")

    ###
    # Job: Push Common Crawl content to Elastic.
    ##
    if "common-crawl-elastic-push" in args.app_jobs:
        print("JOB: COMMON CRAWL ELASTIC PUSH: Initializing...")
        if cc_content_df is None:
            try:
                print("JOB: COMMON CRAWL ELASTIC PUSH: Reading Common Crawl content files...")
                if "csv" in args.az_out_cc_content_file_format:
                    cc_content_df = spark_session \
                        .read \
                        .csv("wasbs://{}@{}.blob.core.windows.net/{}.csv/*.csv".format(args.az_storage_out_container_name, args.az_storage_account, args.az_out_cc_content_file_name))
                
                if "parquet" in args.az_out_cc_content_file_format:
                    cc_content_df = spark_session \
                        .read \
                        .parquet("wasbs://{}@{}.blob.core.windows.net/{}.parquet/*.parquet".format(args.az_storage_out_container_name, args.az_storage_account, args.az_out_cc_content_file_name))
            except Exception as e:
                print("JOB: COMMON CRAWL ELASTIC PUSH: Reading Common Crawl content files failed: {}".format(e))
                print("JOB: COMMON CRAWL ELASTIC PUSH: Have you built the Common Crawl Content?")
        print("DONE")

        job_common_crawl_elastic_push(cc_content_df, args)

    print("SPARK: stopping...")
    spark_session.sparkContext.stop()
    spark_session.stop()
    print("DONE")
