import argparse
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import boto3
import botocore
import botocore.client
import os
import io
import gzip

S3_CC_BUCKET_NAME = "commoncrawl"

"""
    Implements copying of Common Crawl Index parquet files from S3 to Azure.
"""
class CommonCrawlIndexCopier(object):
    def __init__(
        self, 
        args
    ):
        self._cp_az_connection_string = args.cp_az_connection_string
        self._cp_cc_index_id = args.cp_cc_index_id
        self._cp_az_destination_container = args.cp_az_destination_container

        self._az_container_client = self._get_or_create_az_container_client()
        

    """
    """
    def _get_or_create_az_container_client(self):
        az_container_client = None

        # Create an Azure container.
        try:
            print("JOB: COMMON CRAWL INDEX COPIER: creating target Azure container...")

            az_blob_service_client = BlobServiceClient.from_connection_string(
                self._cp_az_connection_string
            )

            az_container_client = az_blob_service_client.create_container(self._cp_az_destination_container)
        # Container exists, get it.
        except:
            print("JOB: COMMON CRAWL INDEX COPIER: target Azure container exists, getting it...")

            az_container_client = az_blob_service_client.get_container_client(self._cp_az_destination_container)
        finally:
            print("DONE")

            return az_container_client
        

    """

    """
    def list_s3_cc_index_files(self):
        s3 = boto3.resource("s3", config = botocore.client.Config(signature_version = botocore.UNSIGNED))
        s3_cc_bucket = s3.Bucket(S3_CC_BUCKET_NAME)

        for obj in s3_cc_bucket.objects.filter(Prefix = "cc-index/table/cc-main/warc/crawl={}/".format(self._cp_cc_index_id)):
            print(obj.key)


    """
        Retrieves a list of Common Crawl index parquet files from the S3 bucket.
    """
    def get_s3_cc_index_files_list(self):
        print("JOB: COMMON CRAWL INDEX COPIER: getting list of S3 files...")
        index_files_list = []

        cc_index_files_path = "cc-index/table/cc-main/warc/crawl={}/subset=warc/".format(self._cp_cc_index_id)

        s3 = boto3.resource("s3", config = botocore.client.Config(signature_version = botocore.UNSIGNED))

        s3_cc_bucket = s3.Bucket(name = S3_CC_BUCKET_NAME)

        for obj in s3_cc_bucket.objects.filter(Prefix = cc_index_files_path):
            index_files_list.append(obj.key)

            print(obj.key)
        print("DONE")

        return index_files_list


    """
        Copies index files from S3 to Azure.
    """
    def copy_s3_cc_index_files(
        self, 
        index_files_list
    ):
        s3 = boto3.resource(
            "s3", 
            config = botocore.client.Config(
                signature_version = botocore.UNSIGNED
            )
        )

        for index_file in index_files_list:
            try:
                print("JOB: COMMON CRAWL INDEX COPIER: Downloading {} to {}".format(index_file, os.path.basename("/cc_index/{}".format(index_file))))
                if not os.path.exists(os.path.dirname(index_file)):
                    os.makedirs(os.path.dirname(index_file))

                # Retrieve S3 file on local OS fs.
                s3.Bucket(S3_CC_BUCKET_NAME).download_file(index_file, index_file)
                print("DONE")

                # Store file on Azure.
                print("JOB: COMMON CRAWL INDEX COPIER: Putting {} to Azure as {}".format(os.path.basename("/cc_index/{}".format(index_file)), index_file))
                with open(index_file, "rb") as data:
                    self._az_container_client.upload_blob(
                        data = data, 
                        name = index_file, 
                        blob_type = "BlockBlob"
                    )
                print("DONE")

                # Delete temp OS file.
                os.remove(index_file)
            except botocore.exceptions.ClientError as e:
                if e.response["Error"]["Code"] == "404":
                    print("JOB: COMMON CRAWL INDEX COPIER: The object does not exist: {}".format(e))
                else:
                    raise
