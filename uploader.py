import threading
import configparser

import boto3
import os
import sys

from boto3.s3.transfer import TransferConfig
from boto3 import s3

TAR_FILE_NAME = "2021.tar"

def multi_part_upload_with_s3(bucket: str,key: str, secret: str):
    # Multipart upload
    config = TransferConfig(multipart_threshold=1024 * 25, max_concurrency=20,
                            multipart_chunksize=1024 * 25, use_threads=True)
    #file_path = os.path.dirname(__file__) + '/2005.tar'
    file_path = f"/media/thibault/Nouveau nom/{TAR_FILE_NAME}"
    key_path = TAR_FILE_NAME

    client = boto3.client(
        "s3",
        aws_access_key_id=key,
        aws_secret_access_key=secret,
    )

    client.upload_file(
        file_path, bucket, key_path,
        ExtraArgs={'ContentType': 'application/tar'},
        Config=config,
        Callback=ProgressPercentage(file_path)
    )


class ProgressPercentage(object):
    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify we'll assume this is hooked up
        # to a single filename.
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            sys.stdout.flush()

if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read("config.ini")

    AWS_KEY = config["aws"]["key"]
    AWS_SECRET = config["aws"]["secret"]
    AWS_BUCKET = config["aws"]["bucket"]

    multi_part_upload_with_s3(bucket=AWS_BUCKET, secret=AWS_SECRET, key=AWS_KEY)
