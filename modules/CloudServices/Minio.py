import logging
import os
from minio import Minio
from minio.error import S3Error
from tqdm import tqdm
from itertools import cycle

from .BaseService import BaseService

class MinioServer(BaseService):
    def __init__(self, endpoint, access_key, secret_key, bucket_name, secure=True):
        """ secure ： 是否使用HTTPS """
        endpoints = endpoint.split(",")
        self.clients = [Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure) for endpoint in
                        endpoints]
        self.bucket_name = bucket_name
        self.part_size = 100 * 1024 * 1024
        self.client_cycle = cycle(self.clients)
        self.__initBucket()

    def get_client(self):
        """获取下一个可用的Minio客户端（简单轮询）。"""
        return next(self.client_cycle)

    def __initBucket(self):
        tmp_client = self.get_client()
        if not tmp_client.bucket_exists(self.bucket_name):
            tmp_client.make_bucket(self.bucket_name)

    def UploadFile(self, prefix, local_path):
        logging.info(f"Uploading {local_path} to {prefix}")
        try:
            tmp_client = self.get_client()
            file_name = os.path.basename(local_path)
            if file_name not in prefix:
                prefix = os.path.normpath(os.path.join(prefix, file_name))
            tmp_client.fput_object(self.bucket_name, prefix, local_path,
                                    part_size=self.part_size, num_parallel_uploads=4)
            return True
        except S3Error as e:
            logging.error(f"上传失败：{e}")
            return False

    def DownloadFile(self, prefix, local_path):
        logging.info(f"Downloading {local_path} from {prefix}")
        try:
            tmp_client = self.get_client()
            tmp_client.fget_object(self.bucket_name, prefix, local_path)
            return True
        except S3Error as e:
            logging.error(f"下载失败：{e}")
            return False

    def UploadFolder(self, prefix, local_path):
        logging.info(f"Uploading {local_path} to {prefix}")
        for root, _, files in os.walk(local_path):
            for file in tqdm(files, desc="上传进度"):
                local_file_path = os.path.join(root, file)
                object_name = os.path.normpath(os.path.join(prefix, os.path.relpath(local_file_path, local_path)))
                if not self.UploadFile(object_name, local_file_path):
                    return False
        return True

    def DownloadFolder(self, prefix, local_path):
        os.makedirs(local_path, exist_ok=True)
        tmp_client = self.get_client()
        objects = tmp_client.list_objects(self.bucket_name, prefix=prefix, recursive=True)

        for obj in objects:
            object_name = obj.object_name
            local_file_path = os.path.join(local_path, os.path.relpath(object_name, prefix))
            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
            if not self.DownloadFile(object_name, local_file_path):
                return False
        return True


    """ 注意：minio没有文件夹概念。判断文件夹是否存在可以通过list_objects判断文件夹下是否有文件 """
    def IsFileExists(self, prefix):
        try:
            tmp_client = self.get_client()
            if prefix.endswith("/"):
                objects = tmp_client.list_objects(
                    self.bucket_name,
                    prefix=prefix,
                    recursive=False
                )
                return any(True for _ in objects)
            else:
                tmp_client.stat_object(self.bucket_name, prefix)
                return True
        except S3Error as exc:
            logging.error(exc.message)
            return False

    def ListFiles(self, prefix, recursive=False):
        result = []
        try:
            tmp_client = self.get_client()
            objects = tmp_client.list_objects(self.bucket_name, prefix=prefix, recursive=recursive)
            for obj in objects:
                result.append(obj.object_name)
            return result
        except S3Error as exc:
            return result