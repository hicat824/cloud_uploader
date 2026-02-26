from .BaseService import BaseService

import logging
import os
import tqdm
from obs import ObsClient

class ObsServer(BaseService):
    def __init__(self, ak, sk, endpoint, bucket_name, secure=False):
        logging.info(f"set obs client secure as {secure}")
        self.client = ObsClient(access_key_id=ak, secret_access_key=sk, server=endpoint, is_secure=secure)
        self.bucket_name = bucket_name
        self.part_size = 100 * 1024 * 1024

    def UploadFile(self, prefix, local_path):
        logging.info(f"Uploading {local_path} to {prefix}")
        try:
            resp = self.client.uploadFile(self.bucket_name, prefix, local_path, self.part_size,
                                          4, True)
            if resp.status < 300:
                return True
            else:
                logging.error(f"upload {local_path} failed, return code = {resp.status}")
        except Exception as e:
            logging.error(f"上传失败：{e}")

        return False

    def DownloadFile(self, prefix, local_path):
        logging.info(f"Downloading {local_path} from {prefix}")
        try:
            resp = self.client.downloadFile(self.bucket_name, prefix, local_path, self.part_size,
                                            4, True)
            if resp.status < 300:
                return True
            else:
                logging.error(f"download {prefix} failed, return code = {resp.status}")
        except Exception as e:
            logging.error(e)
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
        objects = self.client.listObjects(self.bucket_name, prefix, max_keys=1000, encoding_type='url')
        for content in objects.body.contents:
            object_key = content.key
            local_file_path = os.path.join(local_path, os.path.relpath(object_key, prefix))
            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
            if not self.DownloadFile(object_key, local_file_path):
                return False
        return True

    def IsFileExists(self, prefix):
        try:
            resp = self.client.headObject(self.bucket_name, prefix)
            if 200 <= resp.status < 300:
                return True
            else:
                return False
        except Exception as e:
            logging.error(e)
        return False

    def ListFiles(self, prefix, recursive=False):
        all_files = []
        try:
            objects = self.client.listObjects(self.bucket_name, prefix, max_keys=1000, encoding_type='url')
            for content in objects.body.contents:
                all_files.append(content.key)
        except Exception as e:
            logging.error(e)
        return all_files