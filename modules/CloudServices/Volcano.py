import os
import logging
import tos
from tos import TosClientV2
from tos.utils import SizeAdapter
from modules.CloudServices.BaseService import BaseService

class VolcanoServer(BaseService):
    def __init__(self, endpoint, access_key, secret_key, bucket_name, region):
        self.client = TosClientV2(
            endpoint=endpoint,
            region=region,
            ak=access_key,
            sk=secret_key
        )
        self.bucket = bucket_name
        self.part_size = 100 * 1024 * 1024  # 100MB, 分片大小

    def _MultiUpload(self, prefix, local_path):
        logging.info(f"分片上传{local_path}")
        resp = self.client.upload_file(self.bucket, prefix, local_path, task_num=6, part_size=self.part_size)
        if resp.status_code in (200, 201):
            return True
        else:
            return False


    def UploadFile(self, prefix, local_path):
        if prefix.startswith('/'):
            prefix = prefix[1:]
        logging.info(f"Uploading {local_path} to {prefix}")
        try:
            file_size = os.path.getsize(local_path)
            if file_size > self.part_size:
                return self._MultiUpload(prefix, local_path)
            else:
                with open(local_path, "rb") as f:
                    data = f.read()
                resp = self.client.put_object(self.bucket, prefix, content=data)
                if resp.status_code in (200, 201):
                    return True
                else:
                    return False
        except tos.exceptions.TosClientError as e:
            logging.error(f"客户端异常:{e}")
            return False
        except tos.exceptions.TosServerError as e:
            logging.error(f"服务端异常:{e}")
            return False
        except Exception as e:
            logging.error(f"未知错误:{e}")
            return False

    def DownloadFile(self, prefix, local_path):
        logging.info(f"Downloading {local_path} from {prefix}")
        try:
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            file_stream = self.client.get_object(self.bucket, prefix)
            with open(local_path, "wb") as f:
                for content in file_stream:
                    f.write(content)
            return True
        except tos.exceptions.TosClientError as e:
            logging.error(f"客户端异常:{e}")
            return False
        except tos.exceptions.TosServerError as e:
            logging.error(f"服务端异常:{e}")
            return False
        except Exception as e:
            logging.error(f"未知错误:{e}")
            return False

    def UploadFolder(self, prefix, local_path):
        try:
            for root, _, files in os.walk(local_path):
                for file in files:
                    local_file_path = os.path.join(root, file)
                    relative_path = os.path.relpath(local_file_path, local_path)
                    remote_path = os.path.normpath(os.path.join(prefix, relative_path))
                    if not self.UploadFile(remote_path, local_file_path):
                        return False
            return True
        except tos.exceptions.TosClientError as e:
            logging.error(f"客户端异常:{e}")
            return False
        except tos.exceptions.TosServerError as e:
            logging.error(f"服务端异常:{e}")
            return False
        except Exception as e:
            logging.error(f"未知错误:{e}")
            return False

    def DownloadFolder(self, prefix, local_path):
        try:
            os.makedirs(local_path, exist_ok=True)
            files = self.ListFiles(prefix)
            if files is None:
                logging.error("empty source root")
                return False
            for file in files:
                remote_path = file.key
                rel_path = os.path.relpath(file.key, prefix)
                local_file_path = os.path.join(local_path, rel_path)
                logging.info(f"remote_path = {remote_path}, rel_path = {rel_path}, local_file_path = {local_file_path}")
                os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
                if not self.DownloadFile(remote_path, local_file_path):
                    return False
            return True
        except tos.exceptions.TosClientError as e:
            logging.error(f"客户端异常:{e}")
            return False
        except tos.exceptions.TosServerError as e:
            logging.error(f"服务端异常:{e}")
            return False
        except Exception as e:
            logging.error(f"未知错误:{e}")
            return False


    def IsFileExists(self, prefix):
        try:
            self.client.head_object(bucket=self.bucket, key=prefix)
            return True
        except tos.exceptions.TosClientError as e:
            logging.error(f"客户端异常:{e}")
            return False
        except tos.exceptions.TosServerError as e:
            logging.error(f"服务端异常:{e}")
            return False
        except Exception as e:
            logging.error(f"未知错误:{e}")
            return False

    def ListFiles(self, prefix, recursive=False):
        try:
            return self.client.list_objects(
                bucket=self.bucket,
                prefix=prefix,
                max_keys=1000
            ).contents
        except tos.exceptions.TosClientError as e:
            logging.error(f"客户端异常:{e}")
            return False
        except tos.exceptions.TosServerError as e:
            logging.error(f"服务端异常:{e}")
            return False
        except Exception as e:
            logging.error(f"未知错误:{e}")
            return False