import logging
import os, sys
import oss2
from oss2 import ResumableStore

from modules.CloudServices.BaseService import BaseService
from util_modules.log_util import *

class OSSServer(BaseService):
    def __init__(self, access_key, secret_key, bucket_name, end_point, output_root):
        self.auth = oss2.Auth(access_key, secret_key)
        self.end_point = end_point
        self.bucket_name = bucket_name
        self.bucket = oss2.Bucket(self.auth, self.end_point, self.bucket_name, connect_timeout=60)
        self.part_size = 100 * 1024 * 1024
        self.store = ResumableStore(output_root, 'oss_upload_cache')
        self.max_retry_times = 3

    def check_bucket_lightweight(self):
        """
        更轻量级的连接检测
        尝试对一个临时文件进行HEAD请求
        """
        try:
            # 尝试获取不存在的对象的元信息（会返回404，但能测试连接）
            # 或者使用更简单的方法：尝试获取Bucket的访问权限
            self.bucket.get_bucket_acl()
            return True, "连接正常"
        except oss2.exceptions.NoSuchBucket:
            return False, "Bucket不存在"
        except oss2.exceptions.AccessDenied:
            # 有响应，说明连接正常但权限不足
            return True, "连接正常（权限受限）"
        except oss2.exceptions.RequestError as e:
            return False, f"网络连接失败: {e}"
        except Exception as e:
            return False, f"连接异常: {e}"

    """
    eg. /data/20250418_102938.bag --> /cloud_data/20250418_102938.bag
    """
    def UploadFile(self, prefix, local_path):
        upload_mark = False
        for _ in range(self.max_retry_times):
            try:
                file_size = os.path.getsize(local_path)
                logging.info(f"Uploading {local_path} to {prefix}")
                # 上传
                if file_size > self.part_size:
                    res = oss2.resumable_upload(
                        self.bucket,
                        prefix,
                        local_path,
                        part_size=self.part_size,
                        store=self.store,
                        num_threads=4
                    )
                else:
                    with open(local_path, "rb") as f:
                        data = f.read()
                    res = self.bucket.put_object(prefix, data)

                # 检查
                if res.status in (200, 201):
                    logging.info(f"文件{local_path}上传成功")
                    upload_mark = True
            except Exception as e:
                logging.error(f"上传失败：{e}")
                conn_status, desc = self.check_bucket_lightweight()
                if not conn_status:
                    self.bucket = oss2.Bucket(self.auth, self.end_point, self.bucket_name, connect_timeout=60)
            if upload_mark:
                break

        return upload_mark

    """
    eg. /data/20250418_102938 --> /cloud_data/20250418_102938
    """
    def UploadFolder(self, prefix, local_path):
        success = True
        for root, _, files in os.walk(local_path):
            for file in files:
                local_file_path = os.path.join(root, file)
                relative_path = os.path.relpath(local_file_path, local_path)
                oss_path = os.path.join(prefix, relative_path).replace("\\", "/")
                if not self.UploadFile(oss_path, local_file_path):
                    success = False
        return success

    """
    eg. /cloud_data/20250418_102938.bag --> /data/20250418_102938.bag
    """
    def DownloadFile(self, prefix, local_path):
        logging.info(f"Downloading {local_path} from {prefix}")
        try:
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            self.bucket.get_object_to_file(prefix, local_path)
            return True
        except Exception as e:
            logging.error(f"文件下载失败: {e}")
            return False

    """
    eg. /cloud_data/20250418_102938 --> /data/20250418_102938
    """
    def DownloadFolder(self, prefix, local_path):
        logging.info(f"Downloading {local_path} from {prefix}")
        success = True
        for obj in oss2.ObjectIterator(self.bucket, prefix=prefix):
            if not obj.key.endswith("/"):  # 忽略目录对象
                local_file_path = (local_path + obj.key[len(prefix):]).replace('//', '/')
                if not self.DownloadFile(obj.key, local_file_path):
                    success = False
        return success

    def IsFileExists(self, prefix):
        return self.bucket.object_exists(prefix)

    def ListFiles(self, prefix, recursive=False):
        logging.error("fake function") # FIXME
        return []
