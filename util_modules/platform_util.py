from confluent_kafka import Producer
import logging
import json
import time
import requests
from multiprocessing import Process, Queue
from functools import wraps
from enum import IntEnum
import os

class RT(IntEnum):
    SUCCESS = 0,
    DOWNLOAD_ERROR = 1,
    UPLOAD_ERROR = 2,
    HTTP_ERROR = 3,
    INVALID_INPUT = 4,
    GPU_TYPE_ERROR = 5,
    DOWNLOAD_MODEL_ERROR = 6,
    MISSING_PARAMETER_ERROR = 7
    UNKNOWN_ERROR = 99

class PkgStatusCode(IntEnum):
    VALID = 0,
    DECODE_FAILED = 1,
    CHECK_FAILED = 2,
    GEO_COMPLIANCE_FAILED = 3,
    PERSONAL_DATA_ANONYMIZATION_FAILED = 4,
    ENCODE_FAILED = 5,
    UNKNOWN_ERROR = 6

def kafka_callback(err, msg):
    if err is not None:
        logging.info('Message delivery failed: {}'.format(err))
    else:
        logging.info('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

class KafkaProducer:
    def __init__(self, url, usr, pwd, topic):
        self._producer = Producer({'bootstrap.servers': url, 'sasl.mechanisms': 'SCRAM-SHA-256', 'sasl.username': usr, 'sasl.password': pwd, 'security.protocol': 'SASL_PLAINTEXT'})
        self._topic = topic

    def SendMessage(self, msg:str):
        self._producer.poll(1)
        self._producer.produce(self._topic, msg.encode('utf-8'), callback=kafka_callback)
        self._producer.flush()


class KafkaProducerSSL:
    def __init__(self, url, usr, pwd, topic, certfile):
        self._producer = Producer({'bootstrap.servers':url,
	'sasl.mechanisms':'PLAIN',
	'ssl.ca.location':certfile,
	'security.protocol':'SASL_SSL',
    # hostname 校验改成空
	'ssl.endpoint.identification.algorithm':'none',
	'sasl.username':usr,
	'sasl.password':pwd})
        self._topic = topic

    def SendMessage(self, msg:str):
        self._producer.poll(1)
        self._producer.produce(self._topic, msg.encode('utf-8'), callback=kafka_callback)
        self._producer.flush()

# 用于进程超时自动重启
def timeout_retry(max_retry=3, timeout=60):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retry_count = 0
            while retry_count < max_retry:
                result_queue = Queue()
                exc_queue = Queue()

                def worker(rq, eq):
                    try:
                        ret = func(*args, **kwargs)
                        rq.put(ret)
                    except Exception as e:
                        eq.put(e)

                p = Process(target=worker, args=(result_queue, exc_queue))
                p.start()
                p.join(timeout=timeout)

                if p.is_alive():
                    p.terminate()  # 强制终止进程
                    p.join()  # 等待进程资源回收
                    retry_count += 1
                    continue

                if not exc_queue.empty():
                    retry_count += 1
                    continue

                return result_queue.get()

            raise TimeoutError(f"操作在{max_retry}次重试后仍失败")

        return wrapper

    return decorator

def RemoveLocalFile(file_path):
    if not os.path.exists(file_path):
        return
    try:
        if os.path.isfile(file_path):
            os.remove(file_path)
        else:
            cmd = f"rm -r {file_path}"
            if os.system(cmd):
                logging.error(f"failed to remove local file {file_path}")
                return
        logging.info(f"removed local file {file_path}")
    except Exception as e:
        logging.error(f"failed to remove local file {file_path}, got error {e}")

"""
列出 root 下第 level 级的所有文件夹（完整路径）和这些文件夹下的文件（完整路径），分开返回。

说明：
  - level = 0 表示 root 本身
  - level = 1 表示 root 的直接子目录
  - level = 2 表示子目录的子目录，以此类推
"""
def listLevelDirs(root: str, level: int):
    root = os.path.abspath(root)
    dirs = []
    files = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if not d.startswith('.')]
        rel = os.path.relpath(dirpath, root)
        dir_depth = 0 if rel == '.' else (rel.count(os.sep) + 1)

        # 如果这个目录本身处在目标深度 -> 是一个 folder
        if dir_depth == level:
            dirs.append(dirpath)
            # 不继续深入该目录
            dirnames[:] = []

        # 如果当前目录的子文件位于目标深度（文件深度 = dir_depth + 1）
        if dir_depth + 1 == level:
            for fn in filenames:
                files.append(os.path.join(dirpath, fn))

        # 剪枝：已到或超过目标深度就不再深入
        if dir_depth >= level:
            dirnames[:] = []

    return dirs, files


def HttpPostJson(url, data, print_logs: bool = True):
    max_retry_times = 3
    wait_time = 10
    headers = {'Content-Type': 'application/json'}
    if print_logs:
        logging.info(f"url = {url}, data = {data}")
    for attempt in range(max_retry_times):
        logging.info("retry times = {}".format(attempt))
        try:
            response = requests.request("POST", url, headers=headers, json=data,
                                        timeout=60)
            if not response.ok:
                logging.error(f"post data to {url} failed")
                continue
            j_res = json.loads(response.content)
            if j_res['code'] != '0' and j_res['code'] != 0:
                logging.error("wrong post params, return code = {}".format(j_res['code']))
                logging.error("return msg = {}".format(j_res["message"]))
                continue
            return j_res
        except requests.exceptions.Timeout:
            logging.info("timeout, waitting for retry.........")
            time.sleep(wait_time)
        except requests.exceptions.RequestException as e:
            logging.error(e)
            time.sleep(wait_time)

    return None


def HttpGetJson(url):
    try:
        response = requests.request("GET", url, timeout=60)
        if not response.ok:
            raise ConnectionError(f"request {url} failed")
        j_res = json.loads(response.content)
        if int(j_res['code']) != 0:
            raise ConnectionError(
                f"request {url} failed, return code = {j_res['code']}, message = {j_res['message']}")
        res_data = j_res['data']
        return res_data
    except requests.exceptions.RequestException as e:
        logging.error(e)

    return None


def TarLocalFolder(folder_path, output_root, zip_mark=False):
    if not os.path.exists(folder_path):
        return None
    os.makedirs(output_root, exist_ok=True)
    folder_name = os.path.basename(folder_path)
    if zip_mark:
        tar_file = os.path.join(output_root, f"{folder_name}.tgz")
        cmd = f"tar -czf {tar_file} -C {folder_path} ."
    else:
        tar_file = os.path.join(output_root, f"{folder_name}.tar")
        cmd = f"tar -cf {tar_file} -C {folder_path} ."
    if os.path.exists(tar_file):
        if os.system('tar -tvf {} > /dev/null 2>&1'.format(tar_file)):
            os.remove(tar_file)
        else:
            logging.info(f"{tar_file} is already exsited")
            return tar_file
    logging.info(cmd)
    if os.system(cmd):
        return None
    return tar_file

def GetFileSize(file_path, isLogicSize=False):
    try:
        stat = os.stat(file_path)
        logical_size = stat.st_size  # 逻辑大小(字节)
        disk_usage = stat.st_blocks * 512  # 实际磁盘占用(字节)
        if isLogicSize:
            return logical_size
        else:
            return disk_usage
    except FileNotFoundError:
        raise FileNotFoundError(f"错误: 文件 {file_path} 不存在")
    except Exception as e:
        raise e


import subprocess
def GetFolderSize(local_path):
    cmd = f"du -sb {local_path} | cut -f1"
    result = subprocess.run(
        cmd,
        shell=True,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    package_size = int(result.stdout.strip())
    return package_size



from tqdm import tqdm
from threading import Lock
class ProgressManager:
    def __init__(self, total_tasks):
        bar_format = "{l_bar}%s{bar}%s{r_bar}" % ("\033[32m", "\033[0m")
        self.main_bar = tqdm(total=total_tasks, desc="上传进度", position=0, bar_format=bar_format, unit="B", unit_scale=True)
        self.thread_bars = {} # 用于多线程多进度条
        self.lock = Lock()

    def UpdateMain(self, amount):
        with self.lock:
            self.main_bar.update(amount)

    def add_thread_bar(self, thread_id, total):
        with self.lock:
            self.thread_bars[thread_id] = tqdm(
                total=total,
                desc=f"线程{thread_id}",
                position=thread_id + 1,
                leave=False
            )

    def update_thread(self, thread_id, amount=1):
        with self.lock:
            self.thread_bars[thread_id].update(amount)



"""
跨账号发送kafka消息
"""
# class KafkaProducerWithToken:
#     def __init__(self):
#         self._ak = None
#         self._sk = None
#         self._arn = None
#         self._role_name = "kd-producer-session"
#         self._sasl_usr_name = None
#         self._sasl_pwd = None
#         self._kafka_servers = None
#         self._region = None
#         self._duration = 3600
#         self._endpoint = None
#
#     def Init(self, config_file):
#         with open(config_file, "r") as fp:
#             content = json.load(fp)
#             for k, v in content.items():
#                 if k == "ak":
#                     self._ak = v
#                 elif k == "sk":
#                     self._sk = v
#                 elif k == "arn":
#                     self._arn = v
#                 elif k == "sasl-usr-name":
#                     self._sasl_usr_name = v
#                 elif k == "sasl-pwd":
#                     self._sasl_pwd = v
#                 elif k == "kafka-servers":
#                     self._kafka_servers = v
#                 elif k == "region":
#                     self._region = v
#                 elif k == "duration":
#                     self._duration = int(v)
#                 elif k == "endpoint":
#                     self._endpoint = v
#
#     def __create_client(self) -> Sts20150401Client:
#         """
#         使用凭据初始化账号Client
#         @return: Client
#         @throws Exception
#         """
#         # 工程代码建议使用更安全的无AK方式，凭据配置方式请参见：https://help.aliyun.com/document_detail/378659.html。
#         config = open_api_models.Config(
#             access_key_id=self._ak,
#             access_key_secret=self._sk,
#             region_id=self._region,
#             endpoint=self._endpoint
#         )
#         return Sts20150401Client(config)
#
#     def __GetSTSToken(self):
#         client = self.__create_client()
#         assume_role_request = sts_20150401_models.AssumeRoleRequest(
#             duration_seconds=self._duration,
#             role_arn=self._arn,
#             role_session_name=self._role_name
#         )
#         try:
#             # 复制代码运行请自行打印 API 的返回值
#             response = client.assume_role_with_options(assume_role_request, util_models.RuntimeOptions())
#             if response.status_code != 200:
#                 return None
#             # 返回临时凭证
#             return response.body
#         except Exception as error:
#             logging.fatal(f"error occurred while trying to get token : {error}")
#             UtilClient.assert_as_string(error.message)
#
#         return None
#
#     def Run(self):
#         self._refresh_token()
#         threading.Thread(target=self._refresh_loop, daemon=True).start()
#
#     def _refresh_token(self):
#         new_cred = self.__GetSTSToken()
#         if new_cred is None:
#             raise Exception(f"获取sts token失败")
#         logging.info(f"get new token {new_cred}")
#         # 更新生产者配置（需重启生产者）
#         self.producer = Producer({
#             'bootstrap.servers': self._kafka_servers,
#             'security.protocol': 'SASL_SSL',
#             'sasl.mechanism': 'PLAIN',
#             'sasl.username': new_cred.credentials.access_key_id,
#             'sasl.password': f"{new_cred.credentials.access_key_secret}:{new_cred.credentials.security_token}",
#             'ssl.ca.location': '/home/xumaozhou/projects_py/OssUploader/modules/conf/mix-4096-ca-cert',
#         })
#
#     def _refresh_loop(self):
#         while True:
#             time.sleep(self._duration-600)
#             self._refresh_token()