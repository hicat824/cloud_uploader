import logging
import os.path
from abc import ABC, abstractmethod
import subprocess
from enum import IntEnum
from dataclasses import dataclass
import concurrent.futures
import threading
from typing import Dict, List

from util_modules.taskInfo_util import SimpleTaskInfoUtil
from util_modules.log_util import *
from util_modules.platform_util import *
from util_modules.UploadTracker import *
from util_modules.loctime_util import *
from modules.CloudServices.CSFactory import CSFactory

""" --------------------------------------------------------------------------------------------------------- """

class UploadRC(IntEnum):
    SUCCESS = 0,
    MISSING_FILE = 1,
    CONNECTION_ERROR = 2,
    UNKNOWN_ERROR = 99

class FileInfo:
    def __init__(self):
        self.abs_path = None
        self.rel_path = None
        self.size = 0
        self.remove_after_upload = False
        self.compress_before_upload = False

class PackageInfo:
    def __init__(self):
        self.id = 0 # for package map
        self.key = None  # unique package name, for upload records
        self.input_bucket_path = None
        self.output_bucket_path = None
        self.local_root = None
        self.file_size = 0
        self.file_list = []  # FileInfos
        self.st = None
        self.et = None
        self.task_id = None
        self.desc = "uploading"
        self.data_type = None # 如果存在有先使用此处标识的数据类型，用于混合数据上传

        self.mq_msg = None # for gac

    def ToReqjson(self, tenant_id, app_id, data_type):
        fake_name = os.path.basename(self.file_list[0].abs_path)
        j_res = {
            "tenantId": tenant_id,
            "appId": app_id,
            "files": [{
                "fileName": fake_name,
                "fileSize": 0,
                "md5": "111"
            }],
            "mac": data_type
        }
        return j_res

    def ToCallbackMsg(self, sn, add_header=True):
        msg = {
            "package_name": self.key,
            "input_bucket_path": self.input_bucket_path,
            "upload_start_time": self.st,
            "upload_end_time": self.et,
            "desc": self.desc,
            "local_path": self.local_root,
            "sn": sn,
            "taskId": self.task_id
        }
        if add_header:
            return {"log@customer": msg}
        else:
            return msg

""" --------------------------------------------------------------------------------------------------------- """

class BaseUploader:
    def __init__(self, task_info_file, run_mode, sn):
        self.sn = sn
        self.cloud_type = None # 对象存储类型
        self.data_type = None # http://wiki.kuandeng.com/pages/viewpage.action?pageId=68324249 定义的客户ID
        self.source_type = None # 数据源类型
        self.app_id = None
        self.task_info = None
        self.tracker = None

        self._Init(task_info_file, run_mode)

        self.package_map:Dict[int, PackageInfo] = {} # <package.id, package_info>
        self.input_files_size = 0 # 待上传文件大小
        self.unupload_package_count = 0

        self.callback_engine = None
        self.progress_bar = None

    def _CleanUpTarRoot(self):
        if os.path.exists(os.path.join(self.task_info.output_root, "tar_root")):
            RemoveLocalFile(os.path.join(self.task_info.output_root, "tar_root"))

    def Run(self):
        logging.info(f"> {'-' * 15} \033[34m 开始执行上传脚本 \033[0m {'-' * 15} <")
        # 命令行获取硬盘数据大小
        cmd = f"du -sb {self.task_info.input_root} | cut -f1"
        result = subprocess.run(
            cmd,
            shell=True,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        disk_file_size = int(result.stdout.strip())
        logging.info(f"|{'-' * 12} 当前硬盘数据大小:{disk_file_size / pow(1024, 3)}GB")

        self._CleanUpTarRoot()

        groups = self.ListInputPackages()
        if len(groups) == 0:
            logging.error(f"找不到需要上传的数据，退出上传")
            return UploadRC.MISSING_FILE
        logging.info(f"|{'-' * 12} 检测出需要上传的数据大小为:{self.input_files_size / pow(1024, 3)}GB")
        self.progress_bar = ProgressManager(self.input_files_size)

        self.InitCallbackFunction(self.task_info.tags["upload_log_topic"])

        rt = self._UploadProcess(groups)

        self._WriteUploadRecords(disk_file_size)

        # if self.unupload_package_count <= 0:
        #     self.tracker.complete_upload()

        logging.info(f"> {'-' * 15} \033[34m 上传脚本运行完成, return code = {rt} \033[0m {'-' * 15} <")

        self._CleanUpTarRoot()

        return int(rt)

    def _Init(self, task_info_file, run_mode):
        self.task_info = SimpleTaskInfoUtil(task_info_file)
        # init log
        log_root = self.task_info.output_root + "/logs"
        os.makedirs(log_root, exist_ok=True)
        log_file = os.path.join(log_root, "oss_uploader.log")
        LoggingAddTimedRotatingFileHandler(log_file, "D", 10, 1)

        # load local config
        current_script_root = os.path.dirname(os.path.abspath(__file__))
        config_file = os.path.join(os.path.dirname(current_script_root), "conf/platform_config_test.json")
        if run_mode != "test":
            config_file = os.path.join(os.path.dirname(current_script_root), f"conf/platform_config_{run_mode}.json")
        self.cloud_type = self.task_info.tags["cloud_type"]
        self.source_type = self.task_info.tags["source_type"]
        self.data_type = self.task_info.tags["data_type"]

        if os.path.exists(config_file):
            with open(config_file, "r") as fp:
                content = json.load(fp)
                config_datas = content[self.data_type]
                for key, val in config_datas.items():
                    if key == "app_ids":
                        self.app_id = val.get(self.source_type)
                    else:
                        self.task_info.tags[key] = val
        else:
            raise FileNotFoundError(f"missing config file {config_file}")

        # init local data tracker
        local_db_root = "/tmp/cloud_upload_records"
        os.makedirs(local_db_root, exist_ok=True)
        local_data_base_file = os.path.join(local_db_root, f"{self.source_type}.db")
        self.tracker = UploadTracker(local_data_base_file)
        logging.info(f"red bucket name = {self.task_info.tags['red_bucket_name']}, yellow_bucket_name = {self.task_info.tags['yellow_bucket_name']}")

    def _UploadProcess(self, groups):
        cpu_nums = int(self.task_info.tags["cpuNums"])
        with concurrent.futures.ThreadPoolExecutor(max_workers=cpu_nums) as executor:
            future_to_group = {executor.submit(self._UploadSingleGroup, group): group for group in groups}
            for future in concurrent.futures.as_completed(future_to_group):
                try:
                    rt = future.result()
                    if not rt:
                        continue
                except Exception as e:
                    logging.error(f"catch exception during upload group : {e}")
                    return UploadRC.UNKNOWN_ERROR
        return UploadRC.SUCCESS

    def _UploadSingleGroup(self, group):
        if self.package_map[group[0]].input_bucket_path is None:
            j_create_package = self.package_map.get(group[0]).ToReqjson(self.task_info.tags["tenant_id"],
                                                                        self.app_id,
                                                                        self.task_info.tags["data_type"])
            response = HttpPostJson(self.task_info.tags["cs_create_package_url"], j_create_package)
            task_id = response["data"]["packageId"]
            if response is None:
                raise ConnectionError("请求createUploadPackage接口失败，请检查网络连接")
            cloud_prefix = response["data"]["objectKeyRoot"]
            if cloud_prefix.endswith("/"):
                cloud_prefix = cloud_prefix[:-1]
            for id in group:
                package_info = self.package_map.get(id)
                package_info.task_id = response["data"]["packageId"]
                package_info.input_bucket_path = cloud_prefix
        else:
            task_id = self.package_map.get(group[0]).task_id
        if task_id is None:
            raise Exception("task id is None !!!")


        connect_params = {
            "endpoint": self.task_info.tags["endpoint"],
            "ak": self.task_info.tags["ak"],
            "sk": self.task_info.tags["sk"],
            "region": self.task_info.tags["region"],
            "bucket_name": self.task_info.tags["red_bucket_name"],
            "secure": self.task_info.tags["secure"],
            "output_root": self.task_info.output_root
        }
        logging.info(f"connect params = {connect_params}")
        conn = CSFactory.CreateConnector(cloud_type=self.cloud_type, **connect_params)

        failed_count = 0
        for id in group:
            package_info = self.package_map.get(id)
            package_info.output_bucket_path = self.task_info.tags["yellow_bucket_name"]
            package_info.st = GetFormattedTime()

            try:
                if not self._UploadSinglePackage(package_info, conn):
                    package_info.desc = "failed"
                    failed_count += 1
                else:
                    package_info.desc = "success"
                    self.tracker.updateStatus(self.sn, package_info.key, package_info.input_bucket_path,
                                              package_info.task_id, package_info.desc, package_info.file_size)
            except Exception as e:
                logging.error(e)
                package_info.desc = "failed"
                failed_count += 1

            package_info.et = GetFormattedTime()
            self.SendMessage(package_info, self.task_info.tags["upload_log_topic"])

        # 只有一组数据包全部上传成功才通知平台已经上传完成
        if failed_count == 0 and self.task_info.tags["notice_the_platform"] == "true":
            j_callback = {
                "appId": self.app_id,
                "tenantId": self.task_info.tags["tenant_id"],
                "id": task_id
            }
            response = HttpPostJson(self.task_info.tags["cs_uplaod_callback_url"], j_callback)
            if response is None:
                raise ConnectionError("请求上传回调接口失败，请检查网络连接")
        return True

    def _UploadSinglePackage(self, package_info:PackageInfo, conn):
        # 20251208 打包目录添加一级，避免多个上传任务同一个output产生冲突
        tar_root = os.path.join(self.task_info.output_root, "tar_root", str(package_info.task_id), package_info.key)
        os.makedirs(tar_root, exist_ok=True)

        file_num = len(package_info.file_list)
        for i in range(file_num):
            file_info = package_info.file_list[i]
            if not isinstance(file_info, FileInfo):
                continue

            if file_info.compress_before_upload:
                file_info.abs_path = TarLocalFolder(file_info.abs_path, tar_root)
                if file_info.abs_path is None:
                    return False

            file_name = os.path.basename(file_info.abs_path)
            remote_path = os.path.normpath(os.path.join(package_info.input_bucket_path, file_info.rel_path, file_name))
            if os.path.isfile(file_info.abs_path):
                upload_mark = conn.UploadFile(remote_path, file_info.abs_path)
            elif os.path.isdir(file_info.abs_path):
                upload_mark = conn.UploadFolder(package_info.input_bucket_path, file_info.abs_path)
            else:
                logging.error(f"找不到本地文件{file_info.abs_path}")
                return False

            if file_info.remove_after_upload:
                RemoveLocalFile(file_info.abs_path)
            if upload_mark:
                self.progress_bar.UpdateMain(file_info.size)
                package_info.file_size += file_info.size
            else:
                logging.error(f"上传数据{file_info.abs_path}到{package_info.input_bucket_path}失败")
                return False

        return True

    def _WriteUploadRecords(self, disk_file_size):
        timestamp_str = GetFormattedTime()
        output_record_csv = os.path.join(self.task_info.output_root, f"upload_record_{timestamp_str}.csv")
        header = "sn, 数据包名称, 数据类型, 文件大小(GB), 文件数目, 开始上传时间, 结束上传时间, 本地路径, oss路径, 上传状态, 任务ID\n"
        writer = open(output_record_csv, "w", encoding="utf-8")
        writer.write(header)

        upload_file_size = 0
        upload_file_count = 0
        for id, package_info in self.package_map.items():
            if not isinstance(package_info, PackageInfo):
                continue
            size_GB = package_info.file_size / pow(1024, 3)
            record_line = (f"{self.sn}, {package_info.key}, {self.task_info.tags['data_type']}, {size_GB},"
                           f"{len(package_info.file_list)}, {package_info.st}, {package_info.et},"
                           f"{package_info.local_root}, {package_info.input_bucket_path}, {package_info.desc}, {package_info.task_id}\n")
            writer.write(record_line)
            #
            if package_info.desc == "success":
                upload_file_size += package_info.file_size
                upload_file_count += len(package_info.file_list)
            else:
                self.unupload_package_count += 1

        logging.info(f"上传完成：本次上传数据大小={upload_file_size/pow(1024,3)}GB，剩余数据大小={(disk_file_size-upload_file_size)/pow(1024,3)}GB")
        writer.write(f"/,/,/,{upload_file_size / pow(1024, 4)}TB,/,/,/,/,/,/,/\n")
        writer.close()


    @abstractmethod
    def ListInputPackages(self):
        logging.info("fake function")
        return []

    @abstractmethod
    def InitCallbackFunction(self, topic):
        pass

    # 不管成功失败都会发送
    @abstractmethod
    def SendMessage(self, package_info: PackageInfo, topic):
        pass