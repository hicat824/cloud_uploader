import logging
import os
import re
from datetime import datetime
import shutil

from modules.CloudUploader.BaseUploader import *
from util_modules.platform_util import TarLocalFolder

class DJiUploader(BaseUploader):
    def __init__(self, task_info_file, run_mode, sn):
        super().__init__(task_info_file, run_mode, sn)
        self.invalid_package_list = {}

    def _checkTravelDataRoot(self, name):
        m = re.match(r"^car_(\d{4})-(\d{2})-(\d{2})", name)
        if not m:
            return False
        year, month, day = map(int, m.groups())
        # 校验日期是否合法
        try:
            datetime(year, month, day)
            return True
        except ValueError:
            return False

    def _checkProcess(self, package_path, source_type):
        if source_type == 'raw_data':
            storage_info_json = os.path.join(package_path, "storage_info.json")
            if not os.path.exists(storage_info_json):
                logging.error(f"{storage_info_json} is not existed")
                self.invalid_package_list[package_path] = "missing storage_info.json"
                return False
            with open(storage_info_json, "r") as fp:
                content = json.load(fp)
                if "collectInfo" not in content.keys():
                    logging.error(f"collect_info not found in {storage_info_json}")
                    self.invalid_package_list[package_path] = "key 'collectInfo' not found in storage_info.json"
                    return False
        elif source_type == "TTE":
            meta_root = os.path.join(package_path, "metadata")
            if not os.path.exists(meta_root):
                logging.error(f"{meta_root} is not existed")
                self.invalid_package_list[package_path] = "missing metadata folder"
                return False
            vehicle_desc_json = os.path.join(meta_root, "vehicle_desc.json")
            if not os.path.exists(vehicle_desc_json):
                logging.error(f"{vehicle_desc_json} is not existed")
                self.invalid_package_list[package_path] = "missing vehicle_desc.json"
                return False
            with open(vehicle_desc_json, "r") as fp:
                content = json.load(fp)
                if "collect_info" not in content.keys():
                    logging.error(f"collect_info not found in {vehicle_desc_json}")
                    self.invalid_package_list[package_path] = "key 'collect_info' not found in vehicle_desc.json"
                    return False
        else:
            raise TypeError(f"Unsupported source type: {source_type}")

        logging.info(f"{source_type}类型数据{package_path}检查通过")
        return True

    """
    卓驭的数据不需要分组，一个数据包创建一个任务，数据源包含量产与数采数据
    http://wiki.kuandeng.com/pages/viewpage.action?pageId=77827703
    
    20251121 新增一种数据类型以及两种数据的校验规格，参考：http://wiki.kuandeng.com/pages/viewpage.action?pageId=83102236
    """
    def ListInputPackages(self):
        travel_data_root_list = [] # 形成数据目录清单 car_YY-MM-DD_xxx
        level3_folder_list, _ = listLevelDirs(self.task_info.input_root, 3)
        for level3_folder in level3_folder_list:
            if self._checkTravelDataRoot(os.path.basename(level3_folder)):
                travel_data_root_list.append(level3_folder)
        logging.info("-------------- 当前硬盘行程数据目录如下：")
        logging.info(travel_data_root_list)

        package_info_list = []

        for travel_data_root in travel_data_root_list:
            travel_base_name = os.path.basename(travel_data_root)
            tmp_root = os.path.join(self.task_info.output_root, f"tmp/{travel_base_name}")
            os.makedirs(tmp_root, exist_ok=True)
            tar_root = os.path.join(self.task_info.output_root, f"tar_root/{travel_base_name}/common_part")
            os.makedirs(tar_root, exist_ok=True)

            clip_list = []
            rel_path_to_input_root = os.path.relpath(travel_data_root, self.task_info.input_root)
            for sub_name in os.listdir(travel_data_root):
                source_type = "common"
                sub_path = os.path.join(travel_data_root, sub_name)
                if sub_name.startswith("AIPC_DATA") and os.path.isdir(sub_path):
                    source_type = "raw_data"
                elif sub_name.startswith("trigger_") and os.path.isdir(sub_path):
                    source_type = "TTE"
                elif 'dlog' in sub_name:
                    logging.warning(f"跳过当前目录:{sub_name}")
                    continue
                else:
                    target_path = os.path.join(tmp_root, sub_name)
                    if os.path.isdir(sub_path):
                        shutil.copytree(sub_path, target_path, dirs_exist_ok=True)
                    else:
                        shutil.copyfile(sub_path, target_path)
                    continue
                if not self._checkProcess(sub_path, source_type):
                    logging.warning(f"clip = {sub_path}, source type = {source_type}, 检查不通过，跳过数据！！！")
                    continue
                 # generate clip
                file_info = FileInfo()
                file_info.rel_path = rel_path_to_input_root
                file_info.remove_after_upload = True
                file_info.compress_before_upload = True
                file_info.abs_path = sub_path
                file_info.size = self._GetFolderSize(file_info.abs_path)
                clip_list.append(file_info)
            logging.info(f"行程数据目录下clip总计{len(clip_list)}个，处理中......")

            # add common part
            file_info_other = FileInfo()
            file_info_other.rel_path = rel_path_to_input_root
            file_info_other.remove_after_upload = False
            file_info_other.compress_before_upload = False
            file_info_other.abs_path = TarLocalFolder(tmp_root, tar_root)
            file_info_other.size = os.path.getsize(file_info_other.abs_path)

            for file_info in clip_list:
                package_info = PackageInfo()
                package_info.id = len(self.package_map)
                package_info.key = os.path.basename(file_info.abs_path)
                package_info.local_root = self.task_info.input_root
                package_info.file_list.append(file_info)
                package_info.file_list.append(file_info_other)

                upload_record = self.tracker.checkStatus(self.sn, package_info.key)
                if upload_record and upload_record.upload_mark:
                    if "force_upload" in self.task_info.tags and self.task_info.tags["force_upload"] == "true":
                        logging.info(f"数据包{package_info.key}已经上传过，强制上传")
                        self.package_map[package_info.id] = package_info
                        package_info_list.append([package_info.id])
                        self.input_files_size += file_info.size
                    else:
                        logging.info(f"数据包{package_info.key}已经上传过，跳过")
                        package_info.desc = "success"
                        package_info.input_bucket_path = upload_record.oss_root
                        self.package_map[package_info.id] = package_info
                else:
                    self.package_map[package_info.id] = package_info
                    package_info_list.append([package_info.id])
                    self.input_files_size += file_info.size
            self.input_files_size += file_info_other.size
        # 落盘不合规数据记录
        if len(self.invalid_package_list) > 0:
            os.makedirs(os.path.join(self.task_info.output_root, "failed_log"), exist_ok=True)
            timestamp_str = GetFormattedTime()
            output_record_file = os.path.join(self.task_info.output_root, f"failed_log/invalid_package_list_{self.sn}_{timestamp_str}.csv")
            with open(output_record_file, 'w') as fp:
                fp.write("package_path, desc\n")
                for package_path, desc in self.invalid_package_list.items():
                    fp.write(f"{package_path},{desc}\n")
        logging.info(f"all file info size = {self.input_files_size}")
        return package_info_list

    def InitCallbackFunction(self, topic):
        pass

    def SendMessage(self, package_info: PackageInfo, topic):
        msg = package_info.ToCallbackMsg(sn=self.sn)
        HttpPostJson(topic, msg)

    @staticmethod
    def _GetFolderSize(local_path):
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