"""
广汽上传新版本 by xumaozhou 2025/12/31
参考：http://wiki.kuandeng.com/pages/viewpage.action?pageId=83105567
"""
import logging
import os
import csv
import yaml
from typing import Dict

from modules.CloudUploader.BaseUploader import *
from util_modules.platform_util import *
from .ledgerUtil import *

INPUT_PACKAGE_LEVEL = 2 # 输入数据包所在的文件夹层级


class garcndUploader(BaseUploader):
    def __init__(self, task_info_file, run_mode, sn):
        super().__init__(task_info_file, run_mode, sn)
        self.clip_size = 50 * 1024 * 1024 * 1024  # 50GB一组
        current_time = datetime.now()
        self.upload_date = current_time.strftime("%Y%m%d")
        self.ledger_engine = ledgerUtil(self.task_info.tags["cs_gac_data_record_url"])
        self.disk_info = None
        self.kafka_producer = KafkaProducer(
            url=self.task_info.tags["Kafka_url"],
            usr=self.task_info.tags["Kafka_user_name"],
            pwd=self.task_info.tags["Kafka_password"],
            topic=self.task_info.tags["Kafka_topic"]
        )
        self.mq_url = f"http://cs-log.{self.task_info.tags['cs_domain_name']}/cs-log/rabbitLog/jsonLog"


    def __scanGroup(self, local_group_root, bagid_prefix) -> groupInfo:
        group_info = groupInfo()
        package_list, _ = listLevelDirs(local_group_root, INPUT_PACKAGE_LEVEL)
        if len(package_list) == 0:
            logging.error(f"failed to find package from {self.task_info.input_root}, please check input datas")
            return group_info
        package_list = sorted(package_list)
        # clip packages
        clip_size = 0
        bag_infos = []
        for package in package_list:
            bag_info = self.__scanPackage(package, bagid_prefix)
            bag_infos.append(bag_info)
            clip_size += bag_info.size
            if clip_size >= self.clip_size:
                data_info = dataInfo()
                data_info.size = clip_size
                data_info.bag_infos = bag_infos
                group_info.clip_infos.append(data_info)
                clip_size = 0
                bag_infos = []
        if clip_size > 0 and len(bag_infos) > 0:
            data_info = dataInfo()
            data_info.size = clip_size
            data_info.bag_infos = bag_infos
            # data_info.updateDataId()
            group_info.clip_infos.append(data_info)
        logging.info(f"load {len(group_info.clip_infos)} clip from {local_group_root}")
        return group_info



    def __scanDisk(self) -> Tuple[diskInfo, Dict]:
        disk_info = diskInfo()
        disk_info.sn_number = self.sn
        disk_info.data_type = SOURCE_TYPE_MAP.get(self.source_type)
        disk_info.upload_date = self.upload_date
        bag_files_map = {} # bag_id -> fileList

        # 20260126 csv文件group基于主键先去重
        group_info_map = dict()
        for entry in os.listdir(self.task_info.input_root):
            local_entry_path = os.path.join(self.task_info.input_root, entry)
            if not os.path.isfile(local_entry_path):
                continue
            if not entry.endswith(".csv"):
                continue
            with open(local_entry_path, 'r') as fp:
                csv_reader = csv.reader(fp)
                skip_header = False
                for row in csv_reader:
                    if not skip_header:
                        skip_header = True
                        continue
                    collect_date = row[0]
                    if collect_date == 'total':
                        continue
                    vin = row[1]
                    source_disk_sn = row[2]
                    source_bag_count = int(row[3])
                    source_bag_size = int(row[4])
                    #
                    local_collect_root = os.path.join(self.task_info.input_root, collect_date)
                    if not os.path.exists(local_collect_root):
                        logging.error(f"failed to find local collect root: {local_collect_root}")
                        continue
                    for vname in os.listdir(local_collect_root):
                        if vin not in vname:
                            continue
                        local_group_root = os.path.join(local_collect_root, vname)
                        parts = vname.split('_')
                        car_id = None
                        bagid_prefix = vin
                        if len(parts) > 1:
                            car_id = parts[0]
                            bagid_prefix = car_id
                        group_info = self.__scanGroup(local_group_root, bagid_prefix)
                        if len(group_info.clip_infos) == 0:
                            continue
                        group_info.source_disk_sn = source_disk_sn
                        group_info.vin = vin
                        group_info.collect_date = collect_date
                        group_info.group_id = f"{vin}_{collect_date}"
                        group_info.source_bag_size = source_bag_size
                        group_info.source_bag_count = source_bag_count
                        entry_list = os.listdir(local_group_root)
                        entry_root = os.path.join(local_group_root, entry_list[0])
                        rel_path = os.path.relpath(entry_root, self.task_info.input_root)
                        yellow_oss_root = YELLOW_ZONE_OSS_PATH_MAP.get(self.source_type,
                                                                       self.task_info.tags['yellow_bucket_name'])
                        group_info.yellow_oss_path = f"{yellow_oss_root.replace('{bucket_name}', self.task_info.tags['yellow_bucket_name'])}{rel_path}"
                        group_info.car_id = car_id
                        #
                        if group_info.group_id in group_info_map:
                            group_info_map[group_info.group_id].source_bag_size += source_bag_size
                            group_info_map[group_info.group_id].source_bag_count += source_bag_count
                            group_info_map[group_info.group_id].source_disk_sn += f"/{source_disk_sn}"
                        else:
                            group_info_map[group_info.group_id] = group_info
                        # disk_info.group_infos.append(group_info)
        disk_info.group_infos = list(group_info_map.values())
        #
        for group_info in disk_info.group_infos:
            for data_info in group_info.clip_infos:
                for bag_info in data_info.bag_infos:
                    if bag_info.bag_id in bag_files_map:
                        raise Exception(f"存在同名数据包 {bag_info.bag_id}")
                    bag_files_map[bag_info.bag_id] = bag_info
                    self.input_files_size += bag_info.size
        return disk_info, bag_files_map


    def __scanPackage(self, package_root, bagid_prefix) -> bagInfo:
        bag_info = bagInfo()
        bag_info.bag_id = bagid_prefix + "_" + os.path.basename(package_root)
        bag_info.local_path = package_root
        rel_path = os.path.relpath(package_root, self.task_info.input_root)
        bag_info.red_oss_path = f"oss://{self.task_info.tags['red_bucket_name']}/{self.source_type}/gpg/DATAID/{rel_path}"
        yellow_oss_root = YELLOW_ZONE_OSS_PATH_MAP.get(self.source_type, self.task_info.tags['yellow_bucket_name'])
        bag_info.yellow_oss_path = f"{yellow_oss_root.replace('{bucket_name}', self.task_info.tags['yellow_bucket_name'])}{rel_path}"
        bag_info.size = GetFolderSize(package_root)
        #
        for root, dirs, files in os.walk(package_root):
            for file in files:
                file_info = FileInfo()
                file_info.abs_path = os.path.join(root, file)
                file_info.rel_path = os.path.relpath(root, self.task_info.input_root)
                bag_info.file_list.append(file_info)
        for file_info in bag_info.file_list: # fake file info
            file_info.size = bag_info.size / len(bag_info.file_list)
        #
        sn_txt = package_root + "/sn.txt"
        with open(sn_txt, "w") as fp:
            fp.write(self.sn)
        return bag_info


    def LoadMetaDataYaml(self, meta_data_file):
        duration = ""
        nanoseconds_since_epoch = ""
        collection_time = ""

        if not os.path.exists(meta_data_file):
            logging.error(f"missing {meta_data_file}")
            return duration, nanoseconds_since_epoch, collection_time

        try:
            with open(meta_data_file, "r") as fp:
                content = fp.read()
            root_node = yaml.safe_load(content)
            nanoseconds_since_epoch = str(
                root_node["gacbag_bagfile_information"]["starting_time"]["nanoseconds_since_epoch"])
            collection_time = str(root_node["gacbag_bagfile_information"]["starting_time"]["time"])
            duration = str(root_node["gacbag_bagfile_information"]["duration"]["seconds"])
        except Exception as e:
            logging.fatal(f"parse {meta_data_file} failed : {e}")

        return duration, nanoseconds_since_epoch, collection_time


    def __queryDataId(self, file_name, file_size):
        j_req = {
            "tenantId": self.task_info.tags["tenant_id"],
            "appId": self.app_id,
            "mac": self.data_type,
            "files": [
                {
                    "fileName": file_name,
                    "file_size": file_size,
                    "md5": "111"
                }
            ]
        }
        response = HttpPostJson(self.task_info.tags["cs_create_package_url"], j_req)
        if response is None:
            raise ConnectionError("请求createUploadPackage接口失败，请检查网络连接")
        # cloud_prefix = response["data"]["objectKeyRoot"]
        # if cloud_prefix.endswith("/"):
        #     cloud_prefix = cloud_prefix[:-1]
        return response["data"]["packageId"]

    def __writeCacheFile(self):
        upload_cache_json = os.path.join(self.task_info.input_root, "kd_upload_cache.json")
        j_cache = {
            "sn": self.sn,
            "upload_date": self.upload_date
        }
        with open(upload_cache_json, "w") as fp:
            json.dump(j_cache, fp)

    @staticmethod
    def __updateFileList(disk_info: diskInfo, bag_files_map: Dict[str, bagInfo]):
        for group_info in disk_info.group_infos:
            for data_info in group_info.clip_infos:
                for bag_info in data_info.bag_infos:
                    if bag_info.bag_id not in bag_files_map:
                        raise Exception(f"找不到数据包{bag_info.bag_id}对应的文件记录！！！")
                    bag_info.file_list = bag_files_map[bag_info.bag_id].file_list
                    bag_size = bag_files_map[bag_info.bag_id].size
                    bag_info.local_path = bag_files_map[bag_info.bag_id].local_path
                    bag_info.size = bag_size
                    for file_info in bag_info.file_list:
                        file_info.size = bag_size / len(bag_info.file_list)

    def ListInputPackages(self):
        # 先遍历硬盘，获取所有数据包与其对应的文件列表
        logging.info(f"> --------------------------- 开始扫描硬盘文件")
        disk_info, bag_files_map = self.__scanDisk()
        output_tmp_json = os.path.join(self.task_info.output_root, "disk_info_scan.json")
        with open(output_tmp_json, "w") as fp:
            json.dump(disk_info.toJson(), fp)
        #
        upload_cache_json = os.path.join(self.task_info.input_root, "kd_upload_cache.json") # 记录disk_info索引，SN+COLLECT_DATE为主键
        if os.path.exists(upload_cache_json):
            with open(upload_cache_json, "r") as fp:
                j_disk_cache = json.load(fp)
            self.sn = j_disk_cache["sn"]
            self.upload_date = j_disk_cache["upload_date"]
            logging.warning(f'disk {self.sn} is already uploaded, upload date = {self.upload_date}, get disk info from database ...')
            self.disk_info = self.ledger_engine.getDiskInfo(self.sn, self.upload_date)
            logging.info(f"> --------------------------- 成功加载硬盘记录，开始生成上传任务")
        else:
            for group_info in disk_info.group_infos:
                for clip_info in group_info.clip_infos:
                    clip_info.data_id = self.__queryDataId(clip_info.bag_infos[0].bag_id, clip_info.bag_infos[0].size)
                    clip_info.updateDataId()
            resp = self.ledger_engine.createDiskInfo(disk_info)
            if resp is None:
                raise Exception(f"failed to init disk info, response = {resp}")
            self.__writeCacheFile()
            self.disk_info = diskInfo()
            self.disk_info.fromJson(resp)
            logging.warning(
                f'disk {self.sn} is never uploaded, upload date = {self.upload_date}, get disk info from disk ...')
            logging.info(f"> --------------------------- 成功创建硬盘记录，开始生成上传任务")
        self.__updateFileList(self.disk_info, bag_files_map)
        bag_files_map.clear()
        # write disk info
        output_disk_info_json = os.path.join(self.task_info.output_root, "disk_info.json")
        with open(output_disk_info_json, "w") as fp:
            json.dump(self.disk_info.toJson(), fp)
        # to package info
        success_status_set = {uploadState.SUCCESS, uploadState.DESENSITIZING,
                              uploadState.DESENSITIZATION_FAILED, uploadState.DESENSITIZATION_COMPLETE}
        force_upload_mark = self.task_info.tags.get("force_upload", "false")
        logging.info(f"force upload = {force_upload_mark}")
        upload_groups = []
        for group_info in self.disk_info.group_infos:
            for clip_info in group_info.clip_infos:
                upload_group = []
                for bag_info in clip_info.bag_infos:
                    if group_info.state in success_status_set and force_upload_mark == "false":
                        logging.info(f"package {bag_info.bag_id} is already uploaded and success, skip")
                        continue
                    package_info = PackageInfo()
                    package_info.task_id = clip_info.data_id
                    package_info.id = bag_info.id
                    package_info.key = bag_info.bag_id
                    package_info.input_bucket_path = f"{self.source_type}/gpg/{clip_info.data_id}/"
                    package_info.output_bucket_path = bag_info.yellow_oss_path
                    package_info.file_list.extend(bag_info.file_list)
                    package_info.local_root = bag_info.local_path
                    bag_file_list = []
                    for data in package_info.file_list:
                        if data.abs_path.endswith(".bag"):
                            bag_file_list.append(os.path.relpath(data.abs_path, package_info.local_root))
                    # assemble mq msg
                    parts = package_info.local_root.split("/")
                    car_id = parts[-3].split("_")[0]
                    vin = parts[-3].split("_")[-1]
                    date = parts[-1]
                    task_id = f"{car_id}_{date}"
                    meta_file = os.path.join(bag_info.local_path, "metadata.yaml")
                    duration, nanoseconds_since_epoch, collection_time = self.LoadMetaDataYaml(meta_file)
                    package_info.mq_msg = {
                        "task_id": task_id,
                        "car_id": car_id,
                        "vin": vin,
                        "size": bag_info.size,
                        "upload_datetime": GetFormattedTime(),
                        "origin_storage_path": bag_info.red_oss_path,
                        "oss_storage_path": bag_info.yellow_oss_path,
                        "data_type": SOURCE_TYPE_MAP.get(self.source_type),
                        "disk_sn": group_info.source_disk_sn,
                        "bags": bag_file_list,
                        "duration": duration,
                        "collection_time": collection_time,
                        "nanoseconds_since_epoch": nanoseconds_since_epoch
                    }
                    # --------------------------------
                    self.package_map[package_info.id] = package_info
                    upload_group.append(package_info.id)
                if len(upload_group) > 0:
                    upload_groups.append(upload_group)
        return upload_groups


    @staticmethod
    def LogRetransmission(msg, post_url):
        logging.info(f"url = {post_url}, data = {msg}")
        wait_time = 20
        if "rabbitLog" not in post_url:
            custom_log = {
                "log@customer": msg
            }
        else:
            custom_log = msg

        for attempt in range(3):
            logging.info("retry times = {}".format(attempt))
            try:
                response = requests.request("POST", post_url, json=custom_log, timeout=60)
                if not response.ok:
                    logging.error(f"post data to {post_url} failed")
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


    def SendMessage(self, package_info: PackageInfo, topic):
        if package_info.desc == "success":
            # send yellow zone kafka msg
            data = json.dumps(package_info.mq_msg)
            self.kafka_producer.SendMessage(data)
            # send mq msg
            self.LogRetransmission(package_info.mq_msg, self.mq_url)
        # update ledger
        bag_info = bagInfo()
        bag_info.id = package_info.id
        bag_info.bag_id = package_info.key
        bag_info.red_oss_path = package_info.mq_msg["origin_storage_path"]
        bag_info.yellow_oss_path = package_info.output_bucket_path
        bag_info.state = (uploadState.SUCCESS if package_info.desc == "success" else uploadState.FAILED)

        self.ledger_engine.updateBagInfo(bag_info)