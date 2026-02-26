"""
create by xumaozhou 2025/12/31
用于广汽脱敏上传连接台帐系统
base on http://wiki.kuandeng.com/pages/viewpage.action?pageId=83105567
"""
import logging
from typing import Dict, List, Tuple
import json
from enum import IntEnum
from util_modules.platform_util import *

SOURCE_TYPE_MAP = {
    "agb": "ubm",
    "agp": "upm",
    "agt": "t68",
    "agd": "l2_dagger",
    "ag3": "upm_14_15",
    "agc": "upm_21",
    "age": "t68_com",
    "agf": "ay5",
    "ags": "s7",
    "agh": "thor_dagger"
}

YELLOW_ZONE_OSS_PATH_MAP = { # usage : replace {bucket_name}
    "agb": "oss://{bucket_name}/ubm/source/", # UBM
    "agp": "oss://{bucket_name}/upm/source/", # UPM
    "agt": "oss://{bucket_name}/t68/source/", # T68
    "agd": "oss://{bucket_name}/ubm/source/L2_Dagger/", # UBM_L2
    "ag3": "oss://{bucket_name}/upm/source/upm_A13Y_data/", # UBM_14_15
    "agc": "oss://{bucket_name}/upm/source/upm_A13Y_021_data/", # UPM_21
    "age": "oss://{bucket_name}/t68_com/huawei/raw_com_data/", # T68_COM
    "agf": "oss://{bucket_name}/ubm/source/ubmthor_AY5/", # AY5
    "ags": "oss://{bucket_name}/ubm/source/ubmthor_T68/",
    "agh": "oss://{bucket_name}/ubm/source/ubmthor_dagger/"
}


class uploadState(IntEnum):
    INIT = 0
    SUCCESS = 2
    FAILED = 3
    DESENSITIZING = 4
    DESENSITIZATION_FAILED = 5
    DESENSITIZATION_COMPLETE = 6



class diskInfo:
    def __init__(self):
        self.id = None # main key
        self.sn_number = None
        self.data_type = None
        self.upload_date = None

        self.group_infos: List[groupInfo] = []
        self.state = uploadState.INIT

    def fromJson(self, j_di):
        self.group_infos.clear()
        self.id = j_di["id"]
        self.sn_number = j_di["snNum"]
        self.data_type = j_di["dataType"]
        self.upload_date = j_di["uploadDate"]
        self.state = int(j_di["state"])
        for data in j_di["groupInfos"]:
            group_info = groupInfo()
            group_info.fromJson(data)
            self.group_infos.append(group_info)

    def toJson(self):
        j_group_infos = [d.toJson() for d in self.group_infos]
        return {
            "snNum": self.sn_number,
            "dataType": self.data_type,
            "uploadDate": self.upload_date,
            "groupInfos": j_group_infos
        }



class groupInfo:
    def __init__(self):
        self.group_id = None # main key
        self.source_disk_sn = None # '/'分割
        self.car_id = ""
        self.vin = None
        self.collect_date = None
        self.yellow_oss_path = None
        self.source_bag_count: int = 0
        self.source_bag_size: int = 0

        self.clip_infos: List[dataInfo] = []
        self.state = uploadState.INIT

    def fromJson(self, j_gi):
        self.clip_infos.clear()
        self.group_id = j_gi["groupId"]
        self.source_disk_sn = j_gi["sourceDiskSn"]
        self.car_id = j_gi["carId"]
        self.vin = j_gi["vin"]
        self.collect_date = j_gi["collectDate"]
        self.yellow_oss_path = j_gi["yellowOssPath"]
        self.state = int(j_gi["state"])
        self.source_bag_count = int(j_gi["sourceBagCount"])
        self.source_bag_size = int(j_gi["sourceBagSize"])
        for data in j_gi["dataInfos"]:
            data_info = dataInfo()
            data_info.fromJson(data)
            self.clip_infos.append(data_info)

    def toJson(self):
        j_data_infos = [d.toJson() for d in self.clip_infos]
        return {
            "groupId": self.group_id,
            "carId": self.car_id,
            "sourceDiskSn": self.source_disk_sn,
            "vin": self.vin,
            "collectDate": self.collect_date,
            "yellowOssPath": self.yellow_oss_path,
            "dataInfos": j_data_infos,
            "sourceBagCount": self.source_bag_count,
            "sourceBagSize": self.source_bag_size
        }


class dataInfo:
    def __init__(self):
        self.data_id = None # main key
        self.bag_infos: List[bagInfo] = []

        self.size = 0
        self.state = uploadState.INIT

    def fromJson(self, j_di):
        self.bag_infos.clear()
        self.data_id = j_di["dataId"]
        self.state = int(j_di["state"])
        for data in j_di["bagInfos"]:
            bag_info = bagInfo()
            bag_info.fromJson(data)
            self.bag_infos.append(bag_info)

    def toJson(self):
        j_bag_infos = [d.toJson() for d in self.bag_infos]
        return {
            "dataId": self.data_id,
            "bagInfos": j_bag_infos
        }

    def updateDataId(self):
        for bag_info in self.bag_infos:
            true_path = bag_info.red_oss_path.replace('DATAID', str(self.data_id))
            bag_info.red_oss_path = true_path

class bagInfo: # clipInfo
    def __init__(self):
        self.id = None
        self.bag_id = None # main key
        self.red_oss_path = None
        self.yellow_oss_path = None

        self.file_list = []
        self.size = 0
        self.state = uploadState.INIT
        self.local_path = None

    def fromJson(self, j_bi):
        self.file_list.clear()
        self.id = j_bi["id"]
        self.bag_id = j_bi["bagId"]
        self.red_oss_path = j_bi["redOssPath"]
        self.yellow_oss_path = j_bi["yellowOssPath"]
        self.state = int(j_bi["state"])


    def toJson(self):
        return {
            "id": self.id,
            "bagId": self.bag_id,
            "redOssPath": self.red_oss_path,
            "yellowOssPath": self.yellow_oss_path,
            "state": int(self.state)
        }

class ledgerUtil:
    def __init__(self, cs_gac_data_record_url):
        self.cs_gac_data_record_url = cs_gac_data_record_url
        logging.info(f"set cs gac data record url: {self.cs_gac_data_record_url}")

    def getDiskInfo(self, sn, upload_date):
        request_url = f"{self.cs_gac_data_record_url}/disk/findBySnNumAndUploadDate?snNum={sn}&uploadDate={upload_date}"
        logging.info(request_url)
        resp = HttpGetJson(request_url)
        if resp is None:
            return None
        disk_info = diskInfo()
        disk_info.fromJson(resp)
        return disk_info



    def createDiskInfo(self, disk_info: diskInfo):
        request_url = f"{self.cs_gac_data_record_url}/disk/create"
        request_data = disk_info.toJson()
        resp = HttpPostJson(request_url, request_data, print_logs=False)
        if resp is None:
            return None
        return resp["data"]


    def updateBagInfo(self, bag_info: bagInfo):
        request_url = f"{self.cs_gac_data_record_url}/bag/updateState"
        request_data = bag_info.toJson()
        resp = HttpPostJson(request_url, request_data)
        if resp is None:
            return False
        if int(resp['code']) != 0:
            return False
        return True