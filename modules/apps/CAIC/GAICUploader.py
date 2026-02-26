import logging
import os.path
import sys

from modules.CloudUploader.BaseUploader import *
from util_modules.elastic_util import *

class GAICUploader(BaseUploader):
    def __init__(self, task_info_file, run_mode, sn):
        super().__init__(task_info_file, run_mode, sn)
        self.batch_name = self.task_info.output_root.rstrip('/').split('/')[-1]
        logging.info(f"当前批次名为 {self.batch_name}")

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

    def ListInputPackages(self):
        def addPkg(package_info:PackageInfo, groups: list, file_size):
            upload_record = self.tracker.checkStatus(self.sn, package_info.key)
            if upload_record and upload_record.upload_mark:
                logging.info(f"数据包{package_info.key}已经上传过，跳过")
                package_info.desc = "success"
                package_info.input_bucket_path = upload_record.oss_root
                self.package_map[package_info.id] = package_info
            else:
                self.package_map[package_info.id] = package_info
                self.input_files_size += file_size
                groups.append([package_info.id])


        groups = []
        for root, dirs, files in os.walk(self.task_info.input_root):
            dirs[:] = [d for d in dirs if not d.startswith('.')]
            # list M18 folders
            for dir in dirs:
                if dir.startswith("clip_"):
                    package_info = PackageInfo()
                    package_info.data_type = "zgm"
                    package_info.id = len(self.package_map)
                    package_info.key = dir
                    package_info.local_root = self.task_info.input_root
                    package_path = os.path.join(root, dir)
                    file_info = FileInfo()
                    file_info.rel_path = os.path.relpath(package_path, package_info.local_root)
                    file_info.remove_after_upload = True
                    file_info.compress_before_upload = True
                    file_info.abs_path = package_path
                    file_info.size = self._GetFolderSize(package_path)
                    package_info.file_list.append(file_info)
                    # record batch name
                    batch_record_file = os.path.join(package_path, "batch.txt")
                    with open(batch_record_file, 'w') as fp:
                        fp.write(self.batch_name)
                    addPkg(package_info, groups, file_info.size)
            # list AIPC package
            for file in files:
                if file.endswith(".tar") or file.endswith(".tar.gz") or (
                        file.endswith(".json") and file.rsplit(".", 1)[0] == os.path.basename(root)):
                    package_info = PackageInfo()
                    package_info.data_type = "zgs"
                    package_info.id = len(self.package_map)
                    package_info.key = os.path.basename(file)
                    local_file_path = os.path.join(root, file)
                    package_info.local_root = self.task_info.input_root
                    relative_path = os.path.relpath(root, package_info.local_root)
                    file_info = FileInfo()
                    file_info.rel_path = relative_path
                    file_info.remove_after_upload = self.task_info.tags["remove_after_upload"] == "true"
                    file_info.compress_before_upload = False
                    file_info.abs_path = local_file_path
                    file_info.size = os.path.getsize(local_file_path)
                    package_info.file_list.append(file_info)

                    addPkg(package_info, groups, file_info.size)
        logging.info(f"当前待上传分组={len(groups)}")
        return groups

    def InitCallbackFunction(self, topic):
        self.callback_engine = ElasticUtil(host=self.task_info.tags["es_host"],
                                           usr_name=self.task_info.tags["es_usr_name"],
                                           pwd=self.task_info.tags["es_pwd"])
        self.callback_engine.CreateIndex(topic)

    def SendMessage(self, package_info: PackageInfo, topic):
        package_list = []
        for file_info in package_info.file_list:
            if not isinstance(file_info, FileInfo):
                continue
            package_list.append(file_info.rel_path)
        msg = {
            "source": package_info.local_root,
            "input_bucket_path": package_info.input_bucket_path,
            "upload_start_time": package_info.st,
            "uplaod_end_time": package_info.et,
            "file_size": package_info.file_size / pow(1024, 3),
            "package_name": package_list,
            "desc": package_info.desc,
            "taskId": package_info.task_id
        }
        logging.warning(msg)
        self.callback_engine.AddRecord(topic, msg)


# local test
if __name__ == "__main__":
    task_info_file = sys.argv[1]
    run_mode = sys.argv[2]
    sn = sys.argv[3]

    uploader = GAICUploader(task_info_file, run_mode, sn)
    uploader.Run()

    print("end")