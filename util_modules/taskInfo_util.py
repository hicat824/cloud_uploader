import json
import logging
import os, sys

class taskInfoUtil:
    def __init__(self, task_info_file): # FIXME
        self.input_root = "/workdir"
        self.output_root = "/workdir"
        self.cpu_nums = 1
        self.taskId = None
        self.tags = {}
        self.data_infos = None

        if os.path.exists(task_info_file):
            with open(task_info_file, 'r') as fp:
                content = json.load(fp)
                logging.info(f"taskinfo content = {content}")
                self.data_infos = content["input"]["dataInfos"]
                for data in content["input"]["params"]:
                    if data["k"] == "cpu_nums":
                        self.cpu_nums = int(data["v"])
                    elif data["k"] == "taskId":
                        self.taskId = int(data["v"])
                    else:
                        self.tags[data["k"]] = data["v"]
        else:
            raise FileNotFoundError(f"missing task info file {task_info_file}")


class SimpleTaskInfoUtil:
    def __init__(self, task_info_file):
        self.input_root = None
        self.output_root = None
        self.cpu_nums = None
        self.taskId = None
        self.tags = {}

        if os.path.exists(task_info_file):
            with open(task_info_file, 'r') as fp:
                content = json.load(fp)
                for key, val in content.items():
                    if key == "cpu_nums":
                        self.cpu_nums = int(val)
                    elif key == "taskId":
                        self.taskId = int(val)
                    elif key == "input_root":
                        self.input_root = val
                    elif key == "output_root":
                        self.output_root = val
                    else:
                        self.tags[key] = val
        else:
            raise FileNotFoundError(f"missing task info file {task_info_file}")