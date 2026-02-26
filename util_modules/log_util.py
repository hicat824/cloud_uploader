""""
create by longyunhao 2023-04-25
全局logging配置
"""
import logging
import logging.handlers

"""
usage:
 1.from autohdmap_micro_services.util.logging_setup import *
 2.LoggingAddTimedRotatingFileHandler(log_file) OR LoggingAddFileHandler(log_file)

"""
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s.%(msecs)03d] %(levelname)s [%(thread)d] - %(message)s",
    # datefmt='[%a-%d-%b-%Y : %H:%M:%S]',
)

""" 初始化一个全局log文件 """
def LoggingAddFileHandler(file_path):
    handler = logging.FileHandler(file_path, "a", encoding='utf-8')
    formatter = logging.Formatter('%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)


""" 初始化全局日志，按照固定周期创建新的日志文件 """
def LoggingAddTimedRotatingFileHandler(file_path, when, backup_count, interval):
    handler = logging.handlers.TimedRotatingFileHandler(filename=file_path,
                                                        when=when,
                                                        interval=interval,
                                                        backupCount=backup_count,
                                                        encoding='utf-8')
    formatter = logging.Formatter('%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)