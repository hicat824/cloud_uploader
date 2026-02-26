from datetime import datetime
import time

def GetFormattedTime():
    # 获取当前时间
    current_time = datetime.now()
    # 格式化为 "2024-12-10_10:20:33" 格式
    formatted_time = current_time.strftime("%Y-%m-%d %H:%M:%S")
    return formatted_time

def FormatTimeToTimestamp(time_str):
    # 将字符串转换为 datetime 对象
    dt = datetime.strptime(time_str, "%Y-%m-%d_%H:%M:%S.%f")
    # 转换为时间戳（秒）
    timestamp = dt.timestamp()
    # 转换为毫秒
    milliseconds = int(timestamp * 1000)
    return milliseconds

def TimestampToFormattedTime(timestamp):
    # 转换为本地时间struct_time
    local_time = time.localtime(timestamp)
    # 格式化为字符串
    formatted_time = time.strftime("%Y-%m-%d %H:%M:%S", local_time)
    return formatted_time