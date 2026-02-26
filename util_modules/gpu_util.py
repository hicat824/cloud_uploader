

"""
获取当前环境显卡类型
"""
def detect_gpu_model() -> str:
    import GPUtil

    gpu_model = ""
    gpu_infos = GPUtil.getGPUs()
    if len(gpu_infos) == 0:  # 如果没有检测到GPU，则退出程序
        return gpu_model

    # 提取显卡名称
    gpu_name_strs = gpu_infos[0].name.split()
    a10_strs = ["A10", "A", "10"]
    ti2080_strs = ["2080Ti", "2080", "Ti", "T4"]
    orin_strs = ["Orin"]
    l4_strs = ["L4", "4090", "L20"]

    if "A100" in gpu_infos[0].name: # TODO : 创智环境显卡为NVIDIA A100-PCIE-40GB
        gpu_model = "A100"
    elif any(word.lower() == a10_strs[0].lower() for word in gpu_name_strs) or \
            (any(word.lower() == a10_strs[1].lower() for word in gpu_name_strs) and any(
                word.lower() == a10_strs[2].lower() for word in gpu_name_strs)):
        gpu_model = "A10"
    elif any(word.lower() == ti2080_strs[0].lower() for word in gpu_name_strs) \
            or (any(word.lower() == ti2080_strs[1].lower() for word in gpu_name_strs) and any(
        word.lower() == ti2080_strs[2].lower() for word in gpu_name_strs)) \
            or (any(word.lower() == ti2080_strs[3].lower() for word in gpu_name_strs)):
        gpu_model = "2080Ti"
    elif any(word.lower() in orin_strs[0].lower() for word in gpu_name_strs):
        gpu_model = "orin"
    elif any(word.lower() in l4_strs[0].lower() for word in gpu_name_strs) \
            or any(word.lower() in l4_strs[1].lower() for word in gpu_name_strs) \
            or any(word.lower() in l4_strs[2].lower() for word in gpu_name_strs):
        gpu_model = "L4"
    else:
        print(f"不支持的GPU类型{gpu_infos[0].name}")
    return gpu_model