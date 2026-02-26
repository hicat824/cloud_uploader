"""
用于M18数据包在上传之前做ins数据的转换
"""

import sys, os

NovAtelAPP = "/home/rd/Novtel/NovAtelApplicationSuite_64bit.AppImage" # TODO

def InsFileTranslater(package_path):
    rtk_file = None
    ins_file = None

    for root, dirs, files in os.walk(package_path):
        for file in files:
            if file.endswith("rtk.pcap"):
                rtk_file = os.path.join(root, file)
            elif file.endswith("INSPVAX.ASCII"):
                ins_file = os.path.join(root, file)

    if ins_file is None and rtk_file is not None:
        output_root = os.path.dirname(rtk_file)
        cmd = f"{NovAtelAPP} {rtk_file} -a -o={output_root} --split"
        if os.system(cmd) != 0:
            return False

    return True

if __name__ == '__main__':
    root_path = sys.argv[1]
    print(f"processing {root_path}......")
    for root, dirs, files in os.walk(root_path):
        dirs[:] = [d for d in dirs if not d.startswith('.')]
        # list M18 folders
        for dir in dirs:
            if dir.startswith("clip_"):
                package_root = os.path.join(root, dir)
                if not InsFileTranslater(package_root):
                    print(f"[ERROR] : failed to translate ins file of {package_root}")