import logging
import sys
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--task_info_file', type=str, help='task info json')
    parser.add_argument('-t', '--type', type=str, help='data source type')
    parser.add_argument('-m', '--mode', type=str, help='run mode', default="prod")
    parser.add_argument('-s', '--sn', type=str, help='sn num', default="fake_sn_num")
    args = parser.parse_args()

    # TODO : 自动重试
    rt = 0
    if args.type == "ab":
        from modules.apps.DJi.DJiUploader import *
        uploader = DJiUploader(args.task_info_file, args.mode, args.sn)
        rt = uploader.Run()
    elif args.type == "zg":
        from modules.apps.CAIC.GAICUploader import *
        uploader = GAICUploader(args.task_info_file, args.mode, args.sn)
        rt = uploader.Run()
    elif args.type == "ag":
        from modules.apps.gacrnd.gacrndUploader import garcndUploader
        runner = garcndUploader(args.task_info_file, args.mode, args.sn)
        rt = runner.Run()
    else:
        logging.fatal(f"不支持当前操作类型:{args.type}")
        rt = 255
    sys.exit(int(rt))