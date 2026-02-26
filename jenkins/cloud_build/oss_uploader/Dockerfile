FROM kuandeng-acr-registry-vpc.cn-wulanchabu.cr.aliyuncs.com/kd-ad/oss_uploader:baseline

COPY OssUploader/modules/ /home/hadoop/bin/modules/
COPY OssUploader/util_modules/ /home/hadoop/bin/util_modules/
COPY OssUploader/*.py /home/hadoop/bin/

WORKDIR /home/hadoop/bin/