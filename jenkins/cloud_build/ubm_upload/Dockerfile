FROM kuandeng-acr-registry-vpc.cn-wulanchabu.cr.aliyuncs.com/cs/process_ubm_upload:baseline

USER root

RUN rm /home/hadoop/bin/*.py
RUN rm /home/hadoop/bin/gs_bag_encoder
RUN rm -r /home/hadoop/bin/modules

COPY oss_uploader/modules/ /home/hadoop/bin/modules/
COPY oss_uploader/util_modules/ /home/hadoop/bin/util_modules/
COPY oss_uploader/*.py /home/hadoop/bin/
COPY oss_uploader/gs_bag_encoder /home/hadoop/bin/
COPY oss_uploader/libkcoords_plugin.so /home/hadoop/bin/
# USER root
RUN chmod 777 /home/hadoop/bin/gs_bag_encoder
USER hadoop

WORKDIR /home/hadoop/bin/