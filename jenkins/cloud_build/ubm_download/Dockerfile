FROM kuandeng-acr-registry-vpc.cn-wulanchabu.cr.aliyuncs.com/cs/process_ubm_download:baseline

USER root

RUN rm /home/hadoop/bin/*.py
RUN rm /home/hadoop/bin/gs_bag_decoder
RUN rm /home/hadoop/bin/gs_bag_check
RUN rm -r /home/hadoop/bin/modules

COPY oss_uploader/modules/ /home/hadoop/bin/modules/
COPY oss_uploader/util_modules/ /home/hadoop/bin/util_modules/
COPY oss_uploader/*.py /home/hadoop/bin/
COPY oss_uploader/gs_bag_decoder /home/hadoop/bin/
COPY oss_uploader/gs_bag_check /home/hadoop/bin/
# USER root
RUN chmod 777 /home/hadoop/bin/gs_bag_decoder
RUN chmod 777 /home/hadoop/bin/gs_bag_check
USER hadoop

WORKDIR /home/hadoop/bin/