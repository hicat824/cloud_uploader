FROM kuandeng-acr-registry-vpc.cn-wulanchabu.cr.aliyuncs.com/cs/process_ubm_mosaic:baseline

USER root

RUN rm /home/hadoop/bin/*.py
RUN rm /home/hadoop/bin/libs/simplify_camera_compliance_for_video
RUN rm -r /home/hadoop/bin/modules

COPY oss_uploader/modules/ /home/hadoop/bin/modules/
COPY oss_uploader/util_modules/ /home/hadoop/bin/util_modules/
COPY oss_uploader/*.py /home/hadoop/bin/
COPY oss_uploader/gs_libs/simplify_camera_compliance_for_video /home/hadoop/bin/gs_libs/
RUN chmod 777 /home/hadoop/bin/libs/simplify_camera_compliance_for_video

WORKDIR /home/hadoop/bin/