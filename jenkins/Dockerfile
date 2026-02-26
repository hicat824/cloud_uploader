ARG BASE_IMAGE
ARG BASE_IMAGE_VERSION

FROM kd-bd02.kuandeng.com/kd-ad/$BASE_IMAGE:$BASE_IMAGE_VERSION

ARG JENKINS_PROJECT_FOLDER_NAME
LABEL MAINTAINER='xumaozhou@kuandeng.com'

USER hadoop
RUN pip install \
    utm \
    tos \
    tqdm \
    psutil \
    minio \
    aliyun-python-sdk-core \
    aliyun-python-sdk-sts \
    confluent-kafka==1.9.2 \
    elasticsearch==7.13.4

# add execute file into docker
RUN mkdir -p /home/hadoop/bin/
COPY --chown=hadoop:hadoop $JENKINS_PROJECT_FOLDER_NAME/ /home/hadoop/bin/
# install requirements
#RUN pip3 install -r /home/hadoop/bin/$JENKINS_PROJECT_FOLDER_NAME/requirements.txt

WORKDIR /home/hadoop/bin/