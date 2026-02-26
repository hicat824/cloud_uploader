docker build --file /home/hadoop/upload_test_ws/jenkins/Dockerfile -t kuandeng-acr-registry-vpc.cn-wulanchabu.cr.aliyuncs.com/kd-ad/oss_uploader:$1 /home/hadoop/upload_test_ws/
docker tag kuandeng-acr-registry-vpc.cn-wulanchabu.cr.aliyuncs.com/kd-ad/oss_uploader:$1 kuandeng-acr-registry-vpc.cn-wulanchabu.cr.aliyuncs.com/kd-ad/oss_uploader:latest.test
docker push kuandeng-acr-registry-vpc.cn-wulanchabu.cr.aliyuncs.com/kd-ad/oss_uploader:latest.test
docker push kuandeng-acr-registry-vpc.cn-wulanchabu.cr.aliyuncs.com/kd-ad/oss_uploader:$1