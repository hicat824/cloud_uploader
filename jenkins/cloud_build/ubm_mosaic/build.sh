docker build --file /home/kddev/lyh/oss_uploader/jenkins/cloud_build/ubm_mosaic/Dockerfile -t kuandeng-acr-registry-vpc.cn-wulanchabu.cr.aliyuncs.com/cs/process_ubm_mosaic:$1 /home/kddev/lyh/
docker tag kuandeng-acr-registry-vpc.cn-wulanchabu.cr.aliyuncs.com/cs/process_ubm_mosaic:$1 kuandeng-acr-registry-vpc.cn-wulanchabu.cr.aliyuncs.com/cs/process_ubm_mosaic:latest.test
docker push kuandeng-acr-registry-vpc.cn-wulanchabu.cr.aliyuncs.com/cs/process_ubm_mosaic:latest.test
docker push kuandeng-acr-registry-vpc.cn-wulanchabu.cr.aliyuncs.com/cs/process_ubm_mosaic:$1