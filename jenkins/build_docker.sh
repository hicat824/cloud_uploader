#!/bin/bash -e

if [ "$#" -ne 2 ]
  then
  echo "Wrong number of arguments given, exact 1 arguments are required: "
  echo "1. give a new docker name to tag this docker eg. oss_uploader"
  echo "2. give a new version name to tag this docker eg. v1.0.0"
  exit 1
fi

current_project_folder_name=$(basename $(cd .. && pwd))
echo current_project_folder_name=$current_project_folder_name
jenkins_root_project_dir=$(cd ../.. && pwd)
echo jenkins_root_project_dir=$jenkins_root_project_dir

server="kd-bd02.kuandeng.com"
repository="kd-ad"
base_image="oss_uploader"
base_image_version="base"
target_docker_name=$1

base_docker=$server/$repository/$base_image:$base_image_version

echo "****************************************"
echo "   Compiling $target_docker_name:$2"
echo "****************************************"

# STEP 1: pull base docker
echo "docker pull $base_docker"
docker pull $base_docker || exit 5


echo "****************************************"
echo "   Building docker $server/$repository/$target_docker_name:$2"
echo "****************************************"
# STEP 2: build docker
# copy the dockerignore to build root
cp .dockerignore ${jenkins_root_project_dir}

echo " ---------- listing build folder ----------"
ls ${jenkins_root_project_dir}

echo "docker build -f ./Dockerfile ${jenkins_root_project_dir} \
                   -t $server/$repository/$target_docker_name:$2 \
                   --build-arg BASE_IMAGE=$base_image \
                   --build-arg BASE_IMAGE_VERSION=$base_image_version \
                   --build-arg JENKINS_PROJECT_FOLDER_NAME=$current_project_folder_name \
                   || exit 5"

docker build --rm -f ./Dockerfile ${jenkins_root_project_dir} \
             -t $server/$repository/$target_docker_name:$2 \
             --build-arg BASE_IMAGE=$base_image \
             --build-arg BASE_IMAGE_VERSION=$base_image_version \
             --build-arg JENKINS_PROJECT_FOLDER_NAME=$current_project_folder_name \
             || exit 5


echo "****************************************"
echo "   Pushing docker $server/$repository/$target_docker_name:$2   "
echo "****************************************"        
# STEP 3: push docker
echo "docker push $server/$repository/$target_docker_name:$2"
docker push $server/$repository/$target_docker_name:$2 || exit 5

echo "****************************************"
echo "********        Finished        ********"
echo "****************************************"