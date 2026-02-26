from .BaseService import BaseService
class CSFactory:
    @staticmethod
    def CreateConnector(cloud_type, **config) -> BaseService:
        if cloud_type == "minio":
            from .Minio import MinioServer
            secure = config["secure"] == "true"
            return MinioServer(config["endpoint"], config["ak"], config["sk"], config["bucket_name"], secure)
        elif cloud_type == "volcano": # 火山云
            from .Volcano import VolcanoServer
            return VolcanoServer(config["endpoint"], config["ak"], config["sk"], config["bucket_name"], config["region"])
        elif cloud_type == "obs": # 华为云
            from .obs import ObsServer
            secure = config["secure"] == "true"
            return ObsServer(config["ak"], config["sk"], config["endpoint"], config["bucket_name"], secure)
        elif cloud_type == "oss": # 阿里云
            from .oss import OSSServer
            return OSSServer(config["ak"], config["sk"], config["bucket_name"], config["endpoint"], config["output_root"])
        elif cloud_type == "s3": # aws s3 亚马逊云服务
            from .aws import AWSService
            return AWSService(bucket_name=config["bucket_name"], aws_access_key_id=config["ak"],
                              aws_secret_access_key=config["sk"], endpoint_url=config["endpoint"])
        else:
            raise TypeError(f"unsupported cloud type {cloud_type}")