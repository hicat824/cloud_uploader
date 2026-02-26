import logging
import os
import json
import time
from kafka import KafkaProducer, KafkaConsumer
from util_modules.log_util import *

logging.getLogger("kafka").setLevel(logging.ERROR)

class KafkaUtil:
    def __init__(self):
        self.bootstrap_servers = None
        self.username = None
        self.password = None
        self.mechanism = None
        self.protocol = None
        self.base_msg = None
        self.topic = None
        self.max_retry_times = 3
        self.taskinfo_location = None

    def InitFromValues(self, bootstrap_servers, username, password, mechanism, protocol):
        self.bootstrap_servers = bootstrap_servers
        self.username = username
        self.password = password
        self.mechanism = mechanism
        self.protocol = protocol
        logging.info(f"----- > bootstrap_servers = {self.bootstrap_servers}")
        logging.info(f"----- > username = {self.username}")
        logging.info(f"----- > password = {self.password}")
        logging.info(f"----- > mechanism = {self.mechanism}")
        logging.info(f"----- > protocol = {self.protocol}")

    def InitFromFiles(self):
        kafka_username_file = "/etc/kafka-secret-volume/username"
        kafka_password_file = "/etc/kafka-secret-volume/password"
        kafka_mechanism_file = "/etc/kafka-secret-volume/mechanism"
        kafka_protocol_file = "/etc/kafka-secret-volume/protocol"
        platform_config_file = "/etc/platform-config-volume/platform_config.json"
        with open(kafka_username_file, 'r') as file:
            self.username = file.read()
            self.username = self.username.strip()
        with open(kafka_password_file, 'r') as file:
            self.password = file.read()
            self.password = self.password.strip()
        with open(kafka_mechanism_file, 'r') as file:
            self.mechanism = file.read()
            self.mechanism = self.mechanism.strip()
        with open(kafka_protocol_file, 'r') as file:
            self.protocol = file.read()
            self.protocol = self.protocol.strip()
        with open(platform_config_file, 'r') as file:
            content = json.load(file)
            self.bootstrap_servers = content["kafka_server"]
        logging.info(f"----- > bootstrap_servers = {self.bootstrap_servers}")
        logging.info(f"----- > username = {self.username}")
        logging.info(f"----- > password = {self.password}")
        logging.info(f"----- > mechanism = {self.mechanism}")
        logging.info(f"----- > protocol = {self.protocol}")

    def GetProInfoFromBase64String(self, base64String):
        import base64
        try:
            base64_bytes = base64String.encode('utf-8')
            proj_bytes = base64.b64decode(base64_bytes)
            prof_info_str = proj_bytes.decode('utf-8')
            prof_info = json.loads(prof_info_str)

            self.base_msg = {
                "msgType": "",
                "id": prof_info["id"],
                "taskId": prof_info["taskId"],
                "subTaskId": prof_info["subTaskId"],
                "dataId": prof_info["dataId"],
                "bizCode": prof_info["bizCode"],
                "data": ""
            }
            self.topic = prof_info["csTopic"]
            self.taskinfo_location = prof_info["taskInfoLocation"]
            logging.info(f"kafka base msg = {self.base_msg}")
        except json.JSONDecodeError as e:
            raise e

    def SendKafkaMsg(self, topic, message):
        logging.info(f"sending kafka msg... : topic = {topic}, message = {message}")
        for _ in range(self.max_retry_times):
            try:
                producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    security_protocol=self.protocol,
                    sasl_mechanism=self.mechanism,
                    sasl_plain_username=self.username,
                    sasl_plain_password=self.password,
                    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
                    api_version=(2, 8, 2),
                )
                # 发送消息
                producer.send(topic, message)
                # 等待消息发送完成
                producer.flush()
                # 关闭生产者
                producer.close()
                return True
            except Exception as e:
                logging.error(f"{e}")
                time.sleep(3 * (_ + 1))
        return False

    def SendPodMessage(self, msg_type, data):
        message = self.base_msg
        message["msgType"] = msg_type
        message["data"] = data
        self.SendKafkaMsg(self.topic, message)

if __name__ == "__main__":
    kafka_util = KafkaUtil()
    kafka_util.InitFromValues(["192.168.8.116:9092"],
                              "admin",
                              "admin123",
                              "SCRAM-SHA-256",
                              "SASL_PLAINTEXT")
    kafka_util.SendPodMessage("updatePodName", "processing_test")