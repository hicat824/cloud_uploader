import logging

from elasticsearch import Elasticsearch
import json
from datetime import datetime, timezone, timedelta

class ElasticUtil:
    def __init__(self, **config):
        self.client = Elasticsearch(
            config["host"],
            http_auth=(config["usr_name"], config["pwd"])
        )
        logging.info(self.client.info())

    """
    index_name: 索引名称,
    shards: 分片数量,
    replicas: 副本数量,
    mapping: 可选的自定义映射
    """
    def CreateIndex(self, index_name, shards=1, replicas=1, mapping=None):
        if self.client.indices.exists(index_name):
            return True
        body = {
            "settings": {
                "number_of_shards": shards,
                "number_of_replicas": replicas
            }
        }
        if mapping:
            body["mappings"] = mapping
        try:
            response = self.client.indices.create(index=index_name, body=body)
            logging.info(response)
            return True
        except Exception as e:
            logging.error(f"failed to create index {index_name}, error = {e}")
            return False


    """
    index_name: 索引名称
    record: json数据
    doc_id: 可选文档ID
    """
    def AddRecord(self, index_name, record, doc_id=None):
        if isinstance(record, str):
            try:
                record = json.loads(record)
            except json.JSONDecodeError:
                raise ValueError("Invalid JSON string")
        # add timestamp now (在表中的存储格式体现为 date)
        record["@timestamp"] = datetime.now().isoformat()

        self.CreateIndex(index_name)
        try:
            response = self.client.index(index=index_name, id=doc_id, body=record)
            logging.info(response)
            return True
        except Exception as e:
            logging.error(f"failed to add record to index {index_name}, error = {e}")
            return False


    """
    多字段组合查询
    index_name： 索引名称
    conditions: 查询条件列表 (operator: 查询类型 (match, term, wildcard, range))
    [{"field": "name", "value": "John", "operator": "match"},
     {"field": "age", "value": {"gte": 25}, "operator": "range"}]
    size: 返回的结果数量
    """
    def MultiFieldSearch(self, index_name, conditions, size=10):
        query = {
            "query": {
                "bool": {
                    "must": []
                }
            },
            "size": size
        }

        for cond in conditions:
            operator = cond.get("operator", "match")
            field = cond["field"]
            value = cond["value"]

            if operator == "match":
                query["query"]["bool"]["must"].append({"match": {field: value}})
            elif operator == "term":
                query["query"]["bool"]["must"].append({"term": {field: value}})
            elif operator == "wildcard":
                query["query"]["bool"]["must"].append({"wildcard": {field: value}})
            elif operator == "range":
                query["query"]["bool"]["must"].append({"range": {field: value}})
            else:
                raise ValueError(f"Unsupported operator: {operator}. Use 'match', 'term', 'wildcard' or 'range'")

        try:
            result = self.client.search(index=index_name, body=query)
            return self._format_search_results(result)
        except Exception as e:
            return {"status": "error", "message": e}

    def _format_search_results(self, es_response):
        """格式化Elasticsearch响应结果"""
        hits = es_response.get('hits', {}).get('hits', [])
        total = es_response.get('hits', {}).get('total', {}).get('value', 0)

        results = []
        for hit in hits:
            source = hit.get('_source', {})
            source['_id'] = hit.get('_id')
            results.append(source)

        return {
            "total": total,
            "results": results
        }

if __name__ == '__main__':
    es_hosts = "http://192.168.6.105:9200"
    es_usr_name = "elastic"
    es_pwd = "elastic"
    index_name = "kdlog"

    es_engine = ElasticUtil(host=es_hosts, usr_name=es_usr_name, pwd=es_pwd)
    # 查询指定topic所有日志
    conditions = [{"field": "stage", "value": "upload", "operator": "match"}]
    result = es_engine.MultiFieldSearch(index_name, conditions)
    print(result)

    # es_engine.CreateIndex(index_name=index_name)
    # test_record_1 = {
    #     "id": "1",
    #     "desc": "test record 1",
    #     "score": 78
    # }
    # es_engine.AddRecord(index_name, test_record_1)
    # test_record_2 = {
    #     "id": "2",
    #     "desc": "test record 2",
    #     "score": 96
    # }
    # es_engine.AddRecord(index_name, test_record_2)
    # test_record_3 = {
    #     "id": "3",
    #     "desc": "test record 3",
    #     "score": 63,
    #     "extra": "*****"
    # }
    # es_engine.AddRecord(index_name, test_record_3)
    #
    # conditions = [{"field": "id", "value": "3", "operator": "match"}]
    # result = es_engine.MultiFieldSearch(index_name, conditions)
    # print(f"query result 1 = {result}")
    #
    # conditions = [{"field": "score", "value": {"gte": 50, "lte": 80}, "operator": "range"}]
    # result = es_engine.MultiFieldSearch(index_name, conditions)
    # print(f"query result 2 = {result}")
    # print("end")