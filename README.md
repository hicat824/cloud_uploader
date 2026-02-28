# Cloud Uploader

一个用于将本地采集数据上传至云存储的通用工具，支持多种数据源类型与多种云服务提供商。

## 项目简介

Cloud Uploader 是一个模块化的数据上传脚本框架，主要用于将自动驾驶采集的原始数据从本地硬盘批量上传至对象存储（OSS/MinIO/OBS/S3等）。系统以 `main.py` 为入口，通过任务配置文件驱动，支持多线程并发上传、断点续传（本地 SQLite 记录上传状态）、上传后回调通知等功能。

## 架构概览

```
main.py                          # 程序入口，根据 --type 参数路由到对应的上传器
│
├── modules/
│   ├── CloudUploader/
│   │   └── BaseUploader.py      # 上传器基类，封装通用上传流程
│   │
│   ├── apps/                    # 各业务方的具体上传实现
│   │   ├── DJi/DJiUploader.py  # 卓驭数据上传（--type ab）
│   │   ├── CAIC/GAICUploader.py # 中国汽车智能化研究院数据上传（--type zg）
│   │   └── gacrnd/gacrndUploader.py  # 广汽研发数据上传（--type ag）
│   │
│   └── CloudServices/           # 云存储服务适配层
│       ├── BaseService.py       # 云服务接口定义
│       ├── CSFactory.py         # 云服务工厂类
│       ├── Minio.py             # MinIO 实现
│       ├── Volcano.py           # 火山云实现
│       ├── obs.py               # 华为云 OBS 实现
│       ├── oss.py               # 阿里云 OSS 实现
│       └── aws.py               # AWS S3 实现
│
└── util_modules/                # 工具模块
    ├── taskInfo_util.py         # 任务配置文件解析
    ├── UploadTracker.py         # 本地上传状态追踪（SQLite）
    ├── platform_util.py         # HTTP 请求、文件压缩等平台工具
    ├── log_util.py              # 日志配置
    ├── loctime_util.py          # 时间格式化工具
    ├── elastic_util.py          # Elasticsearch 工具
    └── kafka_util.py            # Kafka 消息队列工具
```

## 支持的数据源类型

| `--type` 参数 | 上传器类 | 适用场景 |
|---|---|---|
| `ab` | `DJiUploader` | 卓驭（DJi）采集数据，支持 `raw_data` 和 `TTE` 两种数据规格 |
| `zg` | `GAICUploader` | 中汽研（CAIC）采集数据，支持 clip 文件夹和 AIPC 打包数据 |
| `ag` | `garcndUploader` | 广汽研发（GAC RnD）采集数据，支持基于 CSV 清单的批次化上传 |

## 支持的云存储服务

| `cloud_type` 配置值 | 云服务商 |
|---|---|
| `minio` | MinIO（私有部署） |
| `volcano` | 火山引擎对象存储 |
| `obs` | 华为云 OBS |
| `oss` | 阿里云 OSS |
| `s3` | AWS S3 |

## 安装依赖

```bash
pip install -r requirements.txt
```

或参照 `jenkins/Dockerfile` 安装完整依赖：

```bash
pip install utm tos tqdm psutil minio \
    aliyun-python-sdk-core aliyun-python-sdk-sts \
    confluent-kafka==1.9.2 elasticsearch==7.13.4
```

## 使用方法

### 命令行参数

```bash
python main.py -i <task_info_file> -t <type> [-m <mode>] [-s <sn>]
```

| 参数 | 简写 | 说明 | 默认值 |
|---|---|---|---|
| `--task_info_file` | `-i` | 任务配置 JSON 文件路径（必填） | — |
| `--type` | `-t` | 数据源类型：`ab` / `zg` / `ag`（必填） | — |
| `--mode` | `-m` | 运行模式：`prod` / `test` | `prod` |
| `--sn` | `-s` | 硬盘序列号 | `fake_sn_num` |

### 示例

```bash
# 上传卓驭数据（生产模式）
python main.py -i /path/to/task_info.json -t ab -m prod -s DISK_SN_001

# 上传中汽研数据（测试模式）
python main.py -i /path/to/task_info.json -t zg -m test -s DISK_SN_002

# 上传广汽研发数据
python main.py -i /path/to/task_info.json -t ag -m prod -s DISK_SN_003
```

### Web UI（Flask）

`modules/apps/gacrnd/gacrnd_web.py` 提供基于 Flask + Bootstrap 的 Web 启动器，支持动态添加任务：

```bash
python modules/apps/gacrnd/gacrnd_web.py
```

浏览器访问 `http://localhost:5000`，任务必填字段为 `input_root`、`output_root`、`source_type`、`data_type`、`cloud_type`、`mode`。
下拉选项通过 `modules/apps/gacrnd/task_options.json` 配置。

### 任务配置文件（task_info.json）

任务配置文件为 JSON 格式，定义数据输入/输出路径及上传相关参数，示例结构如下：

```json
{
    "input_root": "/data/input",
    "output_root": "/data/output",
    "cpu_nums": 4,
    "cloud_type": "minio",
    "data_type": "your_data_type",
    "source_type": "your_source_type",
    "red_bucket_name": "red-bucket",
    "yellow_bucket_name": "yellow-bucket",
    "endpoint": "your-endpoint",
    "ak": "your-access-key",
    "sk": "your-secret-key",
    "region": "your-region",
    "secure": "true",
    "tenant_id": "your-tenant-id",
    "upload_log_topic": "http://your-log-endpoint",
    "cs_create_package_url": "http://your-api/createUploadPackage",
    "cs_uplaod_callback_url": "http://your-api/uploadCallback",
    "notice_the_platform": "true"
}
```

### 平台配置文件（conf/）

在 `modules/conf/` 目录下存放各运行环境的平台配置文件，文件名格式为 `platform_config_{mode}.json`（如 `platform_config_prod.json`、`platform_config_test.json`）。配置内容按 `data_type` 字段区分，包含云存储连接参数、API 地址等。

## 核心模块说明

### BaseUploader（上传器基类）

位于 `modules/CloudUploader/BaseUploader.py`，是所有具体上传器的父类，提供以下功能：

- **`Run()`**：上传主流程，依次执行：扫描硬盘 → 列举待上传数据包 → 并发上传 → 写入上传记录
- **`_UploadProcess(groups)`**：基于 `ThreadPoolExecutor` 的多线程并发上传
- **`_UploadSinglePackage(package_info, conn)`**：单个数据包上传，支持上传前压缩（tar）、上传后删除本地文件
- **`_WriteUploadRecords(disk_file_size)`**：将上传结果写入 CSV 记录文件

子类需要实现以下抽象方法：

| 方法 | 说明 |
|---|---|
| `ListInputPackages()` | 扫描本地输入目录，返回待上传的数据包分组列表 |
| `InitCallbackFunction(topic)` | 初始化上传结果回调/通知组件 |
| `SendMessage(package_info, topic)` | 发送单个数据包上传结果通知 |

### UploadTracker（上传状态追踪）

位于 `util_modules/UploadTracker.py`，使用本地 SQLite 数据库（默认路径 `/tmp/cloud_upload_records/{source_type}.db`）记录每个数据包的上传状态，避免重复上传。支持强制重传（通过任务配置中的 `force_upload: "true"` 开关）。

### CSFactory（云服务工厂）

位于 `modules/CloudServices/CSFactory.py`，根据 `cloud_type` 配置动态创建对应的云存储连接实例，所有连接实例均实现 `BaseService` 接口（`UploadFile`、`UploadFolder`、`DownloadFile`、`DownloadFolder`、`IsFileExists`、`ListFiles`）。

## 上传流程

```
程序启动（main.py）
    │
    ▼
初始化上传器（_Init）
    ├── 解析任务配置文件
    ├── 加载平台配置（conf/platform_config_{mode}.json）
    ├── 初始化日志（output_root/logs/oss_uploader.log）
    └── 初始化本地上传状态数据库（SQLite）
    │
    ▼
扫描待上传数据（ListInputPackages）
    ├── 遍历 input_root 目录，识别有效数据包
    ├── 查询本地数据库，过滤已上传数据包
    └── 返回待上传分组列表
    │
    ▼
多线程并发上传（_UploadProcess）
    ├── 调用平台 API 创建上传任务（createUploadPackage）
    ├── 按需压缩数据包（tar）
    ├── 上传至对象存储（红区 Bucket）
    ├── 更新本地上传状态数据库
    └── 发送上传结果通知（Kafka / Elasticsearch / HTTP 回调）
    │
    ▼
写入上传记录 CSV（upload_record_{timestamp}.csv）
```

## Docker 部署

项目提供 Dockerfile，可通过 Jenkins 进行容器化构建与部署，参见 `jenkins/` 目录。

```bash
cd jenkins
bash build_docker.sh
```

## 返回码

| 返回码 | 含义 |
|---|---|
| `0` | 上传全部成功 |
| `1` | 找不到待上传文件 |
| `2` | 云存储连接错误 |
| `99` | 未知错误 |
| `255` | 不支持的数据源类型 |
