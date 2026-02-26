import logging

from .BaseService import BaseService

import boto3
import os
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading


class AWSService(BaseService):
    def __init__(self, bucket_name, aws_access_key_id=None, aws_secret_access_key=None, endpoint_url=None):
        """
        初始化AWS S3服务

        Args:
            bucket_name (str): S3存储桶名称
            aws_access_key_id (str, optional): AWS访问密钥ID
            aws_secret_access_key (str, optional): AWS秘密访问密钥
            end
        """
        self.bucket_name = bucket_name

        # 初始化S3客户端
        if aws_access_key_id and aws_secret_access_key:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                endpoint_url=endpoint_url
            )
        else:
            # 使用默认凭证（如环境变量、IAM角色等）
            self.s3_client = boto3.client('s3', endpoint_url=endpoint_url)

        self.max_workers = 4
        self.multipart_chunksize = 100 * 1024 * 1024

    def _get_incomplete_upload(self, prefix):
        """
        获取未完成的分片上传信息

        Args:
            prefix (str): S3中的目标路径（键）

        Returns:
            tuple: (upload_id, parts_list) 如果找到未完成的上传，否则 (None, [])
        """
        try:
            # 列出未完成的分片上传
            response = self.s3_client.list_multipart_uploads(
                Bucket=self.bucket_name,
                Prefix=prefix
            )

            if 'Uploads' in response and response['Uploads']:
                # 找到匹配的上传
                for upload in response['Uploads']:
                    if upload['Key'] == prefix:
                        upload_id = upload['UploadId']

                        # 获取已上传的分片
                        parts = []
                        part_response = self.s3_client.list_parts(
                            Bucket=self.bucket_name,
                            Key=prefix,
                            UploadId=upload_id
                        )

                        if 'Parts' in part_response:
                            for part in part_response['Parts']:
                                parts.append({
                                    'PartNumber': part['PartNumber'],
                                    'ETag': part['ETag']
                                })

                        return upload_id, parts

            return None, []

        except Exception as e:
            print(f"获取未完成上传信息失败: {e}")
            return None, []

    def _upload_file_multipart(self, prefix, local_path, file_size, chunksize, resume_upload=False):
        """
        使用分片上传文件到S3，支持断点续传

        Args:
            prefix (str): S3中的目标路径（键）
            local_path (str): 本地文件路径
            file_size (int): 文件大小（字节）
            chunksize (int): 分片大小（字节）
            resume_upload (bool): 是否尝试恢复未完成的上传

        Returns:
            bool: 上传成功返回True，失败返回False
        """
        upload_id = None
        existing_parts = []

        try:
            # 计算分片数量
            num_parts = (file_size + chunksize - 1) // chunksize
            print(f"文件将被分成 {num_parts} 个分片上传")

            # 检查是否有未完成的上传
            if resume_upload:
                upload_id, existing_parts = self._get_incomplete_upload(prefix)
                if upload_id:
                    print(f"找到未完成的上传，UploadId: {upload_id}")
                    print(f"已上传的分片: {[p['PartNumber'] for p in existing_parts]}")
                else:
                    print("未找到未完成的上传，开始新的上传")

            # 如果没有找到未完成的上传，初始化新的分片上传
            if not upload_id:
                response = self.s3_client.create_multipart_upload(
                    Bucket=self.bucket_name,
                    Key=prefix
                )
                upload_id = response['UploadId']
                print(f"创建新的分片上传，UploadId: {upload_id}")

            # 上传分片
            parts = existing_parts.copy()  # 复制已上传的分片信息
            with open(local_path, 'rb') as f:
                for part_number in range(1, num_parts + 1):
                    # 检查这个分片是否已经上传
                    already_uploaded = any(p['PartNumber'] == part_number for p in existing_parts)
                    if already_uploaded:
                        print(f"分片 {part_number}/{num_parts} 已上传，跳过")
                        continue

                    # 定位到分片开始位置
                    f.seek((part_number - 1) * chunksize)

                    # 读取分片数据
                    chunk = f.read(chunksize)
                    if not chunk:
                        break

                    print(f"上传分片 {part_number}/{num_parts} (大小: {len(chunk)} bytes)")

                    # 上传单个分片
                    part_response = self.s3_client.upload_part(
                        Bucket=self.bucket_name,
                        Key=prefix,
                        PartNumber=part_number,
                        UploadId=upload_id,
                        Body=chunk
                    )

                    # 保存分片信息
                    parts.append({
                        'PartNumber': part_number,
                        'ETag': part_response['ETag']
                    })

            # 按PartNumber排序parts
            parts.sort(key=lambda x: x['PartNumber'])

            # 完成分片上传
            print("正在完成分片上传...")
            self.s3_client.complete_multipart_upload(
                Bucket=self.bucket_name,
                Key=prefix,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts}
            )

            print("分片上传完成")
            return True

        except Exception as e:
            print(f"分片上传失败: {e}")
            # 不要自动中止上传，以便后续恢复
            print(f"上传已中断，UploadId: {upload_id}")
            print("可以使用 resume_upload=True 参数恢复上传")
            return False

    def _upload_file_multipart_parallel(self, prefix, local_path, file_size, chunksize, resume_upload=False, max_workers=5):
        """
        使用并行分片上传文件到S3，支持断点续传

        Args:
            prefix (str): S3中的目标路径（键）
            local_path (str): 本地文件路径
            file_size (int): 文件大小（字节）
            chunksize (int): 分片大小（字节）
            resume_upload (bool): 是否尝试恢复未完成的上传
            max_workers (int): 最大线程数

        Returns:
            bool: 上传成功返回True，失败返回False
        """
        upload_id = None
        existing_parts = []

        try:
            # 计算分片数量
            num_parts = (file_size + chunksize - 1) // chunksize
            print(f"文件将被分成 {num_parts} 个分片上传，使用 {max_workers} 个线程并行上传")

            # 检查是否有未完成的上传
            if resume_upload:
                upload_id, existing_parts = self._get_incomplete_upload(prefix)
                if upload_id:
                    print(f"找到未完成的上传，UploadId: {upload_id}")
                    print(f"已上传的分片: {[p['PartNumber'] for p in existing_parts]}")
                else:
                    print("未找到未完成的上传，开始新的上传")

            # 如果没有找到未完成的上传，初始化新的分片上传
            if not upload_id:
                response = self.s3_client.create_multipart_upload(
                    Bucket=self.bucket_name,
                    Key=prefix
                )
                upload_id = response['UploadId']
                print(f"创建新的分片上传，UploadId: {upload_id}")

            # 准备需要上传的分片列表
            parts_to_upload = []
            for part_number in range(1, num_parts + 1):
                # 检查这个分片是否已经上传
                already_uploaded = any(p['PartNumber'] == part_number for p in existing_parts)
                if not already_uploaded:
                    parts_to_upload.append(part_number)

            if not parts_to_upload:
                print("所有分片都已上传，直接完成上传")
                # 所有分片都已上传，直接完成上传
                parts = existing_parts.copy()
                parts.sort(key=lambda x: x['PartNumber'])
                self.s3_client.complete_multipart_upload(
                    Bucket=self.bucket_name,
                    Key=prefix,
                    UploadId=upload_id,
                    MultipartUpload={'Parts': parts}
                )
                print("分片上传完成")
                return True

            print(f"需要上传 {len(parts_to_upload)} 个分片")

            # 使用线程池并行上传
            parts_lock = threading.Lock()
            completed_parts = existing_parts.copy()
            completed_count = 0
            total_parts = len(parts_to_upload)

            def upload_part(part_number):
                """上传单个分片"""
                nonlocal completed_count
                try:
                    # 读取分片数据
                    with open(local_path, 'rb') as f:
                        f.seek((part_number - 1) * chunksize)
                        chunk = f.read(chunksize)

                    if not chunk:
                        return None

                    # 上传分片
                    part_response = self.s3_client.upload_part(
                        Bucket=self.bucket_name,
                        Key=prefix,
                        PartNumber=part_number,
                        UploadId=upload_id,
                        Body=chunk
                    )

                    # 保存分片信息
                    with parts_lock:
                        completed_parts.append({
                            'PartNumber': part_number,
                            'ETag': part_response['ETag']
                        })
                        completed_count += 1
                        print(f"完成分片 {part_number}/{num_parts} ({completed_count}/{total_parts})")

                    return part_number

                except Exception as e:
                    print(f"上传分片 {part_number} 失败: {e}")
                    return None

            # 使用线程池并行上传
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 提交所有任务
                future_to_part = {executor.submit(upload_part, part_num): part_num for part_num in parts_to_upload}

                # 等待所有任务完成
                for future in as_completed(future_to_part):
                    part_num = future_to_part[future]
                    try:
                        result = future.result()
                        if result is None:
                            print(f"分片 {part_num} 上传失败")
                            raise Exception(f"分片 {part_num} 上传失败")
                    except Exception as e:
                        print(f"分片 {part_num} 执行失败: {e}")
                        # 中止上传
                        try:
                            self.s3_client.abort_multipart_upload(
                                Bucket=self.bucket_name,
                                Key=prefix,
                                UploadId=upload_id
                            )
                            print("已中止上传")
                        except:
                            pass
                        return False

            # 按PartNumber排序parts
            completed_parts.sort(key=lambda x: x['PartNumber'])

            # 完成分片上传
            print("正在完成分片上传...")
            self.s3_client.complete_multipart_upload(
                Bucket=self.bucket_name,
                Key=prefix,
                UploadId=upload_id,
                MultipartUpload={'Parts': completed_parts}
            )

            print("并行分片上传完成")
            return True

        except Exception as e:
            print(f"并行分片上传失败: {e}")
            # 不要自动中止上传，以便后续恢复
            if upload_id:
                print(f"上传已中断，UploadId: {upload_id}")
                print("可以使用 resume_upload=True 参数恢复上传")
            return False

    def DownloadFile(self, prefix, local_path):
        """
        从S3下载单个文件

        Args:
            prefix (str): S3中的文件路径（键）
            local_path (str): 本地保存路径

        Returns:
            bool: 下载成功返回True，失败返回False
        """
        try:
            # 确保本地目录存在
            os.makedirs(os.path.dirname(local_path), exist_ok=True)

            # 下载文件
            self.s3_client.download_file(
                Bucket=self.bucket_name,
                Key=prefix,
                Filename=local_path
            )
            return True
        except ClientError as e:
            print(f"下载文件失败: {e}")
            return False
        except Exception as e:
            print(f"下载文件时发生错误: {e}")
            return False

    """
    上传单个文件到S3，支持分片上传、断点续传和并行上传

    Args:
        prefix (str): S3中的目标路径（键）
        local_path (str): 本地文件路径
        use_multipart (bool, optional): 是否强制使用分片上传。如果为None，则根据文件大小自动决定
        resume_upload (bool): 是否尝试恢复未完成的上传。仅在分片上传时有效
        parallel_upload (bool): 是否使用并行上传。仅在分片上传时有效
        
        self.max_workers (int, optional): 并行上传的最大线程数。如果为None，使用类初始化时的设置
        self.multipart_chunksize (int, optional): 分片大小（字节）。如果为None，使用类初始化时的设置
    Returns:
        bool: 上传成功返回True，失败返回False
    """
    def UploadFile(self, prefix, local_path, use_multipart=True, resume_upload=False, parallel_upload=True):
        try:
            # 检查本地文件是否存在
            if not os.path.exists(local_path):
                print(f"本地文件不存在: {local_path}")
                return False

            # 获取文件大小
            file_size = os.path.getsize(local_path)

            if use_multipart and file_size > self.multipart_chunksize:
                logging.info(f"使用分片上传文件: {local_path} (大小: {file_size} bytes, 分片大小: {self.multipart_chunksize} bytes)")
                if parallel_upload:
                    logging.info(f"使用并行上传，最大线程数: {self.max_workers}")
                    return self._upload_file_multipart_parallel(prefix, local_path, file_size, self.multipart_chunksize,
                                                                resume_upload, self.max_workers)
                else:
                    return self._upload_file_multipart(prefix, local_path, file_size, self.multipart_chunksize, resume_upload)
            else:
                logging.info(f"使用普通上传文件: {local_path} (大小: {file_size} bytes)")
                # 使用普通上传
                self.s3_client.upload_file(
                    Filename=local_path,
                    Bucket=self.bucket_name,
                    Key=prefix
                )
                return True
        except ClientError as e:
            logging.error(f"上传文件失败: {e}")
            return False
        except Exception as e:
            logging.error(f"上传文件时发生错误: {e}")
            return False

    def UploadFolder(self, prefix, local_path):
        """
        上传整个文件夹到S3

        Args:
            prefix (str): S3中的目标前缀（目录）
            local_path (str): 本地文件夹路径

        Returns:
            bool: 上传成功返回True，失败返回False
        """
        try:
            # 检查本地文件夹是否存在
            if not os.path.exists(local_path):
                print(f"本地文件夹不存在: {local_path}")
                return False

            if not os.path.isdir(local_path):
                print(f"路径不是文件夹: {local_path}")
                return False

            # 遍历文件夹中的所有文件
            for root, dirs, files in os.walk(local_path):
                for file in files:
                    local_file_path = os.path.join(root, file)

                    # 计算S3中的相对路径
                    relative_path = os.path.relpath(local_file_path, local_path)
                    s3_key = os.path.join(prefix, relative_path).replace('\\', '/')

                    # 上传文件
                    success = self.UploadFile(s3_key, local_file_path)
                    if not success:
                        print(f"上传文件失败: {local_file_path}")
                        return False

            return True
        except Exception as e:
            print(f"上传文件夹时发生错误: {e}")
            return False

    def DownloadFolder(self, prefix, local_path):
        """
        从S3下载整个文件夹

        Args:
            prefix (str): S3中的源前缀（目录）
            local_path (str): 本地保存路径

        Returns:
            bool: 下载成功返回True，失败返回False
        """
        try:
            # 确保本地目录存在
            os.makedirs(local_path, exist_ok=True)

            # 列出指定前缀下的所有文件
            files = self.ListFiles(prefix, recursive=True)

            for file_key in files:
                # 计算本地文件路径
                relative_path = file_key[len(prefix):].lstrip('/')
                local_file_path = os.path.join(local_path, relative_path)

                # 确保本地目录存在
                os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

                # 下载文件
                success = self.DownloadFile(file_key, local_file_path)
                if not success:
                    print(f"下载文件失败: {file_key}")
                    return False

            return True
        except Exception as e:
            print(f"下载文件夹时发生错误: {e}")
            return False

    def IsFileExists(self, prefix):
        """
        检查S3中文件是否存在

        Args:
            prefix (str): S3中的文件路径（键）

        Returns:
            bool: 文件存在返回True，不存在返回False
        """
        try:
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=prefix)
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                return True
            else:
                return False
        except ClientError as e:
            # 如果错误码是404，表示文件不存在
            if e.response['Error']['Code'] == '404':
                return False
            else:
                print(f"检查文件存在性时发生错误: {e}")
                return False
        except Exception as e:
            print(f"检查文件存在性时发生错误: {e}")
            return False

    def ListFiles(self, prefix, recursive=False):
        """
        列出S3中指定前缀下的文件

        Args:
            prefix (str): S3中的前缀（目录）
            recursive (bool): 是否递归列出子目录文件

        Returns:
            list: 文件键的列表
        """
        try:
            files = []

            if recursive:
                # 递归列出所有文件
                paginator = self.s3_client.get_paginator('list_objects_v2')
                operation_parameters = {
                    'Bucket': self.bucket_name,
                    'Prefix': prefix
                }

                for page in paginator.paginate(**operation_parameters):
                    if 'Contents' in page:
                        for obj in page['Contents']:
                            files.append(obj['Key'])
            else:
                # 只列出当前目录下的文件（不包含子目录）
                response = self.s3_client.list_objects_v2(
                    Bucket=self.bucket_name,
                    Prefix=prefix,
                    Delimiter='/'
                )

                # 添加文件
                if 'Contents' in response:
                    for obj in response['Contents']:
                        files.append(obj['Key'])

                # 添加子目录（如果用户需要）
                # 注意：这里只返回文件，不返回目录

            return files
        except Exception as e:
            print(f"列出文件时发生错误: {e}")
            return []
