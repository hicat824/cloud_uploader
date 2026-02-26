import os
import sqlite3
from pathlib import Path

class uploadRecord:
    def __init__(self, row):
        self.upload_mark = row[4] == "success"
        self.sn = row[0]
        self.package_id = row[1]
        self.oss_root = row[2]
        self.task_id = row[3]
        self.size = row[5]
        self.status = row[4]

class UploadTracker:
    def __init__(self, db_file="/tmp/oss_upload_records/aliyun.db"):
        self.db_file = db_file
        storge_root = os.path.dirname(db_file)
        self.timeout = 30.0  # 增加超时时间为30秒，提高并发写入的稳定性
        os.makedirs(storge_root, exist_ok=True)
        self._init_db()  # 初始化数据库和表结构

    def _init_db(self):
        """初始化数据库和表结构"""
        with sqlite3.connect(self.db_file, timeout=self.timeout) as conn:
            cursor = conn.cursor()
            # 创建表（如果不存在）
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS upload_records (
                    disk_id TEXT NOT NULL,
                    package_id TEXT NOT NULL,
                    oss_root TEXT,
                    task_id TEXT,
                    status TEXT,
                    size INTEGER,
                    PRIMARY KEY (disk_id, package_id)
                )
            ''')
            conn.commit()

    def initRecord(self, disk_id, package_id, oss_root, task_id, status, size):
        """标记某个package为已上传"""
        with sqlite3.connect(self.db_file, timeout=self.timeout) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute('''
                    INSERT INTO upload_records (disk_id, package_id, oss_root, task_id, status, size)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (disk_id, package_id, oss_root, task_id, status, size))
                conn.commit()
                return True
            except sqlite3.IntegrityError:
                # 已存在的记录会触发唯一约束错误，直接忽略
                return False

    def updateStatus(self, disk_id, package_id, oss_root, task_id, status, size):
        upload_record = self.checkStatus(disk_id, package_id)
        if not upload_record:
            self.initRecord(disk_id, package_id, oss_root, task_id, status, size)
        else:
            with sqlite3.connect(self.db_file, timeout=self.timeout) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                            UPDATE upload_records 
                            SET task_id = ?, status = ?, size = ?
                            WHERE disk_id = ? AND package_id = ?
                            """, (task_id, status, size, disk_id, package_id))
                conn.commit()  # 提交事务

    def checkStatus(self, disk_id, package_id):
        """检查某个package是否已上传"""
        with sqlite3.connect(self.db_file, timeout=self.timeout) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                    SELECT disk_id, package_id, oss_root, task_id, status, size
                    FROM upload_records WHERE disk_id = ? AND package_id = ?
                    """, (disk_id, package_id))
            row = cursor.fetchone()
            if row:
                return uploadRecord(row)
            else:
                return None

    def close(self):
        if os.path.exists(self.db_file):
            os.remove(self.db_file)