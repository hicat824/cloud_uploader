"""
gacrnd 任务图形化启动器
基于 PySide6 开发，参考 gacrnd/run.sh 实现相同的任务编排与上传逻辑。
"""

import re
import sys
import os
import json
import subprocess
from datetime import datetime
from typing import Optional

from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QLineEdit, QPushButton, QTextEdit, QFileDialog,
    QGroupBox, QFormLayout, QTableWidget, QTableWidgetItem,
    QHeaderView, QMessageBox, QSplitter,
)
from PySide6.QtCore import Qt, QThread, Signal
from PySide6.QtGui import QFont, QColor

DEFAULT_USER_ID = "1001"
DEFAULT_DOCKER_IMAGE = "127.0.0.1/kd-ad/oss_uploader_new:latest"


class TaskRunner(QThread):
    """后台线程，执行与 gacrnd/run.sh 等价的任务编排逻辑。"""

    log_signal = Signal(str)              # 日志消息
    status_signal = Signal(int, str)      # (行索引, 状态文字)
    finished_signal = Signal(int)         # 完成信号，参数为失败任务数

    def __init__(self, task_info_file: str, user_id: str, user_pwd: str, docker_image: str):
        super().__init__()
        self.task_info_file = task_info_file
        self.user_id = user_id
        self.user_pwd = user_pwd
        self.docker_image = docker_image
        self._running = True

    def stop(self):
        self._running = False

    def _log(self, msg: str):
        ts = datetime.now().strftime("%H:%M:%S")
        self.log_signal.emit(f"[{ts}] {msg}")

    def _get_disk_sn(self, target_path: str):
        """获取指定路径所在硬盘的序列号（对应 run.sh 中的 get_disk_sn 函数）。"""
        try:
            result = subprocess.run(
                ["realpath", "--", target_path],
                capture_output=True, text=True,
            )
            path = result.stdout.strip()
            if not path:
                return None

            result = subprocess.run(
                ["df", "--output=source", path],
                capture_output=True, text=True,
            )
            lines = result.stdout.strip().splitlines()
            if len(lines) < 2:
                return None
            device = lines[1].strip()
            if not device:
                return None

            if not os.path.exists(device):
                return None

            result = subprocess.run(
                ["lsblk", "-no", "pkname", device],
                capture_output=True, text=True,
            )
            base_device = result.stdout.strip() or os.path.basename(device)

            # Remove trailing partition suffix (e.g. sda1 -> sda, nvme0n1p1 -> nvme0n1)
            full_device = f"/dev/{re.sub(r'p?[0-9]+$', '', base_device)}"
            if not os.path.exists(full_device):
                full_device = f"/dev/{base_device}"

            # 先尝试从原始设备读取序列号
            cmd = (
                f"echo '{self.user_pwd}' | sudo -S smartctl -i {device} 2>/dev/null"
                r" | grep 'Serial' | awk -F':[[:space:]]*' '{gsub(/^[ \t]+|[ \t]+$/, \"\", $2); print $2}'"
            )
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            sn = result.stdout.strip()

            # 如果为空，再尝试从完整设备路径读取
            if not sn:
                cmd = (
                    f"echo '{self.user_pwd}' | sudo -S smartctl -i {full_device} 2>/dev/null"
                    r" | grep 'Serial' | awk -F':[[:space:]]*' '{gsub(/^[ \t]+|[ \t]+$/, \"\", $2); print $2}'"
                )
                result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                sn = result.stdout.strip()

            return sn if sn else None
        except Exception as e:
            self._log(f"获取序列号异常: {e}")
            return None

    def run(self):
        self._log(f"> ----- 任务开始，开始时间：{datetime.now()}")

        # 检查用户是否存在
        result = subprocess.run(["id", self.user_id], capture_output=True)
        if result.returncode != 0:
            self._log(f"错误: 用户ID {self.user_id} 不存在")
            self.finished_signal.emit(1)
            return

        # 检查 taskInfo.json 是否存在
        if not os.path.isfile(self.task_info_file):
            self._log("taskInfo.json文件不存在")
            self.finished_signal.emit(1)
            return
        self._log(f"taskInfo.json文件存在: {self.task_info_file}")

        # 更新 Docker 镜像
        self._log("更新本地docker......")
        result = subprocess.run(
            ["docker", "pull", self.docker_image],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            self._log(f"docker pull 失败:\n{result.stderr.strip()}")
            self.finished_signal.emit(1)
            return

        # 读取任务列表
        try:
            with open(self.task_info_file, "r") as fp:
                tasks = json.load(fp)
        except Exception as exc:
            self._log(f"解析 taskInfo.json 失败: {exc}")
            self.finished_signal.emit(1)
            return

        failed_count = 0
        for index, item in enumerate(tasks, 1):
            if not self._running:
                self._log("任务已被用户中止")
                break

            input_root = item.get("input_root", "")
            output_root = item.get("output_root", "")
            data_type = item.get("data_type", "")
            mode = item.get("run_mode", "prod")

            self._log(f"本地输入路径 ： {input_root}")
            self._log(f"本地输出路径 ： {output_root}")
            self._log(f"数据类型 ： {data_type}")
            self._log(f"模式 ： {mode}")

            self.status_signal.emit(index - 1, "运行中")

            # 判断输入路径是否存在且为目录
            if not os.path.exists(input_root):
                self._log(f"路径不存在: {input_root}")
                failed_count += 1
                self.status_signal.emit(index - 1, "失败")
                continue
            if not os.path.isdir(input_root):
                self._log(f"不是一个目录: {input_root}")
                failed_count += 1
                self.status_signal.emit(index - 1, "失败")
                continue
            self._log(f"路径存在: {input_root}")

            # 获取硬盘序列号
            sn = self._get_disk_sn(input_root)
            if sn is None:
                self._log("无法获取序列号")
                self.finished_signal.emit(2)
                return
            self._log(f"硬盘序列号: {sn}")

            # 构建临时目录
            task_info_dir = os.path.join(output_root, "taskInfos")
            os.makedirs(task_info_dir, exist_ok=True)
            log_root = os.path.join(output_root, "logs")
            os.makedirs(log_root, exist_ok=True)

            # 设置权限
            subprocess.run(
                f"echo '{self.user_pwd}' | sudo -S chmod -R 777 '{input_root}'",
                shell=True,
            )
            subprocess.run(
                f"echo '{self.user_pwd}' | sudo -S chmod -R 777 '{output_root}'",
                shell=True,
            )

            # 生成单任务 taskInfo.json
            sub_task_info_file = os.path.join(task_info_dir, f"taskInfo_{index}.json")
            with open(sub_task_info_file, "w") as fp:
                json.dump(item, fp, indent=2)

            # 启动 Docker 容器
            self._log("创建上传任务......")
            docker_cmd = [
                "docker", "run", "--rm",
                "--user", "hadoop",
                "-v", f"{sub_task_info_file}:{sub_task_info_file}",
                "-v", f"{input_root}:{input_root}",
                "-v", f"{output_root}/logs:/media/xumaozhou/logs",
                "-v", f"{output_root}:{output_root}",
                "-v", "/etc/localtime:/etc/localtime:ro",
                "-v", "/etc/hosts:/etc/hosts",
                self.docker_image,
                "/bin/bash", "-c",
                f"umask 0000 && python3 /home/hadoop/bin/main.py"
                f" -i {sub_task_info_file} -t {data_type} -m {mode} -s {sn}",
            ]
            proc = subprocess.run(docker_cmd, capture_output=True, text=True)
            if proc.stdout:
                self._log(proc.stdout.strip())
            if proc.stderr:
                self._log(proc.stderr.strip())

            if proc.returncode != 0:
                self._log(f"数据上传失败，返回码={proc.returncode}")
                failed_count += 1
                self.status_signal.emit(index - 1, "失败")
            else:
                self.status_signal.emit(index - 1, "成功")

        self._log(f"> ----- 任务结束，结束时间：{datetime.now()}")
        if failed_count > 0:
            self._log(f"上传失败的任务数={failed_count}")
        self.finished_signal.emit(failed_count)


class MainWindow(QMainWindow):
    """GAC RnD 数据上传工具主窗口。"""

    def __init__(self):
        super().__init__()
        self.setWindowTitle("GAC RnD 数据上传工具")
        self.resize(1000, 750)
        self._task_runner: Optional[TaskRunner] = None
        self._tasks: list = []
        self._setup_ui()

    def _setup_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QVBoxLayout(central)
        main_layout.setContentsMargins(10, 10, 10, 10)

        splitter = QSplitter(Qt.Vertical)
        main_layout.addWidget(splitter)

        # ── 上半部分：配置 + 任务列表 ───────────────────────────────────────
        top_widget = QWidget()
        top_layout = QVBoxLayout(top_widget)
        top_layout.setContentsMargins(0, 0, 0, 0)
        splitter.addWidget(top_widget)

        # 配置分组
        config_group = QGroupBox("配置")
        config_form = QFormLayout()
        config_group.setLayout(config_form)

        self._user_id_edit = QLineEdit(DEFAULT_USER_ID)
        config_form.addRow("用户ID:", self._user_id_edit)

        self._user_pwd_edit = QLineEdit()
        self._user_pwd_edit.setPlaceholderText("输入sudo密码...")
        self._user_pwd_edit.setEchoMode(QLineEdit.Password)
        config_form.addRow("用户密码:", self._user_pwd_edit)

        self._docker_image_edit = QLineEdit(DEFAULT_DOCKER_IMAGE)
        config_form.addRow("Docker镜像:", self._docker_image_edit)

        task_info_layout = QHBoxLayout()
        self._task_info_edit = QLineEdit()
        self._task_info_edit.setPlaceholderText("选择 taskInfo.json 文件路径...")
        browse_btn = QPushButton("浏览...")
        browse_btn.clicked.connect(self._browse_task_info)
        task_info_layout.addWidget(self._task_info_edit)
        task_info_layout.addWidget(browse_btn)
        config_form.addRow("taskInfo.json:", task_info_layout)

        top_layout.addWidget(config_group)

        # 按钮行
        btn_layout = QHBoxLayout()
        load_btn = QPushButton("加载任务列表")
        load_btn.clicked.connect(self._load_tasks)
        self._start_btn = QPushButton("开始上传")
        self._start_btn.clicked.connect(self._start_upload)
        self._stop_btn = QPushButton("停止")
        self._stop_btn.clicked.connect(self._stop_upload)
        self._stop_btn.setEnabled(False)
        btn_layout.addWidget(load_btn)
        btn_layout.addStretch()
        btn_layout.addWidget(self._start_btn)
        btn_layout.addWidget(self._stop_btn)
        top_layout.addLayout(btn_layout)

        # 任务列表
        task_group = QGroupBox("任务列表")
        task_layout = QVBoxLayout(task_group)
        self._task_table = QTableWidget(0, 5)
        self._task_table.setHorizontalHeaderLabels(
            ["输入路径", "输出路径", "数据类型", "运行模式", "状态"]
        )
        self._task_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self._task_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self._task_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self._task_table.setSelectionBehavior(QTableWidget.SelectRows)
        task_layout.addWidget(self._task_table)
        top_layout.addWidget(task_group)

        # ── 下半部分：日志 ───────────────────────────────────────────────────
        log_group = QGroupBox("运行日志")
        log_layout = QVBoxLayout(log_group)
        self._log_text = QTextEdit()
        self._log_text.setReadOnly(True)
        self._log_text.setFont(QFont("Courier New", 9))
        log_layout.addWidget(self._log_text)
        splitter.addWidget(log_group)

        splitter.setSizes([420, 300])

    # ── 槽函数 ───────────────────────────────────────────────────────────────

    def _browse_task_info(self):
        default_dir = os.path.dirname(os.path.abspath(__file__))
        path, _ = QFileDialog.getOpenFileName(
            self, "选择 taskInfo.json", default_dir, "JSON Files (*.json)"
        )
        if path:
            self._task_info_edit.setText(path)
            self._load_tasks()

    def _load_tasks(self):
        path = self._task_info_edit.text().strip()
        if not path or not os.path.isfile(path):
            QMessageBox.warning(self, "警告", "请先选择有效的 taskInfo.json 文件")
            return
        try:
            with open(path, "r") as fp:
                self._tasks = json.load(fp)
        except Exception as exc:
            QMessageBox.critical(self, "错误", f"解析 taskInfo.json 失败：{exc}")
            return

        self._task_table.setRowCount(0)
        for item in self._tasks:
            row = self._task_table.rowCount()
            self._task_table.insertRow(row)
            self._task_table.setItem(row, 0, QTableWidgetItem(item.get("input_root", "")))
            self._task_table.setItem(row, 1, QTableWidgetItem(item.get("output_root", "")))
            self._task_table.setItem(row, 2, QTableWidgetItem(item.get("data_type", "")))
            self._task_table.setItem(row, 3, QTableWidgetItem(item.get("run_mode", "prod")))
            self._task_table.setItem(row, 4, QTableWidgetItem("等待"))

        self._append_log(f"已加载 {len(self._tasks)} 个任务")

    def _start_upload(self):
        path = self._task_info_edit.text().strip()
        if not path or not os.path.isfile(path):
            QMessageBox.warning(self, "警告", "请先选择有效的 taskInfo.json 文件")
            return
        if not self._tasks:
            QMessageBox.warning(self, "警告", "请先加载任务列表")
            return

        # 重置状态列
        for row in range(self._task_table.rowCount()):
            self._task_table.setItem(row, 4, QTableWidgetItem("等待"))

        self._start_btn.setEnabled(False)
        self._stop_btn.setEnabled(True)
        self._log_text.clear()

        self._task_runner = TaskRunner(
            task_info_file=path,
            user_id=self._user_id_edit.text().strip(),
            user_pwd=self._user_pwd_edit.text(),
            docker_image=self._docker_image_edit.text().strip(),
        )
        self._task_runner.log_signal.connect(self._append_log)
        self._task_runner.status_signal.connect(self._update_task_status)
        self._task_runner.finished_signal.connect(self._on_finished)
        self._task_runner.start()

    def _stop_upload(self):
        if self._task_runner:
            self._task_runner.stop()
        self._stop_btn.setEnabled(False)

    def _append_log(self, msg: str):
        self._log_text.append(msg)
        self._log_text.ensureCursorVisible()

    def _update_task_status(self, row: int, status: str):
        if 0 <= row < self._task_table.rowCount():
            item = QTableWidgetItem(status)
            color_map = {"成功": QColor("green"), "失败": QColor("red"), "运行中": QColor("blue")}
            if status in color_map:
                item.setForeground(color_map[status])
            self._task_table.setItem(row, 4, item)

    def _on_finished(self, failed_count: int):
        self._start_btn.setEnabled(True)
        self._stop_btn.setEnabled(False)
        if failed_count == 0:
            QMessageBox.information(self, "完成", "所有任务上传成功！")
        else:
            QMessageBox.warning(self, "完成", f"任务完成，失败任务数：{failed_count}")


def main():
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
