"""
gacrnd 任务 Web 启动器
基于 Flask + Bootstrap，支持动态添加任务并驱动上传逻辑。
"""

import json
import os
import re
import subprocess
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from flask import Flask, jsonify, render_template, request

DEFAULT_USER_ID = "1001"
DEFAULT_DOCKER_IMAGE = "127.0.0.1/kd-ad/oss_uploader_new:latest"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_DIR = os.path.join(BASE_DIR, "templates")
OPTIONS_FILE = os.path.join(BASE_DIR, "task_options.json")

app = Flask(__name__, template_folder=TEMPLATE_DIR)


def _append_log(logs: List[str], msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    logs.append(f"[{ts}] {msg}")


def _load_options() -> Dict[str, List[str]]:
    defaults = {
        "source_type": [],
        "data_type": [],
        "cloud_type": [],
        "mode": [],
    }
    try:
        with open(OPTIONS_FILE, "r") as fp:
            content = json.load(fp)
    except (FileNotFoundError, json.JSONDecodeError):
        return defaults
    for key in defaults:
        values = content.get(key, [])
        defaults[key] = values if isinstance(values, list) else []
    return defaults


def _run_sudo(user_pwd: str, cmd: List[str]) -> Optional[subprocess.CompletedProcess]:
    if not user_pwd:
        return None
    return subprocess.run(
        ["sudo", "-S"] + cmd,
        input=f"{user_pwd}\n",
        text=True,
        capture_output=True,
    )


def _parse_serial(output: str) -> Optional[str]:
    for line in output.splitlines():
        if "Serial" not in line:
            continue
        parts = line.split(":", 1)
        if len(parts) == 2:
            return parts[1].strip()
    return None


def _get_disk_sn(target_path: str, user_pwd: str, logs: List[str]) -> Optional[str]:
    try:
        result = subprocess.run(
            ["realpath", "--", target_path],
            capture_output=True,
            text=True,
        )
        path = result.stdout.strip()
        if not path:
            return None

        result = subprocess.run(
            ["df", "--output=source", path],
            capture_output=True,
            text=True,
        )
        lines = result.stdout.strip().splitlines()
        if len(lines) < 2:
            return None
        device = lines[1].strip()
        if not device or not os.path.exists(device):
            return None

        result = subprocess.run(
            ["lsblk", "-no", "pkname", device],
            capture_output=True,
            text=True,
        )
        base_device = result.stdout.strip() or os.path.basename(device)

        full_device = f"/dev/{re.sub(r'p?[0-9]+$', '', base_device)}"
        if not os.path.exists(full_device):
            full_device = f"/dev/{base_device}"

        for candidate in (device, full_device):
            sudo_result = _run_sudo(user_pwd, ["smartctl", "-i", candidate])
            if sudo_result is None:
                _append_log(logs, "未提供 sudo 密码，无法读取硬盘序列号")
                return None
            sn = _parse_serial(sudo_result.stdout)
            if sn:
                return sn
        return None
    except Exception as exc:
        _append_log(logs, f"获取序列号异常: {exc}")
        return None


def _run_tasks(
    tasks: List[Dict[str, str]],
    user_id: str,
    user_pwd: str,
    docker_image: str,
) -> Tuple[List[str], List[Dict[str, str]]]:
    logs: List[str] = []
    results: List[Dict[str, str]] = []
    _append_log(logs, f"> ----- 任务开始，开始时间：{datetime.now()}")

    if not user_id:
        _append_log(logs, "错误: 用户ID不能为空")
        return logs, results
    if not user_pwd:
        _append_log(logs, "错误: sudo 密码不能为空")
        return logs, results

    result = subprocess.run(["id", user_id], capture_output=True)
    if result.returncode != 0:
        _append_log(logs, f"错误: 用户ID {user_id} 不存在")
        return logs, results

    if not docker_image:
        _append_log(logs, "错误: Docker 镜像不能为空")
        return logs, results

    _append_log(logs, "更新本地docker......")
    pull = subprocess.run(
        ["docker", "pull", docker_image],
        capture_output=True,
        text=True,
    )
    if pull.returncode != 0:
        _append_log(logs, f"docker pull 失败:\n{pull.stderr.strip()}")
        return logs, results

    failed_count = 0
    for index, item in enumerate(tasks, 1):
        input_root = (item.get("input_root") or "").strip()
        output_root = (item.get("output_root") or "").strip()
        source_type = (item.get("source_type") or "").strip()
        data_type = (item.get("data_type") or "").strip()
        cloud_type = (item.get("cloud_type") or "").strip()
        mode = (item.get("mode") or item.get("run_mode") or "prod").strip()

        _append_log(logs, f"本地输入路径 ： {input_root}")
        _append_log(logs, f"本地输出路径 ： {output_root}")
        _append_log(logs, f"数据源类型 ： {source_type}")
        _append_log(logs, f"数据类型 ： {data_type}")
        _append_log(logs, f"云服务类型 ： {cloud_type}")
        _append_log(logs, f"模式 ： {mode}")

        status = {"status": "失败"}
        results.append(status)

        if not input_root or not output_root:
            _append_log(logs, "输入/输出路径不能为空")
            failed_count += 1
            continue
        if not os.path.exists(input_root):
            _append_log(logs, f"路径不存在: {input_root}")
            failed_count += 1
            continue
        if not os.path.isdir(input_root):
            _append_log(logs, f"不是一个目录: {input_root}")
            failed_count += 1
            continue

        sn = _get_disk_sn(input_root, user_pwd, logs)
        if sn is None:
            _append_log(logs, "无法获取序列号")
            failed_count += 1
            return logs, results
        _append_log(logs, f"硬盘序列号: {sn}")

        task_info_dir = os.path.join(output_root, "taskInfos")
        log_root = os.path.join(output_root, "logs")
        os.makedirs(task_info_dir, exist_ok=True)
        os.makedirs(log_root, exist_ok=True)

        _run_sudo(user_pwd, ["chmod", "-R", "777", input_root])
        _run_sudo(user_pwd, ["chmod", "-R", "777", output_root])

        sub_task_info_file = os.path.join(task_info_dir, f"taskInfo_{index}.json")
        task_payload = {
            "input_root": input_root,
            "output_root": output_root,
            "source_type": source_type,
            "data_type": data_type,
            "cloud_type": cloud_type,
        }
        with open(sub_task_info_file, "w") as fp:
            json.dump(task_payload, fp, indent=2, ensure_ascii=False)

        _append_log(logs, "创建上传任务......")
        docker_cmd = [
            "docker", "run", "--rm",
            "--user", "hadoop",
            "-v", f"{sub_task_info_file}:{sub_task_info_file}",
            "-v", f"{input_root}:{input_root}",
            "-v", f"{output_root}/logs:/media/xumaozhou/logs",
            "-v", f"{output_root}:{output_root}",
            "-v", "/etc/localtime:/etc/localtime:ro",
            "-v", "/etc/hosts:/etc/hosts",
            docker_image,
            "/bin/bash", "-c",
            "umask 0000 && python3 /home/hadoop/bin/main.py"
            f" -i {sub_task_info_file} -t {data_type} -m {mode} -s {sn}",
        ]
        proc = subprocess.run(docker_cmd, capture_output=True, text=True)
        if proc.stdout:
            _append_log(logs, proc.stdout.strip())
        if proc.stderr:
            _append_log(logs, proc.stderr.strip())

        if proc.returncode != 0:
            _append_log(logs, f"数据上传失败，返回码={proc.returncode}")
            failed_count += 1
        else:
            status["status"] = "成功"

    _append_log(logs, f"> ----- 任务结束，结束时间：{datetime.now()}")
    if failed_count > 0:
        _append_log(logs, f"上传失败的任务数={failed_count}")
    return logs, results


@app.get("/")
def index():
    return render_template(
        "gacrnd_web.html",
        options=_load_options(),
        default_user_id=DEFAULT_USER_ID,
        default_docker_image=DEFAULT_DOCKER_IMAGE,
    )


@app.post("/submit")
def submit():
    payload = request.get_json(silent=True) or {}
    tasks = payload.get("tasks", [])
    if not isinstance(tasks, list) or not tasks:
        return jsonify({"error": "请至少添加一个任务"}), 400

    user_id = (payload.get("user_id") or DEFAULT_USER_ID).strip()
    user_pwd = payload.get("user_pwd") or ""
    docker_image = (payload.get("docker_image") or DEFAULT_DOCKER_IMAGE).strip()

    logs, results = _run_tasks(tasks, user_id, user_pwd, docker_image)
    return jsonify({"logs": logs, "results": results})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
