#!/bin/bash
#set -x # 调试

USER_ID=1002
USER_PWD="Kuandeng@2025"

# 检查用户是否存在
if ! id "$USER_ID" &>/dev/null; then
    echo "错误: 用户ID $USER_ID 不存在"
    exit 1
fi

# 获取指定路径所在硬盘的序列号
get_disk_sn() {
  local sn_num
    target_path="$1"

    # 转换为绝对路径
    path=$(realpath -- "$target_path" 2>/dev/null || echo "")
    [ -z "$path" ] && return 1

    # 通过 df 获取设备名
    device=$(df --output=source "$path" 2>/dev/null | awk 'NR==2 {print $1}')
    [ -z "$device" ] && return 2

    # 提取主设备名（适用于 LVM/LUKS 等场景）
    if [ -b "$device" ]; then
        base_device=$(lsblk -no pkname "$device" 2>/dev/null || basename "$device")
    else
        return 3
    fi

    # 获取完整设备路径
    full_device="/dev/${base_device%%[0-9]*}"  # 移除分区号
    [ -b "$full_device" ] || full_device="/dev/$base_device"

    # 先尝试从原始设备（device）读取序列号
    sn_num=$(echo "$USER_PWD" | sudo -S smartctl -i "$device" 2>/dev/null | grep 'Serial' | awk -F':[[:space:]]*' '{gsub(/^[ \t]+|[ \t]+$/, "", $2); print $2}')

    # 如果为空，再尝试从完整设备路径（full_device）读取
    if [ -z "$sn_num" ]; then
      sn_num=$(echo "$USER_PWD" | sudo -S smartctl -i "$full_device" 2>/dev/null | grep 'Serial' | awk -F':[[:space:]]*' '{gsub(/^[ \t]+|[ \t]+$/, "", $2); print $2}')
    fi

    # 输出序列号信息
    echo "$sn_num"
}

SCRIPT_DIR=$(cd "$(dirname "$0")";pwd)
echo "> ----- 任务开始，开始时间：$(date)，当前脚本所在目录为 ： $SCRIPT_DIR"

TASK_INFO_FILE="$SCRIPT_DIR/taskInfo.json"
if [ -f "$TASK_INFO_FILE" ]; then
  echo "taskInfo.json文件存在:$TASK_INFO_FILE"
else
  echo "taskInfo.json文件不存在"
  exit 1
fi

UPLOAD_DOCKER_IMAGE="127.0.0.1/cs/cloud_uploader:latest.test"
#echo "更新本地docker......"
docker pull $UPLOAD_DOCKER_IMAGE || exit 1

INPUT_ROOT=""
OUTPUT_ROOT=""
INDEX=1
FAILED_COUNT=0
jq -c '.[]' $TASK_INFO_FILE | while read -r item; do
  # 从每个项中提取 input 和 output
  INPUT_ROOT=$(echo "$item" | jq -r '.input_root')
  OUTPUT_ROOT=$(echo "$item" | jq -r '.output_root')
  DATA_TYPE=$(echo "$item" | jq -r '.data_type')
  SOURCE_TYPE=$(echo "$item" | jq -r '.source_type')
  MODE=$(echo "$item" | jq -r '.run_mode')
  echo "本地输入路径 ： $INPUT_ROOT"
  echo "本地输出路径 ： $OUTPUT_ROOT"
  echo "数据类型 ： $DATA_TYPE"

  # 判断路径是否存在
  if [ -e "$INPUT_ROOT" ]; then
    echo "路径存在: $INPUT_ROOT"
  else
    echo "路径不存在: $INPUT_ROOT"
    FAILED_COUNT=$((FAILED_COUNT + 1))
    continue
  fi

  if [ -d "$INPUT_ROOT" ]; then
    echo "是一个目录: $INPUT_ROOT"
  else
    echo "不是一个目录: $INPUT_ROOT"
    FAILED_COUNT=$((FAILED_COUNT + 1))
    continue
  fi

  # 获取硬盘sn号
  if sn=$(get_disk_sn "$INPUT_ROOT"); then
    echo "硬盘序列号: $sn"
  else
    echo "无法获取序列号" >&2
    exit 2
  fi

  # 构建临时目录
  TASK_INFO_DIR="${OUTPUT_ROOT}/taskInfos"
  if [ ! -d "$TASK_INFO_DIR" ]; then
      mkdir -p "$TASK_INFO_DIR"
  fi
  LOG_ROOT="${OUTPUT_ROOT}/logs"
  if [ ! -d "$LOG_ROOT" ]; then
      mkdir -p "$LOG_ROOT"
  fi

  # 设置权限
  echo "$USER_PWD" | sudo -S chmod -R 777 "$INPUT_ROOT"
  echo "$USER_PWD" | sudo -S chmod -R 777 "$OUTPUT_ROOT"

  # m18数据 ins文件转换
  if [ $SOURCE_TYPE = "zgm" ]; then
      echo "执行ins文件转换脚本"
      python3 /home/rd/Novtel/ins_file_parser.py ${INPUT_ROOT}
      if [ $? -ne 0 ]; then
          echo "python脚本执行异常！！！"
          exit 1
      fi
  else
      echo "当前数据类型${SOURCE_TYPE}不需要做文件转换"
  fi

  # 生成单任务taskInfo.json
  SUB_TASK_INFO_FILE=${TASK_INFO_DIR}/taskInfo_${INDEX}.json
  if ! echo "$item" | jq '.' > "$SUB_TASK_INFO_FILE"; then
      echo "生成子任务taskinfo失败"
      exit 1
  fi

  echo "创建上传任务......"
  docker run --rm \
    --user hadoop \
    -v $SUB_TASK_INFO_FILE:$SUB_TASK_INFO_FILE \
    -v $INPUT_ROOT:$INPUT_ROOT \
    -v $OUTPUT_ROOT/logs:/media/xumaozhou/logs \
    -v $OUTPUT_ROOT:$OUTPUT_ROOT \
    -v /etc/localtime:/etc/localtime:ro \
    -v /etc/hosts:/etc/hosts \
    -v /tmp:/tmp \
    --entrypoint /bin/bash \
    $UPLOAD_DOCKER_IMAGE \
    -c "umask 0000 && python3 main.py -i ${SUB_TASK_INFO_FILE} -t ${DATA_TYPE} -m ${MODE} -s ${sn}"

  EXIT_CODE=$?
  if [ $EXIT_CODE -ne 0 ]; then
    echo "数据上传失败，返回码=$EXIT_CODE"
    FAILED_COUNT=$((FAILED_COUNT + 1))
  fi
  INDEX=$((INDEX + 1))
done

echo "> ----- 任务结束，结束时间：$(date)"
if [ $FAILED_COUNT -ne 0 ]; then
  echo "上传失败的任务数=$FAILED_COUNT"
  exit $FAILED_COUNT
else
  exit 0
fi