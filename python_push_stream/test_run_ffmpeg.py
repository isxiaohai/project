import random
import subprocess
import threading
import logging
import logging.handlers
import traceback

# 设置日志
LOG_FILENAME = "mylog.log"
g_logger = logging.getLogger('mai_comfyuiLogger')
logging.basicConfig(level=logging.DEBUG)
formatter = logging.Formatter('[%(filename)s:%(lineno)d] %(asctime)s %(thread)d %(message)s ')
handler = logging.handlers.RotatingFileHandler(LOG_FILENAME, maxBytes=10000000, backupCount=10, encoding="utf-8")
handler.setFormatter(formatter)
g_logger.addHandler(handler)

def run_ffmpeg_cmd(cmd, logger):
    try:
        # 执行 FFmpeg 命令
        process = subprocess.Popen(cmd,
                                   stdout=subprocess.PIPE,  # 输出数据
                                   stderr=subprocess.PIPE,  # 错误数据
                                   shell=True,
                                   universal_newlines=True)

        if process:
            logger.info(f"start_program {cmd} succeeded")
        else:
            info = f"start failed {cmd}"
            logger.info(info)
            return False, info

        while True:
            exit_code = process.poll()
            if exit_code is not None:
                stdout, stderr = process.communicate()
                info = f"run {cmd} failed exit_code : {exit_code}, stdout: {stdout}, stderr: {stderr}"
                logger.info(info)
                return False, info
            else:
                # 读取输出
                stdout, stderr = process.communicate()
                print("1111111111111111111111111111\n")
    except Exception as e:
        strtraceback = traceback.format_exc()
        info = f"{str(e)}: {strtraceback}"
        logger.info(info)
        return False, info

    return True, ""

def load_addresses(file_path):
    """加载地址列表"""
    try:
        with open(file_path, 'r') as file:
            return [line.strip() for line in file if line.strip()]
    except Exception as e:
        g_logger.error(f"Failed to load addresses from {file_path}: {e}")
        return []

# 加载源流地址和目标推流地址
source_streams_file = "source_streams.txt"
rtmp_addresses_file = "rtmp_addresses.txt"
source_streams = load_addresses(source_streams_file)
rtmp_addresses = load_addresses(rtmp_addresses_file)




# 定义推流的线程函数
def push_stream_to_rtmp(rtmp_address,max_retries = 100000):
# 随机选择一个源流
    source_stream = random.choice(source_streams)
    retry_count = 0
    while retry_count < max_retries:
        cmd = f"ffmpeg -re -i {source_stream} -codec copy -f flv {rtmp_address}"

        # 调用函数执行命令
        bret, info = run_ffmpeg_cmd(cmd, g_logger)

        if bret:
            g_logger.info(f"Successfully pushed to {rtmp_address}.")
            print(f"Successfully pushed to {rtmp_address}.")

        else:

            g_logger.warning(f"Failed to push to {rtmp_address}: {info}. Retrying {retry_count}/{max_retries}")
            print(f"Failed to push to {rtmp_address}: {info}. Retrying {retry_count}/{max_retries}")
        retry_count += 1

    if retry_count >= max_retries:  # 如果达到最大重试次数
        g_logger.error(f"Max retries reached for {rtmp_address}. Push failed.")
        print(f"Max retries reached for {rtmp_address}. Push failed.")



# 创建线程列表
threads = []

# 创建并启动线程
for rtmp_address in rtmp_addresses:
    thread = threading.Thread(target=push_stream_to_rtmp, args=(rtmp_address,))
    threads.append(thread)
    thread.start()

# 等待所有线程完成
for thread in threads:
    thread.join()
