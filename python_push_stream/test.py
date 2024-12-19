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

if not source_streams or not rtmp_addresses:
    g_logger.error("Source streams or RTMP addresses are empty. Exiting...")
    exit(1)
# 源流地址
source_streams = [
    "https://live.pull.hebtv.com/live/mzb160.m3u8",
    "https://live.pull.hebtv.com/live/mzb161.m3u8",
    "https://live.pull.hebtv.com/live/mzb162.m3u8",
    "https://live.pull.hebtv.com/live/mzb163.m3u8",
    "https://live.pull.hebtv.com/live/mzb164.m3u8",
    "https://live.pull.hebtv.com/live/mzb165.m3u8",
    "https://live.pull.hebtv.com/live/mzb166.m3u8",
    "https://live.pull.hebtv.com/live/mzb167.m3u8",
    "https://live.pull.hebtv.com/live/mzb168.m3u8",
    "https://live.pull.hebtv.com/live/mzb169.m3u8"
]

# 目标推流地址
rtmp_addresses = [
    "rtmp://192.168.0.5:1935/live/98789ea7a29244ebbd9602227717832c?vhost=rtmp-only.srs-cluster.matrix.cluster",
    "rtmp://192.168.0.5:1935/live/42cff6b1147d4038897deb9b3c6bb6da?vhost=rtmp-only.srs-cluster.matrix.cluster",
    "rtmp://192.168.0.5:1935/live/9fc7366f66bb43389f43b2115d23ec91?vhost=rtmp-only.srs-cluster.matrix.cluster",
    "rtmp://192.168.0.5:1935/live/01e483f46d84491686d540c68ce2cce1?vhost=rtmp-only.srs-cluster.matrix.cluster",
    "rtmp://192.168.0.5:1935/live/09cfcd91df914c94a14d35ae027799d9?vhost=rtmp-only.srs-cluster.matrix.cluster",
    "rtmp://192.168.0.5:1935/live/8b2fa8d36f804786b4c06385b5ed4c08?vhost=rtmp-only.srs-cluster.matrix.cluster",
    "rtmp://192.168.0.5:1935/live/0eca99fb341b4543a70887fe11409bc3?vhost=rtmp-only.srs-cluster.matrix.cluster",
    "rtmp://192.168.0.5:1935/live/68e1d28631da467a8a73a4e80a28de6e?vhost=rtmp-only.srs-cluster.matrix.cluster",
    "rtmp://192.168.0.5:1935/live/d10da1101a794e85a678ee5839a18707?vhost=rtmp-only.srs-cluster.matrix.cluster",
    "rtmp://192.168.0.5:1935/live/b641e20029c2403b8b29112697e656a8?vhost=rtmp-only.srs-cluster.matrix.cluster",
    "rtmp://192.168.0.5:1935/live/6bb3ac56a29544ccb5387815f00cb4da?vhost=rtmp-only.srs-cluster.matrix.cluster",
    "rtmp://192.168.0.5:1935/live/6a5bbc7dc3ec4d71ade9aad716c7bde7?vhost=rtmp-only.srs-cluster.matrix.cluster",
    "rtmp://192.168.0.5:1935/live/1caeee7694824a339b24067b2909186c?vhost=rtmp-only.srs-cluster.matrix.cluster",
    "rtmp://192.168.0.5:1935/live/af9cb22b7ebd40018ceea365f8dbfd2c?vhost=rtmp-only.srs-cluster.matrix.cluster",
    "rtmp://192.168.0.5:1935/live/9e07c0bfd0af4f0aafbc3abce122b918?vhost=rtmp-only.srs-cluster.matrix.cluster",
    "rtmp://192.168.0.5:1935/live/1a02c519a5c44ebbb750f7aebcc2d2bf?vhost=rtmp-only.srs-cluster.matrix.cluster",
    "rtmp://192.168.0.5:1935/live/bf507d95e7744a0187a4be87fa56ae3d?vhost=rtmp-only.srs-cluster.matrix.cluster",
    "rtmp://192.168.0.5:1935/live/f6435509708f44acb5555930d45cf650?vhost=rtmp-only.srs-cluster.matrix.cluster",
    "rtmp://192.168.0.5:1935/live/cc8b27c821144736a5cf7fd02317b58e?vhost=rtmp-only.srs-cluster.matrix.cluster",
    "rtmp://192.168.0.5:1935/live/f299c4009e5d45d1818eebb249b241e0?vhost=rtmp-only.srs-cluster.matrix.cluster",
    "rtmp://192.168.0.5:1935/live/45770df688ca4fee88dc8e6cded30a51?vhost=rtmp-only.srs-cluster.matrix.cluster",
    "rtmp://192.168.0.5:1935/live/aaa5c65776e548c892aa2c9e27640bc4?vhost=rtmp-only.srs-cluster.matrix.cluster",
    "rtmp://192.168.0.5:1935/live/96277826c05a4bcbb12f991ba098caad?vhost=rtmp-only.srs-cluster.matrix.cluster",
    "rtmp://192.168.0.5:1935/live/88b4c3dd793f42f28caf8175de81dc15?vhost=rtmp-only.srs-cluster.matrix.cluster",
    "rtmp://192.168.0.5:1935/live/f53f6ef46f0644df9c9d2b2aed229334?vhost=rtmp-only.srs-cluster.matrix.cluster",
    "rtmp://192.168.0.5:1935/live/bb90eb74278b491cb6dc4ced60258b99?vhost=rtmp-only.srs-cluster.matrix.cluster",
    "rtmp://192.168.0.5:1935/live/17ec686ec3a94ef68ccaac5fdd85b1eb?vhost=rtmp-only.srs-cluster.matrix.cluster",
    "rtmp://192.168.0.5:1935/live/902b3fc615ab4a1fadda4c4051633246?vhost=rtmp-only.srs-cluster.matrix.cluster",
    "rtmp://192.168.0.5:1935/live/57b1de279af246fc98b464948db800e0?vhost=rtmp-only.srs-cluster.matrix.cluster",
    "rtmp://192.168.0.5:1935/live/907da40882b94da3b5d305ac0a3e36a9?vhost=rtmp-only.srs-cluster.matrix.cluster",
    "rtmp://192.168.0.5:1935/live/a27122bd87ef441c9a27e4ee75429f70?vhost=rtmp-only.srs-cluster.matrix.cluster",
    "rtmp://192.168.0.5:1935/live/cce21314310e4c1ea745eb6a668c6acb?vhost=rtmp-only.srs-cluster.matrix.cluster",
    "rtmp://192.168.0.5:1935/live/8b8509740161406e876d5bba49169c76?vhost=rtmp-only.srs-cluster.matrix.cluster",
    "rtmp://192.168.0.5:1935/live/4a4587d597cb4cc89979a94b27f567ef?vhost=rtmp-only.srs-cluster.matrix.cluster",
    "rtmp://192.168.0.5:1935/live/d25f9f036fea47fa8cdec72e4c9a3e59?vhost=rtmp-only.srs-cluster.matrix.cluster",
    "rtmp://192.168.0.5:1935/live/102557abcf4a4446b0ba5b01a0640c88?vhost=rtmp-only.srs-cluster.matrix.cluster",
    "rtmp://192.168.0.5:1935/live/c7898d76df2140e4ad2464f6add678d6?vhost=rtmp-only.srs-cluster.matrix.cluster",
    "rtmp://192.168.0.5:1935/live/fc23ce08f0214744b9ac063ebb636f06?vhost=rtmp-only.srs-cluster.matrix.cluster",
    "rtmp://192.168.0.5:1935/live/47def1e7d64446888244bccfc94663ec?vhost=rtmp-only.srs-cluster.matrix.cluster",
    "rtmp://192.168.0.5:1935/live/e192f23655ad4c0b8ade001cdcdf2eac?vhost=rtmp-only.srs-cluster.matrix.cluster",
    "rtmp://192.168.0.5:1935/live/b9060b37af5d4d20b811379d81d2c0d9?vhost=rtmp-only.srs-cluster.matrix.cluster",
    "rtmp://192.168.0.5:1935/live/efe107194404442696e880b787f68cc4?vhost=rtmp-only.srs-cluster.matrix.cluster",
    "rtmp://192.168.0.5:1935/live/290837b57374458e8b384e7f6fc07c52?vhost=rtmp-only.srs-cluster.matrix.cluster",
    "rtmp://192.168.0.5:1935/live/89bca3ffccbc4f339639c6a3ac22ec0d?vhost=rtmp-only.srs-cluster.matrix.cluster",
    "rtmp://192.168.0.5:1935/live/c37e817caaad487aaebcd8e487cc09a8?vhost=rtmp-only.srs-cluster.matrix.cluster",
    "rtmp://192.168.0.5:1935/live/9511672537ab407bb037862af5ca2ad6?vhost=rtmp-only.srs-cluster.matrix.cluster",
    "rtmp://192.168.0.5:1935/live/9ded2568e7644dc5a720e31dbb2c728d?vhost=rtmp-only.srs-cluster.matrix.cluster",
    "rtmp://192.168.0.5:1935/live/ac28786ff1f84d0586d5bd9cb8ba8fb4?vhost=rtmp-only.srs-cluster.matrix.cluster",
    "rtmp://192.168.0.5:1935/live/4478804afdca472f88d5dd9c88a1a17c?vhost=rtmp-only.srs-cluster.matrix.cluster",
    "rtmp://192.168.0.5:1935/live/1fe7b401623041adb47f27216c98631c?vhost=rtmp-only.srs-cluster.matrix.cluster",
]

# 随机选择一个源流
source_stream = random.choice(source_streams)

# 定义推流的线程函数
def push_stream_to_rtmp(rtmp_address,max_retries = 10):
    retry_count = 0
    while retry_count < max_retries:
        cmd = f"ffmpeg -re -i {source_stream} -c:v copy -f flv {rtmp_address}"

        # 调用函数执行命令
        bret, info = run_ffmpeg_cmd(cmd, g_logger)

        if bret:
            g_logger.info(f"Successfully pushed to {rtmp_address}.")
            print(f"Successfully pushed to {rtmp_address}.")
            return
        else:
            retry_count += 1
            g_logger.warning(f"Failed to push to {rtmp_address}: {info}. Retrying {retry_count}/{max_retries}")
            print(f"Failed to push to {rtmp_address}: {info}. Retrying {retry_count}/{max_retries}")

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
