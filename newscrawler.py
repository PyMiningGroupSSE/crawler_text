from crawler.master import Master
from crawler.slave import Slave
import time
import sys
import getopt
import json

__file_config_master__ = "conf-master.json"
__file_config_slave__ = "conf-slave.json"


def main(argv):
    run_type = parse_args(argv)
    if run_type == "master":
        configs = load_configs(__file_config_master__)                              # 获取Master节点的配置文件
        conf_server = configs["server"]
        conf_entries = configs["entries"]
        master = Master(conf_server["address"], conf_server["port"])              # 创建Master节点，设置监听的地址和端口
        master.analyze_urls(conf_entries)                                           # 从api获取文章url
        master.dispatch()                                                           # 监听网络，向Slave节点发送url
    elif run_type == "slave":
        configs = load_configs(__file_config_slave__)                               # 获取Slave节点的配置文件
        conf_host = configs["host"]
        conf_db = configs["database"]
        conf_xpaths = configs["xpaths"]
        slave = Slave(int(time.time()), conf_host["address"], conf_host["port"])  # 创建Slave节点，设置Master的地址和端口
        slave.set_parse_args(conf_xpaths)                                           # 设置Xpath配置
        slave.run(conf_db["address"], conf_db["port"])                            # 运行Slave节点主程序


# 加载配置文件 #
def load_configs(config_file):
    with open(config_file, 'r') as file_conf:
        configs = json.load(file_conf)
    return configs


# 根据参数判断要运行的时master还是slave #
def parse_args(argv):
    opts, args = getopt.getopt(argv[1:], "", ["master", "slave"])
    run_type = "slave"
    for opt_key, opt_value in opts:
        if opt_key == "--master":
            run_type = "master"
            continue
        if opt_key == "--slave":
            run_type = "slave"
            continue
    return run_type


if __name__ == '__main__':
    main(sys.argv)
