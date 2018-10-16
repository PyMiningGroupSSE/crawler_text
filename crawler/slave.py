from crawler.mongodb import MongoDb
from crawler.pageparser import PageParser
import socket
import json
import time


class Slave:

    __slave_id__ = None           # 当前Slave节点的ID
    __server_addr__ = None        # Master节点的地址
    __server_port__ = None        # Master节点监听的端口
    __parse_args__ = None         # 初始化时读入的xpath解析规则

    # 类构造函数 #
    def __init__(self, slave_id, server_addr, server_port):
        self.__slave_id__ = slave_id
        self.__server_addr__ = server_addr
        self.__server_port__ = int(server_port)

    # 设置xpath解析规则，即要传入PageParser的xpaths数组 #
    def set_parse_args(self, args):
        self.__parse_args__ = args

    # Slave节点运行主程序部分 #
    def run(self, db_addr, db_port):
        parser = PageParser(self.__parse_args__)
        mongodb = MongoDb(db_addr, db_port)
        mongodb.set_collection("Finance")
        # 循环向Master节点发送请求，获取要爬取的url
        while True:
            try:
                url = self.__get_task__()              # 获取需要爬取的页面url
                if url == -1:                          # 如果url为-1，则说明没有页面需要爬取了
                    print("No task to do, will retry after 5 seconds")
                    time.sleep(5)
                    continue
                print("Fetching data from", url)
                data = parser.parse(url)               # 解析页面数据
                self.__format_data__(data)             # 格式化数据
                mongodb.put_news(data)                 # 将数据存入数据库
                self.__done_task__(url)                # 完成爬取数据，通知Master节点
            except socket.error:
                print("Master is offline, will retry after 5 seconds")
                time.sleep(5)

    # 将解析到的数据转为正确的格式 #
    @staticmethod
    def __format_data__(data):
        data["title"] = data["title"][0]
        data["time"] = data["time"][0]
        data["tags"] = list(filter(None, data["tags"][0].split(",")))
        data["author"] = data["author"][0]
        data["url"] = data["url"][0]
        content = ""
        for line in data["content"]:
            if line.isspace():
                continue
            content += (line.lstrip() + "\n")
        data["content"] = content

    # 获取任务函数 #
    def __get_task__(self):
        # 向Master节点发送的结构中包含一个'get'标志和自己的id
        req = dict(
            id=self.__slave_id__,
            cmd="get"
        )
        res = self.__send_msg__(req)
        if res["status"]["code"] == 0:
            return res["data"]["news_url"]
        return -1

    # 完成任务函数 #
    def __done_task__(self, news_url):
        # 这里会向Master节点发送一个完成任务的请求
        req = dict(
            id=self.__slave_id__,
            cmd="done",
            data=dict(
                news_url=news_url
            )
        )
        self.__send_msg__(req)

    # 发送和获取响应函数 #
    def __send_msg__(self, dict_msg):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((self.__server_addr__, self.__server_port__))
        sock.send(json.dumps(dict_msg).encode("utf-8"))
        res = json.loads(sock.recv(1024))
        sock.close()
        return res
