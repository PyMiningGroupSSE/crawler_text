from selenium import webdriver
from urllib import request
import json
import math
import socket
import time


__MAX_SLAVES__ = 50
__ITEMS_PER_PAGE__ = 50


class Master:
    __tasks_waiting__ = None
    __tasks_pending__ = None
    __socket__ = None

    # 类初始化
    def __init__(self, addr, port):
        self.__tasks_waiting__ = list()
        self.__tasks_pending__ = list()
        self.__tasks_period__ = list()
        self.__socket__ = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.__socket__.bind((addr, int(port)))
        self.__socket__.listen(__MAX_SLAVES__)

    # 获取文章URL列表 #
    def analyze_urls(self, entries):
        for entry in entries:
            if "api_json" in entry:
                self.__tasks_waiting__.extend(self.__parse_api_json__(entry, 150))
            elif "url" in entry:
                # TODO: use selenium to parse simple html
                pass

    # 监听网络端口以分发任务 #
    def dispatch(self):
        print("Waiting for slaves...")
        while True:
            # 若任务列表为空，则程序退出
            if self.__is_tasks_list_empty():
                print("All tasks done!")
                break
            # 接受来自Slave节点的连接
            conn, addr = self.__socket__.accept()
            try:
                conn.settimeout(10)
                # 接收请求数据
                msg = json.loads(conn.recv(1024).decode("utf-8"))
                # 若Slave的请求命令为get，则向其发送新任务并将任务加入pending队列中
                if msg["cmd"] == "get":
                    res = dict(
                        status="ok",
                        data=dict(
                            news_url=self.__dispatch_task__()    # 调用dispatch_task()函数时会自动将任务加入pending队列
                        )
                    )
                    conn.send(json.dumps(res).encode("utf-8"))
                    print("Dispatch {0} to slave {1}".format(res["data"], msg["id"]))
                    print("{0} urls in pool".format(len(self.__tasks_waiting__)))
                # 若Slave的请求命令为done，则将pending队列中相应的任务移除
                elif msg["cmd"] == "done":
                    self.__done_task__(msg["data"])
                    res = dict(
                        status="ok"
                    )
                    conn.send(json.dumps(res).encode("utf-8"))
                    print("Slave {0} done fetching '{1}'".format(msg["id"], msg["data"]))
            except socket.timeout:
                print("Connection timeout")
            conn.close()

    # 从API中获取文章URL列表 #
    @staticmethod
    def __parse_api_json__(entry, count):
        max_page = math.ceil(count / __ITEMS_PER_PAGE__)
        url_pattern = entry["api_json"]
        news_urls = list()
        for i in range(1, max_page + 1):
            url = url_pattern.format(__ITEMS_PER_PAGE__, i)
            res = request.urlopen(url, timeout=3)
            data = json.loads(res.read(), encoding='utf8')
            for news_item in data["result"]["data"]:
                news_urls.append(news_item["url"])
            time.sleep(2)
        news_urls = list(set(news_urls))
        return news_urls

    # 分发任务 #
    def __dispatch_task__(self):
        if len(self.__tasks_waiting__) == 0:
            return -1
        news_url = self.__tasks_waiting__.pop()
        self.__tasks_pending__.append(news_url)
        return news_url

    # 完成任务 #
    def __done_task__(self, news_url):
        self.__tasks_pending__.remove(news_url)    # 将任务从pending列表中移除

    # 检查任务列表是否已清空 #
    def __is_tasks_list_empty(self):
        if len(self.__tasks_waiting__) + len(self.__tasks_pending__) == 0:
            return True
        return False

