from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from urllib import request
from lxml import etree
from crawler.tasklist import TaskList
import json
import math
import socket
import time


__MAX_SLAVES__ = 50
__ITEMS_PER_PAGE__ = 50


class Master:
    __task_list__ = None
    __socket__ = None

    # 类初始化
    def __init__(self, addr, port):
        self.__task_list__ = TaskList(30)
        self.__socket__ = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.__socket__.bind((addr, int(port)))
        self.__socket__.listen(__MAX_SLAVES__)

    # 获取文章URL列表 #
    def analyze_urls(self, entries):
        for entry in entries:
            if "api_json" in entry:
                self.__task_list__.put_tasks(self.__parse_api_json__(entry, 150))
            elif "list_url" in entry:
                self.__task_list__.put_tasks(self.__parse_dynamic_page__(entry, 150))

    # 监听网络端口以分发任务 #
    def dispatch(self):
        print("Waiting for slaves...")
        while True:
            # 若任务列表为空，则程序退出
            if self.__task_list__.is_empty():
                print("No task in list")
            # 接受来自Slave节点的连接
            conn, addr = self.__socket__.accept()
            try:
                conn.settimeout(10)
                # 接收请求数据
                req = json.loads(conn.recv(1024).decode("utf-8"))
                # 若Slave的请求命令为get，则向其发送新任务并将任务加入pending队列中
                if req["cmd"] == "get":
                    res = dict(
                        status=dict(
                            code=0,
                            msg="success"
                        ),
                        data=dict(
                            news_url=self.__dispatch_task__()    # 调用dispatch_task()函数时会自动将任务加入pending队列
                        )
                    )
                    conn.send(json.dumps(res).encode("utf-8"))
                    print("Dispatch {0} to slave {1}".format(res["data"], req["id"]))
                # 若Slave的请求命令为done，则将pending队列中相应的任务移除
                elif req["cmd"] == "done":
                    self.__done_task__(req["data"]["news_url"])
                    res = dict(
                        status=dict(
                            code=0,
                            msg="success"
                        ),
                        data=""
                    )
                    conn.send(json.dumps(res).encode("utf-8"))
                    print("Slave {0} done fetching '{1}'".format(req["id"], req["data"]["news_url"]))
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

    @staticmethod
    def __parse_dynamic_page__(entry, count):
        chrome_options = webdriver.ChromeOptions()  # 获取ChromeWebdriver配置文件
        prefs = {"profile.managed_default_content_settings.images": 2}  # 设置不加载图片以加快速度
        chrome_options.add_experimental_option("prefs", prefs)
        chrome_options.add_argument("--headless")  # 不使用GUI界面
        chrome_options.add_argument("--disable-gpu")  # 禁用GPU渲染加速
        driver = webdriver.Chrome(chrome_options=chrome_options)  # 创建ChromeWebdriver
        driver.set_page_load_timeout(10)  # 设置连接超时时间为15s
        driver.get(entry["list_url"])

        url_cnt = 0
        url_list = list()
        while url_cnt < count:
            if url_cnt != 0:
                time.sleep(1)
                driver.find_element_by_xpath(entry["xpaths"]["next"]).click()
            selector = etree.HTML(driver.page_source)
            urls = selector.xpath(entry["xpaths"]["url"])
            url_cnt += len(urls)
            url_list.extend(urls)
        driver.close()
        return url_list

    # 分发任务 #
    def __dispatch_task__(self):
        news_url = self.__task_list__.get_task()
        if news_url is None:
            return -1
        return news_url

    # 完成任务 #
    def __done_task__(self, news_url):
        self.__task_list__.done_task(news_url)    # 将任务从pending列表中移除
