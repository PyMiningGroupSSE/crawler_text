from selenium import webdriver, common
from lxml import etree


class PageParser:
    __webdriver__ = None
    __items__ = None

    # 类构造函数 #
    def __init__(self, items):      # 这里items是conf-slave中的xpaths，即需要查找的元素的xpath
        self.__items__ = items
        chrome_options = webdriver.ChromeOptions()                             # 获取ChromeWebdriver配置文件
        prefs = {"profile.managed_default_content_settings.images": 2}   # 设置不加载图片以加快速度
        chrome_options.add_experimental_option("prefs", prefs)
        chrome_options.add_argument("--headless")                             # 不使用GUI界面
        chrome_options.add_argument("--disable-gpu")                          # 禁用GPU渲染加速
        self.__webdriver__ = webdriver.Chrome(chrome_options=chrome_options)    # 创建ChromeWebdriver
        self.__webdriver__.set_page_load_timeout(10)                            # 设置连接超时时间为15s

    # 类析构函数 #
    def __del__(self):
        self.__webdriver__.close()

    # 使用XPath解析页面元素 #
    def parse(self, url):
        ret = dict()                                            # 返回数据结构
        try_count = 0
        while try_count < 3:
            try:
                self.__webdriver__.get(url)                     # 使用Webdriver获取页面，3次重试
                break
            except common.exceptions.TimeoutException:
                try_count += 1
                if try_count == 3:
                    raise common.exceptions.TimeoutException
                print("Timeout, retry", url)
        selector = etree.HTML(self.__webdriver__.page_source)   # 用lxml.etree解析页面
        for item in self.__items__:                            # 根据规则解析出需要的元素内容
            ret[item["name"]] = selector.xpath(item["xpath"])
        return ret
