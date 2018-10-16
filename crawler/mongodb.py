import pymongo

__DATABASE_NAME__ = "NewsDB"


class MongoDb:
    __mongo_client__ = None
    __collection__ = None

    # 类构造函数 #
    def __init__(self, addr, port, user=None, pwd=None):
        # 创建mongodb连接
        if user is not None and pwd is not None:
            self.__mongo_client__ = pymongo.MongoClient("mongodb://{0}:{1}@{2}:{3}/".format(user, pwd, addr, port))
        else:
            self.__mongo_client__ = pymongo.MongoClient("mongodb://{0}:{1}/".format(addr, port))

    # 类析构函数 #
    def __del__(self):
        self.__mongo_client__.close()

    # 设置使用的集合名称
    def set_collection(self, collection):
        self.__collection__ = self.__mongo_client__[__DATABASE_NAME__][collection]

    # 将解析到的新闻存入数据库
    def put_news(self, data_news):
        # 首先判断数据库中是否已存在该条新闻，不存在才添加
        if self.__collection__.find_one({"url": data_news["url"]}) is None:
            self.__collection__.insert_one(data_news)
