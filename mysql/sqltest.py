# -*- coding: UTF-8 -*-
import pymysql
from pymysql import cursors
from timeit import default_timer

host = 'localhost'
port = 3306
db = 'my_test'
user = 'root'
password = 'weiqing'

# ---- 用pymysql 操作数据库

def get_connection():
    conn = pymysql.connect(host=host,port=port,db=db,user=user,password=password)
    return conn

# def check_it():
#     conn = get_connection()
#     # 使用 cursor() 方法创建一个 dict 格式的游标对象 cursor
#     cursor = conn.cursor(pymysql.cursors.DictCursor)
#     # 使用 execute()  方法执行 SQL 查询
#     cursor.execute("select count(id) as total from Product")
#     # 使用 fetchone() 方法获取单条数据.
#     data = cursor.fetchone()

#     print("-- 当前数量： %d" % data['total'])
#     # 关闭数据库连接
#     cursor.close()
#     conn.close()

# if __name__ == '__main__':
#     check_it()

# 从以上代码可以看到, 如果每次都要打开连接, 关闭连接 .... 代码难看且容易出错. 最好的办法是用 python with 的方式来增加一个上下文管理器. 修改如下:---- 使用 with 的方式来优化代码
class UsingMysql(object):

    def __init__(self, commit=True, log_time=True, log_label='总用时'):
        """

        :param commit: 是否在最后提交事务(设置为False的时候方便单元测试)
        :param log_time:  是否打印程序运行总时间
        :param log_label:  自定义log的文字
        """
        self._log_time = log_time
        self._commit = commit
        self._log_label = log_label

    def __enter__(self):

        # 如果需要记录时间
        if self._log_time is True:
            self._start = default_timer()

        # 在进入的时候自动获取连接和cursor
        conn = get_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        conn.autocommit = False

        self._conn = conn
        self._cursor = cursor
        return self

    def __exit__(self, *exc_info):
        # 提交事务
        if self._commit:
            self._conn.commit()
        # 在退出的时候自动关闭连接和cursor
        self._cursor.close()
        self._conn.close()

        if self._log_time is True:
            diff = default_timer() - self._start
            print('-- %s: %.6f 秒' % (self._log_label, diff))

    @property
    def cursor(self):
        return self._cursor

def check_it():

    with UsingMysql(log_time=True) as um:
        um.cursor.execute("select count(id) as total from Product")
        data = um.cursor.fetchone()
        print("-- 当前数量: %d " % data['total'])

if __name__ == '__main__':
    check_it()