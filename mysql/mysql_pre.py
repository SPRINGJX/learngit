# -*- coding: UTF-8 -*-

import codecs
from pymysql import cursors
from pymysql_comm import UsingMysql

import csv

def select_one(cursor):
    cursor.execute("select * from projects")
    data= cursor.fetchone()
    print("--单条记录：{0}".format(data))
# 新增单条记录
def create_one():

    with UsingMysql(log_time=True) as um:
        sql ="insert into projects(project_id,project_name,discipline_tertiary,project_keys) values(%s,%s,%s,%s)"
        params =('11571017',"p进霍奇理论及其在数论中的应用","p进霍奇理论；p进Langlands纲领；p进模形式","A0103")
        um.cursor.execute(sql,params)
        select_one(um.cursor)
# 新增多条记录
def get_count(cursor):
    cursor.execute("select count(id) as total from Product")
    data = cursor.fetchone()
    print("--当前数量：%d"%data['total'])
# 删除记录
def delete_all(cursor):
    cursor.execute("delete from Product")
# 插入 1000 条记录
def create_many():
    with UsingMysql(log_time=True) as um:
         # 清空之前的记录
         delete_all(um.cursor)
         for i in range(0,1000):
             sql = "insert into Product(name,remark) values(%s,%s)"
             params = ('韦琴%d' % i,'韦琴126%d' % i)
             um.cursor.execute(sql,params)
        
        # 查看结果
         get_count(um.cursor)

def select_one(cursor):
    sql = 'select * from projects'
    cursor.execute(sql)
    data = cursor.fetchone()
    print('--- 已找到名字为%s的项目. ' % data['project_name'])
    return data['name']

def select_one_by_name(cursor, name):
    sql = 'select * from projects where name = %s'
    params = name
    cursor.execute(sql, params)
    data = cursor.fetchone()
    if data:
        print('--- 已找到名字为%s的项目. ' % data['project_name'])
    else:
        print('--- 名字为%s的项目已经没有了' % name)

# 删除单条记录
def check_delete_one():

    with UsingMysql(log_time=True) as um:

        # 查找一条记录
        name = select_one(um.cursor)

        # 删除之
        delete_one(um.cursor, name)

        # 查看还在不在?
        select_one_by_name(um.cursor, name)

# 修改记录
def select_one(cursor):
    sql = 'select * from Product'
    cursor.execute(sql)
    return cursor.fetchone()

def select_one_by_name(cursor, name):
    sql = 'select * from projects where name = %s'
    params = name
    cursor.execute(sql, params)
    data = cursor.fetchone()
    if data:
        print('--- 已找到名字为%s的商品. ' % data['name'])
    else:
        print('--- 名字为%s的商品已经没有了' % name)

# 修改记录
def update_by_pk(cursor, name, pk):
    sql = "update Product set name = '%s' where id = %d" % (name, pk)

    cursor.execute(sql)

def check_update():

    with UsingMysql(log_time=True) as um:

        # 查找一条记录
        data = select_one(um.cursor)
        pk = data['id']
        print('--- 商品{0}: '.format(data))

        # 修改名字
        new_name = '单肩包'
        update_by_pk(um.cursor, new_name, pk)

        # 查看
        select_one_by_name(um.cursor, new_name)
# 查找记录
def fetch_list_by_filter(cursor, pk):
    sql = 'select * from projects where id > %d' % pk
    cursor.execute(sql)
    data_list = cursor.fetchall()
    print('-- 总数: %d' % len(data_list))
    return data_list

# 查找
def fetch_list():

    with UsingMysql(log_time=True) as um:

        # 查找id 大于800的记录
        data_list = fetch_list_by_filter(um.cursor, 37770)

        # 查找id 大于 10000 的记录
        # data_list = fetch_list_by_filter(um.cursor, 10000)

# 分页查询 分页查询主要是用了mysql 的limit 特性, 和pymysql 没太大关系,
def fetch_page_data(cursor, pk, page_size, skip):
    sql = 'select * from Product where id > %d limit %d,%d' % (pk, skip, page_size)
    cursor.execute(sql)
    data_list = cursor.fetchall()
    print('-- 总数: %d' % len(data_list))
    print('-- 数据: {0}'.format(data_list))
    return data_list

# 查找
def check_page():

    with UsingMysql(log_time=True) as um:

        page_size = 10
        pk = 500

        for page_no in range(1, 6):

            print('====== 第%d页数据' % page_no)
            skip = (page_no - 1) * page_size

            fetch_page_data(um.cursor, pk, page_size, skip)

# 查看数据库当前数量
def check_it():
    with UsingMysql(log_time=True) as um:
        um.cursor.execute("select count(id) as total from projects")
        # um.cursor.execute("select count(id) as total from projects")
        data = um.cursor.fetchone()
        print("--当前数量：%d"%data['total'])

'''
        DROP TABLE IF EXISTS `Projects`;
    /*!40101 SET @saved_cs_client     = @@character_set_client */;
    /*!40101 SET character_set_client = utf8 */;
    CREATE TABLE `Projects` (
      `project_id` varchar(40) NOT NULL,
      `project_name` varchar(100) NOT NULL,    /* 项目名称 */
			`discipline_tertiary` varchar(40) NOT NULL,    /* 所属领域*/
			`project_keys` varchar(100) NOT NULL,    /* 项目关键字 */
      PRIMARY KEY (`project_id`)
    ) ENGINE=InnoDB AUTO_INCREMENT=1000 DEFAULT CHARSET=utf8;

    
  '''

import os 
import sys
import pandas as pd

current_path = os.path.dirname(__file__)
# 读取csv数据->存入mysql
class csv2sql():
    # 返回路径下的文件名称
    def return_list_filname(self,filepath):
        # return list
        fileList = []
        files = os.listdir(filepath)
        # return files
        for f in files:
            if(os.path.isfile(filepath + '/' + f)):
                # 添加文件
                fileList.append(f)
        # return fileList
        for file_name in files:
            with open(filepath + '/data/fund' + file_name,'r') as file:
                f_name = file_name.replace(".csv",'')
        return f_name

    # 返回当前的规范化路径
    def return_current_path(self):
        p_origin = os.getcwd()
        z_origin = r"/".join(p_origin.split('\\')) # e:/xxx/xxx
        return z_origin

    # csv文件目录
    def csvlist(self,dir):
        #csv文件目录
        file_list = os.listdir(dir)
        for i in range(len(file_list)):
            file_path = os.path.join(dir, file_list[i])
            if os.path.isfile(file_path):
                df = pd.read_csv(file_path, encoding='utf-8',error_bad_lines=False)
        return df

    def read2csv(self,path):
        df= pd.read_csv(path,'r',encoding='utf-8')
        return df
    
    # 读取csv指定列或者行
    def read_csv_cr(self,i,filepath):  # 读取i列数据
        import csv
        with open(filepath,'r',encoding='utf-8') as f:
        # 读取csv每一行数据
        # reader = f.readline()
        # 读取csv文件的某一列或某几列
            readr=csv.reader(f)
            column_data = [row[i] for row in readr]
            return column_data
    # 按照列属性进行读取
    def reader_column(self,filepath):
        datas= pd.read_csv(filepath,'r',encoding='utf-8',usecols=['项目名称','批准号','项目类别','依托单位','项目负责人','资助经费','批准年度','关键词','研究成果','资助类别','申请代码'])
        return datas


    # 数据库批量插入多条记录
    def multi_insert(self,name,id,keys,disci):
        with UsingMysql(log_time=True) as um:
            sql = 'insert into projects(project_name,project_id,project_keys,discipline_territory) values (%s,%s,%s,%s)'
            for i in range(len(name)):
                params = (name[i],id[i],keys[i],disci[i])
                um.cursor.execute(sql,params)
        return 'success!'

    # 删除记录
    def delete_one(self,cursor, name):
        sql = 'delete from projects where project_name = %s'
        params = name
        cursor.execute(sql, params)
        print('--- 已删除名字为%s的项目. ' % name)
    # 读取csv返回项目文本信息    
    def return_project_text(self,path):
        i_column =pre.read_csv_cr(0,path)
        column_name= i_column[0]
        projects_name = i_column[1:]
        projects_id = pre.read_csv_cr(1,path)[1:]
        projects_keys =pre.read_csv_cr(7,path)[1:]
        discipline_territory = pre.read_csv_cr(10,path)[1:]
        projects_text =[]
        for i in range(len(projects_name)):
            text=str(projects_name[i])+','+str(projects_keys[i])
            projects_text.append(text)
        return projects_text
        # print('项目文本信息： {}'.format(projects_text[1]))
    def w2c(self,discipline,projects_text,filepath):
        # 写入csv
        with open(filepath,'w+',encoding='utf-8') as f:
            f.write('label')
            f.write(',')
            f.write('content')
            f.write('\n')
            for j in range(len(projects_text)):
                f.write(discipline)
                f.write(',')
                f.write(projects_text[j])
                f.write('\n')

# 返回目录下所有csv文件的项目信息和所属学科，再存入csv中

def return_project_text_discipline(filepath):
        
        fileList = []
        first_discipline_list = []
        files = os.listdir(filepath)
        # 目录下的文件
        print(files)
        for f in files:
            if(os.path.isfile(filepath + '/' + f)):
                # 添加文件
                fileList.append(f)
        # print(fileList)  fileList = files
        for f_name in files:
            # 文件名
            file_name = f_name.replace(".csv",'')
            # 取文件名前三个字符存放到列表
            str_third = file_name.encode('utf-8').decode('utf-8')
            first_discipline =str_third[0:3]
            first_discipline_list.append(first_discipline)
            # print(first_discipline)
            
            # 读取csv指定列存入数据库,拼接得到项目文本信息projects_text
            with open(filepath +'/'+ f_name,'r',encoding='utf-8') as f:
                reader = csv.reader(f)
                column_data_0 = [row[0] for row in reader]
                column_data_1 = [row[1] for row in reader]
                column_data_7 = [row[7] for row in reader]
                column_data_10 = [row[10] for row in reader]

                projects_name = column_data_0[1:]
                projects_id = column_data_1[1:]
                projects_keys = column_data_7[1:]
                discipline_territory = column_data_10[1:]
                # 写入数据库
                # with UsingMysql(log_time=True) as um:
                #     sql = 'insert into projects(project_name,project_id,project_keys,discipline_territory) values (%s,%s,%s,%s)'
                #     for i in range(len(projects_name)): # IndexError: list index out of range
                #         params = (projects_name[i],projects_id[i],projects_id[i],discipline_territory[i])
                #         um.cursor.execute(sql,params)
                # pre.multi_insert(projects_name,projects_id,projects_keys,discipline_territory)
                #拼接项目文本信息projects_text
                projects_text = []
                for i in range(len(projects_name)): # IndexError: list index out of range
                    text = str(projects_name[i])+','+str(projects_keys[i])
                    projects_text.append(text)

                with open(filepath,'w+',encoding='utf-8') as f1:
                    # f1.write('label')
                    # f1.write(',')
                    # f1.write('content')
                    # f1.write('\n')
                    for j in range(len(projects_text)):
                        f1.write(first_discipline_list[j])
                        f1.write(',')
                        f1.write(projects_text[j])
                        f1.write('\n')
                
        return 'successfully!'

def return_project_d_list(filepath):
        
        fileList = []
        first_discipline_list = []
        files = os.listdir(filepath)
        # 目录下的文件
        print(files)
        for f in files:
            if(os.path.isfile(filepath + '/' + f)):
                # 添加文件
                fileList.append(f)
        # print(fileList)  fileList = files
        for f_name in files:
            # 文件名
            file_name = f_name.replace(".csv",'')
            # 取文件名前三个字符存放到列表
            str_third = file_name.encode('utf-8').decode('utf-8')
            first_discipline =str_third[0:3]
            first_discipline_list.append(first_discipline)
        return first_discipline_list

# csv 合并
def csv_concat(dir,outputfile):
    file_list = []
    for file_name in os.listdir(dir):
        # print(file_name)
        filename= dir+'/'+''.join(file_name)
        # print(filename)
        data_tmp=pd.read_csv(filename,encoding='utf-8')
        file_list.append(data_tmp)
    all_file = pd.concat(file_list,sort=True)
    all_file.to_csv(outputfile,header=True,index=False,encoding='utf-8')

def return_p_text_discipline(filepath1,filepath2):
 # 读取csv指定列存入数据库,拼接得到项目文本信息projects_text
    with open(filepath1,'r',encoding='utf-8') as f:
        reader = csv.reader(f)
        column_data_1 = [row[1] for row in reader]
        column_data_2 = [row[2] for row in reader]
        column_data_4 = [row[4] for row in reader]
        column_data_8 = [row[8] for row in reader]
        
        projects_name = column_data_8[1:]
        projects_id = column_data_2[1:]
        projects_keys = column_data_1[1:]
        discipline_territory = column_data_4[1:]
        print(discipline_territory)
        # 写入数据库
        # with UsingMysql(log_time=True) as um:
        #     sql = 'insert into projects(project_name,project_id,project_keys,discipline_territory) values (%s,%s,%s,%s)'
        #     for i in range(len(projects_name)): # IndexError: list index out of range
        #         params = (projects_name[i],projects_id[i],projects_id[i],discipline_territory[i])
        #         um.cursor.execute(sql,params)
        # pre.multi_insert(projects_name,projects_id,projects_keys,discipline_territory)
        #拼接项目文本信息projects_text
        projects_text = []
        for i in range(len(projects_name)): # IndexError: list index out of range
            text = str(projects_name[i])+','+str(projects_keys[i])
            projects_text.append(text)
        print(projects_text)
        # 获取第一学科
        territory_first_list=[]
        for strl in discipline_territory:
            third=strl.encode('utf-8').decode('utf-8')
            territory_first_list.append(third)
        print(territory_first_list)
    # 项目文本信息projects_text和discipline写入csv
    with open(filepath2,'w+',encoding='utf-8') as f1:
        f1.write('label')
        f1.write(',')
        f1.write('content')
        f1.write('\n')
        for j in range(len(projects_text)):
            f1.write(territory_first_list[j])
            f1.write(',')
            f1.write(projects_text[j])
            f1.write('\n')
            
# pandas 读取csv to dict
def csv2dict(filepath):
    df=pd.read_csv(filepath)
    data_list =df.to_dict('list')
    return data_list

def return_p_text_discipline(filepath1,filepath2):
 # 读取csv指定列存入数据库,拼接得到项目文本信息projects_text
    df=pd.read_csv(filepath1)
    data_list =df.to_dict('list')
        
    projects_name = data_list['项目名称']
    projects_id = data_list['批准号']
    projects_keys = data_list['关键词']
    discipline_territory = data_list['申请代码']
    print(discipline_territory)
    # 写入数据库
    # with UsingMysql(log_time=True) as um:
    #     sql = 'insert into projects(project_name,project_id,project_keys,discipline_territory) values (%s,%s,%s,%s)'
    #     for i in range(len(projects_name)): # IndexError: list index out of range
    #         params = (projects_name[i],projects_id[i],projects_id[i],discipline_territory[i])
    #         um.cursor.execute(sql,params)
    # pre.multi_insert(projects_name,projects_id,projects_keys,discipline_territory)
    #拼接项目文本信息projects_text
    projects_text = []
    for i in range(len(projects_name)): # IndexError: list index out of range
        text = str(projects_name[i])+','+str(projects_keys[i])
        projects_text.append(text)
    # print(projects_text)
    # 获取第一学科
    territory_first_list=[]
    for strl in discipline_territory:
        third=strl.encode('utf-8').decode('utf-8')
        territory_first_list.append(third[0:3])
    print(territory_first_list)
    # 项目文本信息projects_text和discipline写入csv
    with open(filepath2,'w+',encoding='utf-8') as f1:
        f1.write('label')
        f1.write(',')
        f1.write('content')
        f1.write('\n')
        for j in range(len(projects_text)):
            f1.write(territory_first_list[j])
            f1.write(',')
            f1.write(projects_text[j])
            f1.write('\n')
from sklearn.model_selection import train_test_split
def Train_Test_Split(path1,path2,path3):
    news_text = [x.strip().split(',') for x in codecs.open(path1,encoding='utf-8')]
    x_train,x_test = train_test_split(news_text[:],test_size=0.2, random_state=0)

    with open(path2,'w+',encoding='utf-8',newline='') as f:
        f.write('label')
        f.write(',')
        f.write('content')
        f.write('\n')
        writer = csv.writer(f)
        for data in x_train:
            writer.writerow(data)

    with open(path3,'w+',encoding='utf-8',newline='') as f:
        f.write('label')
        f.write(',')
        f.write('content')
        f.write('\n')
        writer = csv.writer(f)
        for data in x_test:
            writer.writerow(data)       

if __name__ == '__main__':
    # check_it()
    # create_one()
    # create_many()
    # check_delete_one()
    # check_page()
    # print(check_it())
    # filepath = os.getcwd()
    pre = csv2sql()
    # file_list = pre.return_list_filname(filepath)
    # print(file_list)
    # pathz = pre.return_current_path()
    # print(pathz)
    # print(pathz.join('/data/fund/A01_218_2019.csv'))
    # data_df = pd.read_csv(r'E:\SPRING\Desktop\MSC\g_kg\project_cls\mult-label-cls\preprocess\data\fund\A01_218_2019.csv','r',encoding='utf-8',error_bad_lines=False )
    
    # print(data_df.head())
    # dir=r'E:\SPRING\Desktop\MSC\g_kg\project_cls\mult-label-cls\preprocess\data\fund'
    # print(pre.csvlist(dir))

    # print(current_path)
    # A01
    # path = r'E:\SPRING\Desktop\MSC\g_kg\project_cls\mult-label-cls\preprocess\data\fund\A02_218_2019.csv'
    # data_text = pre.return_project_text(path)
    # fpath = r'E:\SPRING\Desktop\MSC\g_kg\project_cls\mult-label-cls\preprocess\data\pre_data\data.csv'
    # w2c('A01',fpath)


    # print('项目名称： {}'.format(projects_name[1]))
    # print('项目id： {}'.format(projects_id))
    # print('项目关键词： {}'.format(projects_keys[1]))
    # print('项目所属学科： {}'.format(discipline_territory))
    # 执行插入数据库
    # pre.multi_insert(projects_name,projects_id,projects_keys,discipline_territory)
    # print("列名称： {0} \n项目: {1}".format(column_name,projects))
    
    # 删除
    # with UsingMysql(log_time=True) as um:
    #     pre.delete_one(um.cursor,"双曲守恒律方程组的二维黎曼问题三次")
    
    
    fpa1 = 'E:/SPRING/Desktop/MSC/g_kg/project_cls/mult-label-cls/preprocess/data/pre_data/data.csv'
    fpa2 = 'E:/SPRING/Desktop/MSC/g_kg/project_cls/mult-label-cls/preprocess/data/pre_data/train.csv'
    fpa3 = 'E:/SPRING/Desktop/MSC/g_kg/project_cls/mult-label-cls/preprocess/data/pre_data/test.csv'

    # return_p_text_discipline(fpath1,fpath2)
    # Train_Test_Split(fpa1,fpa2,fpa3)
    # fetch_list()
   
