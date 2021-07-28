# -*- coding: UTF-8 -*-

from textwrap import indent
from pandas.core.algorithms import mode

import os
import csv

for i in range(2):
    a = ['sd1','2df']
    b = ['1df','2s']
    d=str(a[i])+str(b[i])
    # print(d)
c = '{0},{1}'.format('hell','weq')

# 将字符串转成Unicode形式进行切片
a="小明xiaoming"
a = a.encode("utf-8").decode('utf-8')
a = a[1]
# print(a) # 明

# 返回目录下所有csv文件的项目信息和所属学科，再存入csv中
# fpath = 'E:/SPRING/Desktop/MSC/g_kg/project_cls/mult-label-cls/preprocess/data/fund'

# def return_project_text_discipline(filepath):
        
#         fileList = []
#         first_discipline_list = []
#         files = os.listdir(filepath)
#         # 目录下的文件
#         print(files)
#         for f in files:
#             if(os.path.isfile(filepath + '/' + f)):
#                 # 添加文件
#                 fileList.append(f)
#         # print(fileList)  fileList = files
#         for f_name in files:
#             # 文件名
#             file_name = f_name.replace(".csv",'')
#             # 取文件名前三个字符存放到列表
#             str_third = file_name.encode('utf-8').decode('utf-8')
#             first_discipline =str_third[0:3]
#             first_discipline_list.append(first_discipline)
#             # print(first_discipline)
            
#             # 读取csv指定列存入数据库,拼接得到项目文本信息projects_text
#             with open(filepath +'/'+ f_name,'r',encoding='utf-8') as f:
#                 reader = csv.reader(f)
#                 column_data_0 = [row[0] for row in reader]
#                 column_data_1 = [row[1] for row in reader]
#                 column_data_7 = [row[7] for row in reader]
#                 column_data_10 = [row[10] for row in reader]

#                 projects_name = column_data_0[1:]
#                 projects_id = column_data_1[1:]
#                 projects_keys = column_data_7[1:]
#                 discipline_territory = column_data_10[1:]
#                 # 写入数据库
#                 with UsingMysql(log_time=True) as um:
#                     sql = 'insert into projects(project_name,project_id,project_keys,discipline_territory) values (%s,%s,%s,%s)'
#                     for i in range(len(projects_name)):
#                         params = (projects_name[i],projects_id[i],projects_id[i],discipline_territory[i])
#                         um.cursor.execute(sql,params)
#                 #拼接项目文本信息projects_text
#                 projects_text = []
#                 for i in range(len(projects_name)):
#                     text = str(projects_name[i])+','+str(projects_keys[i])
#                     projects_text.append(text)
                
#                 # 项目文本信息projects_text和discipline写入csv
#                 with open(filepath,'w+',encoding='utf-8') as f1:
#                     # f1.write('label')
#                     # f1.write(',')
#                     # f1.write('content')
#                     # f1.write('\n')
#                     for j in range(len(projects_text)):
#                         f1.write(first_discipline_list[j])
#                         f1.write(',')
#                         f1.write(projects_text[j])
#                         f1.write('\n')
#         return 'successfully!'

# 多个csv文件合并
import pandas as pd
fdir = 'E:/SPRING/Desktop/MSC/g_kg/project_cls/mult-label-cls/preprocess/data/fund'
outputfile = 'E:/SPRING/Desktop/MSC/g_kg/project_cls/mult-label-cls/preprocess/data/fund/outputfile.csv'

def csv2one(inputfile_dir,outputfile):
    for inputfile in os.listdir(inputfile_dir):
        pd.read_csv(inputfile,encoding='utf-8')
        pd.to_csv(outputfile,mode='a',indenx=False,header=False)

def csv_concat(dir):
    file_list = []
    for file_name in os.listdir(dir):
        # print(file_name)
        filename= dir+'/'+''.join(file_name)
        # print(filename)
        data_tmp=pd.read_csv(filename,encoding='utf-8')
        file_list.append(data_tmp)
    all_file = pd.concat(file_list,sort=True)
    all_file.to_csv(outputfile,header=True,index=False,encoding='utf-8')


filepath = r'E:\SPRING\Desktop\MSC\g_kg\project_cls\mult-label-cls\preprocess\data\one\outputfile.csv'

with open(filepath,'r',encoding='utf-8') as f:
    reader = csv.reader(f)
    column_data_1 = [row[1] for row in reader] 
    column_data_2 = [row[2] for row in reader]
    column_data_4 = [row[4] for row in reader]
    column_data_8 = [row[8] for row in reader]
    # print(column_data_1[1])
    # print(column_data_2)
    # print(column_data_4[1])
    # print(column_data_8[1])
# csv_concat(fdir)

df=pd.read_csv(filepath)
name=df.to_dict('list')
# print(name['项目名称'])
print(name['申请代码'][0])

from pymysql_comm import UsingMysql

# 分页查询

def fetch_page_data(cursor,pk,page_size,skip):
        sql = 'select * from projects where id > %d limit %d,%d' % (pk,skip,page_size)
        cursor.execute(sql)
        data_list = cursor.fetchall()
        print('--总条数：%d' % len(data_list))
        # print('--数据：{0}'.format(data_list))
        return data_list

def check_page():
    with UsingMysql(log_time=True) as um:
        page_size = 10
        pk = 500
        for page_no in range(1,2):
            print('====第%d页数据' % page_no)
            skip = (page_no-1)*page_size
            data_list=fetch_page_data(um.cursor,pk,page_size,skip=skip)
    return data_list

# print(check_page())
# data_list = check_page()
# for p_list in data_list:
#     print(p_list['project_name'])

from flask import jsonify
list1 =['sd','asdfa']

list2 = []
list2.append(list1[0])
list2.append('sdla')
list2.append(list1[1])
json_list = jsonify(list2)
print(json_list)