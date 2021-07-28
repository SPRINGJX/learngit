from pymysql_comm import UsingMysql

def select_one(cursor):
    cursor.execute('select * from guizhou_proj')
    data = cursor.fetchone()
    print('----单条记录： {0}'.format(data))
def chect_it():
    with UsingMysql(log_label=True) as um:
        um.cursor.execute('select count(id) as total form projects')
        data = um.cursor.fetchone()
        print('---当前数量： %d',data['total'])

if __name__ == '__main__':
    chect_it()