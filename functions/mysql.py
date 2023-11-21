import pandas as pd
import pymysql
import configparser
from sshtunnel import open_tunnel


defaultPath = 'conf/sourceDefault.ini'
defaultConfig = configparser.ConfigParser()
defaultConfig.read(defaultPath)
defaultMysqlParams = dict(defaultConfig.items('mysql'))
defaultMysqlParams['port'] = int(defaultMysqlParams['port'])
defaultSSHParams = dict(defaultConfig.items('ssh'))
defaultSSHParams['ssh_port'] = int(defaultSSHParams['ssh_port'])


def mysqlConnect(mysqlParams=None, SSH=False, SSHParams=None):
    if mysqlParams is None:
        mysqlParams = defaultMysqlParams
    if SSHParams is None and SSH is True:
        SSHParams = defaultSSHParams
    try:
        if SSH:
            SSHParams['remote_bind_address'] = \
                (mysqlParams['host'], mysqlParams['port'])
            server = open_tunnel(**SSHParams)
        conn = pymysql.connect(**mysqlParams)
        return conn
    except Exception as e:
        return str(e) + '\n' + e.__doc__


# SQLParams = {'host': 'localhost', 'port': 3306, 'user': 'root',
#              'password': 'QingZhong@613', 'database': 'uuu'}
# conn = mysqlConnect(SQLParams)
# cursor = conn.cursor()


def timeFieldsAddSQL(fileDir, DTPairList, encoding='utf-8'):
    """
    根据获取的库表对，生成批量为选取的库-表添加条目创建时间与条目修改时间这两个字段的sql脚本文件
    :param fileDir: 存放生成的sql脚本文件的目录位置与文件名
    :param DTPairList: 库表对列表集，通过select语句从mysql的information_schema库-
                        tables表中获取
    :param encoding: 输出文件编码
    :return: 若函数正常运作，输出OK，否则输出报错原因
    """
    text = 'set character set utf8mb4;\n\n'
    try:
        for DTPair in DTPairList:
            text += 'use %s;\n' % DTPair[0]
            text += 'alter table %s add (\n' \
                    '`create_time` datetime default current_timestamp ' \
                    'comment \'创建时间\',\n' \
                    '`update_time` datetime default current_timestamp ' \
                    'on update current_timestamp comment \'更新时间\'\n' \
                    ');\n\n' % DTPair[1]
        with open(fileDir, 'w', encoding=encoding) as f:
            f.write(text)
        return 'OK'
    except Exception as e:
        return 'ERROR!\n%s' % (str(e) + '\n' + e.__doc__)


# timeFieldsAddSQL('F:/test dir/mmm.sql',
#                  [('uuu', 'layer_dict'), ('uuu', 'layer_dict_relations')],
#                  'utf-8')


def attrReadSQL(DTFDict):
    """
    根据提供的库、表、字段组，生成从mysql的information_schema库-columns表中
    获取选取字段与字段属性列表的sql语句
    :param DTFDict: 库、表、字段组的字典形式
        例：{'uuu': {'layer_dict': ['code', 'type', 'comment'],
            'test': ['id', 'create_time']},
            'meta': {'layer_innercode': ['level']}}
    :return: 若函数正常运作，输出OK，否则输出报错原因
    """
    commandList = []
    try:
        for database in DTFDict:
            for table in DTFDict[database]:
                fieldset = str(DTFDict[database][table]).\
                    replace('[', '(').replace(']', ')')
                commandList.append('select column_name,\n'
                                   'concat(column_type, \' \',\n'
                                   'if(column_key=\'PRI\', '
                                   '\'primary key\', \'\'), \' \',\n'
                                   'if(is_nullable=\'YES\', \'NULL\', '
                                   '\'NOT NULL\'), \' \',\n'
                                   'if(isnull(column_default), \'\', ''concat('
                                   '\'comment \"\', column_comment, \'\"\'))) '
                                   'sentence\n'
                                   'from information_schema.`columns`\n'
                                   'where table_schema = \'%s\' and '
                                   'table_name = \'%s\' and column_name in %s'
                                   % (database, table, fieldset))
        commandText = '\nunion\n'.join(commandList)
        commandText += ';'
        return commandText
    except Exception as e:
        return 'ERROR!\n%s' % (str(e) + '\n' + e.__doc__)

# dtfdic = {'uuu': {'layer_dict': ['code', 'type', 'comment'],
#                   'test': ['id', 'create_time']},
#           'meta': {'layer_innercode': ['level']}}
# print(attrReadSQL(dtfdic))
# cursor.execute(attrReadSQL(dtfdic))
# attrlis = [_ for _ in cursor]


def attrTableSQL(database, table, fieldDict, attrList, fileDir,
                 dropPara=True, encoding='utf-8'):
    """
    根据字段映射、字段属性列表，生成建表sql脚本
    :param database: 待创建的表所在库名
    :param table: 待创建的表名
    :param fieldDict: 字段映射字典，键为旧字段名，值为新字段名
    :param attrList: 字段属性列表，列表中每个元素包含两个元素，第一个元素为旧字段名，
                    第二个元素为字段属性(根据函数attrReadSQL生成)
    :param fileDir: 存放生成的sql脚本文件的目录位置
    :param dropPara: 控制是否添加在创建表前删除表的语句
    :param encoding: 输出文件编码
    :return: 若函数正常运作，输出OK，否则输出报错原因
    """
    text = 'set character set utf8mb4;\n\n'
    text += 'use %s;\n' % database
    if dropPara:
        text += 'drop table if exists %s;\n' % table
    text += 'create table %s (\n' % table
    try:
        tempLis = []
        for attr in attrList:
            fieldAttr = attr[1]
            fieldName = fieldDict[attr[0]]
            tempLis.append(fieldName + ' ' + fieldAttr)
        attrText = ',\n'.join(tempLis)
        text += attrText
        text += ')engine=innoDB default charset=utf8mb4;'
        with open(fileDir + '/' + '%s-%s.sql' % (database, table),
                  'w', encoding=encoding) as f:
            f.write(text)
        return 'OK'
    except Exception as e:
        return 'ERROR!\n%s' % (str(e) + '\n' + e.__doc__)


# fdic = {'code': '编码', 'type': '类型', 'comment': '注释',
#         'id': 'ID', 'CREATE_TIME': '创建时间', 'LEVEL': '层级'}
# attrTableSQL('uuu', '测试表', fdic, attrlis, 'F:/test dir')
#
# node = '25'
# database = 'uuu'

def consanguinityAnalysis(node, database, conn):
    conn.cursor().execute('use %s;' % str(database))
    conn.cursor().execute('set @i:=\'%s\' collate utf8mb4_general_ci;'
                          % str(node))
    command = 'select ' \
              'node, ' \
              'substring_index(downflow_longcode, ' \
              'concat((select downflow_shortcode from shortcode ' \
              'where node = @i), \'z\'), 1) subcode, ' \
              '-1 type ' \
              'from downflow_code ' \
              'where downflow_longcode like ' \
              'concat(\'%%\', (select downflow_shortcode ' \
              'from shortcode where node = @i), \'z\') \n' \
              'union ' \
              'select distinct ' \
              'node, ' \
              'substring_index(downflow_longcode, ' \
              'concat((select downflow_shortcode ' \
              'from shortcode where node = @i), \'z\'), -1) subcode, ' \
              '1 type ' \
              'from downflow_code ' \
              'where downflow_longcode like \'%%E%%\' ' \
              'and downflow_longcode like ' \
              'concat(\'%%\', (select downflow_shortcode ' \
              'from shortcode where node = @i), \'z\', \'%%\') ' \
              'and downflow_longcode not like ' \
              'concat(\'%%\', (select downflow_shortcode ' \
              'from shortcode where node = @i), \'z\');'
    df = pd.read_sql(command, conn)
    res = set()
    res.add((node, '', 0))
    for i in range(len(df)):
        lis = df['subcode'][i].split('z')
        lis.remove('')
        if df['type'][i] == 1:
            lis = [node] + lis
            dic = dict(zip(range(len(lis)), lis))
            for j in dic:
                if j != 0:
                    res.add((dic[j], dic[j - 1], j))
        else:
            lis = lis + [node]
            dic = dict(zip(range(1 - len(lis), 1), lis))
            for j in dic:
                if j != 0:
                    res.add((dic[j], dic[j + 1], j))
    df1 = pd.DataFrame(res, columns=['node', 'last_node', 'level'])\
        .sort_values('level', inplace=False)
    return df1


# df1 = consanguinityAnalysis(22, 'uuu', conn)
#
# set(df1['node'][df1['level'] == 2])
