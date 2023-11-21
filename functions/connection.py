import MySQLdb
import pymysql
from sshtunnel import open_tunnel
from projectDataIntegration.models import SourceInfo
from functions.general import (subDict, errorOutput, rowToDict)
from functions.secure import Password


# def mysqlConnect(mysqlParams=None, SSH=False, SSHParams=None):
#     if mysqlParams is None:
#         mysqlParams = {}
#     if SSH:
#         if not SSHParams:
#             raise ValueError('未提供SSH参数！')
#     try:
#         if SSH:
#             SSHParams['remote_bind_address'] = \
#                 (mysqlParams['host'], mysqlParams['port'])
#             open_tunnel(**SSHParams)
#         conn = pymysql.connect(**mysqlParams)
#         return conn
#     except Exception as e:
#         return errorOutput(e)

mysqlTypeCodeDict = {
    '字符串型': [15, 249, 250, 251, 252, 253, 254],
    '整型': [1, 2, 3, 8, 9],
    '浮点型': [0, 4, 5, 246],
    '时间戳型': [7, 17],
    '日期时间型': [12, 18],
    '空值型': [6],
    '日期型': [10, 13],
    '时间型': [11, 19],
    '布尔型': [16],
    'JSON型': [245],
    '枚举型': [247, 248]}


def sourceConnect(sourceID=None, **kwargs):
    if sourceID:
        fields = ['id', 'host', 'port', 'user', 'password',
                  'source_code', 'source_type_id__type_module']
        params = SourceInfo.objects.filter(id=sourceID)\
            .select_related('source_type_id').values(*fields)[0]
        connectFields = ['host', 'port', 'user', 'password', 'source_code']
        module = params.get('source_type_id__type_module')
        connectParams = subDict(params, connectFields, source_code='database')
        connectParams['password'] = Password(connectParams['password'],
                                             'Decrypt').transform()
        connectParams['port'] = int(connectParams['port'])
    else:
        module = kwargs.get('module')
        connectParams = {'host': kwargs.get('host', ''),
                         'port': int(kwargs.get('port', '0')),
                         'user': kwargs.get('user', ''),
                         'password': kwargs.get('password', ''),
                         'database': kwargs.get('database', '')}
    command = 'import %s as dbConnector\n' % module
    command += 'conn = dbConnector.connect(**connectParams)'
    try:
        loc = locals()
        exec(command)
        conn = loc['conn']
        return conn, connectParams['database']
    except Exception as e:
        raise e
    

def mysqlTablesGet(mysqlCursor, dbName, **kwargs):
    command = 'select ' \
              '`table_name` `table_name`, ' \
              'ifnull(`table_comment`, \"-\") `table_comment`, ' \
              '`table_type` `table_type`, ' \
              '`table_rows` `table_rows`, ' \
              'concat(`create_time`, \"\") `create_time`, ' \
              'concat(`update_time`, \"\") `update_time`' \
              'from `information_schema`.`tables` ' \
              'where 1=1 and `table_schema` = "%s"' % dbName
    mysqlCursor.execute(command)
    title = [_[0] for _ in mysqlCursor.description]
    res = [dict(zip(title, _)) for _ in mysqlCursor]
    if kwargs.get('tablename'):
        res = [_ for _ in res if _['table_name'] == kwargs.get('tablename')]
    if kwargs.get('fields'):
        fieldsList = kwargs.get('fields')
        res = [subDict(_, fieldsList) for _ in res]
    if kwargs.get('tableonly'):
        res = [_ for _ in res if _['table_type'] == 'BASE TABLE']
    if kwargs.get('nameonly'):
        res = [{'key': _['table_name'], 'value': _['table_name']}
                for _ in res]
    return res


timeType = ['date', 'time', 'datetime', 'timestamp']

    
def mysqlFieldsGet(mysqlCursor, dbName, tableName, **kwargs):
    command = 'select ' \
              '`COLUMN_NAME` `field_name`, ' \
              'ifnull(COLUMN_COMMENT, NULL) `field_comment`, ' \
              '`COLUMN_TYPE` `field_type`, ' \
              'ifnull(CHARACTER_MAXIMUM_LENGTH, 0) + ' \
              'ifnull(NUMERIC_PRECISION, 0) + ' \
              'ifnull(DATETIME_PRECISION, 0) `field_length`, ' \
              'ifnull(NUMERIC_SCALE, NULL) `field_scale`, ' \
              '(case when COLUMN_KEY="PRI" then "主键约束" ' \
              'when COLUMN_KEY="UNI" and IS_NULLABLE="NO" then "非空约束，唯一约束" ' \
              'when COLUMN_KEY="UNI" and IS_NULLABLE="YES" then "唯一约束" ' \
              'when COLUMN_KEY="MUL" and IS_NULLABLE="NO" then "非空约束，包含索引" ' \
              'when COLUMN_KEY="MUL" and IS_NULLABLE="YES" then "包含索引" ' \
              'when COLUMN_KEY="" and IS_NULLABLE="NO" then "非空约束" ' \
              'else NULL end) `field_constraint`, ' \
              'ifnull(COLUMN_DEFAULT, NULL) `field_default` ' \
              'from `information_schema`.`columns` ' \
              'where 1=1 and table_schema = "%s" and table_name = "%s"' \
              % (dbName, tableName)
    mysqlCursor.execute(command)
    title = [_[0] for _ in mysqlCursor.description]
    res = [dict(zip(title, _)) for _ in mysqlCursor]
    for i in res:
        if not isinstance(i['field_length'], int):
            i['field_length'] = int(i['field_length'])
        if not isinstance(i['field_scale'], (int, type(None))):
            i['field_scale'] = int(i['field_scale'])
    if kwargs.get('fields'):
        fieldsList = kwargs.get('fields')
        res = [subDict(_, fieldsList) for _ in res]
    if kwargs.get('timeonly'):
        res = [_ for _ in res if _['field_type'] in timeType]
    if kwargs.get('nameonly'):
        res = [{'key': _['field_name'], 'value': _['field_name']}
                for _ in res]
    return res


def mysqlFieldsValuesGet(mysqlCursor, tableName, fieldName):
    command = f'select distinct `{fieldName}` from `{tableName}` order by `' \
              f'{fieldName}`'
    mysqlCursor.execute(command)
    res = {_[0]: _[0] for _ in mysqlCursor}
    return res
    
    
def mysqlTablePreviewGet(mysqlCursor, tableName, pageCurrent=1, pageSize=10):
    command = f'select * from {tableName}'
    mysqlCursor.execute(command)
    totalCount = mysqlCursor.rowcount
    start = (pageCurrent - 1) * pageSize
    totalPage = int(totalCount // pageSize + (totalCount % pageSize != 0))
    command += f' limit {start}, {pageSize}'
    mysqlCursor.execute(command)
    queryCol = [_[0] for _ in mysqlCursor.description]
    queryRes = [_ for _ in mysqlCursor]
    data = [rowToDict(_, queryCol) for _ in queryRes]
    res = {'data': data, 'totalCount': totalCount, 'totalPage': totalPage}
    return res
    