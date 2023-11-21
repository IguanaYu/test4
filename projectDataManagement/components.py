from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore

from functions.general import *
from functions.connection import *
from projectDataIntegration.models import SourceInfo


class DataClass:
    def __init__(self, **kwargs):
        self.totalDB = False
        self.briefIntroduction = ''
        if kwargs.get('is_source_related'):
            self.type = 'source'
            self.sourceID = kwargs.get('source_id')
            self.conn = None
            self.database = None
            self.table = kwargs.get('table_name')
            if self.table == 'ALL':
                self.totalDB = True
            sourceQuery = SourceInfo.objects.filter(id=self.sourceID).values()
            sourceName = '[%s]' % sourceQuery[0].get('source_name')
            tableName = f'-[{self.table}]' if self.table != 'ALL' else ''
            self.briefIntroduction += f'{sourceName}{tableName}'
        elif kwargs.get('file_path'):
            self.type = 'file'
            self.filePath = kwargs.get('file_path')
        else:
            self.type = 'variable'
        self.allFields = kwargs.get('allFields', [])
        self.selectedFields = []
        self.dataContent = kwargs.get('dataContent', [])
        self.batchSize = 1000
        self.batchNum = 1
        self.dataVolume = kwargs.get('dataVolume', 0)
        self.estimatedSpace = kwargs.get('estimatedSpace', 0)
        self.idForOutput = None
    
    def connect(self):
        if self.type != 'source':
            raise TypeError('该类方法只适用于数据库连接相关的数据源！')
        try:
            self.conn, self.database = sourceConnect(self.sourceID)
        except Exception:
            return None
    
    def disconnect(self, cursor=None):
        if self.type != 'source':
            raise TypeError('该类方法只适用于数据库连接相关的数据源！')
        try:
            cursor.close()
        except Exception:
            try:
                self.conn.close()
            except Exception:
                return None
    
    def overviewGet(self):
        if self.totalDB:
            raise ValueError('整库同步不适用该类方法')
        if self.type == 'source':
            self.connect()
            cursor = self.conn.cursor()
            descFields = ['field_name', 'field_comment',
                          'field_type', 'field_constraint']
            fieldAttr = mysqlFieldsGet(cursor, self.database, self.table,
                                       fields=descFields)
            self.allFields = fieldAttr
            self.disconnect(cursor=cursor)
        elif self.type == 'file':
            return '※此处功能为根据数据文件获取数据信息，待完善'
        else:
            return '※此处功能为根据数据变量获取数据信息，待完善'
        return self.allFields
    
    def fieldsGet(self, **kwargs):
        res = self.allFields
        if kwargs.get('nameonly'):
            res = [{'key': _['field_name'], 'value': _['field_name']} for _ in
                   res]
        return res
    
    def selectedFieldsGet(self, listString):
        self.selectedFields = fieldsToList(listString)
    
    def dataGet(self, **kwargs):
        filterDict = kwargs.get('filter')
        incrementDict = kwargs.get('increment')
        currentTime = kwargs.get('currentTime')
        fields = self.selectedFields
        if self.type == 'source':
            fieldsStr = '`' + '`, `'.join(fields) + '`'
            tableStr = '`' + self.table + '`'
            countStr = 'count(*)'
            commandTemplate = 'select {} from ' + tableStr + ' where 1=1'
            command = commandTemplate.format(fieldsStr)
            commandCount = commandTemplate.format(countStr)
            filterCommand = ''
            if filterDict:
                for key in filterDict:
                    keySQL = filterFieldAnalysis(key, filterDict[key],
                                                          fields, cursor=True)
                    filterCommand += ' and ' + keySQL
            if incrementDict:
                for key in incrementDict:
                    if incrementDict[key] is not None:
                        keySQL = ' and `{}` > \"{}\"'\
                            .format(key, incrementDict[key])
                    else:
                        keySQL = ''
                    filterCommand += keySQL + ' and `{}` <= \"{}\"'\
                        .format(key, currentTime)
            commandCount += filterCommand
            self.connect()
            cursor = self.conn.cursor()
            cursor.execute(commandCount)
            self.dataVolume = int(cursor.fetchone()[0])
            self.disconnect(cursor=cursor)
            limitCommand = ' LIMIT {start}, {end}'
            self.dataContent = command + filterCommand + limitCommand
            return '批次数据获取指令生成成功'
        elif self.type == 'file':
            return '※此处功能为根据数据文件获取数据信息，待完善'
    
    def batchInfoGet(self, **kwargs):
        self.batchSize = int(kwargs.get('batchSize', self.batchSize))
        self.batchNum = int(self.dataVolume // self.batchSize + \
                            (self.dataVolume % self.batchSize != 0))
        if self.type == 'source':
            self.connect()
            cursor = self.conn.cursor()
            command = self.dataContent.format(start=0, end=self.batchSize)
            cursor.execute(command)
            firstBatch = [_ for _ in cursor]
            self.disconnect(cursor=cursor)
        elif self.type == 'file':
            firstBatch = '※此处功能为根据数据文件获取数据信息，待完善'
        else:
            firstBatch = self.dataContent[0:self.batchSize]
        self.estimatedSpace = sys.getsizeof(firstBatch) * self.batchNum
        return '批次处理信息获取成功'
    
    def batchDataGet(self, batchIndex=1):
        start = (batchIndex - 1) * self.batchSize
        end = start + self.batchSize
        if self.type == 'source':
            self.connect()
            cursor = self.conn.cursor()
            command = self.dataContent.format(start=start, end=end)
            cursor.execute(command)
            batchData = [_ for _ in cursor]
            self.disconnect(cursor=cursor)
        elif self.type == 'file':
            batchData = '※此处功能为根据数据文件获取批次数据信息，待完善'
        else:
            batchData = self.dataContent[start:end]
        return batchData
    
    def idGet(self, ID):
        self.idForOutput = ID
    
    def dataInfoSave(self, encoding='utf-8'):
        jsonDict = {'type': self.type,
                    'idForOutput': self.idForOutput,
                    'allFields': self.allFields,
                    'selectedFields': self.selectedFields,
                    'dataVolume': self.dataVolume,
                    'estimatedSpace': self.estimatedSpace,
                    'totalDB': self.totalDB}
        jsonGenerate(jsonDict, self.filePath + '.json', encoding=encoding)
    
    def dataPreSave(self):
        if self.type == 'source':
            self.connect()
            cursor = self.conn.cursor()
            cursor.execute(f'create table if not exists `{self.table}__bak` '
                           f'select * from `{self.table}`')
            self.conn.commit()
            self.disconnect(cursor=cursor)
            
    def dataPostSave(self, rollback=True):
        if self.type == 'source':
            self.connect()
            cursor = self.conn.cursor()
            if rollback:
                cursor.execute(f'truncate `{self.table}`')
                self.conn.commit()
                cursor.execute(f'insert into `{self.table}` '
                               f'select * from `{self.table}__bak`')
                self.conn.commit()
            cursor.execute(f'drop table if exists `{self.table}__bak`')
            self.conn.commit()
            self.disconnect(cursor=cursor)
    
    def dataSave(self, dataContent, **kwargs):
        if len(dataContent) == 0:
            return None
        encoding = kwargs.get('encoding', 'utf-8')
        if self.type == 'source':
            self.connect()
            cursor = self.conn.cursor()
            dataStringList = []
            for row in dataContent:
                rowString = sqlResultToString(row)
                dataStringList.append(rowString)
            dataString = ',\n'.join(dataStringList)
            fieldString = '`' + '`, `'.join(self.selectedFields) + '`'
            command = 'INSERT INTO `%s`(%s) VALUES\n%s;' \
                      % (self.table, fieldString, dataString)
            print(command)
            cursor.execute(command)
            self.conn.commit()
            print(111)
            self.disconnect(cursor=cursor)
        elif self.type == 'file':
            with open(self.filePath + '.csv', mode='a',
                      encoding=encoding) as f:
                for row in dataContent:
                    f.write(row)
        else:
            self.dataContent += dataContent
            
    def tableListGet(self):
        if self.type != 'source':
            raise TypeError('该方法只适用于关联数据源的数据类！')
        if not self.totalDB:
            return [self.table]
        else:
            self.connect()
            cursor = self.conn.cursor()
            cursor.execute('show tables')
            res = [_[0] for _ in cursor]
            self.disconnect(cursor=cursor)
            return res
        
    def dataRollback(self):
        return 1
        

class GeneralTrigger(CronTrigger):
    
    def getNextTime(self, dateTimePoint=None):
        if dateTimePoint is None:
            startDateTime = curTimeGet()
        else:
            startDateTime = dateTimePoint
        return self.get_next_fire_time(startDateTime, startDateTime)
    
    def getLastTime(self, dateTimePoint=None):
        return 1

    @classmethod
    def fromCrontab(cls, expr, timezone=None):
        values = expr.split()
        timeList = ['second', 'minute', 'hour', 'day',
                    'month', 'day_of_week', 'year']
        if len(values) == 5:
            timeDict = dict(zip(timeList[1:6], values))
        elif len(values) == 6:
            timeDict = dict(zip(timeList[:6], values))
        elif len(values) == 7:
            timeDict = dict(zip(timeList, values))
        else:
            raise ValueError('cron表达式位数错误，正确位数应在5-7位之间！')
        for value in timeDict:
            if timeDict[value] == '?':
                timeDict[value] = None
        timeDict['timezone'] = timezone
        return cls(**timeDict)
        
    
class GeneralScheConfig:
    timezone = 'Asia/Shanghai'
    apiEnabled = True
    dbURL = 'mysql+pymysql://root:QingZhong%40613' \
            '@localhost:3306/asamount_meta'
    jobstores = {'default': SQLAlchemyJobStore(url=dbURL)}
    executors = {'default': {'type': 'threadpool', 'max_workers': 10},
                 'process': {'type': 'processpool', 'max_workers': 10}}
    job_defaults = {'max_instances': 50,
                    'coalesce': True,
                    'misfire_grace_time': 1800}


scheduler = BackgroundScheduler(jobstores=GeneralScheConfig.jobstores,
                                executors=GeneralScheConfig.executors,
                                job_defaults=GeneralScheConfig.job_defaults)


class FuncClass:
    def __init__(self, **kwargs):
        self.componentParams = kwargs.get('funcParams', {})
        self.dataInput = kwargs.get('dataInput')
        self.dataOutput = kwargs.get('dataOutput')
        self.mapList = kwargs.get('mapList', [])
        self.funcFieldMap = []
        self.filterParams = self.componentParams.get('filter', [])
        self.log = None
        self.passedFieldsDict = dict()
        self.processingFieldsDict = dict()
        for tmpMap in self.mapList:
            if tmpMap['type'] == 'pass':
                self.passedFieldsDict[tmpMap['origin']] = tmpMap['target']
            if tmpMap['type'] == 'processing':
                self.processingFieldsDict[tmpMap['origin']] = tmpMap['target']
        self.batchAllowed = True
    
    def filterDictGet(self):
        res = dict()
        for filterParam in self.filterParams:
            if not isinstance(filterParam, dict):
                raise TypeError('筛选参数应以字典形式提供！')
            fieldName = filterParam.get('fieldName')
            fieldFilterType = filterParam.get('filterType')
            fieldFilterValue = filterParam.get('filterValue')
            if not fieldName:
                raise ValueError('未提供筛选字段！')
            if not fieldFilterValue:
                raise ValueError('未提供字段筛选条件值！')
            if not fieldFilterType:
                fieldFilterType = 'icontain'
            res[fieldName + '__' + fieldFilterType] = fieldFilterValue
        return res
    
    def logHandlerGet(self, taskID, compID, mode='a', encoding='utf-8'):
        tmpDir = dirGet('tmp')
        logFilePath = tmpDir + '{}-{}.log'.format(taskID, compID)
        self.log = logging.FileHandler(filename=logFilePath, mode=mode,
                                       encoding=encoding, delay=False)
        self.log.setFormatter(fmt)
        componentLogger.addHandler(self.log)
    
    def logHandlerRemove(self):
        componentLogger.removeHandler(self.log)
    
    def passedFieldsGet(self, originList=None, targetList=None, total=False):
        if not originList:
            originList = []
        if not total:
            if not targetList:
                targetList = [None for _ in originList]
        else:
            inputFields = self.dataInput.overviewGet()
            originList = [_['field_name'] for _ in inputFields]
            if not targetList:
                targetList = [None for _ in originList]
        self.passedFieldsDict = dict(zip(originList, targetList))
            
    def processingFieldsGet(self, originList=None, targetList=None):
        if not originList:
            originList = []
        if not targetList:
            targetList = [None for _ in originList]
        self.processingFieldsDict = dict(zip(originList, targetList))
        
    def mappingGenerate(self):
        res = []
        for field in self.passedFieldsDict:
            tmpDict = {'origin': field,
                       'target': self.passedFieldsDict[field],
                       'type': 'pass'}
            res.append(tmpDict)
        for fields in self.processingFieldsDict:
            tmpDict = {'origin': fields,
                       'target': self.processingFieldsDict[fields],
                       'type': 'processing'}
            res.append(tmpDict)
        self.mapList = res
        
    def mappingUpdate(self, mapList=None):
        if not mapList:
            return self.mapList
        passOrigin = []
        passTarget = []
        processingOrigin = []
        processingTarget = []
        for tmpMap in mapList:
            if tmpMap['mapping_type'] == 'pass':
                passOrigin.append(tmpMap['origin_field_name'])
                passTarget.append(tmpMap['target_field_name'])
            elif tmpMap['mapping_type'] == 'processing':
                origin = tmpMap['origin']
                target = tmpMap['target']
                processingOrigin.append([_['field_name'] for _ in origin])
                processingTarget.append([_['field_name'] for _ in target])
            elif not tmpMap.get('mapping_type'):
                continue
            else:
                raise ValueError('不支持的映射类型！')
        self.passedFieldsGet(originList=passOrigin, targetList=passTarget)
        self.processingFieldsGet(originList=processingOrigin,
                                 targetList=processingTarget)
        self.mappingGenerate()
        return self.mapList
    
    def prePassedMapGet(self):
        resDict = {}
        inputFields = self.dataInput.overviewGet()
        outputFields = self.dataOutput.overviewGet()
        for inputField in inputFields:
            inName = inputField['field_name']
            for outputField in outputFields:
                outName = outputField['field_name']
                if inName.lower() == outName.lower():
                    resDict[inName] = outName
                    break
        self.passedFieldsDict = resDict
        
    def prePassedMapRemove(self):
        self.passedFieldsDict = dict()
        
    def mapDetailGet(self):
        try:
            totalDB = self.dataInput.totalDB
            if totalDB:
                raise ValueError('该方法不适用于整库操作！')
        except Exception:
            raise ValueError('该方法不适用于整库操作！')
        res = []
        inputFields = self.dataInput.overviewGet()
        inputFieldsDict = {_['field_name']: _ for _ in inputFields}
        outputFields = self.dataOutput.overviewGet()
        outputFieldsDict = {_['field_name']: _ for _ in outputFields}
        lis = ['field_name', 'field_comment', 'field_type', 'field_constraint']
        inputFieldsRename = dict(zip(lis, ['origin_' + _ for _ in lis]))
        outputFieldsRename = dict(zip(lis, ['target_' + _ for _ in lis]))
        for inName in inputFieldsDict:
            inputField = inputFieldsDict[inName]
            tmpDict = subDict(inputField, 'all', **inputFieldsRename)
            tmpDict.update(subDict(dict(zip(lis, [None] * 4)), 'all',
                                   **outputFieldsRename))
            tmpDict.update({'filter_type': None,
                            'filter_value': None,
                            'mapping_type': None})
            for filterParam in self.filterParams:
                if filterParam.get('fieldName') == inName:
                    filterType = filterParam.get('filter_type')
                    filterValue = filterParam.get('filter_value')
                    tmpDict.update({'filter_type': filterType,
                                    'filter_value': filterValue})
                    break
            if self.passedFieldsDict.get(inName):
                outName = self.passedFieldsDict[inName]
                outputField = outputFieldsDict[outName]
                tmpDict.update(subDict(outputField, 'all',
                                       **outputFieldsRename))
                tmpDict.update({'mapping_type': 'pass'})
            res.append(tmpDict)
        for inNames in self.processingFieldsDict:
            outNames = self.processingFieldsDict[inNames]
            inNamesList = [inputFieldsDict[_] for _ in inNames]
            outNamesList = [outputFieldsDict[_] for _ in outNames]
            tmpDict = {'origin': inNamesList,
                       'target': outNamesList,
                       'mapping_type': 'processing'}
            res.append(tmpDict)
        self.funcFieldMap = res
        return self.funcFieldMap
    
    def selectedFieldsListGet(self):
        origin = list(self.passedFieldsDict.keys()) + list(
            self.processingFieldsDict.keys())
        target = list(self.passedFieldsDict.values()) + list(
            self.processingFieldsDict.values())
        return str(origin), str(target)
    
    def run(self, *args, **kwargs):
        pass
    
    def output(self):
        return self.dataOutput


class IntegrationClass(FuncClass):
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.incrementParams = {}
        if self.componentParams:
            if self.componentParams.get('incremental', False) is True:
                timeField = self.componentParams.get('incrementalField', None)
                defaultTime = '0000-01-01 00:00:00'
                timePoint = self.componentParams.get('timePoint', defaultTime)
                if timeField is not None:
                    self.incrementParams = {timeField: timePoint}
        
        if not self.dataInput:
            raise ValueError('未配置集成数据来源！')
        if not self.dataOutput:
            raise ValueError('未配置集成数据目标！')
        self.totalDatabase = False
        if self.dataInput.totalDB:
            self.totalDatabase = True
    
    def run(self, taskID, mapID, batchSize=None):
        originFieldList = [_['origin'] for _ in self.mapList if _['type']
                            == 'pass']
        targetFieldList = [_['target'] for _ in self.mapList if _['type']
                            == 'pass']
        startTime = curTimeGet()
        self.logHandlerGet(taskID, mapID)
        componentLogger.info(f'任务组件ID：{mapID}，该组件的数据集成任务部分开始。')
        try:
            self.dataInput.connect()
            inputCursor = self.dataInput.conn.cursor()
            self.dataInput.disconnect(cursor=inputCursor)
        except Exception as e:
            msg = '数据来源数据源连接失败，集成任务停止。'
            return generalFuncResult(componentLogger, msg, startTime,
                                     0, str(e), self.log)
        try:
            self.dataOutput.connect()
            outputCursor = self.dataOutput.conn.cursor()
            self.dataOutput.disconnect(cursor=outputCursor)
        except Exception as e:
            msg = '数据目标数据源连接失败，集成任务停止。'
            return generalFuncResult(componentLogger, msg, startTime,
                                     0, str(e), self.log)
        originInfo = f'数据来源：{self.dataInput.briefIntroduction}'
        targetInfo = f'数据目标：{self.dataOutput.briefIntroduction}'
        if self.totalDatabase:
            tableList = self.dataInput.tableListGet()
            componentLogger.info(f'集成类型：整库同步。{originInfo}，{targetInfo}')
            for table in tableList:
                componentLogger.info('开始同步数据表%s' % table)
                try:
                    inputCommand = 'select * from %s' % table
                    inputCursor.execute(inputCommand)
                    data = [sqlResultToString(_) for _ in inputCursor]
                    outputCommand = 'insert into %s values\n' % table
                    outputCommand += ',\n'.join(data)
                    outputCursor.execute(outputCommand)
                    self.dataOutput.conn.commit()
                    componentLogger.info('数据表%s同步完毕。' % table)
                except Exception as e:
                    msg = '同步失败。'
                    self.dataInput.disconnect()
                    self.dataOutput.disconnect()
                    return generalFuncResult(componentLogger, msg, startTime,
                                             0, str(e), self.log)
        else:
            componentLogger.info(f'集成类型：单表同步。{originInfo}，{targetInfo}')
            try:
                self.dataInput.overviewGet()
                self.dataInput.selectedFieldsGet(originFieldList)
                self.dataInput.dataGet(filter=self.filterDictGet(),
                                       increment=self.incrementParams,
                                       currentTime=startTime)
                self.dataInput.batchInfoGet()
                batchSize = self.dataInput.batchSize
                batchNum = self.dataInput.batchNum
                dataVolume = self.dataInput.dataVolume
                componentLogger.info('本次集成任务，目标同步%d条数据，设定批次容量'
                                     '为%d，将分%d批次集成' %
                                     (dataVolume, batchSize, batchNum))
                self.dataOutput.overviewGet()
                self.dataOutput.selectedFieldsGet(targetFieldList)
                self.dataOutput.dataPreSave()
                for batchIndex in range(1, batchNum + 1):
                    try:
                        dataContent = self.dataInput.batchDataGet(batchIndex)
                        print(dataContent)
                        self.dataOutput.dataSave(dataContent)
                        componentLogger.info('第%d批次数据同步完毕' % batchIndex)
                    except Exception as e:
                        msg = '第%d批次数据同步失败。\n' \
                              '组件调度中止，执行数据回滚。' % batchIndex
                        self.dataInput.disconnect()
                        self.dataOutput.disconnect()
                        self.dataOutput.dataPostSave(rollback=True)
                        return generalFuncResult(componentLogger, msg,
                                                 startTime, 0, str(e),
                                                 self.log)
                self.dataOutput.dataPostSave(rollback=False)
            except Exception as e:
                msg = '同步失败。'
                self.dataInput.disconnect()
                self.dataOutput.disconnect()
                return generalFuncResult(componentLogger, msg, startTime,
                                         0, str(e), self.log)
        msg = '任务组件ID为%s的集成任务部分成功完成。' % str(mapID)
        self.dataInput.disconnect()
        self.dataOutput.disconnect()
        return generalFuncResult(componentLogger, msg, startTime, 1,
                                 handler=self.log)
    
    
class FilterClass(FuncClass):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        

class StandardCheckClass(FuncClass):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class StandardCleaningClass(FuncClass):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)