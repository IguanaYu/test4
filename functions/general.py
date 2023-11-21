import datetime as dt
from dateutil.relativedelta import relativedelta as rtd
import json
import logging

from django.db import models
from django.http import HttpResponse
from django.db.models import Q
from rest_framework.viewsets import ViewSet
from rest_framework.decorators import action

from functions.secure import Password


componentLogger = logging.getLogger('component')
componentLogger.setLevel(logging.INFO)
fmt = logging.Formatter(fmt='%(asctime)s [%(levelname)s]:%(message)s')


def dirGet(typ='tmp'):
    if typ == 'tmp':
        resDir = './tmp/'
    elif typ == 'log':
        resDir = './log/'
    else:
        resDir = './'
    return resDir


def curTimeGet():
    return dt.datetime.now()


def listCount(valueList):
    countDict = dict()
    for value in valueList:
        if value in countDict:
            countDict[value] += 1
        else:
            countDict[value] = 1
    res = set(countDict.values())
    return res


def errorOutput(exception, description=None):
    if not isinstance(exception, Exception):
        raise TypeError('提供参数不属于异常类！')
    res = '异常类型：' + exception.__class__.__name__
    doc = f'{exception.__doc__}' if exception.__doc__ else '无'
    res += f'\n异常类型说明：{doc}'
    res += '\n本次异常详情：' + ''.join(['\n\t' + str(_) for _ in exception.args])
    if description is not None:
        res += f'\n本次异常备注：{description}'
    return res


def fieldDelete(model, queryset, deleteType='cascade',
                deleteField='is_deleted',
                updateField='time_last_updated',
                upperValue=None):
    deleteInfoDict = {deleteField: 1, updateField: curTimeGet()}
    pkName = model._meta.pk.name
    print(f'pkName:{pkName}')
    if len(queryset) > 0:
        queryset.update(**deleteInfoDict)
    pkList = [_[pkName] for _ in queryset.values(pkName)]
    fieldMap = model._meta.fields_map
    if len(fieldMap) > 0:
        for relName in fieldMap:
            relClass = fieldMap[relName]
            relModel = relClass.related_model
            relField = relClass.field.name
            filterInfoDict = {relField + '__in': pkList}
            subQueryset = relModel.objects.filter(**filterInfoDict)
            if len(subQueryset) > 0:
                if deleteType == 'cascade':
                    fieldDelete(relModel, subQueryset, deleteType,
                                deleteField, updateField)
                elif deleteType in ['null', 'highest', 'upper']:
                    valueDict = {'null': None,
                                 'highest': 1,
                                 'upper': upperValue}
                    try:
                        subQueryset.update(**{relField: valueDict[deleteType]})
                    except Exception:
                        deleteType = 'cascade'
                        fieldDelete(relModel, subQueryset, deleteType,
                                    deleteField, updateField)


class GeneralInfoModel(models.Model):
    class Meta:
        abstract = True
    
    id = models.AutoField(primary_key=True)
    creator = models.CharField(
        verbose_name='创建者',
        max_length=16,
        null=True,
        blank=True)
    time_created = models.DateTimeField(
        verbose_name='创建时间',
        auto_now_add=True)
    # default=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    updator = models.CharField(
        verbose_name='修改者',
        max_length=16,
        null=True,
        blank=True)
    time_last_updated = models.DateTimeField(
        verbose_name='最新修改时间',
        default=curTimeGet().strftime("%Y-%m-%d %H:%M:%S"))
    comment = models.TextField(
        verbose_name='描述',
        max_length=5000,
        null=True,
        blank=True)
    is_deleted = models.BooleanField(
        verbose_name='是否逻辑删除',
        default=0)


statusCode = [200, 400]
statusInfo = ['success', 'failed']
statusCodeDict = dict(zip(statusCode, [str(_) for _ in statusCode]))
statusInfoDict = dict(zip(statusCode, statusInfo))


def generalJson(data=None, code=200, info=None, msg=None):
    if info is None:
        info = statusInfoDict[code]
    if data is None:
        data = dict()
    resJson = {
        "statusCode": statusCodeDict[code],
        "statusInfo": info,
        "msg": msg,
        "data": data
        }
    return resJson


def generalFuncResult(logger, msg=None, startTime=None,
                      status=None, info=None, handler=None):
    endTime = curTimeGet()
    res = {'msg': msg, 'status': status, 'startTime': startTime,
           'endTime': endTime, 'info': info}
    if status == 0:
        logger.error(msg + '故障：' + info)
        logger.removeHandler(handler)
    if status == 1:
        logger.info(msg)
        logger.removeHandler(handler)
    return res


class CronAnalysis:
    def __init__(self, cron):
        self.cron = cron.lower()
        self.timePoint = curTimeGet()
        timeTypeList = ['second', 'minute', 'hour',
                        'day', 'month', 'weekday', 'year']
        self.cronDict = dict(zip(timeTypeList, ['*'] * 7))
        timeStepList = ['seconds', 'minutes', 'hours',
                        'days', 'months', 'years']
        self.timeStep = dict(zip(timeStepList, ['*'] * 6))
        timeRangeList = ['second', 'minute', 'hour',
                         'day', 'month', 'year']
        self.timeRange = dict(zip(timeRangeList, [[]] * 6))
        cronList = self.cron.split()
        if len(cronList) == 5:
            cronDict = dict(zip(timeTypeList[1:6], cronList))
        elif len(cronList) == 6:
            cronDict = dict(zip(timeTypeList[:6], cronList))
        elif len(cronList) == 7:
            cronDict = dict(zip(timeTypeList, cronList))
        else:
            raise ValueError('cron表达式位数错误，正确位数应在5-7位之间！')
        self.cronDict.update(cronDict)
        self.lastCloseTime = None
        self.nextCloseTime = None
        self.startTime = self.timePoint
        self.endTime = self.timePoint
    
    def timePointGet(self, timePoint=None):
        if timePoint is not None:
            self.timePoint = timePoint
        try:
            microseconds = int(self.timePoint.microsecond)
        except Exception:
            microseconds = 0
        self.lastCloseTime = self.timePoint + rtd(
            microseconds=-microseconds)
        self.nextCloseTime = self.timePoint + rtd(
            seconds=1, microseconds=-microseconds)
    
    @classmethod
    def cronJudge(cls, cronString, timeType, maxDay=None):
        defaultSymbolSet = {',', '-', '*', '/'}
        if timeType in ['second', 'minute']:
            numSet = set([str(_) for _ in range(60)])
            symbolSet = defaultSymbolSet.copy()
            for symbol in symbolSet:
                cronString = cronString.replace(symbol, '$' + symbol + '$')
            cronList = cronString.split('$')
            cronList = [_ for _ in cronList if _ != '']
        elif timeType == 'hour':
            numSet = set([str(_) for _ in range(24)])
            symbolSet = defaultSymbolSet.copy()
            for symbol in symbolSet:
                cronString = cronString.replace(symbol, '$' + symbol + '$')
            cronList = cronString.split('$')
            cronList = [_ for _ in cronList if _ != '']
        elif timeType == 'month':
            numSet = set([str(_) for _ in range(1, 13)])
            symbolSet = defaultSymbolSet.copy()
            for symbol in symbolSet:
                cronString = cronString.replace(symbol, '$' + symbol + '$')
            cronList = cronString.split('$')
            cronList = [_ for _ in cronList if _ != '']
            monthStrList = ['jan', 'feb', 'mar', 'apr', 'may', 'jun',
                            'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
            monthStrDict = dict(zip(monthStrList,
                                    [str(_) for _ in range(1, 13)]))
            cronList = [monthStrDict[_[:3]] if _[:3] in
                                               monthStrDict else _ for _ in
                        cronList]
        elif timeType == 'year':
            numSet = set([str(_) for _ in range(10)])
            symbolSet = defaultSymbolSet.copy()
            cronList = list(cronString)
        elif timeType == 'day':
            try:
                maxDay = int(maxDay)
            except Exception:
                raise ValueError('未提供月日的最大值！')
            numSet = set([str(_) for _ in range(1, maxDay + 1)])
            symbolSet = defaultSymbolSet | {'l', '?', 'w'}
            for symbol in symbolSet:
                cronString = cronString.replace(symbol, '$' + symbol + '$')
            cronList = cronString.split('$')
            cronList = [_ for _ in cronList if _ != '']
        else:
            numSet = set([str(_) for _ in range(7)])
            symbolSet = defaultSymbolSet | {'l', '?', '#'}
            for symbol in symbolSet:
                cronString = cronString.replace(symbol, '$' + symbol + '$')
            cronList = cronString.split('$')
            cronList = [_ for _ in cronList if _ != '']
            weekStrList = ['sun', 'mon', 'tue', 'wed',
                           'thu', 'fri', 'sat', '7']
            weekStrDict = dict(zip(weekStrList,
                                   ['0', '1', '2', '3', '4', '5', '6', '0']))
            cronList = [weekStrDict[_[:3]] if _[:3] in
                                              weekStrDict else _ for _ in
                        cronList]
        compairSet = numSet | symbolSet
        if set(cronList) - compairSet:
            raise ValueError('cron表达式中{}内容不合规！'.format(timeType))
        cronString = ''.join(cronList)
        return cronString
    
    @classmethod
    def maxDayGet(cls, month, year=None):
        if month in [1, 3, 5, 7, 8, 10, 12]:
            return 31
        elif month in [4, 6, 9, 11]:
            return 30
        elif month == 2:
            if year is None:
                raise ValueError('推算该月最大天数需提供年份数据！')
            if year % 4 == 0:
                if year % 100 == 0 and year % 400 != 0:
                    return 28
                else:
                    return 29
            else:
                return 28
    
    @classmethod
    def cronRange(cls, cronString, defaultMin, defaultMax=None):
        if defaultMax is None:
            defaultMax = defaultMin + 9
        if cronString is None or cronString == '*':
            valueRange = list(range(defaultMin, defaultMax + 1))
            valueStep = None
        elif '/' in cronString:
            valueStep = int(cronString.split('/')[-1])
            strRange = cronString.split('/')[0]
            if '-' not in strRange:
                if strRange == '*':
                    valueMin = defaultMin
                    valueMax = defaultMax
                else:
                    valueMin = int(strRange)
                    valueMax = max(valueMin, defaultMin) + 9
            else:
                valueMin, valueMax = [int(_) for _ in strRange.split('-')]
            valueRange = list(range(valueMin, valueMax + 1, valueStep))
        else:
            valueStep = None
            tmpRange = [_ for _ in cronString.split(',')]
            valueRange = []
            for item in tmpRange:
                if '-' not in item:
                    valueRange += [int(item)]
                else:
                    tmpMin, tmpMax = [int(_) for _ in item.split('-')]
                    valueRange += list(range(tmpMin, tmpMax + 1))
        valueRange.sort()
        return valueRange, valueStep
    
    @classmethod
    def cronDayRange(cls, cronDay, month, year, cronWeekday=None):
        maxDay = cls.maxDayGet(month, year)
        resStep = None
        if cronDay != '?':
            cronString = cronDay
            if 'l-' in cronString:
                biasValue = int(cronString.split('l-')[-1])
                resRange = [maxDay + 1 - biasValue]
            else:
                cronString = cronString.replace('l', str(maxDay))
                if cronString == 'w':
                    tmpTime = dt.datetime(year, month, 1)
                    tmpWeekday = tmpTime.weekday()
                    resRange = [_ + 1 for _ in range(maxDay)
                                if (_ + tmpWeekday) % 7 not in [5, 6]]
                elif 'w' in cronString:
                    tmpDay = int(cronString.split('w')[0])
                    tmpTime = dt.datetime(year, month, tmpDay)
                    tmpWeekday = tmpTime.weekday()
                    if tmpWeekday in [0, 1, 2, 3, 4]:
                        resRange = [tmpDay]
                    elif tmpWeekday == 5 and tmpDay == 1:
                        resRange = [tmpDay + 2]
                    elif tmpWeekday == 5:
                        resRange = [tmpDay - 1]
                    elif tmpWeekday == 6 and tmpDay == maxDay:
                        resRange = [tmpDay - 2]
                    else:
                        resRange = [tmpDay + 1]
                else:
                    resRange, resStep = cls.cronRange(cronString, 1, maxDay)
        else:
            cronString = cronWeekday
            tmpTime = dt.datetime(year, month, 1)
            tmpWeekday = tmpTime.weekday()
            tmpWeekDict = dict()
            for weekday in range(7):
                tmpWeekDict[(tmpWeekday + weekday) % 7] = \
                    list(range(weekday + 1, maxDay + 1, 7))
            if cronString == 'l':
                resRange = tmpWeekDict[5]
            elif 'l' in cronString:
                tmpWeekday = int(cronString.split('l'))
                resRange = [tmpWeekDict[tmpWeekday][-1]]
            else:
                tmpList = cronString.split(',')
                resRange = []
                for item in tmpList:
                    if '#' in item:
                        tmpWeekday, tmpWeek = [int(_) for _ in item.split('#')]
                        tmpWeekday = (tmpWeekday - 2) % 7
                        if tmpWeek > len(tmpWeekDict[tmpWeekday]) \
                                or tmpWeek <= 0:
                            raise ValueError('周cron表达式指定日期不存在')
                        else:
                            resRange += [tmpWeekDict[tmpWeekday][tmpWeek - 1]]
                    elif '-' in item:
                        tmpItemList = [int(_) for _ in item.split('-')]
                        tmpWeekdayList = list(range(tmpItemList[0],
                                                    tmpItemList[1] + 1))
                        for tmpWeekday in tmpWeekdayList:
                            tmpValue = (tmpWeekday - 2) % 7
                            resRange += tmpWeekDict[tmpValue]
                    else:
                        tmpWeekday = (int(item) - 2) % 7
                        resRange += tmpWeekDict[tmpWeekday]
            resRange.sort()
        return resRange, resStep
    
    @classmethod
    def nextValueGet(cls, value, rangeList):
        changeStatus = 0
        rangeList.sort()
        if value < rangeList[0]:
            res = rangeList[0]
            changeStatus = 2
        elif value > rangeList[-1]:
            res = rangeList[0]
            changeStatus = 1
        elif value in rangeList:
            res = value
        else:
            indexList = [(_ - value) < 0 for _ in rangeList]
            nextIndex = indexList.index(False)
            res = rangeList[nextIndex]
            changeStatus = 2
        return res, changeStatus
    
    @classmethod
    def lastValueGet(cls, value, rangeList):
        changeStatus = 0
        rangeList.sort()
        if value < rangeList[0]:
            res = rangeList[-1]
            changeStatus = 1
        elif value > rangeList[-1]:
            res = rangeList[-1]
            changeStatus = 2
        elif value in rangeList:
            res = value
        else:
            indexList = [(_ - value) < 0 for _ in rangeList]
            nextIndex = indexList.index(False)
            res = rangeList[nextIndex - 1]
            changeStatus = 2
        return res, changeStatus
    
    def dateUpdate(self, timeType, startTimeRange, endTimeRange=None):
        if endTimeRange is None:
            endTimeRange = startTimeRange
        startDict = {timeType: startTimeRange[0]}
        endDict = {timeType: endTimeRange[-1]}
        self.startTime = self.startTime.replace(**startDict)
        self.endTime = self.endTime.replace(**endDict)
    
    def cronReasoning(self):
        cronYear = self.cronJudge(self.cronDict['year'], 'year')
        defaultMinYear = self.timePoint.year
        yearRange, yearStep = self.cronRange(cronYear, defaultMinYear)
        self.timeStep['years'] = yearStep
        self.timeRange['year'] = yearRange
        self.dateUpdate('year', yearRange)
        cronMonth = self.cronJudge(self.cronDict['month'], 'month')
        monthRange, monthStep = self.cronRange(cronMonth, 1, 12)
        self.timeStep['months'] = monthStep
        self.timeRange['month'] = monthRange
        self.dateUpdate('month', monthRange)
        endYear = yearRange[-1]
        endMonth = monthRange[-1]
        startYear = yearRange[0]
        startMonth = monthRange[0]
        cronDay = self.cronDict['day']
        cronWeekday = self.cronDict['weekday']
        startDayRange, dayStep = self.cronDayRange(cronDay, startMonth,
                                                   startYear, cronWeekday)
        endDayRange, _ = self.cronDayRange(cronDay, endMonth,
                                           endYear, cronWeekday)
        self.dateUpdate('day', startDayRange, endDayRange)
        self.timeStep['days'] = dayStep
        defaultMin = {'hour': 0, 'minute': 0, 'second': 0}
        defaultMax = {'hour': 23, 'minute': 59, 'second': 59}
        for timeType in ['hour', 'minute', 'second']:
            cronStr = self.cronJudge(self.cronDict[timeType], timeType)
            cronRange, cronStep = self.cronRange(cronStr, defaultMin[
                timeType], defaultMax[timeType])
            self.timeStep[timeType + 's'] = cronStep
            self.timeRange[timeType] = cronRange
            self.dateUpdate(timeType, cronRange)
    
    def nextTimeUpdate(self, timePoint=None):
        self.timePointGet(timePoint)
        timeTypeList = ['year', 'month', 'day', 'hour', 'minute', 'second']
        upSymbol = [None] * 6
        indicator = 1
        initList = [self.nextCloseTime.__getattribute__(_)
                    for _ in timeTypeList]
        index = 0
        while indicator:
            value = initList[index]
            if index != 2:
                valueRange = self.timeRange[timeTypeList[index]]
            else:
                year = initList[0]
                month = initList[1]
                cronDay = self.cronDict['day']
                cronWeekday = self.cronDict['weekday']
                valueRange, _ = self.cronDayRange(cronDay, month,
                                                  year, cronWeekday)
            if upSymbol[index] is None:
                nextValue, changeStatus = self.nextValueGet(value, valueRange)
            else:
                nextValue = valueRange[0]
                changeStatus = 0
            initList[index] = nextValue
            if changeStatus > 0:
                upSymbol = upSymbol[:(index + 1)] + ['min'] * (5 - index)
                if changeStatus == 1:
                    index -= 1
                    if index < 0:
                        initList[index + 1] = None
                        indicator = 0
                    else:
                        initList[index] += 1
            else:
                index += 1
                if index == 6:
                    indicator = 0
        if initList[0] is None:
            return None
        while 'min' in upSymbol:
            minIndex = upSymbol.index('min')
            if minIndex != 2:
                valueRange = self.timeRange[timeTypeList[minIndex]]
            else:
                year = initList[0]
                month = initList[1]
                cronDay = self.cronDict['day']
                cronWeekday = self.cronDict['weekday']
                valueRange, _ = self.cronDayRange(cronDay, month,
                                                  year, cronWeekday)
            upSymbol[minIndex] = None
            initList[minIndex] = valueRange[0]
        return dt.datetime(*initList)
    
    def lastTimeUpdate(self, timePoint=None):
        self.timePointGet(timePoint)
        timeTypeList = ['year', 'month', 'day', 'hour', 'minute', 'second']
        downSymbol = [None] * 6
        indicator = 1
        initList = [self.lastCloseTime.__getattribute__(_)
                    for _ in timeTypeList]
        index = 0
        while indicator:
            value = initList[index]
            if index != 2:
                valueRange = self.timeRange[timeTypeList[index]]
            else:
                year = initList[0]
                month = initList[1]
                cronDay = self.cronDict['day']
                cronWeekday = self.cronDict['weekday']
                valueRange, _ = self.cronDayRange(cronDay, month,
                                                  year, cronWeekday)
            if downSymbol[index] is None:
                lastValue, changeStatus = self.lastValueGet(value, valueRange)
            else:
                lastValue = valueRange[-1]
                changeStatus = 0
            initList[index] = lastValue
            if changeStatus > 0:
                downSymbol = downSymbol[:(index + 1)] + ['max'] * (5 - index)
                if changeStatus == 1:
                    index -= 1
                    if index < 0:
                        initList[index + 1] = None
                        indicator = 0
                    else:
                        initList[index] -= 1
            else:
                index += 1
                if index == 6:
                    indicator = 0
        if initList[0] is None:
            return None
        while 'max' in downSymbol:
            maxIndex = downSymbol.index('max')
            if maxIndex != 2:
                valueRange = self.timeRange[timeTypeList[maxIndex]]
            else:
                year = initList[0]
                month = initList[1]
                cronDay = self.cronDict['day']
                cronWeekday = self.cronDict['weekday']
                valueRange, _ = self.cronDayRange(cronDay, month,
                                                  year, cronWeekday)
            downSymbol[maxIndex] = None
            initList[maxIndex] = valueRange[-1]
        return dt.datetime(*initList)
    
    def nextTimeGet(self, total=5):
        timeStep = {_: self.timeStep[_] for _ in self.timeStep if
                    self.timeStep[_] is not None}
        resList = []
        timePoint = self.timePoint
        if not timeStep:
            for num in range(total):
                res = self.nextTimeUpdate(timePoint)
                resList.append(res)
                timePoint = res
        else:
            res = self.nextTimeUpdate(timePoint)
            for num in range(total):
                resList.append(res)
                res += rtd(**timeStep)
        return resList
    
    def lastTimeGet(self, total=5):
        timeStep = {_: self.timeStep[_] for _ in self.timeStep if
                    self.timeStep[_] is not None}
        resList = []
        timePoint = self.timePoint
        if not timeStep:
            for num in range(total):
                res = self.lastTimeUpdate(timePoint)
                resList.append(res)
                timePoint = res - rtd(seconds=1)
        else:
            res = self.lastTimeUpdate(timePoint)
            for num in range(total):
                resList.append(res)
                res -= rtd(**timeStep)
        return resList


def dtStandard(timePoint):
    timeType = ['year', 'month', 'day', 'hour', 'minute', 'second']
    timeList = [timePoint.__getattribute__(_) for _ in timeType]
    timeDict = dict(zip(timeType, timeList))
    return dt.datetime(**timeDict)


def dtDeltaStandard(timeDelta):
    days = timeDelta.days
    rawSeconds = timeDelta.seconds
    hours = rawSeconds // 3600
    minutes = (rawSeconds - 3600 * hours) // 60
    seconds = rawSeconds - 3600 * hours - 60 * minutes
    resStr = (days > 0) * '{}日 '.format(days)
    resStr += (days + hours > 0) * '{}时'.format(hours)
    resStr += (days + hours + minutes > 0) * '{}分'.format(minutes)
    resStr += '{}秒'.format(seconds)
    return resStr


def subDict(dic, subKey, **kwargs):
    """
    用于从给定字典中提取子集并根据给定的旧键-新键对重命名键
    :param dic: 给定字典
    :param subKey: 需要从字典中提取的键集合，若为'all'，则遍历给定字典中的全部键
    :param kwargs: 给定的需要重命名的旧键-新键对
    :return: 提取出的重命名键后的子集
    示例：
    dic: {'a': 2, 'b': 3, 'C': 4}
    subKey: 'all'
    kwargs: {'C': 'c'}
    return: {'a': 2, 'b': 3, 'c': 4}
    """
    resDict = {}
    if subKey == 'all':
        subKey = list(dic.keys())
    for key in subKey:
        if kwargs.get(key):
            resDict[kwargs.get(key)] = dic.get(key)
        elif key in dic.keys():
            resDict[key] = dic.get(key)
    return resDict


def allstrip(string, lis):
    res = string.strip()
    for char in lis:
        res = res.strip(str(char))
    return res


def sqlResultToString(row):
    res = []
    if not row:
        return None
    for item in row:
        if isinstance(item, bool):
            res.append('\"' + str(int(item)) + '\"')
        elif item is None:
            res.append('NULL')
        elif isinstance(item, (list, tuple, dict, set)):
            res.append(str(item))
        else:
            try:
                tmpStr = item.striftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                tmpStr = str(item)
            for specialSymbol in ['\"', '\\', '\'']:
                if specialSymbol in tmpStr:
                    tmpStr = tmpStr.replace(specialSymbol, specialSymbol * 2)
            res.append('\"%s\"' % str(tmpStr))
    resString = '(' + ', '.join(res) + ')'
    return resString


def rowToDict(row, col):
    return dict(zip(col, [f'{_}' for _ in row]))


def fieldsToList(fieldsStr):
    if fieldsStr:
        if not isinstance(fieldsStr, str):
            try:
                fieldsStr = str(fieldsStr)
            except Exception:
                raise ValueError('该字段组无法转换为列表格式')
        string = allstrip(fieldsStr, ['[', ']', '(', ')', '{', '}'])
        resList = string.split(',')
        resList = [allstrip(_, ['`', '\'', '\"']) for _ in resList]
    else:
        resList = None
    return resList


def dictTransform(dic, transformDict=None, **kwargs):
    if transformDict is None:
        transformDict = dict()
    for key in dic:
        func = None
        if key in transformDict:
            if isinstance(transformDict[key], dict):
                def tmpFunc(value):
                    others = transformDict[key].get('#others#', None)
                    return transformDict[key].get(value, others)
                
                func = tmpFunc
            else:
                func = transformDict[key]
        dic[key] = transform(dic[key], func=func, **kwargs)
    return dic


def transform(obj, func=None, **kwargs):
    """
    提供对象，根据对象的类型和一些额外的关键字参数，将对象转换为符合条件的格式
    :param obj: 提供对象
    :param kwargs:
        boolTrans参数用于对布尔类型数据进行文字的‘是、否‘显示转换；
        passwordTrans参数用于对密码类型数据进行明、密文转换
    :param func: 用于提供对数据进行转换的函数
    :return: 转换后的对象
    """
    if isinstance(obj, dt.datetime):
        res = obj.strftime("%Y-%m-%d %H:%M:%S") if func is None else func(obj)
    elif isinstance(obj, dt.date):
        res = obj.strftime("%Y-%m-%d") if func is None else func(obj)
    elif isinstance(obj, dt.time):
        res = obj.strftime("%H:%M:%S") if func is None else func(obj)
    elif isinstance(obj, bool):
        valueDict = {True: '是', False: '否'}
        if kwargs.get('boolTrans'):
            res = valueDict[obj] if func is None else func(obj)
        else:
            res = obj if func is None else func(obj)
    elif isinstance(obj, Password):
        if not kwargs.get('passwordTrans'):
            res = obj.text
        else:
            res = obj.transform()
    elif isinstance(obj, (int, float, str)):
        res = obj if func is None else func(obj)
    elif obj is None:
        res = None if func is None else func(obj)
    else:
        try:
            res = str(obj) if func is None else func(obj)
        except Exception:
            raise ValueError('该对象无法根据提供的函数进行格式转换！')
    return res


filterSymbolSet = {'exact', 'iexact', 'contains', 'icontains',
                   'gt', 'gte', 'lt', 'lte', 'in',
                   'startswith', 'istartswith', 'endswith', 'iendswith',
                   'range', 'year', 'month', 'day', 'isnull'}


def filterFieldAnalysis(filterField, value, fieldsSet, cursor=False,
                        collation='utf8mb4_general_ci'):
    if not isinstance(filterField, str):
        try:
            filterField = str(filterField)
        except Exception:
            raise TypeError('不支持对该字段类型解析！')
    suffix = filterField.split('__')[-1]
    fieldText = filterField[:-(len(suffix) + 2)]
    if suffix[:3] == 'not':
        notSymbol = True
        suffix = suffix[3:]
    else:
        notSymbol = False
    if suffix not in filterSymbolSet:
        fieldText = filterField
        suffix = 'icontains'
    if not cursor:
        if suffix == 'not':
            tmpDict = {fieldText: value}
            res = ~Q(**tmpDict)
            return res
        if '_OR_' in fieldText:
            fieldList = [_.strip() for _ in fieldText.split('_OR_')]
            try:
                res = None
                for field in fieldList:
                    if field in fieldsSet:
                        if field == fieldList[0]:
                            tmpDict = {(field + '__' + suffix): value}
                            if not notSymbol:
                                res = Q(**tmpDict)
                            else:
                                res = ~Q(**tmpDict)
                        else:
                            tmpDict = {(field + '__' + suffix): value}
                            if not notSymbol:
                                res1 = Q(**tmpDict)
                            else:
                                res1 = ~Q(**tmpDict)
                            res = res | res1
                return res
            except Exception as e:
                raise ValueError('联合检索异常，请检查搜索字段与搜索条件！')
        elif fieldText in fieldsSet:
            tmpDict = {(fieldText + '__' + suffix): value}
            if not notSymbol:
                res = Q(**tmpDict)
            else:
                res = ~Q(**tmpDict)
            return res
        else:
            return None
    else:
        res = ''
        if not (value or (suffix == 'isnull')):
            return res
        if '_OR_' in fieldText:
            fieldList = [_.strip() for _ in fieldText.split('_OR_')]
        else:
            fieldList = [fieldText]
        sqlList = []
        for field in fieldList:
            field = '`' + field + '`'
            field += ' collate %s' % collation
            if suffix == 'exact':
                if notSymbol:
                    sqlText = 'binary %s<>"%s"' % (field, str(value))
                else:
                    sqlText = 'binary %s="%s"' % (field, str(value))
            elif suffix == 'iexact':
                if notSymbol:
                    sqlText = '%s<>"%s"' % (field, str(value))
                else:
                    sqlText = '%s="%s"' % (field, str(value))
            elif suffix == 'contains':
                if notSymbol:
                    sqlText = 'binary %s not like "%%%s%%"' % (field,
                                                               str(value))
                else:
                    sqlText = 'binary %s like "%%%s%%"' % (field, str(value))
            elif suffix == 'icontains':
                if notSymbol:
                    sqlText = '%s not like "%%%s%%"' % (field, str(value))
                else:
                    sqlText = '%s like "%%%s%%"' % (field, str(value))
            elif suffix == 'gt':
                if notSymbol:
                    sqlText = '%s<="%s"' % (field, str(value))
                else:
                    sqlText = '%s>"%s"' % (field, str(value))
            elif suffix == 'gte':
                if notSymbol:
                    sqlText = '%s<"%s"' % (field, str(value))
                else:
                    sqlText = '%s>="%s"' % (field, str(value))
            elif suffix == 'lt':
                if notSymbol:
                    sqlText = '%s>="%s"' % (field, str(value))
                else:
                    sqlText = '%s<"%s"' % (field, str(value))
            elif suffix == 'lte':
                if notSymbol:
                    sqlText = '%s>"%s"' % (field, str(value))
                else:
                    sqlText = '%s<="%s"' % (field, str(value))
            elif suffix == 'startswith':
                if notSymbol:
                    sqlText = 'binary %s not like "%s%%"' % (field,
                                                             str(value))
                else:
                    sqlText = 'binary %s like "%s%%"' % (field, str(value))
            elif suffix == 'istartswith':
                if notSymbol:
                    sqlText = '%s not like "%s%%"' % (field, str(value))
                else:
                    sqlText = '%s like "%s%%"' % (field, str(value))
            elif suffix == 'endswith':
                if notSymbol:
                    sqlText = 'binary %s not like "%%%s"' % (field,
                                                             str(value))
                else:
                    sqlText = 'binary %s like "%%%s"' % (field, str(value))
            elif suffix == 'iendswith':
                if notSymbol:
                    sqlText = '%s not like "%%%s"' % (field, str(value))
                else:
                    sqlText = '%s like "%%%s"' % (field, str(value))
            elif suffix == 'in':
                valueText = '("' + '","'.join(value) + '")'
                if notSymbol:
                    sqlText = '%s not in %s' % (field, str(valueText))
                else:
                    sqlText = '%s in %s' % (field, str(valueText))
            elif suffix == 'range':
                if notSymbol:
                    sqlText = '%s not between "%s" and "%s"' \
                              % (field, str(value[0]), str(value[1]))
                else:
                    sqlText = '%s between "%s" and "%s"' % (
                        field, str(value[0]),
                        str(value[1]))
            elif suffix == 'year':
                if notSymbol:
                    sqlText = 'year(%s)<>"%s"' % (field, str(value))
                else:
                    sqlText = 'year(%s)="%s"' % (field, str(value))
            elif suffix == 'month':
                if notSymbol:
                    sqlText = 'month(%s)<>"%s"' % (field, str(value))
                else:
                    sqlText = 'month(%s)="%s"' % (field, str(value))
            elif suffix == 'day':
                if notSymbol:
                    sqlText = 'day(%s)<>"%s"' % (field, str(value))
                else:
                    sqlText = 'day(%s)="%s"' % (field, str(value))
            else:
                if value:
                    sqlText = 'isnull(%s)' % field
                else:
                    sqlText = 'not isnull(%s)' % field
            sqlList.append('(' + sqlText + ')')
        if len(sqlList) == 1:
            sqlFullText = sqlList[0]
        else:
            sqlFullText = '(' + ' or '.join(sqlList) + ')'
        return sqlFullText


def fieldSqlGet(field, fieldRename='', fieldValueMap=None):
    if fieldValueMap is None or (not isinstance(fieldValueMap, dict)):
        fieldValueMap = {}
    if fieldRename != '':
        fieldRename = f' as `{fieldRename}`'
    fieldStr = '`' + field + '`'
    if len(fieldValueMap) > 0:
        fieldStr = '(case ' + fieldStr
        for value in fieldValueMap:
            if value != '#others#':
                fieldStr += f'\nwhen \'{value}\' ' \
                            f'then \'{fieldValueMap[value]}\''
            else:
                fieldStr += f'else \'{fieldValueMap[value]}\''
        fieldStr += '\nend)'
    res = f'{fieldStr}{fieldRename}'
    return res


def defaultCronGet():
    defaultTaskTime = curTimeGet() + dt.timedelta(days=1)
    year = defaultTaskTime.year
    month = defaultTaskTime.month
    day = defaultTaskTime.day
    defaultCron = '0 0 0 %d %d ? %d' % (day, month, year)
    return defaultCron


class StandardFilters:
    def __init__(self, queryset, fieldsSet):
        if isinstance(queryset, models.QuerySet):
            self.queryset = queryset
        else:
            try:
                self.queryset = dict(queryset)
            except Exception:
                raise TypeError('查询结果类型错误！')
        if isinstance(fieldsSet, list):
            self.fieldsSet = fieldsSet
        else:
            self.fieldsSet = set()
    
    def filterBy(self, filterDict=None, orderDict=None, distinct=False):
        if (not filterDict) or (not isinstance(filterDict, dict)):
            filterDict = {}
        if (not orderDict) or (not isinstance(orderDict, dict)):
            orderDict = {}
        res = self.queryset
        if len(filterDict) != 0:
            try:
                qList = []
                pDict = {}
                for filterField in filterDict:
                    tmp = filterFieldAnalysis(filterField,
                                              filterDict[filterField],
                                              self.fieldsSet)
                    if isinstance(tmp, Q):
                        qList.append(tmp)
                    elif isinstance(tmp, dict):
                        pDict.update(tmp)
                res = res.filter(*qList, **pDict)
            except Exception:
                return ValueError('筛选条件不在可选范围内！')
        if len(orderDict) != 0:
            try:
                orderParams = []
                for orderField in orderDict:
                    if orderDict[orderField].lower() == 'desc':
                        orderParams.append('-' + orderField)
                    else:
                        orderParams.append(orderField)
                res = res.order_by(*orderParams)
            except Exception:
                return ValueError('排序条件不在可选范围内！')
        if distinct:
            res = res.distinct()
        return res


class StandardPagination:
    
    def __init__(self, queryset, orderDict, transFuncDict, pageVolume=None):
        if isinstance(queryset, models.QuerySet):
            orderList = []
            for orderField in orderDict:
                if orderDict[orderField] == 'desc':
                    orderList.append('-' + orderField)
                else:
                    orderList.append(orderField)
            self.queryset = queryset.order_by(*orderList)
        else:
            try:
                self.queryset = queryset
                for orderField in orderDict:
                    if orderDict[orderField] == 'desc':
                        reverse = True
                    else:
                        reverse = False
                    self.queryset = sorted(self.queryset,
                                           key=lambda x: x[orderField],
                                           reverse=reverse)
            except Exception:
                raise TypeError('查询结果类型错误！')
        if pageVolume is None:
            pageVolume = 20
        else:
            try:
                pageVolume = int(pageVolume)
            except Exception:
                raise TypeError('页码容量类型错误！')
        self.pageVolume = pageVolume
        self.querysetOutput = \
            [dictTransform(_, boolTrans=True, transformDict=transFuncDict)
             for _ in self.queryset]
        self.totalVolume = int(len(self.queryset))
        self.totalPage = self.totalVolume // self.pageVolume + \
                         (self.totalVolume % self.pageVolume != 0)
    
    def updateOutput(self, newPageVolume=None):
        if newPageVolume:
            try:
                pageVolume = int(newPageVolume)
                if pageVolume != self.pageVolume:
                    self.pageVolume = pageVolume
                    self.totalPage = self.totalVolume // self.pageVolume + \
                                     (self.totalVolume % self.pageVolume != 0)
            except Exception:
                raise TypeError('页码容量类型错误！')
    
    def getOutput(self, currentPage=None):
        if currentPage is None:
            return self.querysetOutput
        try:
            currentPage = int(currentPage)
        except Exception:
            raise TypeError('当前页码类型错误！')
        if 1 <= currentPage:
            if currentPage > self.totalPage:
                currentPage = 1
            start = (currentPage - 1) * self.pageVolume
            end = start + self.pageVolume
            return self.querysetOutput[start:end], \
                   self.totalVolume, self.totalPage
        else:
            raise ValueError('页码超出范围！')


class GeneralView(ViewSet):
    def __init__(self, **kwargs):
        super().__init__()
        
        self.model = kwargs.get('model', None)
        self.fields = kwargs.get('fields', None)
        self.totalFields = []
        self.queryset = kwargs.get('queryset', None)
        self.msg = generalJson()
        self.comment = kwargs.get('comment', None)
        self.uniqueFields = dict()
        self.transFuncDict = dict()
        self.indexFields = dict()
        self.orderFields = {'time_last_updated': 'desc'}
    
    @classmethod
    def requestAnalysis(cls, request):
        headParams = request.GET
        bodyContent = request.data
        if bodyContent:
            bodyParams = dict(bodyContent)
        else:
            bodyParams = dict()
        method = request.method
        return headParams, bodyParams, method
    
    def uniqueJudge(self, formData, ID=None, logicDeleteField='is_deleted'):
        formDataRes = formData.copy()
        if ID is not None:
            try:
                dataToBeModified = self.model.objects.get(id=ID)
            except Exception as e:
                e.args.append(f'指定{self.comment}条目不存在！')
                raise e
            for field in formData:
                old = getattr(dataToBeModified, field)
                new = formData[field]
                if old == new:
                    del formDataRes[field]
        for fieldGroup in self.uniqueFields:
            uniqueDict = {logicDeleteField: 0}
            for field in fieldGroup:
                if formDataRes.get(field, None) is not None:
                    uniqueDict[field] = formDataRes.get(field)
            if len(uniqueDict) > 1:
                existedRes = self.model.objects.filter(**uniqueDict)
                if len(existedRes) > 0:
                    return False, fieldGroup, formDataRes
        return True, None, formDataRes
    
    @action(methods=['get', 'post'], detail=False, url_path='detail')
    def getDetail(self, request, *args, **kwargs):
        headParams, bodyParams, method = self.requestAnalysis(request)
        currentPage = headParams.get('pageNum', bodyParams.get('page_num', 1))
        pageSize = headParams.get('pageSize', bodyParams.get('page_size', 10))
        showTotalItem = kwargs.get('totalItem', False)
        if showTotalItem:
            pageSize = 100000
        if method == 'GET':
            if headParams.get('id'):
                queryset = self.model.objects.filter(id=headParams.get(
                    'id'), is_deleted=0).values()
                data = queryset[0]
                self.msg['data'] = dictTransform(
                    data, boolTrans=False, transformDict=self.transFuncDict)
                self.msg['msg'] = '指定%s信息获取成功' % self.comment
            else:
                pageClass = StandardPagination(self.queryset,
                                               self.orderFields,
                                               self.transFuncDict,
                                               pageSize)
                data, totalVolume, totalPage = pageClass.getOutput(currentPage)
                self.msg['data'] = {'data': data,
                                    'total': totalVolume}
                self.msg['msg'] = '%s信息获取成功' % self.comment
        elif method == 'POST':
            for param in headParams:
                if param not in ['pageNum', 'pageSize']:
                    raise ValueError('POST方法不支持请求头参数传递')
            orderDict = bodyParams.get('order_by', dict())
            filterDict = bodyParams.get('filter_by', dict())
            indexDict = bodyParams.get('index_by', dict())
            for indexField in indexDict:
                if indexField in self.indexFields:
                    indexList = indexDict[indexField]
                    if not isinstance(indexList, list):
                        indexList = [indexList]
                    filterDict[indexField + '__in'] = \
                        self.indexFields[indexField](indexList)
            filterClass = StandardFilters(self.queryset, self.fields)
            queryset = filterClass.filterBy(filterDict, orderDict)
            a = len(queryset)
            if len(self.orderFields) > 0:
                for field in self.orderFields:
                    if field not in orderDict:
                        orderDict[field] = self.orderFields[field]
            pageClass = StandardPagination(queryset, orderDict,
                                           self.transFuncDict, pageSize)
            data, totalVolume, totalPage = \
                pageClass.getOutput(currentPage)
            self.msg['data'] = {'data': data,
                                'total': totalVolume,
                                'param': bodyParams}
            self.msg['msg'] = '%s筛选信息获取成功' % self.comment
        httpOutput = kwargs.get('httpOutput', True)
        if httpOutput is True:
            return HttpResponse(json.dumps(self.msg, ensure_ascii=False))
    
    @action(methods=['post'], detail=False, url_path='modify')
    def modify(self, request, *args, **kwargs):
        headParams, bodyParams, method = self.requestAnalysis(request)
        formParams = kwargs.get('formParams', None)
        if formParams is None:
            formParams = subDict(bodyParams, self.totalFields)
        ID = headParams.get('id', None)
        if ID is None:
            raise ValueError(f'未提供待修改{self.comment}条目id')
        dataToBeModified = self.model.objects.get(id=ID)
        uniqueJudge, fieldGroup, formParams = self.uniqueJudge(formParams, ID)
        if not uniqueJudge:
            self.msg['msg'] = f'{self.comment}修改失败' \
                              f'，{self.uniqueFields[fieldGroup]}已存在'
            self.msg['statusCode'] = 400
            return HttpResponse(json.dumps(self.msg, ensure_ascii=False))
        for param in formParams:
            setattr(dataToBeModified, param, formParams[param])
            dataToBeModified.time_last_updated = curTimeGet()
            dataToBeModified.save()
        queryset = self.queryset.filter(id=ID).values()
        data = queryset[0]
        self.msg['data'] = dictTransform(data, boolTrans=False,
                                         transformDict=self.transFuncDict)
        self.msg['msg'] = '指定%s信息修改成功' % self.comment
        httpOutput = kwargs.get('httpOutput', True)
        if httpOutput is True:
            return HttpResponse(json.dumps(self.msg, ensure_ascii=False))
        else:
            return ID
    
    @action(methods=['post'], detail=False, url_path='add')
    def add(self, request, *args, **kwargs):
        headParams, bodyParams, method = self.requestAnalysis(request)
        formParams = kwargs.get('formParams', None)
        if formParams is None:
            formParams = subDict(bodyParams, self.totalFields)
        uniqueJudge, fieldGroup, formParams = self.uniqueJudge(formParams)
        if not uniqueJudge:
            self.msg['msg'] = f'{self.comment}新增失败' \
                              f'，{self.uniqueFields[fieldGroup]}已存在'
            self.msg['statusCode'] = 400
            return HttpResponse(json.dumps(self.msg, ensure_ascii=False))
        try:
            res = self.model.objects.create(**formParams)
            newID = res.id
            data = self.queryset.filter(id=newID).values()[0]
            self.msg['data'] = dictTransform(data, boolTrans=False,
                                             transformDict=self.transFuncDict)
            self.msg['msg'] = '%s新增成功' % self.comment
        except Exception as e:
            self.msg['msg'] = '%s新增失败' % self.comment
            self.msg['statusCode'] = 400
            self.msg['statusInfo'] = errorOutput(e)
            return HttpResponse(json.dumps(self.msg, ensure_ascii=False))
        httpOutput = kwargs.get('httpOutput', True)
        if httpOutput is True:
            return HttpResponse(json.dumps(self.msg, ensure_ascii=False))
        else:
            return newID
    
    @action(methods=['post'], detail=False, url_path='delete')
    def delete(self, request, *args, **kwargs):
        headParams, bodyParams, method = self.requestAnalysis(request)
        IDList = kwargs.get('id', bodyParams.get('id'))
        deleteType = kwargs.get('deleteType', bodyParams.get(
            'delete_type', 'delete'))
        relatedField = kwargs.get('relatedField', None)
        if not IDList:
            IDList = []
        if not isinstance(IDList, list):
            IDList = [IDList]
        if deleteType in ['cascade', 'null', 'highest']:
            query = self.model.objects.filter(id__in=IDList)
            fieldDelete(self.model, query, deleteType=deleteType)
        elif deleteType in ['upper']:
            for ID in IDList:
                query = self.model.objects.filter(id=ID)
                try:
                    upperValue = query.values().first().\
                        get(f'{relatedField}_id')
                except Exception:
                    raise ValueError('无法获得上级字段的取值！')
                fieldDelete(self.model, query, deleteType=deleteType,
                            upperValue=upperValue)
        else:
            query = self.model.objects.filter(id__in=IDList)
            fieldDelete(self.model, query, deleteType='cascade')
        self.msg['msg'] = '指定%s删除成功' % self.comment
        httpOutput = kwargs.get('httpOutput', True)
        if httpOutput is True:
            return HttpResponse(json.dumps(self.msg, ensure_ascii=False))
    
    @action(methods=['post'], detail=False, url_path='test')
    def test(self, request, *args, **kwargs):
        msg = generalJson('这是%s测试接口') % self.comment
        return HttpResponse(json.dumps(msg, ensure_ascii=False))


class PasswordView(GeneralView):
    def __init__(self, **kwargs):
        super().__init__()
        
        self.passwordFields = kwargs.get('passwordFields', tuple())
        self.verifyFunc = kwargs.get('passwordFields', dict())
    
    @action(methods=['post'], detail=False, url_path='modify')
    def modify(self, request, *args, **kwargs):
        headParams, bodyParams, method = self.requestAnalysis(request)
        formParams = kwargs.get('formParams', None)
        if formParams is None:
            formParams = subDict(bodyParams, self.totalFields)
        for field in self.passwordFields:
            self.transFuncDict.update(
                {field: lambda x: Password(x, 'Decrypt').transform()})
            if field in formParams:
                formParams[field] = \
                    Password(formParams[field], 'Encrypt').transform()
        ID = super().modify(request, *args, formParams=formParams,
                            httpOutput=False, **kwargs)
        for field in self.passwordFields:
            self.verifyFunc[field](ID)
        return HttpResponse(json.dumps(self.msg, ensure_ascii=False))
    
    @action(methods=['post'], detail=False, url_path='add')
    def add(self, request, *args, **kwargs):
        headParams, bodyParams, method = self.requestAnalysis(request)
        formParams = kwargs.get('formParams', None)
        if formParams is None:
            formParams = subDict(bodyParams, self.totalFields)
        ID = super().add(request, *args, formParams=formParams,
                         httpOutput=False, **kwargs)
        for field in self.passwordFields:
            self.verifyFunc[field](ID)
        return HttpResponse(json.dumps(self.msg, ensure_ascii=False))


def subItemGet(model, relatedTuple, valueList):
    resList = valueList
    if not isinstance(valueList, list):
        resList = [valueList]
    stepList = resList.copy()
    while len(stepList) > 0:
        filterDict = {relatedTuple[0] + '__in': stepList,
                      'is_deleted': 0}
        stepQueryset = model.objects.filter(**filterDict).values()
        stepList = [_[relatedTuple[1]] for _ in stepQueryset]
        resList += stepList
    return resList


class IndexView(GeneralView):
    def __init__(self, **kwargs):
        super().__init__()
        
        self.relationField = tuple()
    
    def subItemGet(self, valueList):
        return subItemGet(self.model, self.relationField, valueList)
    
    @action(methods=['get', 'post'], detail=False, url_path='detail')
    def getDetail(self, request, *args, **kwargs):
        return super().getDetail(request, *args, totalItem=True, **kwargs)
    
    @action(methods=['post'], detail=False, url_path='delete')
    def delete(self, request, *args, **kwargs):
        headParams, bodyParams, method = self.requestAnalysis(request)
        idList = bodyParams.get('id')
        deleteType = bodyParams.get('delete_type', 'cascade')
        if deleteType == 'null':
            deleteType = 'highest'
        relatedField = self.relationField[0]
        return super().delete(request, *args, id=idList,
                              deleteType=deleteType,
                              relatedField=relatedField, **kwargs)


class VersionView(GeneralView):
    def __init__(self, **kwargs):
        super().__init__()
        
        # 键为版本字段，值为控制版本是否使用的布尔字段
        self.versionFields = kwargs.get('versionFields', dict())
        self.versionFunc = {_: versionControl for _ in self.versionFields}
        self.querysetForVersion = None
        self.querysetForTotal = None
    
    @action(methods=['post'], detail=False, url_path='versionadd')
    def versionAdd(self, request, *args, **kwargs):
        headParams, bodyParams, method = self.requestAnalysis(request)
        formParams = kwargs.get('formParams', None)
        existedVersionList = bodyParams.get('existed_list', [])
        versionType = bodyParams.get('version_type')
        if formParams is None:
            formParams = subDict(bodyParams, self.totalFields)
        existedID = formParams.get('id', None)
        existedItem = dict()
        if existedID is not None:
            del formParams['id']
            existedItem = self.model.objects.get(id=existedID)
        for field in self.versionFields:
            if existedID is not None:
                setattr(existedItem, self.versionFields[field], 0)
                existedItem.time_last_updated = curTimeGet()
                existedItem.save()
            if formParams.get(field, None) is None:
                formParams[field] = \
                    self.versionFunc[field](versionType='initial')
            else:
                formParams[field] = \
                    self.versionFunc[field](formParams[field],
                                            versionType=versionType,
                                            existedList=existedVersionList)
            formParams[self.versionFields[field]] = True
        return super().add(request, *args, formParams=formParams, **kwargs)
    
    @action(methods=['get', 'post'], detail=False, url_path='versionswitch')
    def versionSwitch(self, request, *args, **kwargs):
        headParams, bodyParams, method = self.requestAnalysis(request)
        oldID = headParams.get('current_id', bodyParams.get('current_id'))
        newID = headParams.get('switch_id', bodyParams.get('switch_id'))
        if method == 'GET':
            self.msg['data'] = {'comment':
                self.model.objects.filter(id=newID).first().extra_comment}
            return HttpResponse(json.dumps(self.msg, ensure_ascii=False))
        extraComment = bodyParams.get('extra_comment')
        oldItem = self.model.objects.get(id=oldID)
        newItem = self.model.objects.get(id=newID)
        newItem.extra_comment = f'{newItem.extra_comment}\n' \
                                f'【版本切换时间】{curTimeGet()}\n' \
                                f'【版本切换原因】{extraComment}'
        for field in self.versionFields:
            setattr(oldItem, self.versionFields[field], 0)
            setattr(newItem, self.versionFields[field], 1)
            oldItem.time_last_updated = curTimeGet()
            newItem.time_last_updated = curTimeGet()
            oldItem.save()
            newItem.save()
        queryset = self.querysetForVersion.filter(id=newID).values()
        data = queryset[0]
        self.msg['data'] = dictTransform(data, boolTrans=False,
                                         transformDict=self.transFuncDict)
        self.msg['msg'] = '指定%s版本切换成功' % self.comment
        return HttpResponse(json.dumps(self.msg, ensure_ascii=False))
    
    @action(methods=['post'], detail=False, url_path='versiondetail')
    def versionDetail(self, request, *args, **kwargs):
        self.queryset = self.querysetForVersion
        headParams, bodyParams, method = self.requestAnalysis(request)
        formParams = kwargs.get('formParams', None)
        if formParams is None:
            formParams = subDict(bodyParams, self.totalFields)
        return super().getDetail(request, *args, formParams=formParams,
                                 httpOutput=True, **kwargs)
    
    @action(methods=['get', 'post'], detail=False, url_path='detail')
    def getDetail(self, request, *args, **kwargs):
        self.queryset = self.querysetForTotal
        headParams, bodyParams, method = self.requestAnalysis(request)
        formParams = kwargs.get('formParams', None)
        if formParams is None:
            formParams = subDict(bodyParams, self.totalFields)
        return super().getDetail(request, *args, formParams=formParams,
                                 httpOutput=True, **kwargs)


def versionControl(version=None, versionType='initial', existedList=None):
    if version is None or versionType == 'initial':
        return 'v1.0.0'
    if existedList is None:
        existedList = []
    try:
        mainSerial, subSerial, bugSerial = version[1:].split('.')
    except Exception as e:
        e.args.append('非法版本编号格式！')
        raise e
    indicator = 1
    while indicator:
        if versionType == 'main':
            mainSerial = int(mainSerial) + 1
            subSerial = 0
            bugSerial = 0
        elif versionType == 'sub':
            subSerial = int(subSerial) + 1
            bugSerial = 0
        elif versionType == 'bug':
            bugSerial = int(bugSerial) + 1
        res = f'v{mainSerial}.{subSerial}.{bugSerial}'
        if res not in existedList:
            indicator = 0
    return res


def jsonGenerate(jsonDict, filePath, mode='w', encoding='utf-8'):
    with open(filePath, mode=mode, encoding=encoding) as f:
        json.dump(jsonDict, f, indent=4)


def jsonRead(jsonFilePath, encoding='utf-8'):
    with open(jsonFilePath, mode='r', encoding=encoding) as f:
        data = json.load(f)
    return data
