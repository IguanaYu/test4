import time
import re

from functions.general import *


def datetimeTransform(string):
    try:
        return dt.datetime.strptime(string, '%Y-%m-%d %H:%M:%S')
    except Exception as e:
        e.args.append('该字符串无法转换为日期时间型格式数据！')
        raise e


def dateTransform(string):
    try:
        res = dt.datetime.strptime(string, '%Y-%m-%d %H:%M:%S')
    except Exception:
        try:
            res = dt.datetime.strptime(string, '%Y-%m-%d')
        except Exception as e:
            e.args.append('该字符串无法转换为日期型格式数据！')
            raise e
    return dt.datetime.date(res)


def timeTransform(string):
    try:
        res = dt.datetime.strptime(string, '%Y-%m-%d %H:%M:%S')
    except Exception:
        try:
            res = dt.datetime.strptime(string, '%H:%M:%S')
        except Exception as e:
            e.args.append('该字符串无法转换为时间型格式数据！')
            raise e
    return dt.datetime.time(res)


def enumerationListAnalysis(enumerationList):
    funcDict = {'str': str, 'int': int, 'float': float,
                'datetime': datetimeTransform,
                'date': dateTransform, 'time': timeTransform}
    resList = []
    for item in enumerationList:
        itemType = item['enumRangeType']
        oldValue = item['value']
        try:
            newValue = funcDict[itemType](oldValue)
            resList.append(newValue)
        except Exception as e:
            e.args.append(f'配置取值{oldValue}无法根据配置类型{itemType}进行转换！')
    return resList


def standardRangeJudge(value, **params):
    enumerationParams = params.get('enumeration', [])
    enumerationRange = enumerationListAnalysis(enumerationParams)
    intervalRange = params.get('interval', [])
    if value in enumerationRange:
        return True
    for interval in intervalRange:
        start = interval['intervalRangeStart']
        end = interval['intervalRangeEnd']
        try:
            start = float(start)
            end = float(end)
        except Exception:
            try:
                start = datetimeTransform(start)
                end = datetimeTransform(end)
            except Exception:
                raise ValueError('提供了错误类型的区间起点值或区间终点值！')
        if start > end:
            raise ValueError('区间范围起点值应不大于区间范围终点值！')
        if start <= value <= end:
            return True
    return False


def standardLengthJudge(value, **params):
    try:
        valueLength = len(value)
    except Exception:
        valueLength = 0
    return standardRangeJudge(valueLength, **params)


def standardNonEmptinessJudge(value, **params):
    emptyString = params.get('empty_string', '')
    emptyList = emptyString.split(',')
    judgeList = []
    for item in emptyList:
        if '\"' in item:
            judgeList.append(item.strip('\"'))
        elif '.' in item:
            try:
                judgeList.append(float(item))
            except Exception:
                judgeList.append(item)
        else:
            try:
                judgeList.append(int(item))
            except Exception:
                judgeList.append(item)
    if value is None:
        return False
    if value in judgeList:
        return False
    return True


def standardRepeatabilityJudge(value, **params):
    if value is None:
        value = []
    listCountSet = listCount(value)
    for countValue in listCountSet:
        judgeRes = standardRangeJudge(countValue, **params)
        if not judgeRes:
            return False
    return True


def standardTimelinessJudge(value, **params):
    try:
        timeFormat = params.get('format', '%Y-%m-%d %H:%M:%S')
        timeValue = time.strptime(value, timeFormat)
    except Exception:
        timeValue = value
    return standardRangeJudge(timeValue, **params)


def formatJudge(string, judgeValue, formatType):
    string = string.lower()
    judgeValue = judgeValue.lower()
    if formatType == 'icontains':
        if judgeValue in string:
            return True
    elif formatType == 'istartswith':
        if string[:len(judgeValue)] == judgeValue:
            return True
    elif formatType == 'iendswith':
        if string[-len(judgeValue):] == judgeValue:
            return True
    elif formatType == 'noticontains':
        if judgeValue not in string:
            return True
    elif formatType == 'notistartswith':
        if string[:len(judgeValue)] != judgeValue:
            return True
    elif formatType == 'iendswith':
        if string[-len(judgeValue):] != judgeValue:
            return True
    return False


def consistJudge(string, judgeValueList, specialSymbols=None):
    totalSet = {}
    if 'sz' in judgeValueList:
        totalSet.update(set('1234567890'))
    if 'dx' in judgeValueList:
        totalSet.update(set('ABCDEFGHIJKLMNOPQRSTUVWXYZ'))
    if 'xx' in judgeValueList:
        totalSet.update(set('abcdefghijklmnopqrstuvwxyz'))
    if specialSymbols is not None and len(specialSymbols) > 0:
        specialSymbols = repr(specialSymbols)[1:-1]
        specialSymbolList = specialSymbols.split(',')
        specialSymbolList.replace('制表符', '\t')
        specialSymbolList.replace('换行符', '\n')
        specialSymbolList.replace('回车符', '\r')
        totalSet.update(set(specialSymbolList))
    resistSet = set(string) - totalSet
    if len(resistSet) == 0:
        return True
    if 'hz' in judgeValueList:
        for item in resistSet:
            if '\u4e00' <= item <= '\u9fff':
                continue
            elif 'tszf' not in judgeValueList:
                return False
    return True
    

def standardFormatJudge(value, **params):
    formatList = params.get('enumeration', [])
    indicatorOR = False
    iterFormatList = iter(formatList)
    while not indicatorOR:
        try:
            formatGroup = next(iterFormatList)
        except StopIteration:
            return indicatorOR
        indicatorAND = True
        iterFormat = iter(formatGroup['conditionList'])
        while indicatorAND:
            try:
                strFormat = next(iterFormat)
            except StopIteration:
                indicatorAND = False
                indicatorOR = True
                strFormat = []
            formatType = strFormat['type']
            formatValue = strFormat['value']
            if formatType == 'consistof':
                consistType = strFormat['characterValue']
                specialSymbols = strFormat['specialCharacterValue']
                if not consistJudge(value, consistType, specialSymbols):
                    indicatorAND = False
            elif formatType == 'custom':
                rePattern = repr(formatValue)[1:-1]
                if not re.match(rePattern, value):
                    indicatorAND = False
            else:
                if not formatJudge(value, formatValue, formatType):
                    indicatorAND = False
    return indicatorOR


def standardFuncGet(standardType, path=None, funcName=None):
    typeFuncDict = {
        'range': standardRangeJudge,
        'length': standardLengthJudge,
        'repeatability': standardRepeatabilityJudge,
        'nonemptiness': standardNonEmptinessJudge,
        'timeliness': standardTimelinessJudge,
        'format': standardFormatJudge,
        }
    if standardType in typeFuncDict:
        return typeFuncDict[standardType]
    else:
        try:
            command = f'from {path} import {funcName}\ntempFunc = {funcName}'
            loc = locals()
            exec(command)
            tempFunc = loc['tempFunc']
            return tempFunc
        except Exception as e:
            e.args.append('无法根据指定的函数文件目录与函数文件名称获取标准函数！')
            raise e


class StandardClass:
    def __init__(self):
        self.levelDict = {'others': {'test_data': None,
                                     'params': dict(),
                                     'severity': '4'}}
        self.standardFunc = None
    
    def funcGet(self, func):
        self.standardFunc = func
    
    def testDataCheck(self, verifyDict):
        testData = verifyDict.get('test_data', None)
        params = verifyDict.get('params', dict())
        res = self.standardFunc(value=testData, **params)
        if res:
            return res, '数据标准验证通过'
        return False, '测试数据验证失败，请检查测试数据的提供与标准参数配置是否正确'
    
    def levelDictGet(self, levelDict):
        for level in levelDict:
            if level != 'others':
                if level in self.levelDict:
                    self.levelDict[level].update(levelDict[level])
                else:
                    self.levelDict[level] = levelDict[level]
            else:
                self.levelDict['others'].update(levelDict[level])
    
    def levelDictVerify(self):
        severityIndicator = 0
        for level in self.levelDict:
            verifyDict = {
                'test_data': self.levelDict[level].get('test_data', None),
                'params': self.levelDict[level].get('params', None)}
            severity = self.levelDict[level].get('severity', None)
            if severity == '0':
                severityIndicator = 1
            verifyResult, _ = self.testDataCheck(verifyDict)
            if not verifyResult:
                return False, '存在未通过测试数据验证的数据标准配置项！'
            self.levelDict[level]['test_result'] = True
            self.levelDict[level]['weight'] = 100
        if severityIndicator == 0:
            return False, '未配置数据标准结果为正常的配置项！'
        return True, None
    
    def weightUpdate(self, weightDict):
        pass
    
    def scoreCount(self, data):
        pass
