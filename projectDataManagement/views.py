from user.views import LoginRequiredMixin
from projectDataManagement.models import *
from projectDataManagement.components import *


class TaskTemplateInfoViewSet(LoginRequiredMixin, GeneralView):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
        self.model = TaskTemplateInfo
        self.fields = ['id', 'task_template_code', 'task_template_type',
                       'task_template_name', 'alert_type', 'alert_param',
                       'task_executor_type', 'task_route_type',
                       'task_block_type', 'timeout', 'retry_count', 'comment']
        self.totalFields = [_.name + '_id' if isinstance(_, models.ForeignKey)
                            else _.name for _ in self.model._meta.get_fields()]
        self.queryset = self.model.objects.filter(
            is_deleted=0).values(*self.fields)
        self.comment = '任务模板'
        self.msg = generalJson()
        self.uniqueFields = {('task_template_code', ): '模板编码',
                             ('task_template_name', ): '模板名称'}
    
    @action(methods=['get'], detail=False, url_path='choice')
    def choice(self, request, *args, **kwargs):
        queryset = self.model.objects.filter(is_deleted=0).values(*self.fields)
        self.msg['data'] = [dictTransform(_) for _ in queryset]
        self.msg['msg'] = '%s列表获取成功' % self.comment
        return HttpResponse(json.dumps(self.msg, ensure_ascii=False))


class TaskInfoViewSet(LoginRequiredMixin, GeneralView):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
        self.model = TaskInfo
        self.fields = ['id', 'task_code', 'task_name', 'task_type', 'comment',
                       'task_template_id__task_template_code',
                       'task_template_id__task_template_name',
                       'task_template_id__comment',
                       'cron_expression', 'is_enable']
        self.totalFields = [_.name + '_id' if isinstance(_, models.ForeignKey)
                            else _.name for _ in self.model._meta.get_fields()]
        self.queryset = self.model.objects.filter(is_deleted=0) \
            .select_related('task_template_id').values(*self.fields)
        self.comment = '任务属性'
        self.msg = generalJson()
        self.uniqueFields = {('task_code', ): '任务编码',
                             ('task_name', ): '任务名称'}
    
    @action(methods=['post'], detail=False, url_path='jobstatus')
    def jobStatus(self, request, *args, **kwargs):
        bodyContent = request.data
        if bodyContent:
            bodyParams = dict(request.data)
        else:
            bodyParams = {}
        taskID = bodyParams.get('id')
        if taskID:
            status = bodyParams.get('status')
            jobQuery = self.model.objects.get(id=taskID)
            dispatchClass = TaskDispatchClass(taskID)
            if status is False:
                try:
                    dispatchClass.dispatchPause()
                    jobQuery.is_enable = False
                    jobQuery.time_last_updated = curTimeGet()
                    jobQuery.save()
                    self.msg['msg'] = '任务停止'
                except Exception as e:
                    self.msg['msg'] = '任务停止失败'
                    self.msg['statusCode'] = 400
                    self.msg['statusInfo'] = errorOutput(e)
            else:
                try:
                    dispatchClass.dispatchStart()
                    jobQuery.is_enable = True
                    jobQuery.time_last_updated = curTimeGet()
                    jobQuery.save()
                    self.msg['msg'] = '任务启动'
                except Exception as e:
                    self.msg['msg'] = '任务启动失败，请检查任务配置是否正常'
                    self.msg['statusCode'] = 400
                    self.msg['statusInfo'] = errorOutput(e)
        else:
            raise ValueError('未提供任务id')
        return HttpResponse(json.dumps(self.msg, ensure_ascii=False))
    
    @action(methods=['post'], detail=False, url_path='testrun')
    def testRun(self, request, *args, **kwargs):
        bodyContent = request.data
        if bodyContent:
            bodyParams = dict(request.data)
        else:
            bodyParams = {}
        taskID = bodyParams.get('id')
        if taskID:
            jobQuery = self.model.objects.get(id=taskID)
            status = jobQuery.is_enable
            dispatchClass = TaskDispatchClass(taskID)
            if status is False:
                raise ValueError('任务未启动状态下，无法进行执行一次操作！')
            dispatchClass.taskRun('now')
            self.msg['msg'] = '任务执行一次成功！'
        else:
            raise ValueError('未提供任务id')
        return HttpResponse(json.dumps(self.msg, ensure_ascii=False))


class TaskComponentsInfoViewSet(LoginRequiredMixin, GeneralView):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
        self.model = TaskToComponentsInfo
        self.fields = ['id', 'is_input_from_source', 'origin_source',
                       'origin_table', 'origin_file_path',
                       'is_output_to_source', 'target_source',
                       'target_table', 'target_file_path',
                       'origin_fields_list', 'target_fields_list',
                       'processing_mapping_info',
                       'component', 'component_params',
                       'component_id__is_component_available',
                       'component_id__component_file_path',
                       'component_id__component_class_name']
        self.totalFields = [_.name + '_id' if isinstance(_, models.ForeignKey)
                            else _.name for _ in self.model._meta.get_fields()]
        self.queryset = self.model.objects.filter(is_deleted=0) \
            .select_related('task_template_id').values(*self.fields)
        self.comment = '任务组件'
        self.msg = generalJson()
    
    @action(methods=['post'], detail=False, url_path='add')
    def add(self, request, *args, **kwargs):
        bodyContent = request.data
        if bodyContent:
            bodyParams = dict(request.data)
        else:
            bodyParams = {}
        paramsDictList = []
        inputTemplate = {'task_id': bodyParams.get('task_id'),
                         'component_id': bodyParams.get('component_id')}
        if bodyParams.get('origin_source'):
            inputTemplate['is_input_from_source'] = True
            inputTemplate['origin_source_id'] = bodyParams.get('origin_source')
        else:
            inputTemplate['is_input_from_source'] = False
            # 针对数据开发部分，这里还要扩展的
        if bodyParams.get('target_source'):
            inputTemplate['is_output_to_source'] = True
            inputTemplate['target_source_id'] = bodyParams.get('target_source')
        else:
            inputTemplate['is_output_to_source'] = False
            # 针对数据开发部分，这里还要扩展的
        if bodyParams.get('is_total_database'):
            inputParams = inputTemplate.copy()
            inputParams['origin_table'] = 'ALL'
            sConn, sdb = sourceConnect(bodyParams.get('origin_source'))
            sourceTables = mysqlTablesGet(sConn.cursor(), sdb,
                                          tableonly=True, nameonly=True)
            tConn, tdb = sourceConnect(bodyParams.get('target_source'))
            targetTables = mysqlTablesGet(tConn.cursor(), tdb,
                                          tableonly=True, nameonly=True)
            for table in sourceTables:
                if table not in targetTables:
                    self.msg['msg'] = '来源数据源中，数据表%s无法在目标数据源中找到对应' \
                                      '表，无法进行整库同步操作。请先确保目标数据源中包含' \
                                      '与数据表%s同名且结构相同的数据表或在下方配置对应' \
                                      '表。' % (table, table)
                    self.msg['statusCode'] = 400
                    return HttpResponse(json.dumps(self.msg,
                                                   ensure_ascii=False))
            paramsDictList = [inputParams]
            sConn.close()
            tConn.close()
        elif bodyParams.get('map_list'):
            mapList = bodyParams.get('map_list')
            for pair in mapList:
                inputParams = inputTemplate.copy()
                inputParams['origin_table'] = pair.get('origin_table')
                inputParams['target_table'] = pair.get('target_table')
                if pair.get('is_incremental_integration'):
                    timeField = pair.get('incremental_integration_field')
                    inputParams['component_params'] = \
                        {'incremental': True, 'incrementalField': timeField}
                else:
                    inputParams['component_params'] = \
                        {'incremental': False, 'incrementalField': None}
                paramsDictList.append(inputParams)
        else:
            raise ValueError('未进行任何数据表对应关系配置！')
        try:
            newIDList = []
            for fieldParams in paramsDictList:
                res = self.model.objects.create(**fieldParams)
                newIDList.append(res.id)
            data = self.queryset.filter(id__in=newIDList).values()
            self.msg['data'] = {'task_id': bodyParams.get('task_id'),
                                'added_data': [dictTransform(_) for _ in
                                               data]}
            self.msg['msg'] = '%s新增成功' % self.comment
        except Exception as e:
            self.msg['msg'] = '%s新增失败' % self.comment
            self.msg['statusCode'] = 400
            self.msg['statusInfo'] = errorOutput(e)
        return HttpResponse(json.dumps(self.msg, ensure_ascii=False))
    
    @action(methods=['get', 'post'], detail=False, url_path='tablemodify')
    def tableModify(self, request, *args, **kwargs):
        headParams = request.GET
        bodyContent = request.data
        if bodyContent:
            bodyParams = bodyContent
        else:
            bodyParams = {}
        if headParams.get('id'):
            taskID = headParams.get('id')
            fields = ['id', 'task_id', 'component_id', 'component_params',
                      'is_input_from_source', 'origin_source_id',
                      'origin_table', 'origin_file_path',
                      'is_output_to_source', 'target_source_id',
                      'target_table', 'target_file_path']
            data = self.model.objects.filter(task=taskID, is_deleted=0) \
                .values(*fields)
            if len(data) == 0:
                self.msg['data'] = None
                self.msg['msg'] = '该任务下未进行任何组件配置'
                return HttpResponse(json.dumps(self.msg, ensure_ascii=False))
            commonFields = ['task_id', 'component_id',
                            'is_input_from_source', 'origin_source_id',
                            'origin_file_path',
                            'is_output_to_source', 'target_source_id',
                            'target_file_path']
            privateFields = ['id', 'component_params',
                             'origin_table', 'target_table']
            allIDList = []
            if data[0]['is_input_from_source']:
                if data[0]['origin_table'] == 'ALL':
                    resData = subDict(data[0], commonFields)
                    resData['is_total_database'] = True
                    allIDList.append(data[0]['id'])
                else:
                    resData = subDict(data[0], commonFields)
                    resData['map_list'] = []
                    for subData in data:
                        params = subData['component_params']
                        allIDList.append(subData['id'])
                        judge = params.get('incremental')
                        timeField = params.get('incrementalField')
                        subRes = subDict(subData, privateFields)
                        subRes['is_incremental_integration'] = judge
                        subRes['incremental_integration_field'] = timeField
                        resData['map_list'].append(subRes)
            else:
                resData = []
                # 等待数据开发中若输入数据不是来源于数据源的原型
            if request.method == 'GET':
                self.msg['data'] = resData
                self.msg['msg'] = '%s表级信息获取成功' % self.comment
            elif request.method == 'POST':
                newIDList = []
                modifiedIDList = []
                deletedIDList = []
                paramsTemplate = subDict(data[0], commonFields)
                if bodyParams.get('is_total_database'):
                    if data[0]['origin_table'] != 'ALL':
                        params = paramsTemplate.copy()
                        deletedIDList = [_['id'] for _ in data]
                        row = self.model.objects.filter(id__in=deletedIDList)
                        fieldDelete(self.model, row)
                        params['origin_table'] = 'ALL'
                        params['target_table'] = 'ALL'
                        res = self.model.objects.create(**params)
                        newIDList.append(res.id)
                if bodyParams.get('map_list'):
                    paramsDictList = []
                    mapDetailList = bodyParams.get('map_list')
                    for pair in mapDetailList:
                        pairID = pair.get('id')
                        if pairID:
                            if pairID in allIDList:
                                allIDList.remove(pairID)
                            row = self.model.objects.get(id=pairID)
                            if pair.get('origin_table') != row.origin_table:
                                row.origin_table = pair.get('origin_table')
                                row.time_last_updated = curTimeGet()
                                if pairID not in modifiedIDList:
                                    modifiedIDList.append(pairID)
                                row.save()
                            if pair.get('target_table') != row.target_table:
                                row.target_table = pair.get('target_table')
                                row.time_last_updated = curTimeGet()
                                if pairID not in modifiedIDList:
                                    modifiedIDList.append(pairID)
                                row.save()
                            componentParams = row.component_params.copy()
                            judge = pair.get('is_incremental_integration',
                                             False)
                            judgeOld = componentParams \
                                .get('incremental', False)
                            timeField = pair.get(
                                'incremental_integration_field')
                            timeFieldOld = componentParams.get(
                                'incrementalField')
                            indicator = 0
                            if judge != judgeOld or timeField != timeFieldOld:
                                componentParams['incremental'] = judge
                                componentParams['incrementalField'] = timeField
                                indicator = 1
                            if indicator:
                                row.component_params = componentParams
                                row.time_last_updated = curTimeGet()
                                if pairID not in modifiedIDList:
                                    modifiedIDList.append(pairID)
                                row.save()
                        else:
                            inputParams = paramsTemplate.copy()
                            inputParams['origin_table'] = pair.get(
                                'origin_table')
                            inputParams['target_table'] = pair.get(
                                'target_table')
                            if pair.get('is_incremental_integration'):
                                timeField = pair.get(
                                    'incremental_integration_field')
                                inputParams['component_params'] = \
                                    {'incremental': True,
                                     'incrementalField': timeField}
                            else:
                                inputParams['component_params'] = \
                                    {'incremental': False,
                                     'incrementalField': None}
                            paramsDictList.append(inputParams)
                    if len(paramsDictList) > 0:
                        for fieldParams in paramsDictList:
                            res = self.model.objects.create(**fieldParams)
                            newIDList.append(res.id)
                deletedIDList = allIDList
                row = self.model.objects.filter(id__in=deletedIDList)
                fieldDelete(self.model, row)
                createdData = self.queryset.filter(id__in=newIDList).values()
                modifiedData = self.queryset.filter(
                    id__in=modifiedIDList).values()
                deletedData = self.queryset.filter(
                    id__in=deletedIDList).values()
                self.msg['data'] = {'task_id': taskID,
                                    'created': [dictTransform(_) for
                                                _ in createdData],
                                    'modified': [dictTransform(_) for
                                                 _ in modifiedData],
                                    'deleted': [dictTransform(_) for
                                                _ in deletedData]}
                self.msg['msg'] = '%s表级信息修改成功' % self.comment
            else:
                raise TypeError('方法不支持')
        else:
            raise ValueError('未提供任务id')
        return HttpResponse(json.dumps(self.msg, ensure_ascii=False))
    
    @action(methods=['get', 'post'], detail=False, url_path='modify')
    def modify(self, request, *args, **kwargs):
        headParams = request.GET
        bodyContent = request.data
        if bodyContent:
            bodyParams = bodyContent
        else:
            bodyParams = {}
        mapID = headParams.get('mapid')
        if mapID:
            row = self.model.objects.get(id=mapID)
            componentClass = TaskComponentClass(mapID)
            componentParams = row.component_params.copy()
            if request.method == 'GET':
                try:
                    auto = int(headParams.get('auto', 0))
                    if auto not in [0, 1, 2]:
                        raise ValueError('该参数取值只能在0, 1, 2之间！')
                except Exception:
                    raise ValueError('该参数取值只能在0, 1, 2之间！')
                target = headParams.get('target')
                if not target:
                    self.msg['data'] = {'mapid': mapID,
                                        'maplist': componentClass.preMapGet(
                                            auto=auto)}
                    self.msg['msg'] = '%s字段映射信息获取成功' % self.comment
                else:
                    self.msg['data'] = \
                        {'mapid': mapID,
                         'fields_list': componentClass.outputDataFieldDetail()}
            elif request.method == 'POST':
                maplist = bodyParams.get('maplist')
                mappingList = componentClass.funcClass.mappingUpdate(maplist)
                oFields, tFields = \
                    componentClass.funcClass.selectedFieldsListGet()
                filterList = []
                commentList = []
                for tmpMap in maplist:
                    oFieldName = tmpMap.get('origin_field_name')
                    tFieldName = tmpMap.get('target_field_name')
                    filterType = tmpMap.get('filter_type')
                    filterValue = tmpMap.get('filter_value')
                    tFieldComment = tmpMap.get('target_field_comment')
                    if filterValue:
                        filterList.append({'fieldName': oFieldName,
                                           'filterType': filterType,
                                           'filterValue': filterValue})
                    if tFieldComment:
                        commentList.append({'fieldName': tFieldName,
                                            'fieldComment': tFieldComment})
                componentParams['filter'] = filterList
                componentParams['comment'] = commentList
                modifiedParams = {'component_params': componentParams,
                                  'origin_fields_list': oFields,
                                  'target_fields_list': tFields,
                                  'processing_mapping_info': mappingList}
                for param in modifiedParams:
                    old = getattr(row, param)
                    new = modifiedParams[param]
                    if old != new:
                        setattr(row, param, new)
                        row.time_last_updated = curTimeGet()
                        row.save()
                queryset = self.queryset.filter(id=mapID).values()
                self.msg['data'] = dictTransform(queryset[0])
                self.msg['msg'] = '指定%s字段映射信息修改成功' % self.comment
            else:
                raise TypeError('方法不支持')
            return HttpResponse(json.dumps(self.msg, ensure_ascii=False))
        else:
            raise ValueError('未提供映射id')
    
    @action(methods=['post'], detail=False, url_path='testrun')
    def testrun(self, request, *args, **kwargs):
        headParams = request.GET
        mapID = headParams.get('mapid')
        if mapID:
            componentClass = TaskComponentClass(mapID)
            msg = componentClass.funcClass.run(mapID=mapID)
            self.msg['msg'] = msg['msg']
            self.msg['statusCode'] = int(400 - msg['status'] * 200)
            self.msg['statusInfo'] = msg['info']
            return HttpResponse(json.dumps(self.msg, ensure_ascii=False))
        else:
            raise ValueError('未提供映射id')


class DispatchViewSet(LoginRequiredMixin, GeneralView):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
        self.model = TaskDispatchInfo
        self.fields = ['id',
                       'task_dispatched_id__task_name',
                       'task_dispatched_id__task_type',
                       'task_dispatched_id__task_code',
                       'task_dispatched_id__comment',
                       'dispatch_start_time',
                       'dispatch_end_time',
                       'dispatch_during_time',
                       'dispatch_status', 'execute_log_file_path']
        self.totalFields = [_.name + '_id' if isinstance(_, models.ForeignKey)
                            else _.name for _ in self.model._meta.get_fields()]
        self.queryset = self.model.objects.filter(is_deleted=0) \
            .select_related('task_dispatched_id').values(*self.fields)
        self.comment = '任务调度'
        self.msg = generalJson()

    @action(methods=['get'], detail=False, url_path='log')
    def logShow(self, request, *args, **kwargs):
        headParams = request.GET
        if headParams.get('id'):
            dispatchID = headParams.get('id')
            queryset = self.model.objects.get(id=dispatchID, is_deleted=0)
            logFile = queryset.execute_log_file_path
            with open(logFile, 'r', encoding='utf-8') as f:
                logText = f.read()
            self.msg['data'] = logText
            self.msg['msg'] = '日志内容获取成功'
        else:
            raise ValueError('未提供任务id')
        return HttpResponse(json.dumps(self.msg, ensure_ascii=False))


# Create your views here.
def testView(request):
    data = TaskTemplateInfo.objects.filter(id=1).values()
    msg = generalJson([dictTransform(_) for _ in data])
    return HttpResponse(json.dumps(msg, ensure_ascii=False))


class TaskComponentClass:
    def __init__(self, mapID, **kwargs):
        self.mapID = mapID
        self.batchSize = 1000
        if kwargs.get('batchSize'):
            try:
                self.batchSize = int(kwargs.get('batchSize'))
            except Exception:
                raise ValueError('数据批次容量值设置错误')
        fields = ['task_id', 'is_input_from_source', 'origin_source',
                  'origin_table', 'origin_file_path', 'origin_fields_list',
                  'component', 'component_params', 'processing_mapping_info',
                  'is_output_to_source', 'target_source',
                  'target_table', 'target_file_path', 'target_fields_list']
        rawInfo = TaskToComponentsInfo.objects.filter(id=mapID) \
            .values(*fields)[0]
        self.taskID = rawInfo['task_id']
        dataInput = kwargs.get('dataInput')
        dataDictRerame = ['is_source_related', 'source_id', 'table_name',
                          'file_path', 'field_list']
        if not dataInput:
            inputFields = ['is_input_from_source', 'origin_source',
                           'origin_table', 'origin_file_path',
                           'origin_fields_list']
            dataInputInfo = subDict(rawInfo, inputFields,
                                    **dict(zip(inputFields, dataDictRerame)))
            self.dataInput = DataClass(**dataInputInfo)
        else:
            if isinstance(dataInput, [list, tuple, set]):
                for subInput in dataInput:
                    if not isinstance(subInput, DataClass):
                        raise ValueError('输入数据不符合规定格式！')
            elif not isinstance(dataInput, DataClass):
                raise ValueError('输入数据不符合规定格式！')
            self.dataInput = dataInput
        self.dataOutput = None
        if rawInfo.get('is_output_to_source') or rawInfo.get(
                'target_file_path'):
            outputFields = ['is_output_to_source', 'target_source',
                            'target_table', 'target_file_path',
                            'target_fields_list']
            dataOutputInfo = subDict(rawInfo, outputFields,
                                     **dict(zip(outputFields, dataDictRerame)))
            self.dataOutput = DataClass(**dataOutputInfo)
        componentID = rawInfo['component']
        funcParams = {'dataInput': self.dataInput,
                      'dataOutput': self.dataOutput,
                      'funcParams': rawInfo.get('component_params'),
                      'mapList': rawInfo.get('processing_mapping_info')}
        try:
            componentInfo = ComponentInfo.objects.get(id=componentID)
            if componentInfo.is_component_available == 0:
                componentName = componentInfo.component_name
                componentInfo = ComponentInfo.objects.get(
                    component_name=componentName,
                    is_component_available=1,
                    is_deleted=0)
            module = componentInfo.component_file_path
            funcName = componentInfo.component_class_name
            command = f'from {module} import {funcName}\ncurrentFunc = ' \
                      f'{funcName}(**funcParams)'
            loc = locals()
            exec(command)
            self.funcClass = loc['currentFunc']
        except Exception as e:
            raise TypeError(f'数据处理组件类方法调用失败！')
    
    def inputDataFieldDetail(self):
        return self.dataInput.overviewGet()
    
    def outputDataGet(self):
        if not self.dataOutput:
            self.dataOutput = self.funcClass.outputDataGet()
        return self.dataOutput
    
    def outputDataFieldDetail(self):
        return self.dataOutput.overviewGet()
    
    def preMapGet(self, auto=0):
        if auto == 1:
            self.funcClass.prePassedMapGet()
        elif auto == 2:
            self.funcClass.prePassedMapRemove()
        self.funcClass.mappingGenerate()
        return self.funcClass.mapDetailGet()
    
    def mappingUpdate(self, mapList=None):
        return self.funcClass.mappingUpdate(mapList)
    
    def run(self):
        return self.funcClass.run(taskID=self.taskID, mapID=self.mapID)


class TaskDispatchClass:
    def __init__(self, taskID):
        self.taskID = taskID
        taskQuery = TaskInfo.objects.get(id=taskID)
        self.taskDetail = taskQuery.task_id.filter(is_deleted=0).values()
        templateList = ['timeout', 'retry_count',
                        'task_block_type', 'task_executor_type']
        taskTemplate = TaskTemplateInfo.objects.filter(
            id=taskQuery.task_template_id).values(*templateList)[0]
        self.cronClass = CronAnalysis(taskQuery.cron_expression)
        self.cronClass.cronReasoning()
        self.trigger = GeneralTrigger.fromCrontab(taskQuery.cron_expression)
        if not taskTemplate['timeout']:
            taskTemplate['timeout'] = 0
        if taskTemplate['task_block_type'] == '单机串行':
            timeOutTolarent = 86400
        elif taskTemplate['task_block_type'] == '丢弃后续调度':
            timeOutTolarent = taskTemplate['timeout'] * 60
        else:
            timeOutTolarent = 5
        self.forceExecute = (taskTemplate['task_block_type'] == '覆盖之前调度')
        if not taskTemplate['retry_count']:
            self.retryCount = 0
        else:
            self.retryCount = int(taskTemplate['retry_count'])
        self.queueDict = {'func': None,
                          'args': None,
                          'kwargs': None,
                          'trigger': self.trigger,
                          'id': 'task' + str(self.taskID),
                          'name': '任务编号' + str(self.taskID),
                          'jobstore': 'default',
                          'executor': taskTemplate['task_executor_type'],
                          'replace_existing': True,
                          'misfire_grace_time': timeOutTolarent,
                          'jitter': 60}
        self.componentIDs = [_['id'] for _ in self.taskDetail]
        self.dispatchDict = dict()
        for ID in self.componentIDs:
            ups = self.taskDetail.get(id=ID).get('upstream_task_components')
            if ups is not None:
                try:
                    upsList = [int(_.strip()) for _ in ups.split(',')]
                except Exception:
                    raise ValueError('上级节点无法被转换为节点列表！')
            else:
                upsList = None
            self.dispatchDict[ID] = {'status': 0,
                                     'outputData': None,
                                     'upstreams': upsList}
    
    def queueCheck(self):
        status = 1
        successIndicator = 1
        nextComponents = set()
        for ID in self.dispatchDict:
            if self.dispatchDict[ID]['status'] == 0:
                successIndicator = 0
                upstreams = self.dispatchDict[ID]['upstreams']
                if upstreams is None:
                    nextComponents.add(ID)
                else:
                    indicator = 1
                    for upstreamID in upstreams:
                        tmpStatus = self.dispatchDict[upstreamID]['status']
                        if tmpStatus != 1:
                            indicator = 0
                            break
                    if indicator == 1:
                        nextComponents.add(ID)
            elif self.dispatchDict[ID]['status'] == -1:
                successIndicator = 0
                status = -1
            elif self.dispatchDict[ID]['status'] == 1:
                pass
        if successIndicator == 1:
            status = 9
        return status, nextComponents
    
    def nextRunTimeGet(self, toString=False, form='%Y-%m-%d %H%M%S'):
        timePoint = self.trigger.getNextTime()
        timePoint = dtStandard(timePoint)
        if not toString:
            return timePoint
        else:
            return timePoint.strftime(form)
    
    def lastRunTimeGet(self, toString=False, form='%Y-%m-%d %H%M%S'):
        timePoint = self.cronClass.lastTimeUpdate(curTimeGet())
        timePoint = dtStandard(timePoint)
        if not toString:
            return timePoint
        else:
            return timePoint.strftime(form)
    
    def dispatchQueryGet(self, typ='last', **kwargs):
        if typ == 'last':
            timePoint = self.lastRunTimeGet()
        elif typ == 'next':
            timePoint = self.nextRunTimeGet()
        else:
            timePoint = kwargs.get('timePoint', curTimeGet())
        try:
            query = TaskDispatchInfo.objects \
                .get(task_dispatched=self.taskID,
                     dispatch_estimated_time=timePoint,
                     is_deleted=0)
        except Exception:
            nextTimeString = timePoint.strftime('%Y-%m-%d %H%M%S')
            logDir = dirGet('log')
            logFile = logDir + 'task{}_{}.log'.format(self.taskID,
                                                      nextTimeString)
            dispatchParams = {'task_dispatched_id': self.taskID,
                              'dispatch_estimated_time': timePoint,
                              'execute_log_file_path': logFile,
                              'dispatch_status': '未开始'}
            dispatchParams.update(kwargs)
            query = TaskDispatchInfo(**dispatchParams)
            query.save()
            newID = query.id
            query = TaskDispatchInfo.objects.get(id=newID)
        return query
    
    def taskRun(self, typ='last'):
        statusDict = {0: '未开始', 1: '进行中', 9: '成功', -1: '失败'}
        taskStatus = 1
        dispatchQuery = self.dispatchQueryGet(typ=typ)
        logFile = open(dispatchQuery.execute_log_file_path, mode='a',
                       encoding='utf-8')
        startTime = curTimeGet()
        dispatchQuery.dispatch_start_time = startTime
        dispatchQuery.time_last_updated = curTimeGet()
        dispatchQuery.save()
        logFile.write('任务编号：{}\n调度开始时间：{}\n----------\n'
                      .format(self.taskID, startTime))
        successIDs = set()
        failedIDs = set()
        retryNum = 0
        while taskStatus not in (-1, 9) and self.retryCount >= retryNum:
            taskStatus, nextComponents = self.queueCheck()
            dispatchQuery.dispatch_status = statusDict[taskStatus]
            dispatchQuery.current_components_id = list(nextComponents)
            dispatchQuery.time_last_updated = curTimeGet()
            dispatchQuery.save()
            if len(nextComponents) == 0:
                logFile.write('无可执行的任务组件\n')
            else:
                for nextID in nextComponents:
                    componentClass = TaskComponentClass(nextID)
                    msg = componentClass.run()
                    updateDict = {'status': int(2 * msg['status'] - 1),
                                  'outputData': componentClass.funcClass.output}
                    self.dispatchDict[nextID].update(updateDict)
                    compLogFile = componentClass.funcClass.log.baseFilename
                    with open(compLogFile, 'r', encoding='utf-8') as f:
                        componentLog = f.read()
                        logFile.write(componentLog)
                        logFile.write('\n-----\n')
                    with open(compLogFile, 'w', encoding='utf-8') as f:
                        f.write('')
                    if msg['status'] == 0:
                        failedIDs.add(nextID)
                        dispatchQuery.failed_components_id = list(failedIDs)
                        dispatchQuery.time_last_updated = curTimeGet()
                        dispatchQuery.save()
                        break
                    successIDs.add(nextID)
                    dispatchQuery.success_components_id = list(successIDs)
                    dispatchQuery.time_last_updated = curTimeGet()
                    dispatchQuery.save()
                    taskComponentQuery = TaskToComponentsInfo.objects.get(
                        id=nextID)
                    tmpParams = taskComponentQuery.component_params
                    tmpParams['timePoint'] = \
                        msg['startTime'].strftime('%Y-%m-%d %H:%M:%S')
                    taskComponentQuery.component_params = tmpParams
                    taskComponentQuery.time_last_updated = curTimeGet()
                    taskComponentQuery.save()
            if taskStatus == -1:
                retryNum += 1
                if retryNum <= self.retryCount:
                    logFile.write('\n-----\n进行第{}次任务调度重试\n-----\n'
                                  .format(retryNum))
                    taskStatus = 1
                    for resetID in self.dispatchDict:
                        if self.dispatchDict[resetID]['status'] == -1:
                            self.dispatchDict[resetID]['status'] = 0
        if taskStatus == 9:
            logFile.write('任务调度成功')
        else:
            logFile.write('任务调度失败')
        dispatchQuery.current_components_id = []
        dispatchQuery.dispatch_status = statusDict[taskStatus]
        endTime = curTimeGet()
        dispatchQuery.dispatch_end_time = endTime
        duringTime = endTime - dispatchQuery.dispatch_start_time
        dispatchQuery.dispatch_during_time = dtDeltaStandard(duringTime)
        dispatchQuery.time_last_updated = curTimeGet()
        dispatchQuery.save()
        logFile.close()
        
    def dispatchPrepare(self, *args, **kwargs):
        self.queueDict['func'] = self.taskRun
        self.queueDict['args'] = args
        self.queueDict['kwargs'] = kwargs
        
    def dispatchStart(self, *args, **kwargs):
        self.dispatchPrepare(*args, **kwargs)
        try:
            scheduler.add_job(**self.queueDict)
            scheduler.start()
        except Exception:
            pass
        scheduler.resume_job(self.queueDict['id'])
    
    def dispatchPause(self):
        scheduler.pause_job(self.queueDict['id'])
