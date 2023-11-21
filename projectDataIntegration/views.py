from functions.general import *
from functions.secure import *
from user.views import LoginRequiredMixin
from projectDataIntegration.models import *
from projectDataManagement.models import *
from dictionary.models import SourceTypeInfo
from functions.connection import *
from user.views import jwtLogin


# Create your views here.
class SourceInfoViewSet(LoginRequiredMixin, PasswordView):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
        self.model = SourceInfo
        self.fields = ['id', 'source_type_id__type_name', 'source_code',
                       'source_name', 'source_group', 'source_dept', 'host',
                       'port', 'user', 'is_inner_source', 'comment']
        self.totalFields = [_.name + '_id' if isinstance(_, models.ForeignKey)
                            else _.name for _ in self.model._meta.get_fields()]
        self.queryset = self.model.objects.filter(is_deleted=0) \
            .select_related('source_type_id').values(*self.fields)
        self.comment = '数据源'
        self.msg = generalJson()
        self.uniqueFields = {
            ('source_name', ): '数据源名称',
            ('host', 'port', 'source_code'): '主机、地址、数据库名称组合'}
        self.transFuncDict = {
            'is_inner_source': {True: '是', False: '否'}}
        self.passwordFields = ('password', )
        self.verifyFunc = {'password': self.passwordVerify}
    
    def passwordVerify(self, ID):
        row = self.model.objects.get(id=ID)
        try:
            conn, _ = sourceConnect(ID)
            conn.close()
            row.is_available = 1
            row.save()
        except Exception as e:
            self.msg['msg'] += f'，但该{self.comment}测试连接失败，请检查数据源信息'
            self.msg['statusInfo'] = errorOutput(e)
            row.is_available = 0
            row.save()
        
    
    @action(methods=['get', 'post'], detail=False, url_path='detail')
    def getDetail(self, request, *args, **kwargs):
        headParams = request.GET
        # bodyContent = request.data
        # if bodyContent:
        #     bodyParams = bodyContent
        # else:
        #     bodyParams = {}
        method = request.method
        currentPage = headParams.get('pageNum')
        pageSize = headParams.get('pageSize')
        if not currentPage:
            currentPage = 1
        if not pageSize:
            pageSize = 10
        if method == 'GET':
            if headParams.get('id'):
                queryset = self.model.objects.filter(id=headParams.get(
                    'id')).values()
                data = queryset[0]
                data['password'] = Password(data['password'], 'Decrypt')
                self.msg['data'] = dictTransform(data, boolTrans=False,
                                                 passwordTrans=True)
                self.msg['msg'] = '指定数据源信息获取成功'
            else:
                sourcePagenation = StandardPagination(self.queryset, pageSize)
                data, totalVolume, totalPage = \
                    sourcePagenation.getOutput(currentPage)
                self.msg['data'] = {'data': data,
                                    'total': totalVolume}
                self.msg['msg'] = '数据源信息获取成功'
        elif method == 'POST':
            return super().getDetail(request, *args, **kwargs)
        return HttpResponse(json.dumps(self.msg, ensure_ascii=False))
    
    @action(methods=['post'], detail=False, url_path='modify')
    def modify(self, request, *args, **kwargs):
        headParams = request.GET
        bodyContent = request.data
        if bodyContent:
            bodyParams = bodyContent
        else:
            bodyParams = {}
        if headParams.get('id'):
            ID = headParams.get('id')
            dataToBeModified = self.model.objects.get(id=ID)
            SourceFields = self.totalFields
            for fieldsGroup in self.uniqueFields:
                uniqueDict = {'is_deleted': 0}
                for field in fieldsGroup:
                    uniqueDict[field] = bodyParams.get(field, None)
                existedRes = self.model.objects.filter(**uniqueDict)
                if len(existedRes) > 0:
                    self.msg['msg'] = f'{self.comment}新增失败' \
                                      f'，{self.uniqueFields[fieldsGroup]}已存在'
                    self.msg['statusCode'] = 400
                    return HttpResponse(
                        json.dumps(self.msg, ensure_ascii=False))
            for param in bodyParams:
                if param in SourceFields:
                    old = getattr(dataToBeModified, param)
                    new = bodyParams[param]
                    if param == 'password':
                        old = Password(old, 'Decrypt').transform()
                        new1 = Password(new, 'Encrypt').transform()
                    else:
                        new1 = new
                    if old != new:
                        setattr(dataToBeModified, param, new1)
                        dataToBeModified.time_last_updated = curTimeGet()
                        dataToBeModified.save()
            queryset = self.queryset.filter(id=ID).values()
            data = queryset[0]
            data['password'] = Password(data['password'], 'Decrypt')
            self.msg['data'] = dictTransform(data, boolTrans=False,
                                             passwordTran=False)
            self.msg['msg'] = '指定%s信息修改成功' % self.comment
            row = self.model.objects.get(id=ID)
            try:
                conn, _ = sourceConnect(ID)
                conn.close()
                row.is_available = 1
                row.save()
            except Exception as e:
                self.msg['msg'] += f'，但该{self.comment}测试连接失败，请检查数据源信息'
                self.msg['statusInfo'] = errorOutput(e)
                row.is_available = 0
                row.save()
        return HttpResponse(json.dumps(self.msg, ensure_ascii=False))
    
    @action(methods=['post'], detail=False, url_path='add')
    def add(self, request, *args, **kwargs):
        bodyContent = request.data
        if bodyContent:
            bodyParams = dict(request.data)
        else:
            bodyParams = {}
        fieldParams = {}
        SourceFields = self.totalFields
        for fieldsGroup in self.uniqueFields:
            uniqueDict = {'is_deleted': 0}
            for field in fieldsGroup:
                uniqueDict[field] = bodyParams.get(field, None)
            existedRes = self.model.objects.filter(**uniqueDict)
            if len(existedRes) > 0:
                self.msg['msg'] = f'{self.comment}新增失败' \
                                  f'，{self.uniqueFields[fieldsGroup]}已存在'
                self.msg['statusCode'] = 400
                return HttpResponse(json.dumps(self.msg, ensure_ascii=False))
        for param in bodyParams:
            if param in SourceFields:
                content = bodyParams[param]
                if param == 'password':
                    content = Password(content, 'Encrypt').transform()
                fieldParams[param] = content
        res = self.model.objects.create(**fieldParams)
        newID = res.id
        data = self.queryset.filter(id=newID).values()[0]
        self.msg['data'] = dictTransform(data, boolTrans=False)
        self.msg['msg'] = '%s新增成功' % self.comment
        row = self.model.objects.get(id=newID)
        try:
            conn, _ = sourceConnect(newID)
            conn.close()
            row.is_available = 1
            row.save()
        except Exception as e:
            self.msg['msg'] += '，但该%s测试连接失败，请检查数据源信息' % self.comment
            self.msg['statusInfo'] = errorOutput(e)
            row.is_available = 00
            row.save()
        return HttpResponse(json.dumps(self.msg, ensure_ascii=False))
    
    @action(methods=['post'], detail=False, url_path='verify')
    def verify(self, request, *args, **kwargs):
        headParams = request.GET
        bodyContent = request.data
        if bodyContent:
            bodyParams = dict(request.data)
        else:
            bodyParams = {}
        dbID = headParams.get('id')
        if not dbID:
            try:
                host = bodyParams.get('host', '')
                port = bodyParams.get('port', '')
                user = bodyParams.get('user', '')
                password = bodyParams.get('password', '')
                database = bodyParams.get('source_code', '')
                source_type = int(bodyParams['source_type_id'])
                module = SourceTypeInfo.objects.filter(
                    id=source_type)[0].type_module
                paramDict = dict(zip(['host', 'port', 'user', 'password',
                                      'database', 'module'],
                                     [host, int(port), user, password,
                                      database, module]))
                conn, _ = sourceConnect(**paramDict)
                conn.close()
                self.msg['msg'] = '测试连接成功'
            except Exception:
                self.msg['msg'] = '测试连接失败'
                self.msg['statusCode'] = 400
        else:
            try:
                fields = ['host', 'port', 'user', 'password', 'source_code']
                queryset = self.model.objects.filter(
                    id=dbID, is_deleted=0)
                paramDict = subDict(queryset.values()[0], fields,
                                    source_code='database')
                paramDict['password'] = Password(paramDict['password'],
                                                 'Decrypt').transform()
                paramDict['port'] = int(paramDict['port'])
                paramDict['module'] = queryset.first().source_type.type_module
                conn, _ = sourceConnect(**paramDict)
                conn.close()
                row = self.model.objects.get(id=dbID)
                row.is_available = True
                row.save()
                self.msg['msg'] = '测试连接成功'
            except Exception as e:
                self.msg['msg'] = '测试连接失败'
                self.msg['statusCode'] = 400
                self.msg['statusInfo'] = errorOutput(e)
        return HttpResponse(json.dumps(self.msg, ensure_ascii=False))
    
    @action(methods=['get'], detail=False, url_path='choicelist')
    def sourceGet(self, request, *args, **kwargs):
        data = self.model.objects.filter(is_deleted=0, is_available=1) \
            .values('id', 'source_name')
        self.msg['data'] = [{'key': _['id'], 'value': _['source_name']}
                            for _ in data]
        return HttpResponse(json.dumps(self.msg, ensure_ascii=False))


class DataFieldInfoViewSet(LoginRequiredMixin, GeneralView):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
        self.model = DataFieldInfo
        self.fields = ['id', 'data_field_code', 'data_field_name',
                       'upstream_data_field',
                       'upstream_data_field_id__data_field_code',
                       'upstream_data_field_id__data_field_name',
                       'comment', 'creator', 'time_created']
        self.totalFields = [_.name + '_id' if isinstance(_, models.ForeignKey)
                            else _.name for _ in self.model._meta.get_fields()]
        self.queryset = self.model.objects.filter(is_deleted=0). \
            select_related('upstream_data_field_id').values(*self.fields)
        self.comment = '数据域'
        self.msg = generalJson()
        self.uniqueFields = {('data_field_code', ): '数据域编码',
                             ('data_field_name', ): '数据域名称'}
    
    # @action(methods=['post'], detail=False, url_path='add')
    # def add(self, request, *args, **kwargs):
    #     bodyContent = request.data
    #     if bodyContent:
    #         bodyParams = dict(request.data)
    #     else:
    #         bodyParams = {}
    #     fieldParams = {}
    #     fields = self.totalFields
    #     for param in bodyParams:
    #         if param in fields:
    #             content = bodyParams[param]
    #             fieldParams[param] = content
    #     fieldName = fieldParams.get('data_field_name', None)
    #     existedRes = self.model.objects.filter(data_field_name=fieldName,
    #                                            is_deleted=0)
    #     if len(existedRes) > 0:
    #         self.msg['msg'] = '%s新增失败，检查数据域名称是否已存在！' \
    #                           % self.comment
    #         self.msg['statusCode'] = 400
    #     else:
    #         res = self.model.objects.create(**fieldParams)
    #         newID = res.id
    #         data = self.queryset.filter(id=newID).values()[0]
    #         self.msg['data'] = dictTransform(data, boolTrans=False)
    #         self.msg['msg'] = '%s新增成功' % self.comment
    #     return HttpResponse(json.dumps(self.msg, ensure_ascii=False))
    
    @action(methods=['post'], detail=False, url_path='delete')
    def delete(self, request, *args, **kwargs):
        bodyContent = request.data
        if bodyContent:
            bodyParams = bodyContent
        else:
            bodyParams = {}
        IDList = bodyParams.get('id')
        if not isinstance(IDList, list):
            IDList = [IDList]
        indicator = 0
        while indicator == 0:
            print(IDList)
            data = self.model.objects.filter(id__in=IDList)
            fieldDelete(self.model, data)
            subdata = self.model.objects.filter\
                (upstream_data_field_id__in=IDList)
            if len(subdata.values()) > 0:
                IDList = [_['id'] for _ in subdata.values()]
            else:
                indicator = 1
            print(indicator)
        self.msg['msg'] = '指定%s删除成功' % self.comment
        return HttpResponse(json.dumps(self.msg, ensure_ascii=False))
    
    # @action(methods=['post'], detail=False, url_path='modify')
    # def modify(self, request, *args, **kwargs):
    #     headParams = request.GET
    #     bodyContent = request.data
    #     if bodyContent:
    #         bodyParams = bodyContent
    #     else:
    #         bodyParams = {}
    #     if headParams.get('id'):
    #         ID = headParams.get('id')
    #         dataToBeModified = self.model.objects.get(id=ID)
    #         fields = self.totalFields
    #         for param in bodyParams:
    #             if param in fields:
    #                 old = getattr(dataToBeModified, param)
    #                 new = bodyParams[param]
    #                 if param == 'data_field_name':
    #                     existedRes = self.model.objects.filter(
    #                         data_field_name=new,
    #                         is_deleted=0)
    #                     if len(existedRes) > 0:
    #                         self.msg['msg'] = '%s修改失败，检查数据域名称是否已存在！' \
    #                                           % self.comment
    #                         self.msg['statusCode'] = 400
    #                         return HttpResponse(
    #                             json.dumps(self.msg, ensure_ascii=False))
    #                 if old != new:
    #                     setattr(dataToBeModified, param, new)
    #                     dataToBeModified.time_last_updated = curTimeGet()
    #                     dataToBeModified.save()
    #         queryset = self.queryset.filter(id=ID).values()
    #         data = queryset[0]
    #         self.msg['data'] = dictTransform(data, boolTrans=False)
    #         self.msg['msg'] = '指定%s信息修改成功' % self.comment
    #     return HttpResponse(json.dumps(self.msg, ensure_ascii=False))


class DataFieldLabelInfoViewSet(LoginRequiredMixin, GeneralView):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
        self.model = DataFieldLabelInfo
        self.fields = ['id', 'table_name', 'column_name', 'comment', 'creator',
                       'source_id__source_name', 'source_id__source_code',
                       'source_id__source_type_id__type_name',
                       'data_field_label_id__data_field_code',
                       'data_field_label_id__data_field_name']
        self.totalFields = [_.name + '_id' if isinstance(_, models.ForeignKey)
                            else _.name for _ in self.model._meta.get_fields()]
        self.queryset = self.model.objects.filter(is_deleted=0) \
            .select_related('data_field_label_id') \
            .select_related('field_source_id') \
            .select_related('source_type_id').values(*self.fields)
        self.comment = '数据域标签'
        self.msg = generalJson()
        self.uniqueFields = {('source_id', 'field_label_id', 'table_name',
                              'column_name'): '标签、数据源、数据表、字段组合'}
    
    @action(methods=['post'], detail=False, url_path='add')
    def add(self, request, *args, **kwargs):
        bodyContent = request.data
        if bodyContent:
            bodyParams = dict(request.data)
        else:
            bodyParams = {}
        paramsList = []
        selectedList = bodyParams.get('selected_list', [])
        fieldLabelID = bodyParams.get('field_label_id')
        if fieldLabelID is None:
            raise ValueError('未提供数据域标签id！')
        for db in selectedList:
            paramsDatabaseDict = {'source_id': db['source_id'],
                                  'data_field_label_id': fieldLabelID}
            if not db['tables']:
                paramsList.append(paramsDatabaseDict)
            else:
                for table in db['tables']:
                    paramsTableDict = paramsDatabaseDict.copy()
                    paramsTableDict['table_name'] = table['name']
                    if not table['fields']:
                        paramsList.append(paramsTableDict)
                    else:
                        for field in table['fields']:
                            paramsFieldDict = paramsTableDict.copy()
                            paramsFieldDict['column_name'] = field['name']
                            paramsList.append(paramsFieldDict)
        try:
            newIDList = []
            deletedIDList = []
            for fieldParams in paramsList:
                db = fieldParams.get('source_id', None)
                tableName = fieldParams.get('table_name', None)
                fieldName = fieldParams.get('column_name', None)
                query = self.model.objects.filter(
                    source_id=db, data_field_label=fieldLabelID, is_deleted=0)
                query1 = query.filter(table_name=None, column_name=None)
                query2 = query.filter(table_name=tableName, column_name=None)
                query3 = query.filter(table_name=tableName,
                                      column_name=fieldName)
                if (not query1) and (not query2) and (not query3):
                    res = self.model.objects.create(**fieldParams)
                    newIDList.append(res.id)
                else:
                    pass
            for row in query:
                searchDict = {'source_id': row.source_id,
                              'data_field_label_id': row.data_field_label_id,
                              'table_name': row.table_name,
                              'column_name': row.column_name,
                              'is_deleted': 0}
                if searchDict not in paramsList:
                    res = self.model.objects.get(**searchDict)
                    res.is_deleted = 1
                    res.time_last_updated = curTimeGet()
                    res.save()
                    deletedIDList += [_['id'] for _ in res.values()]
            dataAdd = self.model.objects.filter(id__in=newIDList).values()
            dataDel = self.model.objects.filter(id__in=deletedIDList).values()
            self.msg['data'] = {'added': [dictTransform(_, boolTrans=False)
                                          for _ in dataAdd],
                                'deleted': [dictTransform(_, boolTrans=False)
                                            for _ in dataDel]}
            if len(dataAdd) > 0 and len(dataDel) == 0:
                self.msg['msg'] = f'{self.comment}新增成功'
            elif len(dataAdd) > 0:
                self.msg['msg'] = f'{self.comment}新增成功，并根据新增{self.comment}' \
                                  f'结果对原有{self.comment}进行合并'
            else:
                self.msg['msg'] = f'{self.comment}新增失败，数据已标注该{self.comment}'
        except Exception as e:
            self.msg['msg'] = f'{self.comment}新增失败'
            self.msg['statusCode'] = 400
            self.msg['statusInfo'] = errorOutput(e)
        return HttpResponse(json.dumps(self.msg, ensure_ascii=False))
    
    @action(methods=['get'], detail=False, url_path='treedetail')
    def treedetail(self, request, *args, **kwargs):
        headParams = request.GET
        if not headParams.get('id'):
            raise ValueError('未提供数据域标签id！')
        row = self.model.objects.get(id=headParams.get('id'))
        labelID = row.data_field_label_id
        sourceID = row.source_id
        query = self.model.objects.filter(data_field_label_id=labelID,
                                          source_id=sourceID,
                                          is_deleted=0) \
            .values_list('source_id', 'table_name', 'column_name')
        resDict = {'field_label_id': labelID,
                   'selected_list': [{
                       'source_id': sourceID,
                       'tables': None
                       }]}
        tableDict = dict()
        for item in query:
            if item[1] in tableDict:
                tableDict[item[1]].append({'name': item[2]})
            else:
                tableDict[item[1]] = [{'name': item[2]}]
        if tableDict:
            tmpList = [{'name': _, 'fields': tableDict[_]} for _ in tableDict]
            resDict['selected_list'][0]['tables'] = tmpList
        self.msg['data'] = resDict
        self.msg['msg'] = '指定%s信息获取成功' % self.comment
        return HttpResponse(json.dumps(self.msg, ensure_ascii=False))
    
    @action(methods=['post'], detail=False, url_path='treemodify')
    def treemodify(self, request, *args, **kwargs):
        bodyContent = request.data
        if bodyContent:
            bodyParams = dict(request.data)
        else:
            bodyParams = {}
        paramsList = []
        selectedList = bodyParams.get('selected_list', [])
        fieldLabelID = bodyParams.get('field_label_id')
        if fieldLabelID is None:
            raise ValueError('未提供数据域标签id！')
        db = selectedList[0]
        paramsDatabaseDict = {'source_id': db['source_id'],
                              'data_field_label_id': fieldLabelID,
                              'table_name': None,
                              'column_name': None}
        if not db['tables']:
            paramsList.append(paramsDatabaseDict)
        else:
            for table in db['tables']:
                paramsTableDict = paramsDatabaseDict.copy()
                paramsTableDict['table_name'] = table['name']
                if not table['fields']:
                    paramsList.append(paramsTableDict)
                else:
                    for field in table['fields']:
                        paramsFieldDict = paramsTableDict.copy()
                        paramsFieldDict['column_name'] = field['name']
                        paramsList.append(paramsFieldDict)
        try:
            newIDList = []
            deletedIDList = []
            query = self.model.objects.filter(
                source_id=db['source_id'], data_field_label=fieldLabelID,
                is_deleted=0)
            for fieldParams in paramsList:
                tableName = fieldParams.get('table_name', None)
                fieldName = fieldParams.get('column_name', None)
                query1 = query.filter(table_name=None, column_name=None)
                query2 = query.filter(table_name=tableName, column_name=None)
                query3 = query.filter(table_name=tableName,
                                      column_name=fieldName)
                if (not query1) and (not query2) and (not query3):
                    res = self.model.objects.create(**fieldParams)
                    newIDList.append(res.id)
                else:
                    pass
            for row in query:
                searchDict = {'source_id': row.source_id,
                              'data_field_label_id': row.data_field_label_id,
                              'table_name': row.table_name,
                              'column_name': row.column_name}
                if searchDict not in paramsList:
                    res = self.model.objects.get(**searchDict)
                    res.is_deleted = 1
                    res.time_last_updated = curTimeGet()
                    res.save()
                    deletedIDList += [_['id'] for _ in res.values()]
            dataAdd = self.model.objects.filter(id__in=newIDList).values()
            dataDel = self.model.objects.filter(id__in=deletedIDList).values()
            self.msg['data'] = {'added': [dictTransform(_, boolTrans=False)
                                          for _ in dataAdd],
                                'deleted': [dictTransform(_, boolTrans=False)
                                            for _ in dataDel]}
            self.msg['msg'] = '%s修改成功' % self.comment
        except Exception as e:
            self.msg['msg'] = '%s修改失败' % self.comment
            self.msg['statusCode'] = 400
            self.msg['statusInfo'] = errorOutput(e)
        return HttpResponse(json.dumps(self.msg, ensure_ascii=False))


def testView(request):
    headContent = request.GET
    bodyContent = request.body
    if headContent:
        headParams = dict(headContent)
    else:
        headParams = {}
    if bodyContent:
        bodyParams = json.loads(bodyContent)
    else:
        bodyParams = {}
    msg = generalJson([headParams, bodyParams])
    return HttpResponse(json.dumps(msg, ensure_ascii=False))


@jwtLogin
def dataGetView(request, *args, **kwargs):
    try:
        if request.method == 'POST':
            body = json.loads(request.body)
            sourceID = body.get('source_id')
            tableName = body.get('table_name')
            nameonly = body.get('nameonly', True)
            timeonly = body.get('timeonly', False)
            tablePreview = body.get('preview', False)
            pageCurrent = body.get('pageCurrent', 1)
            pageSize = body.get('pageSize', 10)
            conn, database = sourceConnect(sourceID)
            cursor = conn.cursor()
            if not tableName:
                data = mysqlTablesGet(cursor, database, nameonly=nameonly)
            elif not tablePreview:
                data = mysqlFieldsGet(cursor, database, tableName,
                                      nameonly=nameonly, timeonly=timeonly)
            else:
                data = mysqlTablePreviewGet(cursor, tableName,
                                            pageCurrent, pageSize)
            msg = generalJson(data, msg='数据表列表获取成功')
            conn.close()
        else:
            msg = generalJson(msg='方法不支持', code=400)
    except Exception as e:
        msg = generalJson(code=400, info=errorOutput(e))
    return HttpResponse(json.dumps(msg, ensure_ascii=False))
