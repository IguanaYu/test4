from functions.standard import *
from user.views import LoginRequiredMixin
from projectDataQuality.models import *


# from projectDataManagement.models import *
# from dictionary.models import SourceTypeInfo
# from functions.connection import *
# from user.views import jwtLogin

class StandardSetViewSet(LoginRequiredMixin, IndexView):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.model = StandardSetInfo
        self.fields = ['id', 'standard_set_code', 'standard_set_name',
                       'upstream_standard_set',
                       'upstream_standard_set_id__standard_set_code',
                       'upstream_standard_set_id__standard_set_name',
                       'comment', 'creator', 'time_created']
        self.totalFields = [_.name + '_id' if isinstance(_, models.ForeignKey)
                            else _.name for _ in self.model._meta.get_fields()]
        self.queryset = self.model.objects.filter(is_deleted=0) \
            .select_related('upsteam_set_id').values(*self.fields)
        self.comment = '数据标准集'
        self.msg = generalJson()
        self.uniqueFields = {
            ('standard_set_code', ): '数据标准集编码',
            ('standard_set_name', ): '数据标准集名称'}
        self.relationField = ('upstream_standard_set', 'id')


class StandardViewSet(LoginRequiredMixin, VersionView):
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
        self.model = StandardInfo
        self.fields = ['id', 'standard_code', 'standard_name', 'comment',
                       'standard_type', 'extra_comment', 'standard_status',
                       'standard_version', 'standard_online_time',
                       'standard_set_id__id', 'is_standard_available']
        self.totalFields = [_.name + '_id' if isinstance(_, models.ForeignKey)
                            else _.name for _ in self.model._meta.get_fields()]
        self.versionFields = {'standard_version': 'is_standard_available'}
        self.versionFunc = {_: versionControl for _ in self.versionFields}
        filterDict = {'is_deleted': 0}
        for versionField in self.versionFields:
            filterDict[self.versionFields[versionField]] = 1
        self.querysetForTotal = self.model.objects.filter(**filterDict) \
            .select_related('standard_set_id').values(*self.fields)
        self.querysetForVersion = self.model.objects.filter(is_deleted=0)\
            .select_related('standard_set_id').values(*self.fields)
        self.comment = '数据标准'
        self.msg = generalJson()
        self.uniqueFields = {
            ('standard_code', 'standard_version'): '数据标准编码',
            ('standard_name', 'standard_version'): '数据标准名称'}
        self.indexFields = {
            'standard_set_id__id': StandardSetViewSet().subItemGet}

    @action(methods=['post'], detail=False, url_path='verify')
    def verify(self, request, *args, **kwargs):
        headParams, bodyParams, method = self.requestAnalysis(request)
        standardType = bodyParams.get('standard_type')
        verifyDict = bodyParams.get('verify_dict')
        standardClass = StandardClass()
        standardClass.funcGet(standardFuncGet(standardType))
        try:
            verifyResult, verifyInfo = standardClass.testDataCheck(verifyDict)
            self.msg['msg'] = verifyInfo
            self.msg['statusCode'] = 200 if verifyResult else 400
        except Exception as e:
            self.msg['msg'] = '测试数据验证失败，请检查数据标准功能是否正常运作'
            self.msg['statusCode'] = 400
            self.msg['statusInfo'] = errorOutput(e)
        return HttpResponse(json.dumps(self.msg, ensure_ascii=False))
        
    @action(methods=['post'], detail=False, url_path='versionadd')
    def versionAdd(self, request, *args, **kwargs):
        headParams, bodyParams, method = self.requestAnalysis(request)
        formParams = subDict(bodyParams, self.totalFields)
        standardParams = formParams.get('standard_paragrams')
        standardType = formParams.get('standard_type')
        standardClass = StandardClass()
        standardClass.funcGet(standardFuncGet(standardType))
        standardClass.levelDictGet(standardParams)
        try:
            verifyResult, verifyInfo = standardClass.levelDictVerify()
            if not verifyResult:
                self.msg['msg'] = verifyInfo
                self.msg['statusCode'] = 400
                return HttpResponse(json.dumps(self.msg, ensure_ascii=False))
        except Exception as e:
            self.msg['msg'] = '数据标准配置验证失败'
            self.msg['statusCode'] = 400
            self.msg['statusInfo'] = errorOutput(e)
            return HttpResponse(json.dumps(self.msg, ensure_ascii=False))
        formParams['standard_paragrams'] = standardClass.levelDict
        return super().add(request, *args, formParams=formParams, **kwargs)
        

def testView(request):
    msg = '测试数据'
    return HttpResponse(json.dumps(msg, ensure_ascii=False))
