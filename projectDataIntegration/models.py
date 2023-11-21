from django.db import models
from functions.general import GeneralInfoModel
from dictionary.models import SourceTypeInfo


prefix = 'integration_'
prefix_ch = '数据集成-'


class SSHSignOnInfo(GeneralInfoModel):
    class Meta:
        db_table = prefix + 'ssh_sign_on'
        verbose_name = prefix_ch + 'SSH登录信息表'
    
    host = models.CharField(
        verbose_name='主机地址',
        max_length=128)
    port = models.IntegerField(
        verbose_name='端口')
    user = models.CharField(
        verbose_name='登录用户名',
        max_length=128)
    password = models.CharField(
        verbose_name='登录密码',
        max_length=128)
    is_secretkey = models.BooleanField(
        verbose_name='是否密钥登录',
        default=0)
    secretkey_path = models.CharField(
        verbose_name='密钥文件路径',
        max_length=128,
        null=True)


class SourceInfo(GeneralInfoModel):
    class Meta:
        db_table = prefix + 'source_info'
        verbose_name = prefix_ch + '数据源信息表'
    
    source_type = models.ForeignKey(
        SourceTypeInfo,
        null=True,
        blank=True,
        related_name='source_type_id',
        verbose_name='数据源类型id',
        on_delete=models.SET_NULL,
        db_constraint=False)
    source_code = models.CharField(
        verbose_name='数据源编码',
        max_length=32,
        null=True)
    source_name = models.CharField(
        verbose_name='数据源名称',
        max_length=32)
    source_group = models.CharField(
        verbose_name='数据源分组',
        max_length=32,
        null=True)
    source_dept = models.CharField(
        verbose_name='数据源所属组织',
        max_length=32,
        null=True)
    host = models.CharField(
        verbose_name='主机地址',
        max_length=128)
    port = models.IntegerField(
        verbose_name='端口')
    user = models.CharField(
        verbose_name='登录用户名',
        max_length=128)
    password = models.CharField(
        verbose_name='登录密码',
        max_length=128)
    is_inner_source = models.BooleanField(
        verbose_name='是否为平台数据源',
        default=0)
    is_available = models.BooleanField(
        verbose_name='测试连接是否通过',
        default=0)
    ssh = models.ForeignKey(
        SSHSignOnInfo,
        null=True,
        blank=True,
        related_name='ssh_id',
        verbose_name='SSH登录参数组id',
        on_delete=models.SET_NULL,
        db_constraint=False)
    
    
class DataFieldInfo(GeneralInfoModel):
    class Meta:
        db_table = prefix + 'data_field_info'
        verbose_name = prefix_ch + '数据域信息表'
        
    data_field_code = models.CharField(
        verbose_name='数据域编码',
        max_length=128)
    data_field_name = models.CharField(
        verbose_name='数据域名称',
        max_length=128)
    upstream_data_field = models.ForeignKey(
        'self',
        null=True,
        blank=True,
        related_name='upstream_id',
        verbose_name='上级数据域id',
        on_delete=models.SET_NULL,
        db_constraint=False)


class DataFieldLabelInfo(GeneralInfoModel):
    class Meta:
        db_table = prefix + 'data_field_label_info'
        verbose_name = prefix_ch + '数据域标签信息表'

    source = models.ForeignKey(
        SourceInfo,
        null=True,
        blank=True,
        related_name='field_source_id',
        verbose_name='数据域数据源id',
        on_delete=models.SET_NULL,
        db_constraint=False)
    table_name = models.CharField(
        verbose_name='数据表名称',
        max_length=128,
        null=True)
    column_name = models.CharField(
        verbose_name='字段名称',
        max_length=128,
        null=True)
    data_field_label = models.ForeignKey(
        DataFieldInfo,
        null=True,
        blank=True,
        related_name='data_field_label_id',
        verbose_name='数据域标签id',
        on_delete=models.SET_NULL,
        db_constraint=False)