from django.db import models
from functions.general import GeneralInfoModel
from projectDataIntegration.models import SourceInfo


# Create your models here.
prefix = 'quality_'
prefix_ch = '数据质量-'


class StandardSetInfo(GeneralInfoModel):
    class Meta:
        db_table = prefix + 'standard_set'
        verbose_name = prefix_ch + '数据标准集信息表'
        unique_together = ('standard_set_name', 'is_deleted')

    standard_set_code = models.CharField(
        verbose_name='数据标准集编码',
        max_length=128)
    standard_set_name = models.CharField(
        verbose_name='数据标准集名称',
        max_length=128)
    upstream_standard_set = models.ForeignKey(
        'self',
        null=True,
        blank=True,
        related_name='upstream_standard_set_id',
        verbose_name='上级数据标准集id',
        on_delete=models.SET_NULL,
        db_constraint=False)


class StandardInfo(GeneralInfoModel):
    class Meta:
        db_table = prefix + 'standard'
        verbose_name = prefix_ch + '数据标准信息表'
        
    standard_set = models.ForeignKey(
        StandardSetInfo,
        null=True,
        blank=True,
        related_name='standard_set_id',
        verbose_name='数据标准集id',
        on_delete=models.SET_NULL,
        db_constraint=False)
    standard_code = models.CharField(
        verbose_name='数据标准编码',
        max_length=32)
    standard_name = models.CharField(
        verbose_name='数据标准名称',
        max_length=32)
    standard_type = models.CharField(
        verbose_name='数据标准类型',
        max_length=32,
        null=True)
    # 审批状态推荐作为外键关联，关联以后管理用的审批表id，目前先默认为已生效
    standard_status = models.CharField(
        verbose_name='数据标准审批状态',
        max_length=32,
        default='已生效')
    standard_version = models.CharField(
        verbose_name='数据标准版本',
        max_length=32,
        default='1.0.0')
    extra_comment = models.TextField(
        verbose_name='数据标准版本说明',
        default='初始版本')
    standard_function_file_path = models.TextField(
        verbose_name='数据标准功能文件路径',
        null=True)
    standard_function_name = models.CharField(
        verbose_name='数据标准功能函数名称',
        max_length=128,
        null=True)
    is_standard_available = models.BooleanField(
        verbose_name='数据标准是否可用',
        default=0)
    standard_params = models.JSONField(
        verbose_name='数据标准功能参数',
        default=dict)
    standard_online_time = models.DateTimeField(
        verbose_name='标准上线时间',
        null=True)
    
        
class QualityInfo(GeneralInfoModel):
    class Meta:
        db_table = prefix + 'quality'
        verbose_name = prefix_ch + '数据质量管理表'
        
    quality_code = models.CharField(
        verbose_name='数据质量编码',
        max_length=32)
    quality_name = models.CharField(
        verbose_name='数据质量名称',
        max_length=32)
    source = models.ForeignKey(
        SourceInfo,
        null=True,
        blank=True,
        related_name='quality_source_id',
        verbose_name='质量校验-数据源id',
        on_delete=models.SET_NULL,
        db_constraint=False)
    table_name = models.CharField(
        verbose_name='质量校验-数据表名称',
        max_length=128)
    template_file_path = models.TextField(
        verbose_name='质量报告模板文件路径',
        null=True)
    quality_params = models.JSONField(
        verbose_name='质量校验参数',
        default=dict)
    alert_type = models.CharField(
        verbose_name='报警类型',
        max_length=32)
    alert_param = models.JSONField(
        verbose_name='报警参数',
        default=dict)

class QualityStandardRelationInfo(GeneralInfoModel):
    class Meta:
        db_table = prefix + 'relation_quality_standard'
        verbose_name = prefix_ch + '质量标准关联表'

    quality = models.ForeignKey(
        QualityInfo,
        null=True,
        blank=True,
        related_name='quality_id',
        verbose_name='数据质量id',
        on_delete=models.SET_NULL,
        db_constraint=False)
    standard = models.ForeignKey(
        StandardInfo,
        null=True,
        blank=True,
        verbose_name='数据标准id',
        related_name='standard_id',
        on_delete=models.SET_NULL,
        db_constraint=False)
    standard_type = models.CharField(
        verbose_name='数据标准类型',
        max_length=32,
        default=1)
    standard_fields = models.CharField(
        verbose_name='数据标准应用字段/联合字段',
        max_length=128,
        null=True)
    standard_params = models.JSONField(
        verbose_name='数据标准参数',
        default=dict)
    standard_level = models.IntegerField(
        verbose_name='数据标准问题级别',
        default=0)
    standard_weight = models.DecimalField(
        verbose_name='数据标准问题权重',
        default=1.0,
        max_digits=8,
        decimal_places=4)


class QualityCheckInfo(GeneralInfoModel):
    class Meta:
        db_table = prefix + 'quality_check'
        verbose_name = prefix_ch + '数据质量校验表'

    quality_checked = models.ForeignKey(
        QualityInfo,
        null=True,
        blank=True,
        related_name='quality_checked_id',
        verbose_name='数据质量id',
        on_delete=models.SET_NULL,
        db_constraint=False)
    check_estimated_time = models.DateTimeField(
        verbose_name='质量校验预计开始时间',
        null=True)
    check_start_time = models.DateTimeField(
        verbose_name='质量校验开始时间',
        null=True)
    check_end_time = models.DateTimeField(
        verbose_name='质量校验结束时间',
        null=True)
    check_during_time = models.CharField(
        verbose_name='质量校验时长',
        max_length=128,
        null=True)
    check_status = models.CharField(
        verbose_name='质量校验状态',
        max_length=16,
        null=True)
    check_log_file_path = models.TextField(
        verbose_name='质量日志文件路径',
        null=True)
    check_result_file_path = models.TextField(
        verbose_name='质量校验报告文件路径',
        null=True)