from django.db import models
from functions.general import GeneralInfoModel, defaultCronGet
from projectDataIntegration.models import SourceInfo


# Create your models here.
prefix = 'management_'
prefix_ch = '数据治理-'


class ProjectInfo(GeneralInfoModel):
    class Meta:
        db_table = prefix + 'project'
        verbose_name = prefix_ch + '项目管理信息表'
        
    project_code = models.CharField(
        verbose_name='项目编码',
        max_length=32)
    project_name = models.CharField(
        verbose_name='项目名称',
        max_length=32)
    project_type = models.CharField(
        verbose_name='项目类型',
        max_length=32,
        null=True)
    project_group = models.CharField(
        verbose_name='项目分组',
        max_length=32,
        null=True)
    
    
class TaskTemplateInfo(GeneralInfoModel):
    class Meta:
        db_table = prefix + 'template'
        verbose_name = prefix_ch + '任务模板信息表'
        
    task_template_code = models.CharField(
        verbose_name='任务模板编码',
        max_length=32)
    task_template_name = models.CharField(
        verbose_name='任务模板名称',
        max_length=32)
    task_template_type = models.CharField(
        verbose_name='任务模板类型',
        max_length=32,
        null=True)
    task_executor_type = models.CharField(
        verbose_name='任务执行器类型',
        max_length=32,
        null=True)
    task_route_type = models.CharField(
        verbose_name='任务路由策略',
        max_length=32,
        null=True)
    task_block_type = models.CharField(
        verbose_name='任务阻塞处理',
        max_length=32,
        null=True)
    alert_type = models.CharField(
        verbose_name='报警类型',
        max_length=32)
    alert_param = models.JSONField(
        verbose_name='报警参数',
        default=dict)
    timeout = models.IntegerField(
        verbose_name='超时时间（分钟）',
        null=True)
    retry_count = models.IntegerField(
        verbose_name='重试次数',
        null=True)
    
    
class TaskInfo(GeneralInfoModel):
    class Meta:
        db_table = prefix + 'task'
        verbose_name = prefix_ch + '任务信息表'
        
    task_code = models.CharField(
        verbose_name='任务编码',
        max_length=32)
    task_name = models.CharField(
        verbose_name='任务名称',
        max_length=32)
    task_type = models.CharField(
        verbose_name='任务类型',
        max_length=32,
        null=True)
    task_template = models.ForeignKey(
        TaskTemplateInfo,
        null=True,
        blank=True,
        related_name='task_template_id',
        verbose_name='任务模板id',
        on_delete=models.SET_NULL,
        db_constraint=False)
    project = models.ForeignKey(
        ProjectInfo,
        null=True,
        blank=True,
        related_name='project_id',
        verbose_name='项目id',
        on_delete=models.SET_NULL,
        db_constraint=False)
    cron_expression = models.CharField(
        verbose_name='cron表达式',
        max_length=128,
        default=defaultCronGet())
    task_params = models.JSONField(
        verbose_name='任务参数',
        max_length=5000,
        null=True)
    is_online = models.BooleanField(
        verbose_name='是否上线',
        default=0)
    is_enable = models.BooleanField(
        verbose_name='是否启用',
        default=0)


class ComponentIndexInfo(GeneralInfoModel):
    class Meta:
        db_table = prefix + 'component_index'
        verbose_name = prefix_ch + '开发组件目录管理表'
        unique_together = ('component_index_name', 'is_deleted')

    component_index_code = models.CharField(
        verbose_name='开发组件目录编码',
        max_length=128)
    component_index_name = models.CharField(
        verbose_name='开发组件目录名称',
        max_length=128)
    upstream_component_index = models.ForeignKey(
        'self',
        null=True,
        blank=True,
        related_name='upstream_id',
        verbose_name='上级开发组件目录id',
        on_delete=models.SET_NULL,
        db_constraint=False)


class ComponentInfo(GeneralInfoModel):
    class Meta:
        db_table = prefix + 'component'
        verbose_name = prefix_ch + '开发组件管理表'
        
    component_code = models.CharField(
        verbose_name='组件编码',
        max_length=32)
    component_name = models.CharField(
        verbose_name='组件名称',
        max_length=32)
    component_index = models.ForeignKey(
        ComponentIndexInfo,
        null=True,
        blank=True,
        related_name='component_index_id',
        verbose_name='开发组件目录id',
        on_delete=models.SET_NULL,
        db_constraint=False)
    component_file_path = models.TextField(
        verbose_name='组件类文件路径',
        null=True)
    component_class_name = models.CharField(
        verbose_name='组件类名称',
        max_length=128,
        null=True)
    component_version = models.CharField(
        verbose_name='组件版本',
        max_length=32,
        default='V1.0.0')
    is_component_available = models.BooleanField(
        verbose_name='组件是否可用',
        default=0)
    

class TaskToComponentsInfo(GeneralInfoModel):
    class Meta:
        db_table = prefix + 'relation_task_components'
        verbose_name = prefix_ch + '任务-组件关联信息表'
        
    task = models.ForeignKey(
        TaskInfo,
        null=True,
        blank=True,
        related_name='task_id',
        verbose_name='任务id',
        on_delete=models.SET_NULL,
        db_constraint=False)
    component = models.ForeignKey(
        ComponentInfo,
        null=True,
        blank=True,
        verbose_name='组件id',
        related_name='component_id',
        on_delete=models.SET_NULL,
        db_constraint=False)
    upstream_task_components = models.CharField(
        verbose_name='上游任务组件id组',
        max_length=5000,
        null=True)
    component_params = models.JSONField(
        verbose_name='组件参数',
        max_length=5000,
        default=dict)
    is_input_from_source = models.BooleanField(
        verbose_name='组件输入数据是否源于数据源',
        default=0)
    origin_source = models.ForeignKey(
        SourceInfo,
        null=True,
        blank=True,
        related_name='origin_source_id',
        verbose_name='来源数据源id',
        on_delete=models.SET_NULL,
        db_constraint=False)
    origin_table = models.CharField(
        verbose_name='来源数据表',
        max_length=128,
        null=True)
    origin_file_path = models.TextField(
        verbose_name='来源数据文件路径',
        null=True)
    is_output_to_source = models.BooleanField(
        verbose_name='组件输出数据是否传入数据源',
        default=0)
    target_source = models.ForeignKey(
        SourceInfo,
        null=True,
        blank=True,
        related_name='target_source_id',
        verbose_name='目标数据源id',
        on_delete=models.SET_NULL,
        db_constraint=False)
    target_table = models.CharField(
        verbose_name='目标数据表',
        max_length=128,
        null=True)
    target_file_path = models.TextField(
        verbose_name='目标数据文件路径',
        null=True)
    origin_fields_list = models.TextField(
        verbose_name='输入数据字段列表',
        null=True)
    target_fields_list = models.TextField(
        verbose_name='输出数据字段列表',
        null=True)
    processing_mapping_info = models.JSONField(
        verbose_name='数据处理字段映射信息',
        default=list)


class TaskDispatchInfo(GeneralInfoModel):
    class Meta:
        db_table = prefix + 'task_dispatch'
        verbose_name = prefix_ch + '任务调度信息表'

    task_dispatched = models.ForeignKey(
        TaskInfo,
        null=True,
        blank=True,
        related_name='task_dispatched_id',
        verbose_name='任务id',
        on_delete=models.SET_NULL,
        db_constraint=False)
    dispatch_estimated_time = models.DateTimeField(
        verbose_name='任务调度预计开始时间',
        null=True)
    dispatch_start_time = models.DateTimeField(
        verbose_name='任务调度开始时间',
        null=True)
    dispatch_end_time = models.DateTimeField(
        verbose_name='任务调度结束时间',
        null=True)
    dispatch_during_time = models.CharField(
        verbose_name='任务调度时长',
        max_length=128,
        null=True)
    dispatch_status = models.CharField(
        verbose_name='任务调度状态',
        max_length=16,
        null=True)
    success_components_id = models.JSONField(
        verbose_name='任务调度成功组件id组',
        default=list)
    current_components_id = models.JSONField(
        verbose_name='任务调度当前组件id组',
        default=list)
    failed_components_id = models.JSONField(
        verbose_name='任务调度失败组件id组',
        default=list)
    execute_log_file_path = models.TextField(
        verbose_name='任务执行日志文件路径',
        null=True)
    