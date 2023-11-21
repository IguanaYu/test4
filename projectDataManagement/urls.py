from django.urls import path
from rest_framework.routers import DefaultRouter

import projectDataManagement.views as management_views


urlpatterns = [
    path('test/', management_views.testView, name='test'),
    ]

router = DefaultRouter()
router.register(r'template', management_views.TaskTemplateInfoViewSet,
                basename='task_template')
router.register(r'task', management_views.TaskInfoViewSet, basename='task')
router.register(r'taskdetail', management_views.TaskComponentsInfoViewSet,
                basename='task_detail')
router.register(r'dispatch', management_views.DispatchViewSet,
                basename='task_dispatch')
urlpatterns += router.urls

# routerTask = DefaultRouter()
# routerTask.register(r'task', management_views.TaskInfoViewSet, basename='task')
# urlpatterns += routerTask.urls
