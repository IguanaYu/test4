from django.urls import path
from rest_framework.routers import DefaultRouter

import projectDataIntegration.views as integration_views


urlpatterns = [
    path('test/', integration_views.testView, name='test'),
    path('datachoice/', integration_views.dataGetView, name='datachoice'),
    ]

router = DefaultRouter()
router.register(r'source', integration_views.SourceInfoViewSet,
                basename='source')
router.register(r'datafield', integration_views.DataFieldInfoViewSet,
                basename='datafield')
router.register(r'datafieldlabel', integration_views.DataFieldLabelInfoViewSet,
                basename='datafieldlabel')

urlpatterns += router.urls
