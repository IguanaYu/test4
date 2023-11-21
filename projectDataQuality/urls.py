from django.urls import path
from rest_framework.routers import DefaultRouter

import projectDataQuality.views as quality_views


urlpatterns = [
    path('test/', quality_views.testView, name='test'),
    ]

routerTask = DefaultRouter()
routerTask.register(r'standard', quality_views.StandardViewSet,
                    basename='standard')
routerTask.register(r'standardset', quality_views.StandardSetViewSet,
                    basename='standardset')
urlpatterns += routerTask.urls
