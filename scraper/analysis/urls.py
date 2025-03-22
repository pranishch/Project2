from django.urls import path
from . import views

urlpatterns = [
    path('broker-volumes/', views.calculate_volumes, name='calculate_volumes'),
    path('', views.index, name='index'),
    path('get_broker_data/', views.get_broker_data, name='get_broker_data'),
]