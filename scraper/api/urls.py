from django.urls import path
from . import views

urlpatterns = [
    path('brokers/', views.get_broker_tracker, name='broker-volume'),
    # path('floorsheet/', views.get_Floorsheet_Data, name='floorsheet-data'),

]