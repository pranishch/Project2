from django.urls import path
from . import views

urlpatterns = [
    path('broker-volumes/', views.calculate_volumes, name='calculate_volumes'),
]