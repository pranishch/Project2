from django.urls import path
from .views import stock_list

urlpatterns = [
    path('stocks/', stock_list, name='stock_prices'),
]