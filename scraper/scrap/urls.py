from django.urls import path
from .views import stock_list, stock_FS

urlpatterns = [
    path('stocks/', stock_list, name='stock_prices'),
    # path('floor/', stock_FS, name='stock_sheet'),
]